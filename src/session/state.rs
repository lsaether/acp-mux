//! Per-session state: agent subprocess + attached subscribers + the
//! actor task that serializes all mutations.
//!
//! All state mutation flows through a single tokio task driven by an mpsc
//! `SessionMsg` queue. Subscribers push inbound frames via
//! `InboundFromSubscriber` and detach via `Detach`. The agent's stdout pump
//! task forwards each NDJSON line as `AgentStdoutLine` and signals exit
//! via `AgentDied`.
//!
//! ## Routing contract
//!
//! Inbound (subscriber → agent), per JSON-RPC envelope shape:
//! - `notification` → forward to agent unchanged.
//! - `request` → check the `initialize` / `session/new` response cache; if
//!   present, answer the subscriber locally without touching the agent.
//!   Otherwise allocate a per-session `mux_id`, store the
//!   `(peer_id, original_id)` mapping, rewrite the `id`, and forward.
//!   Substantive (non-`initialize`) requests also mark the sender as the
//!   current "driving subscriber" — surfaced in `amux/turn_started` and
//!   `/debug/sessions`. Agent-initiated requests are broadcast (see the
//!   Inbound (agent → subscribers) `request` arm below), so the driver
//!   no longer has a privileged role at routing time.
//! - `session/prompt` requests participate in turn serialization: while a
//!   prompt is in flight, a second `session/prompt` is rejected locally
//!   with JSON-RPC error code `-32001` ("session busy"). The active turn
//!   clears when the matching response returns from the agent.
//! - `response` → forward unchanged. Subscriber-originated responses only
//!   show up as replies to agent-initiated requests, whose ids belong to
//!   the agent's own id space (never our `mux_id` space), so they round
//!   trip without rewriting.
//!
//! Inbound (agent → subscribers):
//! - `notification` → broadcast to every attached subscriber.
//! - `response` → look up `mux_id`, restore `original_id`, send to the
//!   originator only. If the original request was the first `initialize`
//!   or `session/new`, cache the `result` for later joiners. If it matches
//!   `active_turn_mux_id`, clear the active turn.
//! - `request` → broadcast to every attached subscriber and record the
//!   agent's request id as `InFlight`. Whichever subscriber replies first
//!   gets its response forwarded to the agent; the id transitions to
//!   `Consumed` and any later responses with the same id are dropped with
//!   a debug log. On the InFlight → Consumed transition the mux also
//!   broadcasts `amux/agent_request_resolved { requestId, resolvedBy,
//!   result | error }` so peers that lost the race (or never replied)
//!   can dismiss the request from their UI. This lets any attached peer
//!   (not just the driver) confirm an agent-initiated request while
//!   preserving the JSON-RPC contract that the agent sees exactly one
//!   reply per id.
//!
//! Turn-end cleanup: when the session/prompt response arrives and
//! `active_turn_mux_id` clears, the mux sweeps every `agent_pending`
//! entry still `InFlight`, transitions them to `Consumed`, and
//! broadcasts `amux/agent_request_resolved { resolvedBy:
//! "mux:turn-ended", result: null, error: null }` for each. This catches
//! the case where the agent times out an unanswered permission
//! internally (e.g. hermes' 60s default) and proceeds without writing a
//! response frame — without that sweep, TUI clients would be stuck
//! displaying a permission the agent has already abandoned.
//!
//! Frames that fail JSON-RPC envelope parsing fall back to raw broadcast
//! on the agent → subscribers direction (so non-JSON debug output, if any,
//! still reaches clients), and are dropped with a warn on the
//! subscriber → agent direction (we will not feed garbage to a real ACP
//! server).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Duration;

use bytes::Bytes;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::agent::process::AgentProcess;
use crate::cli::ReplayTurns;
use crate::multiplex::subscriber::{OutMsg, Subscriber};
use crate::protocol::amux::{self, AmuxTurnId};
use crate::protocol::jsonrpc::{
    Id, Incoming, IncomingRequest, IncomingResponse, JsonRpcError, JsonRpcVersion,
};

const SESSION_QUEUE_CAPACITY: usize = 256;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

/// Mux ids start at 1; 0 is reserved as a sentinel.
const FIRST_MUX_ID: u64 = 1;

/// JSON-RPC error code returned to a subscriber that issues a second
/// `session/prompt` while another turn is already in flight. The
/// -32000..=-32099 range is reserved by the spec for implementation
/// defined errors; -32001 was chosen by the ROADMAP.
const SESSION_BUSY_ERROR_CODE: i64 = -32001;

/// WebSocket close code used when the agent subprocess exits while
/// subscribers are still attached. 1011 = "internal error" per RFC 6455.
const WS_CLOSE_AGENT_DEAD: u16 = 1011;

/// JSON-RPC method name for the cancellation notification (request-cancellation
/// RFD; LSP-derived). Either direction may emit it.
const CANCEL_REQUEST_METHOD: &str = "$/cancel_request";

/// Extract a non-null `requestId` from a `$/cancel_request` params object.
/// Per the RFD the field is `number | string`; we additionally treat
/// `Id::Null` as invalid (cancellation of an id-less notification is
/// meaningless).
fn parse_cancel_request_id(params: Option<&Value>) -> Option<Id> {
    let id_value = params.and_then(|v| v.get("requestId"))?.clone();
    let id: Id = serde_json::from_value(id_value).ok()?;
    match id {
        Id::Null => None,
        other => Some(other),
    }
}

/// Build a `$/cancel_request` notification frame as NDJSON bytes (no
/// trailing newline; the writer adds framing). Used both for forwarding
/// a subscriber-originated cancel with a translated id and for
/// synthesizing one on behalf of `amux/cancel_active_turn`.
fn build_cancel_request(request_id: Id) -> Vec<u8> {
    #[derive(serde::Serialize)]
    struct CancelParams<'a> {
        #[serde(rename = "requestId")]
        request_id: &'a Id,
    }
    #[derive(serde::Serialize)]
    struct CancelFrame<'a> {
        jsonrpc: &'static str,
        method: &'static str,
        params: CancelParams<'a>,
    }
    serde_json::to_vec(&CancelFrame {
        jsonrpc: "2.0",
        method: CANCEL_REQUEST_METHOD,
        params: CancelParams {
            request_id: &request_id,
        },
    })
    .expect("cancel_request frame is always serializable")
}

fn replay_log_update_counts_by_acp_session_id(log: &VecDeque<Bytes>) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for frame in log {
        let Ok(value) = serde_json::from_slice::<Value>(frame) else {
            continue;
        };
        if value.get("method").and_then(Value::as_str) != Some("session/update") {
            continue;
        }
        let Some(session_id) = value
            .get("params")
            .and_then(|params| params.get("sessionId"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        *counts.entry(session_id.to_string()).or_insert(0) += 1;
    }
    counts
}

pub enum SessionMsg {
    Attach {
        subscriber: Subscriber,
        ack: oneshot::Sender<Result<(), AttachError>>,
    },
    Detach {
        peer_id: String,
    },
    InboundFromSubscriber {
        peer_id: String,
        bytes: Vec<u8>,
    },
    AgentStdoutLine(Vec<u8>),
    AgentDied,
    /// Build a JSON snapshot of session state for `/debug/sessions`.
    Snapshot {
        ack: oneshot::Sender<SessionSnapshot>,
    },
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayResetSnapshot {
    pub loaded_session_id: String,
    pub replay_generation: u64,
    pub dropped_frame_count: usize,
    pub retained_frame_count: usize,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionSnapshot {
    pub session_id: String,
    pub subscribers: Vec<SubscriberSnapshot>,
    pub pending_request_count: usize,
    pub initialize_cached: bool,
    pub cached_session_id: Option<String>,
    pub active_turn_mux_id: Option<u64>,
    pub active_amux_turn_id: Option<String>,
    pub driving_subscriber: Option<String>,
    pub subprocess_dead: bool,
    pub ttl_pending: bool,
    pub replay_log_len: Option<usize>,
    pub replay_generation: u64,
    pub replay_log_update_frames_by_acp_session_id: Option<BTreeMap<String, usize>>,
    pub last_replay_reset: Option<ReplayResetSnapshot>,
    pub next_mux_id: u64,
    pub next_amux_turn_id: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriberSnapshot {
    pub peer_id: String,
    pub peer_name: Option<String>,
    pub role: Option<String>,
    pub is_driving: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttachError {
    PeerIdInUse,
}

#[derive(Clone)]
pub struct SessionHandle {
    pub tx: mpsc::Sender<SessionMsg>,
}

impl SessionHandle {
    pub fn is_alive(&self) -> bool {
        !self.tx.is_closed()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HandshakeKind {
    Initialize,
    SessionNew,
    /// `session/load` request that asked the agent to switch to an
    /// existing session id. On success the room's canonical session id
    /// is rebound to this value so late joiners' `session/new` calls
    /// return the loaded session (not the original one). Captured at
    /// request-translation time from the client's `params.sessionId`
    /// — re-reading it off the response isn't reliable because not
    /// every agent echoes the loaded id back.
    SessionLoad {
        loaded_session_id: String,
        replay_start_len: usize,
    },
}

#[derive(Debug)]
struct PendingRequest {
    peer_id: String,
    original_id: Id,
    handshake: Option<HandshakeKind>,
}

/// Lifecycle of an agent-initiated request id while we wait for the first
/// subscriber to reply. `InFlight` accepts the next response; `Consumed`
/// drops all further responses for the same id with a debug log so the
/// agent never receives duplicate replies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentReqState {
    InFlight,
    Consumed,
}

struct SessionInner {
    session_id: String,
    subscribers: HashMap<String, Subscriber>,
    next_mux_id: u64,
    pending: HashMap<u64, PendingRequest>,
    initialize_cache: Option<Value>,
    session_new_cache: Option<Value>,
    /// Last subscriber to issue a substantive (non-`initialize`) request.
    /// Target for agent-initiated requests. Cleared when that subscriber
    /// detaches; falls back to an arbitrary subscriber at routing time.
    driving_subscriber_peer_id: Option<String>,
    /// `mux_id` of the in-flight `session/prompt`, if any. While set, a
    /// second `session/prompt` is rejected locally with `-32001`.
    active_turn_mux_id: Option<u64>,
    /// `amuxTurnId` paired with the in-flight `session/prompt`. Used to
    /// bookend `amux/turn_started` and `amux/turn_complete`.
    active_amux_turn_id: Option<AmuxTurnId>,
    /// Monotonic per-session counter for `amuxTurnId` allocation.
    next_amux_turn_id: u64,
    /// Replay log. `None` when policy is `Disabled` (saves memory).
    /// Otherwise, every broadcast-tier frame (amux/* + agent notifications)
    /// is appended; new subscribers receive a snapshot at attach time.
    replay_log: Option<VecDeque<Bytes>>,
    /// Incremented whenever a successful `session/load` establishes a new
    /// canonical upstream ACP session and the replay log is segmented.
    replay_generation: u64,
    /// Last successful replay segmentation event, exposed through
    /// `/debug/sessions` for operator diagnostics.
    last_replay_reset: Option<ReplayResetSnapshot>,
    /// State for every agent-initiated request id we have ever broadcast
    /// in this session. `InFlight` until the first subscriber reply
    /// arrives; `Consumed` thereafter. We keep `Consumed` ids around for
    /// the session lifetime so late/duplicate responses can be recognized
    /// and dropped instead of leaking back to the agent.
    agent_pending: HashMap<Id, AgentReqState>,
}

impl SessionInner {
    fn new(session_id: String, replay_policy: ReplayTurns) -> Self {
        let replay_log = match replay_policy {
            ReplayTurns::Disabled => None,
            ReplayTurns::Unbounded => Some(VecDeque::new()),
            ReplayTurns::Bounded(n) => {
                tracing::warn!(
                    bound = n,
                    "--replay-turns N (bounded eviction) accepted but not yet implemented; behaving as unbounded for v0.1",
                );
                Some(VecDeque::new())
            }
        };
        Self {
            session_id,
            subscribers: HashMap::new(),
            next_mux_id: FIRST_MUX_ID,
            pending: HashMap::new(),
            initialize_cache: None,
            session_new_cache: None,
            driving_subscriber_peer_id: None,
            active_turn_mux_id: None,
            active_amux_turn_id: None,
            next_amux_turn_id: 1,
            replay_log,
            replay_generation: 0,
            last_replay_reset: None,
            agent_pending: HashMap::new(),
        }
    }

    /// Attach a subscriber. Order:
    ///
    /// 1. Snapshot the replay log (before newcomer's own peer_joined enters).
    /// 2. Emit amux/peer_joined → broadcast to existing subs (newcomer is
    ///    not in the map yet) + append to log.
    /// 3. Insert newcomer into subscriber map.
    /// 4. Deliver the snapshot to the newcomer's outbound. The snapshot
    ///    contains every broadcast-tier frame that happened before this
    ///    attach, in order, so the newcomer reconstructs the session.
    ///
    /// Because the actor serializes all SessionMsg handling, no live frames
    /// can interleave during this sequence.
    fn attach(&mut self, subscriber: Subscriber) -> Result<(), AttachError> {
        if self.subscribers.contains_key(&subscriber.peer_id) {
            return Err(AttachError::PeerIdInUse);
        }
        let snapshot: Vec<Bytes> = self
            .replay_log
            .as_ref()
            .map(|log| log.iter().cloned().collect())
            .unwrap_or_default();

        let frame = amux::peer_joined(
            &self.session_id,
            &subscriber.peer_id,
            subscriber.peer_name.as_deref(),
            subscriber.role.as_deref(),
        );
        self.broadcast(frame);

        let peer_id = subscriber.peer_id.clone();
        tracing::info!(
            session = %self.session_id,
            peer_id = %peer_id,
            replay_frames = snapshot.len(),
            "subscriber joined session",
        );
        self.subscribers.insert(peer_id.clone(), subscriber);

        if let Some(sub) = self.subscribers.get(&peer_id) {
            for frame in snapshot {
                if sub.outbound.send(OutMsg::Frame(frame)).is_err() {
                    tracing::debug!(%peer_id, "newcomer dropped during replay");
                    break;
                }
            }
        }
        Ok(())
    }

    /// Build a serializable snapshot of session state for /debug/sessions.
    fn build_snapshot(&self, ttl_pending: bool) -> SessionSnapshot {
        let subs: Vec<SubscriberSnapshot> = self
            .subscribers
            .values()
            .map(|s| SubscriberSnapshot {
                peer_id: s.peer_id.clone(),
                peer_name: s.peer_name.clone(),
                role: s.role.clone(),
                is_driving: self.driving_subscriber_peer_id.as_ref() == Some(&s.peer_id),
            })
            .collect();
        let cached_session_id = self.session_new_cache.as_ref().and_then(|v| {
            v.get("sessionId")
                .and_then(|s| s.as_str())
                .map(|s| s.to_string())
        });
        SessionSnapshot {
            session_id: self.session_id.clone(),
            subscribers: subs,
            pending_request_count: self.pending.len(),
            initialize_cached: self.initialize_cache.is_some(),
            cached_session_id,
            active_turn_mux_id: self.active_turn_mux_id,
            active_amux_turn_id: self.active_amux_turn_id.map(|t| t.formatted()),
            driving_subscriber: self.driving_subscriber_peer_id.clone(),
            subprocess_dead: false,
            ttl_pending,
            replay_log_len: self.replay_log.as_ref().map(|l| l.len()),
            replay_generation: self.replay_generation,
            replay_log_update_frames_by_acp_session_id: self
                .replay_log
                .as_ref()
                .map(replay_log_update_counts_by_acp_session_id),
            last_replay_reset: self.last_replay_reset.clone(),
            next_mux_id: self.next_mux_id,
            next_amux_turn_id: self.next_amux_turn_id,
        }
    }

    /// Close every attached subscriber with a structured WS close frame.
    /// Used on subprocess crash to emit code 1011 cleanly. After this
    /// returns, the subscribers map is left intact — drop callers should
    /// `clear()` it explicitly if they want the senders gone.
    fn close_all_subscribers(&self, code: u16, reason: &str) {
        for (peer_id, sub) in &self.subscribers {
            let msg = OutMsg::Close {
                code,
                reason: reason.to_string(),
            };
            if sub.outbound.send(msg).is_err() {
                tracing::debug!(%peer_id, "subscriber already gone during close");
            }
        }
    }

    /// Returns true if the session should end (no subscribers left).
    /// Emits `amux/peer_left` to every remaining subscriber.
    fn detach(&mut self, peer_id: &str) -> bool {
        if self.subscribers.remove(peer_id).is_some() {
            tracing::info!(session = %self.session_id, %peer_id, "subscriber detached");
            let frame = amux::peer_left(&self.session_id, peer_id);
            self.broadcast(frame);
        }
        if self.driving_subscriber_peer_id.as_deref() == Some(peer_id) {
            self.driving_subscriber_peer_id = None;
        }
        if self.subscribers.is_empty() {
            tracing::info!(session = %self.session_id, "last subscriber gone; ending session");
            return true;
        }
        false
    }

    /// Process one inbound frame from a subscriber. Returns Ok(Some(bytes))
    /// when a frame should be written to the agent stdin; Ok(None) means
    /// the frame was either dropped, answered locally from cache, or
    /// otherwise handled without touching the agent.
    fn handle_inbound(&mut self, peer_id: &str, bytes: Vec<u8>) -> Option<Vec<u8>> {
        let frame = match Incoming::parse(&bytes) {
            Ok(f) => f,
            Err(err) => {
                tracing::warn!(
                    session = %self.session_id,
                    %peer_id,
                    error = %err,
                    "invalid JSON-RPC frame from subscriber; dropping",
                );
                return None;
            }
        };
        match frame {
            Incoming::Notification(notif) => {
                self.handle_subscriber_notification(peer_id, notif, bytes)
            }
            Incoming::Response(resp) => self.gate_subscriber_response(peer_id, resp, bytes),
            Incoming::Request(req) => self.translate_outbound_request(peer_id, req),
        }
    }

    /// Intercept proxy-handled subscriber-emitted notifications
    /// (`$/cancel_request`, `amux/cancel_active_turn`) before forwarding
    /// the bytes verbatim. Returns Ok(Some(bytes)) when the frame should
    /// be written to the agent stdin, Ok(None) when it was handled
    /// entirely in the proxy.
    fn handle_subscriber_notification(
        &mut self,
        peer_id: &str,
        notif: crate::protocol::jsonrpc::IncomingNotification,
        bytes: Vec<u8>,
    ) -> Option<Vec<u8>> {
        match notif.method.as_str() {
            CANCEL_REQUEST_METHOD => self.handle_subscriber_cancel(peer_id, notif),
            amux::METHOD_CANCEL_ACTIVE_TURN => self.handle_amux_cancel_active_turn(peer_id, notif),
            _ => Some(bytes),
        }
    }

    /// Strict `$/cancel_request` from a subscriber: cancel the
    /// subscriber's *own* in-flight request. Find `(peer_id,
    /// original_id)` in `pending`, rewrite the notification's
    /// `requestId` to the corresponding `mux_id`, forward to the agent.
    /// Subscribers cannot cancel other subscribers' requests — the
    /// JSON-RPC id space is per-connection — they'd use
    /// `amux/cancel_active_turn` for cross-peer "stop this turn"
    /// instead.
    fn handle_subscriber_cancel(
        &mut self,
        peer_id: &str,
        notif: crate::protocol::jsonrpc::IncomingNotification,
    ) -> Option<Vec<u8>> {
        let original_id = match parse_cancel_request_id(notif.params.as_ref()) {
            Some(id) => id,
            None => {
                tracing::debug!(
                    session = %self.session_id,
                    %peer_id,
                    "subscriber $/cancel_request with invalid/null requestId; dropping",
                );
                return None;
            }
        };

        let Some(mux_id) = self.find_pending_mux_id(peer_id, &original_id) else {
            tracing::debug!(
                session = %self.session_id,
                %peer_id,
                id = ?original_id,
                "subscriber $/cancel_request for unknown id; dropping",
            );
            return None;
        };

        tracing::info!(
            session = %self.session_id,
            %peer_id,
            ?original_id,
            mux_id,
            "forwarding subscriber-initiated $/cancel_request to agent",
        );
        Some(build_cancel_request(Id::Number(mux_id as i64)))
    }

    /// `amux/cancel_active_turn`: any attached peer can cancel the
    /// in-flight turn. Resolves to a synthesized `$/cancel_request`
    /// toward the agent using the active-turn `mux_id`. Broadcasts
    /// `amux/turn_cancelled` to all peers immediately (intent), while
    /// `amux/turn_complete` follows later when the agent settles.
    fn handle_amux_cancel_active_turn(
        &mut self,
        peer_id: &str,
        notif: crate::protocol::jsonrpc::IncomingNotification,
    ) -> Option<Vec<u8>> {
        let Some(active_mux_id) = self.active_turn_mux_id else {
            tracing::debug!(
                session = %self.session_id,
                %peer_id,
                "amux/cancel_active_turn with no active turn; dropping",
            );
            return None;
        };
        let Some(amux_turn_id) = self.active_amux_turn_id else {
            tracing::warn!(
                session = %self.session_id,
                %peer_id,
                "active_turn_mux_id set but active_amux_turn_id missing; dropping cancel",
            );
            return None;
        };
        let Some(pending) = self.pending.get(&active_mux_id) else {
            tracing::warn!(
                session = %self.session_id,
                %peer_id,
                active_mux_id,
                "active turn has no pending entry; dropping cancel",
            );
            return None;
        };
        let original_driver = pending.peer_id.clone();
        let reason = notif
            .params
            .as_ref()
            .and_then(|v| v.get("reason"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        tracing::info!(
            session = %self.session_id,
            cancelled_by = %peer_id,
            %original_driver,
            active_mux_id,
            reason = ?reason,
            "amux/cancel_active_turn synthesizing $/cancel_request to agent",
        );

        let frame = amux::turn_cancelled(
            &self.session_id,
            amux_turn_id,
            peer_id,
            &original_driver,
            reason.as_deref(),
        );
        self.broadcast(frame);

        Some(build_cancel_request(Id::Number(active_mux_id as i64)))
    }

    /// Agent-emitted `$/cancel_request`: cancels an agent-initiated
    /// request that's still InFlight in `agent_pending` (in practice
    /// always `session/request_permission`, the only agent-initiated
    /// request the spec defines today). Forward the cancellation to
    /// every subscriber so their UIs dismiss, mark the entry Consumed
    /// to swallow late replies, and emit the
    /// `amux/agent_request_resolved` sibling for amux clients.
    fn handle_agent_cancel(
        &mut self,
        notif: crate::protocol::jsonrpc::IncomingNotification,
        line: Vec<u8>,
    ) -> bool {
        let request_id = match parse_cancel_request_id(notif.params.as_ref()) {
            Some(id) => id,
            None => {
                tracing::debug!(
                    session = %self.session_id,
                    "agent $/cancel_request with invalid/null requestId; dropping",
                );
                return false;
            }
        };
        match self.agent_pending.get_mut(&request_id) {
            Some(state @ AgentReqState::InFlight) => {
                *state = AgentReqState::Consumed;
                tracing::info!(
                    session = %self.session_id,
                    id = ?request_id,
                    "agent cancelled in-flight agent-initiated request; broadcasting",
                );
            }
            Some(AgentReqState::Consumed) => {
                tracing::debug!(
                    session = %self.session_id,
                    id = ?request_id,
                    "agent $/cancel_request for already-consumed id; broadcasting anyway so late UIs dismiss",
                );
            }
            None => {
                tracing::debug!(
                    session = %self.session_id,
                    id = ?request_id,
                    "agent $/cancel_request for unknown id; broadcasting anyway",
                );
            }
        }

        // Forward the raw cancellation notification to every subscriber
        // so RFD-aware clients see the standard JSON-RPC form. Capture
        // the empty-after-fanout signal so the caller can wind down the
        // session if this drained the last subscriber.
        let mut session_empty = self.broadcast(line);

        // Emit the amux-namespace sibling so amux-aware clients
        // dismiss without needing to recognize $/cancel_request.
        // (The second broadcast is harmless if the map is already
        // empty — it appends to the replay log either way.)
        let request_id_value = match serde_json::to_value(&request_id) {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(
                    session = %self.session_id,
                    error = %err,
                    "failed to serialize cancelled request id; skipping amux/agent_request_resolved",
                );
                return session_empty;
            }
        };
        let frame = amux::agent_request_resolved(
            &self.session_id,
            &request_id_value,
            amux::RESOLVED_BY_AGENT_CANCELLED,
            None,
            None,
        );
        session_empty |= self.broadcast(frame);
        session_empty
    }

    /// Linear search through `pending` for the entry matching
    /// `(peer_id, original_id)`. N is bounded by concurrent in-flight
    /// requests per session — small in practice. Returns the `mux_id`
    /// key.
    fn find_pending_mux_id(&self, peer_id: &str, original_id: &Id) -> Option<u64> {
        self.pending
            .iter()
            .find(|(_, pr)| pr.peer_id == peer_id && &pr.original_id == original_id)
            .map(|(mux_id, _)| *mux_id)
    }

    /// First-reply-wins gate for subscriber-originated responses. If the
    /// id matches an agent-initiated request we broadcast, only the first
    /// reply is forwarded; later replies for the same id are dropped.
    /// Responses whose id is not tracked (stray replies, or replies
    /// against ids the agent never asked us to multiplex) are forwarded
    /// unchanged for robustness — the agent will ignore them if it has
    /// no matching outstanding request.
    fn gate_subscriber_response(
        &mut self,
        peer_id: &str,
        resp: IncomingResponse,
        bytes: Vec<u8>,
    ) -> Option<Vec<u8>> {
        enum Decision {
            Forward,
            Drop,
            Passthrough,
        }
        let decision = match self.agent_pending.get_mut(&resp.id) {
            Some(state @ AgentReqState::InFlight) => {
                *state = AgentReqState::Consumed;
                Decision::Forward
            }
            Some(AgentReqState::Consumed) => Decision::Drop,
            None => Decision::Passthrough,
        };
        match decision {
            Decision::Forward => {
                tracing::debug!(
                    session = %self.session_id,
                    %peer_id,
                    id = ?resp.id,
                    "first reply to agent-initiated request; forwarding to agent",
                );
                self.emit_agent_request_resolved(peer_id, &resp);
                Some(bytes)
            }
            Decision::Drop => {
                tracing::debug!(
                    session = %self.session_id,
                    %peer_id,
                    id = ?resp.id,
                    "duplicate reply to agent-initiated request; dropping",
                );
                None
            }
            Decision::Passthrough => Some(bytes),
        }
    }

    /// Transition every `InFlight` `agent_pending` entry to `Consumed`
    /// and broadcast a cleanup `amux/agent_request_resolved` for each.
    /// Called at turn-end (`route_agent_response` clearing
    /// `active_turn_mux_id`) — by that point any unresolved
    /// agent-initiated request has been abandoned by the agent (hermes,
    /// for example, internally times out at 60s and proceeds without
    /// writing a response frame), so peers need to dismiss the prompt.
    /// `result` and `error` are both `null` on the broadcast since no
    /// reply was ever forwarded to the agent.
    fn sweep_stale_agent_pending(&mut self, reason: &str) {
        let stale_ids: Vec<Id> = self
            .agent_pending
            .iter()
            .filter(|(_, state)| matches!(state, AgentReqState::InFlight))
            .map(|(id, _)| id.clone())
            .collect();
        if stale_ids.is_empty() {
            return;
        }
        for id in &stale_ids {
            if let Some(state) = self.agent_pending.get_mut(id) {
                *state = AgentReqState::Consumed;
            }
        }
        tracing::info!(
            session = %self.session_id,
            stale_count = stale_ids.len(),
            reason,
            "sweeping unresolved agent-initiated requests",
        );
        for id in stale_ids {
            let request_id_value = match serde_json::to_value(&id) {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(
                        session = %self.session_id,
                        error = %err,
                        "failed to serialize stale agent-request id; skipping cleanup broadcast",
                    );
                    continue;
                }
            };
            let frame = amux::agent_request_resolved(
                &self.session_id,
                &request_id_value,
                reason,
                None,
                None,
            );
            self.broadcast(frame);
        }
    }

    /// Broadcast `amux/agent_request_resolved` so peers can dismiss the
    /// matching pending UI. Called once per agent-initiated request id
    /// (on the InFlight → Consumed transition). The frame echoes the
    /// winning subscriber's `result` or `error` verbatim; for the only
    /// agent-initiated request the protocol currently has —
    /// `session/request_permission` — the result is derived entirely
    /// from `options[]` that was already broadcast in the request, so
    /// no new information leaks.
    fn emit_agent_request_resolved(&mut self, resolved_by: &str, resp: &IncomingResponse) {
        let request_id_value = match serde_json::to_value(&resp.id) {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(
                    session = %self.session_id,
                    error = %err,
                    "failed to serialize agent-request id; skipping resolved broadcast",
                );
                return;
            }
        };
        let error_value = resp
            .error
            .as_ref()
            .and_then(|e| serde_json::to_value(e).ok());
        let frame = amux::agent_request_resolved(
            &self.session_id,
            &request_id_value,
            resolved_by,
            resp.result.as_ref(),
            error_value.as_ref(),
        );
        self.broadcast(frame);
    }

    fn translate_outbound_request(
        &mut self,
        peer_id: &str,
        mut req: IncomingRequest,
    ) -> Option<Vec<u8>> {
        // Cache short-circuits. A cached `session/new` still updates the
        // driving subscriber — the subscriber asked the session a question,
        // even if we answered it locally.
        if req.method == "initialize"
            && let Some(cached) = self.initialize_cache.clone()
        {
            self.send_cached_response(peer_id, req.id, cached);
            return None;
        }
        if req.method == "session/new"
            && let Some(cached) = self.session_new_cache.clone()
        {
            self.note_driving_subscriber(peer_id);
            self.send_cached_response(peer_id, req.id, cached);
            return None;
        }

        // Turn serialization: a second concurrent `session/prompt` is
        // rejected locally with -32001 and does NOT update the driver
        // (the in-flight turn's originator stays the driver). Also broadcast
        // an amux/session_busy notification so peers see the rejection.
        if req.method == "session/prompt"
            && let Some(active) = self.active_turn_mux_id
        {
            let held_by = self.pending.get(&active).map(|pr| pr.peer_id.clone());
            tracing::warn!(
                session = %self.session_id,
                %peer_id,
                active_turn = active,
                held_by = ?held_by,
                "rejecting concurrent session/prompt with -32001",
            );
            let busy_frame = amux::session_busy(&self.session_id, true, held_by.as_deref());
            self.broadcast(busy_frame);
            self.send_error_response(
                peer_id,
                req.id,
                SESSION_BUSY_ERROR_CODE,
                "session busy: another turn is in flight",
            );
            return None;
        }

        if req.method != "initialize" {
            self.note_driving_subscriber(peer_id);
        }

        let handshake = match req.method.as_str() {
            "initialize" => Some(HandshakeKind::Initialize),
            "session/new" => Some(HandshakeKind::SessionNew),
            "session/load" => {
                let replay_start_len = self.replay_log.as_ref().map(|log| log.len()).unwrap_or(0);
                req.params
                    .as_ref()
                    .and_then(|p| p.get("sessionId"))
                    .and_then(|s| s.as_str())
                    .map(|s| HandshakeKind::SessionLoad {
                        loaded_session_id: s.to_string(),
                        replay_start_len,
                    })
            }
            _ => None,
        };

        let mux_id = self.next_mux_id;
        self.next_mux_id += 1;
        let original_id = req.id.clone();
        let is_prompt = req.method == "session/prompt";
        self.pending.insert(
            mux_id,
            PendingRequest {
                peer_id: peer_id.to_string(),
                original_id,
                handshake,
            },
        );
        req.id = Id::Number(mux_id as i64);

        match serde_json::to_vec(&req) {
            Ok(out) => {
                if is_prompt {
                    self.active_turn_mux_id = Some(mux_id);
                    let turn_id = AmuxTurnId(self.next_amux_turn_id);
                    self.next_amux_turn_id += 1;
                    self.active_amux_turn_id = Some(turn_id);
                    tracing::info!(
                        session = %self.session_id,
                        %peer_id,
                        mux_id,
                        amux_turn_id = %turn_id.formatted(),
                        "session/prompt forwarded; active turn opened",
                    );
                    self.emit_turn_started(peer_id, turn_id, req.params.as_ref());
                }
                Some(out)
            }
            Err(err) => {
                tracing::error!(
                    session = %self.session_id,
                    mux_id,
                    error = %err,
                    "failed to serialize translated request; dropping",
                );
                self.pending.remove(&mux_id);
                None
            }
        }
    }

    /// Build and broadcast `amux/turn_started`. The `content` field carries
    /// `params.prompt` verbatim; if missing we send `null`.
    fn emit_turn_started(&mut self, peer_id: &str, turn_id: AmuxTurnId, params: Option<&Value>) {
        let null = Value::Null;
        let content = params.and_then(|p| p.get("prompt")).unwrap_or(&null);
        let (peer_name, role) = self
            .subscribers
            .get(peer_id)
            .map(|s| (s.peer_name.as_deref(), s.role.as_deref()))
            .unwrap_or((None, None));
        let frame =
            amux::turn_started(&self.session_id, turn_id, peer_id, peer_name, role, content);
        self.broadcast(frame);
    }

    /// Build and broadcast `amux/turn_complete`. `stop_reason` is the
    /// `result.stopReason` value if present, else `null` (abnormal turns
    /// land here in chunk 9; for chunk 7 only the happy path is wired).
    fn emit_turn_complete(&mut self, turn_id: AmuxTurnId, result: Option<&Value>) {
        let null = Value::Null;
        let stop_reason = result.and_then(|r| r.get("stopReason")).unwrap_or(&null);
        let frame = amux::turn_complete(&self.session_id, turn_id, stop_reason);
        self.broadcast(frame);
    }

    /// Rebind the room's canonical session id to `loaded` after a
    /// successful `session/load`. Two cases:
    ///
    /// - **Existing `session_new_cache`**: mutate the `sessionId` field
    ///   in place. Other fields the upstream agent included in its
    ///   `session/new` response (e.g. agent-specific metadata) are
    ///   preserved so a late joiner's `session/new` call still gets
    ///   a structurally-valid response.
    /// - **No prior `session_new_cache`** (client opened with
    ///   `initialize` → `session/load` directly): synthesize a minimal
    ///   `{"sessionId": loaded}` value. A late joiner gets just the id
    ///   — enough to operate, missing any agent-specific session/new
    ///   fields the room never observed.
    ///
    /// Idempotent; safe to call multiple times for the same loaded id.
    fn rebind_canonical_session(&mut self, loaded: &str) {
        match self.session_new_cache.as_mut() {
            Some(Value::Object(obj)) => {
                let previous = obj
                    .insert("sessionId".to_string(), Value::String(loaded.to_string()))
                    .and_then(|v| v.as_str().map(|s| s.to_string()));
                tracing::info!(
                    session = %self.session_id,
                    previous = ?previous,
                    loaded,
                    "session/load: rebound canonical session id (existing cache)",
                );
            }
            _ => {
                self.session_new_cache = Some(serde_json::json!({
                    "sessionId": loaded,
                }));
                tracing::info!(
                    session = %self.session_id,
                    loaded,
                    "session/load: rebound canonical session id (synthesized cache)",
                );
            }
        }
    }

    fn reset_replay_generation_after_load(&mut self, loaded: &str, replay_start_len: usize) {
        self.replay_generation += 1;
        let presence_frames = self.current_peer_joined_replay_frames();
        let mut dropped_frame_count = 0;
        let mut retained_frame_count = 0;

        if let Some(log) = self.replay_log.as_mut() {
            dropped_frame_count = replay_start_len.min(log.len());
            if dropped_frame_count > 0 {
                log.drain(..dropped_frame_count);
            }

            let mut segmented = VecDeque::with_capacity(presence_frames.len() + log.len());
            for frame in presence_frames {
                segmented.push_back(Bytes::from(frame));
            }
            segmented.append(log);
            retained_frame_count = segmented.len();
            *log = segmented;
        }

        self.last_replay_reset = Some(ReplayResetSnapshot {
            loaded_session_id: loaded.to_string(),
            replay_generation: self.replay_generation,
            dropped_frame_count,
            retained_frame_count,
        });
        tracing::info!(
            session = %self.session_id,
            loaded,
            replay_generation = self.replay_generation,
            dropped_frame_count,
            retained_frame_count,
            "session/load: segmented replay generation",
        );
    }

    fn current_peer_joined_replay_frames(&self) -> Vec<Vec<u8>> {
        let mut peers: Vec<_> = self.subscribers.values().collect();
        peers.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));
        peers
            .into_iter()
            .map(|subscriber| {
                amux::peer_joined(
                    &self.session_id,
                    &subscriber.peer_id,
                    subscriber.peer_name.as_deref(),
                    subscriber.role.as_deref(),
                )
            })
            .collect()
    }

    fn note_driving_subscriber(&mut self, peer_id: &str) {
        if self.driving_subscriber_peer_id.as_deref() != Some(peer_id) {
            tracing::debug!(session = %self.session_id, %peer_id, "driving subscriber updated");
            self.driving_subscriber_peer_id = Some(peer_id.to_string());
        }
    }

    fn send_error_response(&self, peer_id: &str, original_id: Id, code: i64, message: &str) {
        let resp = IncomingResponse {
            jsonrpc: JsonRpcVersion,
            id: original_id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.to_string(),
                data: None,
            }),
        };
        let bytes = match serde_json::to_vec(&resp) {
            Ok(b) => Bytes::from(b),
            Err(err) => {
                tracing::error!(error = %err, "failed to serialize error response");
                return;
            }
        };
        if let Some(sub) = self.subscribers.get(peer_id)
            && sub.outbound.send(OutMsg::Frame(bytes)).is_err()
        {
            tracing::debug!(%peer_id, "subscriber dropped before error response delivered");
        }
    }

    fn send_cached_response(&self, peer_id: &str, original_id: Id, cached: Value) {
        let resp = IncomingResponse {
            jsonrpc: JsonRpcVersion,
            id: original_id,
            result: Some(cached),
            error: None,
        };
        let bytes = match serde_json::to_vec(&resp) {
            Ok(b) => Bytes::from(b),
            Err(err) => {
                tracing::error!(error = %err, "failed to serialize cached response");
                return;
            }
        };
        if let Some(sub) = self.subscribers.get(peer_id)
            && sub.outbound.send(OutMsg::Frame(bytes)).is_err()
        {
            tracing::debug!(%peer_id, "subscriber dropped before cached response delivered");
        }
    }

    /// Process one stdout line from the agent. Returns true if every
    /// subscriber has dropped during fan-out and the session should end.
    fn handle_agent_line(&mut self, line: Vec<u8>) -> bool {
        let frame = match Incoming::parse(&line) {
            Ok(f) => f,
            Err(err) => {
                tracing::warn!(
                    session = %self.session_id,
                    error = %err,
                    "invalid JSON-RPC frame from agent; falling back to raw broadcast",
                );
                return self.broadcast(line);
            }
        };
        match frame {
            Incoming::Notification(notif) => self.handle_agent_notification(notif, line),
            Incoming::Response(resp) => {
                self.route_agent_response(resp);
                false
            }
            Incoming::Request(req) => {
                self.route_agent_request(req.id, line);
                false
            }
        }
    }

    /// Agent-emitted notifications: most are broadcast-tier (forwarded to
    /// every subscriber and appended to the replay log). `$/cancel_request`
    /// is special — it cancels an *agent-initiated* request that's still
    /// InFlight in `agent_pending`. We translate by id, forward the
    /// cancellation to every subscriber, mark the entry Consumed, and emit
    /// `amux/agent_request_resolved { resolvedBy: "agent:cancelled" }` so
    /// peer UIs dismiss.
    fn handle_agent_notification(
        &mut self,
        notif: crate::protocol::jsonrpc::IncomingNotification,
        line: Vec<u8>,
    ) -> bool {
        if notif.method == CANCEL_REQUEST_METHOD {
            return self.handle_agent_cancel(notif, line);
        }
        self.broadcast(line)
    }

    /// Fan out an agent-initiated request to every attached subscriber and
    /// record the request id in `agent_pending` so the first subscriber
    /// reply wins. Not broadcast-tier — not appended to the replay log
    /// (replies are per-subscriber, and rejoining peers shouldn't be
    /// asked to confirm something already resolved).
    fn route_agent_request(&mut self, id: Id, line: Vec<u8>) {
        if self.subscribers.is_empty() {
            tracing::warn!(
                session = %self.session_id,
                id = ?id,
                "agent-initiated request with no attached subscribers; dropping",
            );
            return;
        }
        self.agent_pending
            .insert(id.clone(), AgentReqState::InFlight);
        tracing::debug!(
            session = %self.session_id,
            id = ?id,
            subscribers = self.subscribers.len(),
            "broadcasting agent-initiated request",
        );
        let frame = Bytes::from(line);
        self.subscribers.retain(|peer_id, sub| {
            match sub.outbound.send(OutMsg::Frame(frame.clone())) {
                Ok(()) => true,
                Err(_) => {
                    tracing::debug!(%peer_id, "outbound channel closed; dropping subscriber");
                    false
                }
            }
        });
    }

    fn route_agent_response(&mut self, mut resp: IncomingResponse) {
        let mux_id = match resp.id {
            Id::Number(n) if n >= 0 => n as u64,
            ref other => {
                tracing::warn!(
                    session = %self.session_id,
                    id = ?other,
                    "agent response with non-numeric or negative id; dropping",
                );
                return;
            }
        };
        let Some(pr) = self.pending.remove(&mux_id) else {
            tracing::warn!(session = %self.session_id, mux_id, "no pending request matches agent response; dropping");
            return;
        };

        if self.active_turn_mux_id == Some(mux_id) {
            self.active_turn_mux_id = None;
            let turn_id = self.active_amux_turn_id.take();
            tracing::info!(
                session = %self.session_id,
                mux_id,
                amux_turn_id = ?turn_id.map(|t| t.formatted()),
                "session/prompt response received; active turn cleared",
            );
            // Sweep before emitting amux/turn_complete so subscribers see
            // any abandoned-request cleanup events ahead of the turn
            // closure. Any agent-initiated request still InFlight at this
            // point was given up on by the agent (e.g. hermes' 60s
            // permission timeout fires internally without writing a
            // response frame).
            self.sweep_stale_agent_pending("mux:turn-ended");
            if let Some(turn_id) = turn_id {
                self.emit_turn_complete(turn_id, resp.result.as_ref());
            }
        }

        // First-success handshake response caching. `session/load`
        // success rebinds the room's canonical session to the loaded
        // id — late joiners that call `session/new` get the loaded
        // session, not the previously-cached one. A failed load
        // (error response) leaves the existing cache untouched.
        if let Some(kind) = pr.handshake
            && let Some(result) = &resp.result
        {
            match kind {
                HandshakeKind::Initialize => {
                    if self.initialize_cache.is_none() {
                        tracing::info!(session = %self.session_id, "caching initialize result");
                        self.initialize_cache = Some(result.clone());
                    }
                }
                HandshakeKind::SessionNew => {
                    if self.session_new_cache.is_none() {
                        tracing::info!(session = %self.session_id, "caching session/new result");
                        self.session_new_cache = Some(result.clone());
                    }
                }
                HandshakeKind::SessionLoad {
                    loaded_session_id,
                    replay_start_len,
                } => {
                    self.rebind_canonical_session(&loaded_session_id);
                    self.reset_replay_generation_after_load(&loaded_session_id, replay_start_len);
                }
            }
        }

        resp.id = pr.original_id;
        let bytes = match serde_json::to_vec(&resp) {
            Ok(b) => Bytes::from(b),
            Err(err) => {
                tracing::error!(error = %err, "failed to serialize translated response");
                return;
            }
        };
        if let Some(sub) = self.subscribers.get(&pr.peer_id) {
            if sub.outbound.send(OutMsg::Frame(bytes)).is_err() {
                tracing::debug!(peer_id = %pr.peer_id, "subscriber dropped before response delivered");
            }
        } else {
            tracing::debug!(peer_id = %pr.peer_id, "originator no longer attached; dropping response");
        }
    }

    /// Send `frame` to every subscriber and append to the replay log if
    /// enabled. Drops subscribers whose outbound channel has closed.
    /// Returns `true` only when fan-out *drained* subscribers — i.e.
    /// the map was non-empty going in and is empty coming out. A
    /// broadcast against an already-empty map (e.g. the initial
    /// `amux/peer_joined` emitted before the first subscriber is
    /// inserted) returns `false` and emits no "ending session" log,
    /// since no subscribers were lost.
    ///
    /// Only broadcast-tier frames flow through here: amux/* notifications
    /// and the agent's session/update (and other notification-shaped)
    /// frames. Per-subscriber frames (responses, agent-initiated requests)
    /// do NOT go through `broadcast` and are NOT logged.
    fn broadcast(&mut self, frame: impl Into<Bytes>) -> bool {
        let frame: Bytes = frame.into();
        if let Some(log) = self.replay_log.as_mut() {
            log.push_back(frame.clone());
        }
        let pre_fanout = self.subscribers.len();
        self.subscribers.retain(|peer_id, sub| {
            match sub.outbound.send(OutMsg::Frame(frame.clone())) {
                Ok(()) => true,
                Err(_) => {
                    tracing::debug!(%peer_id, "outbound channel closed; dropping subscriber");
                    false
                }
            }
        });
        if pre_fanout > 0 && self.subscribers.is_empty() {
            tracing::info!(session = %self.session_id, "no live subscribers after fan-out; ending session");
            return true;
        }
        false
    }
}

pub fn spawn_session(
    initial_subscriber: Subscriber,
    mut agent: AgentProcess,
    session_id: String,
    replay_policy: ReplayTurns,
    session_ttl: Duration,
) -> (SessionHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel::<SessionMsg>(SESSION_QUEUE_CAPACITY);
    let stdout_rx = agent
        .take_stdout_rx()
        .expect("AgentProcess::take_stdout_rx must succeed on a fresh process");

    let pump_tx = tx.clone();
    let pump_session_id = session_id.clone();
    let pump = tokio::spawn(async move {
        let mut rx = stdout_rx;
        while let Some(line) = rx.recv().await {
            if pump_tx
                .send(SessionMsg::AgentStdoutLine(line))
                .await
                .is_err()
            {
                return;
            }
        }
        let _ = pump_tx.send(SessionMsg::AgentDied).await;
        tracing::debug!(session = %pump_session_id, "stdout pump finished");
    });

    let actor = tokio::spawn(run_session(
        rx,
        agent,
        initial_subscriber,
        pump,
        session_id,
        replay_policy,
        session_ttl,
    ));
    (SessionHandle { tx }, actor)
}

/// Reason the session loop exited. Drives the teardown sequence — agent
/// death gets a structured 1011 close to subscribers; TTL expiry and
/// natural shutdown drop subscriber senders without a close frame.
enum ExitReason {
    LastSubscriberLeft, // followed TTL grace; subscribers map already empty
    TtlExpired,         // subscribers map still empty after grace
    AgentDied,
    ChannelClosed, // registry dropped tx; uncommon
}

async fn run_session(
    mut rx: mpsc::Receiver<SessionMsg>,
    mut agent: AgentProcess,
    initial_subscriber: Subscriber,
    pump: JoinHandle<()>,
    session_id: String,
    replay_policy: ReplayTurns,
    session_ttl: Duration,
) {
    let mut inner = SessionInner::new(session_id.clone(), replay_policy);
    inner
        .attach(initial_subscriber)
        .expect("initial subscriber cannot collide on an empty map");
    tracing::info!(session = %session_id, subscribers = inner.subscribers.len(), "session started");

    // None when at least one subscriber is attached; Some(sleep) while in
    // TTL grace. The select! arm only fires when the sleep is armed.
    let mut ttl_sleep: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;

    let reason = loop {
        let exit = tokio::select! {
            biased;
            msg = rx.recv() => {
                match msg {
                    None => Some(ExitReason::ChannelClosed),
                    Some(SessionMsg::Attach { subscriber, ack }) => {
                        let result = inner.attach(subscriber);
                        if result.is_ok() && ttl_sleep.take().is_some() {
                            tracing::info!(
                                session = %session_id,
                                "TTL grace cancelled by attach",
                            );
                        }
                        let _ = ack.send(result);
                        None
                    }
                    Some(SessionMsg::Detach { peer_id }) => {
                        let now_empty = inner.detach(&peer_id);
                        if now_empty {
                            tracing::info!(
                                session = %session_id,
                                ttl_secs = session_ttl.as_secs_f64(),
                                "last subscriber gone; starting TTL grace",
                            );
                            ttl_sleep = Some(Box::pin(tokio::time::sleep(session_ttl)));
                        }
                        None
                    }
                    Some(SessionMsg::InboundFromSubscriber { peer_id, bytes }) => {
                        if let Some(out) = inner.handle_inbound(&peer_id, bytes)
                            && let Err(err) = agent.send(&out).await
                        {
                            tracing::warn!(
                                session = %session_id,
                                %peer_id,
                                error = %err,
                                "agent stdin write failed",
                            );
                        }
                        None
                    }
                    Some(SessionMsg::AgentStdoutLine(line)) => {
                        if inner.handle_agent_line(line) {
                            Some(ExitReason::LastSubscriberLeft)
                        } else {
                            None
                        }
                    }
                    Some(SessionMsg::AgentDied) => {
                        Some(ExitReason::AgentDied)
                    }
                    Some(SessionMsg::Snapshot { ack }) => {
                        let snap = inner.build_snapshot(ttl_sleep.is_some());
                        let _ = ack.send(snap);
                        None
                    }
                }
            }
            _ = async {
                match ttl_sleep.as_mut() {
                    Some(s) => s.as_mut().await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                if inner.subscribers.is_empty() {
                    Some(ExitReason::TtlExpired)
                } else {
                    // Shouldn't happen — sleep was armed when no subs were
                    // present, attach disarms it. Defensive: clear and
                    // continue.
                    ttl_sleep = None;
                    None
                }
            }
        };
        if let Some(r) = exit {
            break r;
        }
    };

    match reason {
        ExitReason::AgentDied => {
            tracing::warn!(session = %session_id, "agent subprocess exited; closing subscribers with 1011");
            inner.close_all_subscribers(WS_CLOSE_AGENT_DEAD, "agent subprocess exited");
        }
        ExitReason::TtlExpired => {
            tracing::info!(session = %session_id, "TTL expired; tearing down session");
        }
        ExitReason::LastSubscriberLeft => {
            tracing::info!(session = %session_id, "session ended (no subscribers)");
        }
        ExitReason::ChannelClosed => {
            tracing::info!(session = %session_id, "session ended (channel closed)");
        }
    }

    inner.subscribers.clear();
    if let Err(err) = agent.shutdown(SHUTDOWN_TIMEOUT).await {
        tracing::warn!(session = %session_id, error = %err, "agent shutdown error");
    }
    pump.abort();
    tracing::info!(session = %session_id, "session task exiting");
}
