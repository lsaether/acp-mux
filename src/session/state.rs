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

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use bytes::Bytes;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::agent::process::AgentProcess;
use crate::cli::ReplayTurns;
use crate::multiplex::subscriber::{OutMsg, Subscriber};
use crate::protocol::amux::{self, AmuxTurnId};
use crate::protocol::attach;
use crate::protocol::attach::{
    AttachParams, AttachResult, ConnectedClient, DetachParams, DetachResult, HistoryEntry,
    HistoryPolicy,
};
use crate::protocol::jsonrpc::{
    Id, Incoming, IncomingRequest, IncomingResponse, JsonRpcError, JsonRpcVersion,
};
use crate::protocol::session_update;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandshakeKind {
    Initialize,
    SessionNew,
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
    /// State for every agent-initiated request id we have ever broadcast
    /// in this session. `InFlight` until the first subscriber reply
    /// arrives; `Consumed` thereafter. We keep `Consumed` ids around for
    /// the session lifetime so late/duplicate responses can be recognized
    /// and dropped instead of leaking back to the agent.
    agent_pending: HashMap<Id, AgentReqState>,
    /// Original frames for in-flight agent-initiated `session/request_permission`
    /// requests. Used to re-issue the request to a client that calls
    /// `session/attach` after the broadcast — without this, a late
    /// joiner sees the permission in `history` but has no actionable
    /// request to reply to (the original broadcast was sent to the
    /// subscribers attached at that moment). Entries are inserted in
    /// `route_agent_request` for permissions only, removed on the
    /// InFlight → Consumed transition in `gate_subscriber_response`
    /// and in `sweep_stale_agent_pending`.
    pending_permission_frames: HashMap<Id, Bytes>,
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
            agent_pending: HashMap::new(),
            pending_permission_frames: HashMap::new(),
        }
    }

    /// ACP session id (returned by the agent in its `session/new` response),
    /// distinct from `self.session_id` which is the proxy session handle from
    /// the WS query. RFD-compliant `session/update` variants use the ACP
    /// session id so an RFD-aware client can correlate frames by sessionId
    /// without knowing about the amux proxy at all.
    fn acp_session_id(&self) -> Option<&str> {
        self.session_new_cache
            .as_ref()
            .and_then(|v| v.get("sessionId"))
            .and_then(|s| s.as_str())
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
    /// Emits `amux/peer_left` and (when an ACP session id is known) the
    /// RFD #533 `session/update { type: "client_disconnected" }` sibling
    /// to every remaining subscriber.
    fn detach(&mut self, peer_id: &str) -> bool {
        let removed = self.subscribers.remove(peer_id);
        if let Some(sub) = removed.as_ref() {
            tracing::info!(session = %self.session_id, %peer_id, "subscriber detached");
            let amux_frame = amux::peer_left(&self.session_id, peer_id);
            let su_frame = self.acp_session_id().map(|acp_id| {
                session_update::client_disconnected(acp_id, peer_id, sub.peer_name.as_deref())
            });
            self.broadcast(amux_frame);
            if let Some(frame) = su_frame {
                self.broadcast(frame);
            }
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
            Incoming::Notification(_) => Some(bytes),
            Incoming::Response(resp) => self.gate_subscriber_response(peer_id, resp, bytes),
            Incoming::Request(req) => self.translate_outbound_request(peer_id, req),
        }
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
                self.pending_permission_frames.remove(&resp.id);
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
            self.pending_permission_frames.remove(id);
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
        let amux_frame = amux::agent_request_resolved(
            &self.session_id,
            &request_id_value,
            resolved_by,
            resp.result.as_ref(),
            error_value.as_ref(),
        );
        self.broadcast(amux_frame);
        if let Some(acp_id) = self.acp_session_id() {
            let acp_id = acp_id.to_string();
            // Lift `result.outcome.optionId` into the RFD's `chosenOptionId`
            // shortcut when the winning reply matches the ACP permission
            // result shape. Bail to None if any layer is missing — the
            // proxy stays envelope-only.
            let chosen_option_id = resp
                .result
                .as_ref()
                .and_then(|r| r.get("outcome"))
                .and_then(|o| o.get("optionId"))
                .and_then(|s| s.as_str())
                .map(|s| s.to_string());
            let resolved_by_name = self
                .subscribers
                .get(resolved_by)
                .and_then(|s| s.peer_name.clone());
            let su_frame = session_update::permission_resolved(
                &acp_id,
                &request_id_value,
                resolved_by,
                resolved_by_name.as_deref(),
                chosen_option_id.as_deref(),
                resp.result.as_ref(),
                error_value.as_ref(),
            );
            self.broadcast(su_frame);
        }
    }

    fn translate_outbound_request(
        &mut self,
        peer_id: &str,
        mut req: IncomingRequest,
    ) -> Option<Vec<u8>> {
        // RFD #533 attach/detach are handled entirely in the proxy — they
        // never reach the agent. They sit at the very top so cache and
        // turn-busy gates don't interfere.
        if req.method == attach::METHOD_ATTACH {
            self.handle_attach(peer_id, req);
            return None;
        }
        if req.method == attach::METHOD_DETACH {
            self.handle_detach(peer_id, req);
            return None;
        }

        // Cache short-circuits. A cached `session/new` still updates the
        // driving subscriber — the subscriber asked the session a question,
        // even if we answered it locally.
        if req.method == "initialize"
            && let Some(mut cached) = self.initialize_cache.clone()
        {
            // Defensive: cache was populated by `route_agent_response`
            // which already injects the capability, but inject again so
            // an older cache (or one populated through some future code
            // path) still advertises attach correctly. The helper is
            // idempotent.
            inject_attach_capability(&mut cached);
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
    /// `params.prompt` verbatim; if missing we send `null`. Also emits the
    /// RFD #533 `session/update { type: "prompt_received" }` sibling when
    /// an ACP session id is known.
    fn emit_turn_started(&mut self, peer_id: &str, turn_id: AmuxTurnId, params: Option<&Value>) {
        let null = Value::Null;
        let content = params.and_then(|p| p.get("prompt")).unwrap_or(&null);
        let (peer_name, role) = self
            .subscribers
            .get(peer_id)
            .map(|s| (s.peer_name.clone(), s.role.clone()))
            .unwrap_or((None, None));
        let amux_frame = amux::turn_started(
            &self.session_id,
            turn_id,
            peer_id,
            peer_name.as_deref(),
            role.as_deref(),
            content,
        );
        let acp_id = self.acp_session_id().map(|s| s.to_string());
        let content_owned = acp_id.as_ref().map(|_| content.clone());
        self.broadcast(amux_frame);
        if let (Some(acp_id), Some(content_owned)) = (acp_id, content_owned) {
            let su_frame = session_update::prompt_received(
                &acp_id,
                &content_owned,
                peer_id,
                peer_name.as_deref(),
            );
            self.broadcast(su_frame);
        }
    }

    /// Build and broadcast `amux/turn_complete`. `stop_reason` is the
    /// `result.stopReason` value if present, else `null`. Also emits
    /// the RFD #533 `session/update { type: "turn_complete" }` sibling
    /// when an ACP session id is known.
    fn emit_turn_complete(&mut self, turn_id: AmuxTurnId, result: Option<&Value>) {
        let null = Value::Null;
        let stop_reason = result.and_then(|r| r.get("stopReason")).unwrap_or(&null);
        let amux_frame = amux::turn_complete(&self.session_id, turn_id, stop_reason);
        self.broadcast(amux_frame);
        if let Some(acp_id) = self.acp_session_id() {
            let acp_id = acp_id.to_string();
            let stop_reason_owned = stop_reason.clone();
            let su_frame = session_update::turn_complete(&acp_id, &stop_reason_owned);
            self.broadcast(su_frame);
        }
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

    /// Send an arbitrary JSON `result` to a subscriber as a JSON-RPC
    /// response. Used by the local-answer paths (`session/attach`,
    /// `session/detach`).
    fn send_local_result(&self, peer_id: &str, original_id: Id, result: Value) {
        let resp = IncomingResponse {
            jsonrpc: JsonRpcVersion,
            id: original_id,
            result: Some(result),
            error: None,
        };
        let bytes = match serde_json::to_vec(&resp) {
            Ok(b) => Bytes::from(b),
            Err(err) => {
                tracing::error!(error = %err, "failed to serialize local response");
                return;
            }
        };
        if let Some(sub) = self.subscribers.get(peer_id)
            && sub.outbound.send(OutMsg::Frame(bytes)).is_err()
        {
            tracing::debug!(%peer_id, "subscriber dropped before local response delivered");
        }
    }

    /// RFD #533 `session/attach` handler. The transport-level attach
    /// already happened during the WS upgrade (peer_id / peer_name from
    /// the query). This logical handshake returns the connected-peer
    /// roster, optionally a history snapshot shaped by `historyPolicy`,
    /// and re-issues any unresolved `session/request_permission` so the
    /// new client can answer them.
    fn handle_attach(&mut self, peer_id: &str, req: IncomingRequest) {
        let params: AttachParams = req
            .params
            .as_ref()
            .map(|v| serde_json::from_value(v.clone()).unwrap_or_default())
            .unwrap_or_default();

        let policy = params.history_policy.unwrap_or_default();
        let effective_policy = match policy {
            // amux doesn't implement message-id based delta sync yet
            // (depends on the Message ID RFD); fall back to Full per the
            // RFD #533 spec.
            HistoryPolicy::AfterMessage => HistoryPolicy::Full,
            other => other,
        };

        // sessionId: prefer cached ACP id; fall back to proxy id for
        // clients that attach before session/new has succeeded. If the
        // client passed an explicit sessionId and it mismatches the
        // cached one, return -32001 (RFD: "Session not found").
        let resolved_session_id = self
            .acp_session_id()
            .map(|s| s.to_string())
            .unwrap_or_else(|| self.session_id.clone());
        if let Some(requested) = params.session_id.as_deref()
            && !requested.is_empty()
            && requested != resolved_session_id
            && requested != self.session_id
        {
            tracing::warn!(
                session = %self.session_id,
                %peer_id,
                requested,
                "session/attach with mismatched sessionId; returning -32001",
            );
            self.send_error_response(
                peer_id,
                req.id,
                attach::ATTACH_ERR_NOT_FOUND,
                "session not found",
            );
            return;
        }

        let echoed_client_id = params.client_id.unwrap_or_else(|| peer_id.to_string());

        let connected_clients: Vec<ConnectedClient> = self
            .subscribers
            .values()
            .map(|s| ConnectedClient {
                client_id: s.peer_id.clone(),
                name: s.peer_name.clone(),
            })
            .collect();

        let history = match effective_policy {
            HistoryPolicy::None => None,
            HistoryPolicy::Full => Some(self.history_full()),
            HistoryPolicy::PendingOnly => Some(self.history_pending_only()),
            HistoryPolicy::AfterMessage => unreachable!("normalized above"),
        };

        let result = AttachResult {
            session_id: resolved_session_id,
            client_id: echoed_client_id,
            history_policy: effective_policy,
            connected_clients,
            history,
        };
        let result_value = match serde_json::to_value(result) {
            Ok(v) => v,
            Err(err) => {
                tracing::error!(error = %err, "failed to serialize attach result");
                self.send_error_response(
                    peer_id,
                    req.id,
                    attach::ATTACH_ERR_UNSUPPORTED,
                    "attach result serialization failed",
                );
                return;
            }
        };
        self.send_local_result(peer_id, req.id, result_value);

        self.reissue_pending_permissions(peer_id);
    }

    /// Project the replay log into RFD-style `history` entries. Each
    /// entry carries the broadcast frame's `method` and `params` verbatim
    /// — clients interpret `session/update` and the `amux/*` namespace
    /// using the standard ACP / amux shapes.
    fn history_full(&self) -> Vec<HistoryEntry> {
        let log = match self.replay_log.as_ref() {
            Some(l) => l,
            None => return Vec::new(),
        };
        log.iter()
            .filter_map(|bytes| serde_json::from_slice::<Value>(bytes).ok())
            .filter_map(|v| {
                let method = v.get("method")?.as_str()?.to_string();
                let params = v.get("params").cloned().unwrap_or(Value::Null);
                Some(HistoryEntry { method, params })
            })
            .collect()
    }

    /// `pending_only`: just the unresolved `session/request_permission`
    /// frames we're still waiting on a reply for. Used by notification /
    /// dashboard clients that only care about actionable items.
    fn history_pending_only(&self) -> Vec<HistoryEntry> {
        self.pending_permission_frames
            .values()
            .filter_map(|bytes| serde_json::from_slice::<Value>(bytes).ok())
            .filter_map(|v| {
                let method = v.get("method")?.as_str()?.to_string();
                let params = v.get("params").cloned().unwrap_or(Value::Null);
                Some(HistoryEntry { method, params })
            })
            .collect()
    }

    /// Re-deliver every InFlight `session/request_permission` frame to
    /// the just-attached subscriber. Same id as the original broadcast,
    /// so the proxy's first-writer-wins gate handles the reply normally
    /// (whichever client answers first is the winner; later replies for
    /// the same id are dropped). Without this, a late joiner sees the
    /// permission in `history` but has no actionable request to reply
    /// to.
    fn reissue_pending_permissions(&self, peer_id: &str) {
        if self.pending_permission_frames.is_empty() {
            return;
        }
        let Some(sub) = self.subscribers.get(peer_id) else {
            return;
        };
        for frame in self.pending_permission_frames.values() {
            if sub.outbound.send(OutMsg::Frame(frame.clone())).is_err() {
                tracing::debug!(%peer_id, "newcomer dropped during pending-permission re-issue");
                return;
            }
        }
        tracing::info!(
            session = %self.session_id,
            %peer_id,
            count = self.pending_permission_frames.len(),
            "re-issued unresolved session/request_permission to attaching client",
        );
    }

    /// RFD #533 `session/detach` handler. Acks the request, then triggers
    /// a graceful WebSocket close. The amux session continues as long as
    /// any subscriber remains attached; once the last one is gone the
    /// usual TTL grace applies (and the agent subprocess is reused if a
    /// new subscriber joins within the grace window).
    fn handle_detach(&mut self, peer_id: &str, req: IncomingRequest) {
        let _params: DetachParams = req
            .params
            .as_ref()
            .map(|v| serde_json::from_value(v.clone()).unwrap_or_default())
            .unwrap_or_default();

        let resolved_session_id = self
            .acp_session_id()
            .map(|s| s.to_string())
            .unwrap_or_else(|| self.session_id.clone());
        let result = DetachResult {
            session_id: resolved_session_id,
            status: "detached",
        };
        let result_value = match serde_json::to_value(result) {
            Ok(v) => v,
            Err(err) => {
                tracing::error!(error = %err, "failed to serialize detach result");
                return;
            }
        };
        self.send_local_result(peer_id, req.id, result_value);

        // Signal the WS-out task to close the socket cleanly after the
        // response above is flushed. Removing the subscriber from the map
        // also drops the outbound sender, which independently ends the
        // ws_out task; the explicit Close is for a structured close
        // frame (1000 = normal closure).
        if let Some(sub) = self.subscribers.get(peer_id) {
            let _ = sub.outbound.send(OutMsg::Close {
                code: 1000,
                reason: "client requested detach".to_string(),
            });
        }
        // Run the detach broadcast (peer_left + client_disconnected) and
        // bookkeeping. The outer ws handler will also send Detach when
        // the WS finally tears down; that second call is a no-op because
        // the subscriber is already removed.
        let _ = self.detach(peer_id);
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
            Incoming::Notification(_) => self.broadcast(line),
            Incoming::Response(resp) => {
                self.route_agent_response(resp);
                false
            }
            Incoming::Request(req) => {
                self.route_agent_request(req.id, req.method, line);
                false
            }
        }
    }

    /// Fan out an agent-initiated request to every attached subscriber and
    /// record the request id in `agent_pending` so the first subscriber
    /// reply wins. Not broadcast-tier — not appended to the replay log
    /// (replies are per-subscriber, and rejoining peers shouldn't be
    /// asked to confirm something already resolved).
    fn route_agent_request(&mut self, id: Id, method: String, line: Vec<u8>) {
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
            %method,
            subscribers = self.subscribers.len(),
            "broadcasting agent-initiated request",
        );
        let frame = Bytes::from(line);
        // Remember the original frame for `session/request_permission` so a
        // client that calls `session/attach` after the broadcast can have
        // it re-issued (see `reissue_pending_permissions`). Other
        // agent-initiated methods aren't re-issued — only permissions are
        // "actionable" per RFD #533.
        if method == "session/request_permission" {
            self.pending_permission_frames
                .insert(id.clone(), frame.clone());
        }
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

        // First-success handshake response caching. For `initialize`, the
        // proxy mutates the result to advertise its own multi-client
        // capability (RFD #533 `sessionCapabilities.attach`) before
        // caching and before sending to the originator — the upstream
        // agent doesn't know it's being multiplexed, so the proxy
        // synthesizes the capability on top of the agent's reply.
        if let Some(kind) = pr.handshake
            && resp.result.is_some()
        {
            match kind {
                HandshakeKind::Initialize => {
                    if let Some(result) = resp.result.as_mut() {
                        inject_attach_capability(result);
                    }
                    if self.initialize_cache.is_none()
                        && let Some(result) = resp.result.as_ref()
                    {
                        tracing::info!(session = %self.session_id, "caching initialize result");
                        self.initialize_cache = Some(result.clone());
                    }
                }
                HandshakeKind::SessionNew => {
                    if self.session_new_cache.is_none()
                        && let Some(result) = resp.result.as_ref()
                    {
                        tracing::info!(session = %self.session_id, "caching session/new result");
                        self.session_new_cache = Some(result.clone());
                    }
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
    /// Returns true if the session has no live subscribers afterward.
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
        self.subscribers.retain(|peer_id, sub| {
            match sub.outbound.send(OutMsg::Frame(frame.clone())) {
                Ok(()) => true,
                Err(_) => {
                    tracing::debug!(%peer_id, "outbound channel closed; dropping subscriber");
                    false
                }
            }
        });
        if self.subscribers.is_empty() {
            tracing::info!(session = %self.session_id, "no live subscribers after fan-out; ending session");
            return true;
        }
        false
    }
}

/// Mutate the agent's `initialize` result to advertise the proxy's RFD
/// #533 multi-client attach capability. Idempotent. Drops silently if
/// `result` is not a JSON object — the agent gave us something we can't
/// extend, so we leave it alone.
fn inject_attach_capability(result: &mut Value) {
    let Some(root) = result.as_object_mut() else {
        return;
    };
    let agent_caps = root
        .entry("agentCapabilities")
        .or_insert_with(|| Value::Object(Default::default()));
    let Some(agent_caps_obj) = agent_caps.as_object_mut() else {
        return;
    };
    let session_caps = agent_caps_obj
        .entry("sessionCapabilities")
        .or_insert_with(|| Value::Object(Default::default()));
    let Some(session_caps_obj) = session_caps.as_object_mut() else {
        return;
    };
    // The latest RFD revision removed role enumeration, so the value is
    // a boolean flag. Keep the field shape consistent with the spec.
    session_caps_obj.insert("attach".to_string(), Value::Bool(true));
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
