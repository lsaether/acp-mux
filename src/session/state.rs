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
//!   Otherwise allocate a per-session `bridge_id`, store the
//!   `(peer_id, original_id)` mapping, rewrite the `id`, and forward.
//!   Substantive (non-`initialize`) requests also mark the sender as the
//!   current "driving subscriber" — the target for agent-initiated requests.
//! - `session/prompt` requests participate in turn serialization: while a
//!   prompt is in flight, a second `session/prompt` is rejected locally
//!   with JSON-RPC error code `-32001` ("session busy"). The active turn
//!   clears when the matching response returns from the agent.
//! - `response` → forward unchanged. Subscriber-originated responses only
//!   show up as replies to agent-initiated requests, whose ids belong to
//!   the agent's own id space (never our `bridge_id` space), so they round
//!   trip without rewriting.
//!
//! Inbound (agent → subscribers):
//! - `notification` → broadcast to every attached subscriber.
//! - `response` → look up `bridge_id`, restore `original_id`, send to the
//!   originator only. If the original request was the first `initialize`
//!   or `session/new`, cache the `result` for later joiners. If it matches
//!   `active_turn_bridge_id`, clear the active turn.
//! - `request` → route to the driving subscriber if it is still attached,
//!   otherwise fall back to one arbitrary attached subscriber. If none,
//!   drop with a warn. Broadcasting would invite duplicate replies back to
//!   the agent.
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
use crate::protocol::jsonrpc::{
    Id, Incoming, IncomingRequest, IncomingResponse, JsonRpcError, JsonRpcVersion,
};

const SESSION_QUEUE_CAPACITY: usize = 256;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

/// Bridge ids start at 1; 0 is reserved as a sentinel.
const FIRST_BRIDGE_ID: u64 = 1;

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
    pub active_turn_bridge_id: Option<u64>,
    pub active_amux_turn_id: Option<String>,
    pub driving_subscriber: Option<String>,
    pub subprocess_dead: bool,
    pub ttl_pending: bool,
    pub replay_log_len: Option<usize>,
    pub next_bridge_id: u64,
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

struct SessionInner {
    session_id: String,
    subscribers: HashMap<String, Subscriber>,
    next_bridge_id: u64,
    pending: HashMap<u64, PendingRequest>,
    initialize_cache: Option<Value>,
    session_new_cache: Option<Value>,
    /// Last subscriber to issue a substantive (non-`initialize`) request.
    /// Target for agent-initiated requests. Cleared when that subscriber
    /// detaches; falls back to an arbitrary subscriber at routing time.
    driving_subscriber_peer_id: Option<String>,
    /// `bridge_id` of the in-flight `session/prompt`, if any. While set, a
    /// second `session/prompt` is rejected locally with `-32001`.
    active_turn_bridge_id: Option<u64>,
    /// `amuxTurnId` paired with the in-flight `session/prompt`. Used to
    /// bookend `amux/turn_started` and `amux/turn_complete`.
    active_amux_turn_id: Option<AmuxTurnId>,
    /// Monotonic per-session counter for `amuxTurnId` allocation.
    next_amux_turn_id: u64,
    /// Replay log. `None` when policy is `Disabled` (saves memory).
    /// Otherwise, every broadcast-tier frame (amux/* + agent notifications)
    /// is appended; new subscribers receive a snapshot at attach time.
    replay_log: Option<VecDeque<Bytes>>,
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
            next_bridge_id: FIRST_BRIDGE_ID,
            pending: HashMap::new(),
            initialize_cache: None,
            session_new_cache: None,
            driving_subscriber_peer_id: None,
            active_turn_bridge_id: None,
            active_amux_turn_id: None,
            next_amux_turn_id: 1,
            replay_log,
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
            active_turn_bridge_id: self.active_turn_bridge_id,
            active_amux_turn_id: self.active_amux_turn_id.map(|t| t.formatted()),
            driving_subscriber: self.driving_subscriber_peer_id.clone(),
            subprocess_dead: false,
            ttl_pending,
            replay_log_len: self.replay_log.as_ref().map(|l| l.len()),
            next_bridge_id: self.next_bridge_id,
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
            Incoming::Notification(_) => Some(bytes),
            Incoming::Response(_) => Some(bytes),
            Incoming::Request(req) => self.translate_outbound_request(peer_id, req),
        }
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
            && let Some(active) = self.active_turn_bridge_id
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

        let bridge_id = self.next_bridge_id;
        self.next_bridge_id += 1;
        let original_id = req.id.clone();
        let is_prompt = req.method == "session/prompt";
        self.pending.insert(
            bridge_id,
            PendingRequest {
                peer_id: peer_id.to_string(),
                original_id,
                handshake,
            },
        );
        req.id = Id::Number(bridge_id as i64);

        match serde_json::to_vec(&req) {
            Ok(out) => {
                if is_prompt {
                    self.active_turn_bridge_id = Some(bridge_id);
                    let turn_id = AmuxTurnId(self.next_amux_turn_id);
                    self.next_amux_turn_id += 1;
                    self.active_amux_turn_id = Some(turn_id);
                    tracing::info!(
                        session = %self.session_id,
                        %peer_id,
                        bridge_id,
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
                    bridge_id,
                    error = %err,
                    "failed to serialize translated request; dropping",
                );
                self.pending.remove(&bridge_id);
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
            Incoming::Notification(_) => self.broadcast(line),
            Incoming::Response(resp) => {
                self.route_agent_response(resp);
                false
            }
            Incoming::Request(_) => {
                self.route_agent_request(line);
                false
            }
        }
    }

    /// Route an agent-initiated request frame. Prefers the current driving
    /// subscriber; falls back to one arbitrary attached subscriber if the
    /// driver has detached; drops with a warn if no one is attached.
    /// Not broadcast-tier — not appended to the replay log.
    fn route_agent_request(&mut self, line: Vec<u8>) {
        let target = self
            .driving_subscriber_peer_id
            .as_deref()
            .filter(|peer_id| self.subscribers.contains_key(*peer_id))
            .map(str::to_string)
            .or_else(|| self.subscribers.keys().next().cloned());
        match target {
            Some(peer_id) => {
                let drove = self.driving_subscriber_peer_id.as_deref() == Some(&peer_id);
                tracing::debug!(
                    session = %self.session_id,
                    %peer_id,
                    drove,
                    "routing agent-initiated request",
                );
                if let Some(sub) = self.subscribers.get(&peer_id)
                    && sub.outbound.send(OutMsg::Frame(Bytes::from(line))).is_err()
                {
                    tracing::debug!(%peer_id, "subscriber dropped while delivering agent request");
                }
            }
            None => {
                tracing::warn!(
                    session = %self.session_id,
                    "agent-initiated request with no attached subscribers; dropping",
                );
            }
        }
    }

    fn route_agent_response(&mut self, mut resp: IncomingResponse) {
        let bridge_id = match resp.id {
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
        let Some(pr) = self.pending.remove(&bridge_id) else {
            tracing::warn!(session = %self.session_id, bridge_id, "no pending request matches agent response; dropping");
            return;
        };

        if self.active_turn_bridge_id == Some(bridge_id) {
            self.active_turn_bridge_id = None;
            let turn_id = self.active_amux_turn_id.take();
            tracing::info!(
                session = %self.session_id,
                bridge_id,
                amux_turn_id = ?turn_id.map(|t| t.formatted()),
                "session/prompt response received; active turn cleared",
            );
            if let Some(turn_id) = turn_id {
                self.emit_turn_complete(turn_id, resp.result.as_ref());
            }
        }

        // First-success handshake response caching.
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
