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

use std::collections::HashMap;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::agent::process::AgentProcess;
use crate::multiplex::subscriber::Subscriber;
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
}

impl SessionInner {
    fn new(session_id: String, initial: Subscriber) -> Self {
        let mut subs = HashMap::new();
        subs.insert(initial.peer_id.clone(), initial);
        Self {
            session_id,
            subscribers: subs,
            next_bridge_id: FIRST_BRIDGE_ID,
            pending: HashMap::new(),
            initialize_cache: None,
            session_new_cache: None,
            driving_subscriber_peer_id: None,
            active_turn_bridge_id: None,
        }
    }

    fn attach(&mut self, subscriber: Subscriber) -> Result<(), AttachError> {
        if self.subscribers.contains_key(&subscriber.peer_id) {
            return Err(AttachError::PeerIdInUse);
        }
        tracing::info!(
            session = %self.session_id,
            peer_id = %subscriber.peer_id,
            "subscriber joined existing session",
        );
        self.subscribers
            .insert(subscriber.peer_id.clone(), subscriber);
        Ok(())
    }

    /// Returns true if the session should end (no subscribers left).
    fn detach(&mut self, peer_id: &str) -> bool {
        if self.subscribers.remove(peer_id).is_some() {
            tracing::info!(session = %self.session_id, %peer_id, "subscriber detached");
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
        // (the in-flight turn's originator stays the driver).
        if req.method == "session/prompt"
            && let Some(active) = self.active_turn_bridge_id
        {
            tracing::warn!(
                session = %self.session_id,
                %peer_id,
                active_turn = active,
                "rejecting concurrent session/prompt with -32001",
            );
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
                    tracing::info!(
                        session = %self.session_id,
                        %peer_id,
                        bridge_id,
                        "session/prompt forwarded; active turn opened",
                    );
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
            Ok(b) => b,
            Err(err) => {
                tracing::error!(error = %err, "failed to serialize error response");
                return;
            }
        };
        if let Some(sub) = self.subscribers.get(peer_id)
            && sub.outbound.send(bytes).is_err()
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
            Ok(b) => b,
            Err(err) => {
                tracing::error!(error = %err, "failed to serialize cached response");
                return;
            }
        };
        if let Some(sub) = self.subscribers.get(peer_id)
            && sub.outbound.send(bytes).is_err()
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
                    && sub.outbound.send(line).is_err()
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
            tracing::info!(
                session = %self.session_id,
                bridge_id,
                "session/prompt response received; active turn cleared",
            );
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
            Ok(b) => b,
            Err(err) => {
                tracing::error!(error = %err, "failed to serialize translated response");
                return;
            }
        };
        if let Some(sub) = self.subscribers.get(&pr.peer_id) {
            if sub.outbound.send(bytes).is_err() {
                tracing::debug!(peer_id = %pr.peer_id, "subscriber dropped before response delivered");
            }
        } else {
            tracing::debug!(peer_id = %pr.peer_id, "originator no longer attached; dropping response");
        }
    }

    /// Send `line` to every subscriber. Drops subscribers whose outbound
    /// channel has closed. Returns true if the session has no live
    /// subscribers afterward.
    fn broadcast(&mut self, line: Vec<u8>) -> bool {
        self.subscribers
            .retain(|peer_id, sub| match sub.outbound.send(line.clone()) {
                Ok(()) => true,
                Err(_) => {
                    tracing::debug!(%peer_id, "outbound channel closed; dropping subscriber");
                    false
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

    let actor = tokio::spawn(run_session(rx, agent, initial_subscriber, pump, session_id));
    (SessionHandle { tx }, actor)
}

async fn run_session(
    mut rx: mpsc::Receiver<SessionMsg>,
    mut agent: AgentProcess,
    initial_subscriber: Subscriber,
    pump: JoinHandle<()>,
    session_id: String,
) {
    let mut inner = SessionInner::new(session_id.clone(), initial_subscriber);
    tracing::info!(session = %session_id, subscribers = inner.subscribers.len(), "session started");

    while let Some(msg) = rx.recv().await {
        match msg {
            SessionMsg::Attach { subscriber, ack } => {
                let result = inner.attach(subscriber);
                let _ = ack.send(result);
            }
            SessionMsg::Detach { peer_id } => {
                if inner.detach(&peer_id) {
                    break;
                }
            }
            SessionMsg::InboundFromSubscriber { peer_id, bytes } => {
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
            }
            SessionMsg::AgentStdoutLine(line) => {
                if inner.handle_agent_line(line) {
                    break;
                }
            }
            SessionMsg::AgentDied => {
                tracing::warn!(session = %session_id, "agent subprocess exited; ending session");
                break;
            }
        }
    }

    inner.subscribers.clear();
    if let Err(err) = agent.shutdown(SHUTDOWN_TIMEOUT).await {
        tracing::warn!(session = %session_id, error = %err, "agent shutdown error");
    }
    pump.abort();
    tracing::info!(session = %session_id, "session task exiting");
}
