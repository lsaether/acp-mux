//! Per-mux actor: one ACP agent subprocess, N WebSocket subscribers.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde_json::{Map, Value, json};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until};

use crate::agent::process::AgentProcess;
use crate::attach::{
    self, AttachParams, AttachResult, ConnectedClient, DetachParams, DetachResult, HistoryEntry,
    HistoryPolicy,
};
use crate::cli::{ClientToolMode, ClientToolPolicy, ReplayTurns};
use crate::extension::{
    Disposition, MuxCtx, MuxExtension, NoopExtension, NotifyDisposition, ResolvedBy,
};
use crate::jsonrpc::{
    Id, Incoming, IncomingNotification, IncomingRequest, IncomingResponse, JsonRpcError,
    JsonRpcVersion,
};
use crate::replay_store::{ReplayStore, RoomReplayStore};
use crate::subscriber::{OutMsg, Subscriber};

const MUX_QUEUE_CAPACITY: usize = 256;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const FIRST_MUX_ID: u64 = 1;
const SESSION_BUSY_ERROR_CODE: i64 = -32001;
const CLIENT_TOOL_BLOCKED_ERROR_CODE: i64 = -32000;
const WS_CLOSE_AGENT_DEAD: u16 = 1011;
const CANCEL_REQUEST_METHOD: &str = "$/cancel_request";

pub enum MuxMsg {
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
    AgentStderrLine(Vec<u8>),
    AgentDied,
    ExtensionWake(Vec<u8>),
    Snapshot {
        ack: oneshot::Sender<MuxSnapshot>,
    },
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MuxSnapshot {
    pub mux_id: String,
    pub agent_cwd: String,
    pub subscribers: Vec<SubscriberSnapshot>,
    pub pending_request_count: usize,
    pub initialize_cached: bool,
    pub cached_session_id: Option<String>,
    pub canonical_session_id: Option<String>,
    pub prompt_in_flight: Option<u64>,
    pub subprocess_dead: bool,
    pub ttl_pending: bool,
    pub replay_log_len: Option<usize>,
    pub next_mux_id: u64,
    pub pending_agent_request_count: usize,
    pub pending_permission_count: usize,
    #[serde(flatten, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriberSnapshot {
    pub peer_id: String,
    pub peer_name: Option<String>,
    pub role: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttachError {
    PeerIdInUse,
}

#[derive(Clone)]
pub struct MuxHandle {
    pub tx: mpsc::Sender<MuxMsg>,
}

impl MuxHandle {
    pub fn is_alive(&self) -> bool {
        !self.tx.is_closed()
    }
}

#[derive(Debug, Clone)]
pub struct MuxOptions {
    pub replay_policy: ReplayTurns,
    pub mux_ttl: Duration,
    pub client_tool_policy: ClientToolPolicy,
    pub agent_cwd: String,
    pub replay_store: Option<Arc<ReplayStore>>,
}

#[derive(Debug, Clone)]
pub struct ReplayView<'a> {
    pub seq: u64,
    pub ext_tag: u64,
    pub recorded_at: &'a str,
    pub frame: &'a Bytes,
}

#[derive(Debug, Clone)]
pub(crate) struct ReplayEntry {
    pub(crate) frame: Bytes,
    pub(crate) recorded_at: String,
    pub(crate) seq: u64,
    pub(crate) ext_tag: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HandshakeKind {
    Initialize,
    SessionNew,
    SessionLoad { loaded_session_id: String },
}

#[derive(Debug)]
pub(crate) struct PendingRequest {
    pub(crate) peer_id: String,
    original_id: Id,
    handshake: Option<HandshakeKind>,
    deliver_response: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentReqState {
    InFlight,
    Consumed,
}

pub struct MuxCore {
    pub(crate) mux_id: String,
    pub(crate) agent_cwd: String,
    pub(crate) subscribers: HashMap<String, Subscriber>,
    next_mux_id: u64,
    pub(crate) pending: HashMap<u64, PendingRequest>,
    initialize_cache: Option<Value>,
    session_new_cache: Option<Value>,
    pub(crate) canonical_session_id: Option<String>,
    pub(crate) replay_log: Option<VecDeque<ReplayEntry>>,
    replay_store: Option<RoomReplayStore>,
    next_replay_seq: u64,
    pub(crate) replay_tag: u64,
    pub(crate) prompt_in_flight: Option<u64>,
    client_tool_policy: ClientToolPolicy,
    agent_pending: HashMap<Id, AgentReqState>,
    pub(crate) pending_permissions: Vec<(Id, Bytes)>,
    pub(crate) agent_outbox: Vec<Vec<u8>>,
    pub(crate) self_tx: mpsc::Sender<MuxMsg>,
}

struct Mux {
    core: MuxCore,
    ext: Box<dyn MuxExtension>,
}

impl MuxCore {
    fn new(mux_id: String, options: MuxOptions, self_tx: mpsc::Sender<MuxMsg>) -> Self {
        let (replay_log, replay_store, next_replay_seq) = match options.replay_policy {
            ReplayTurns::Disabled => (None, None, 1),
            ReplayTurns::Unbounded => hydrate_replay_store(&mux_id, options.replay_store.as_ref()),
            ReplayTurns::Bounded(n) => {
                tracing::warn!(
                    bound = n,
                    "--replay-turns N (bounded eviction) accepted but not yet implemented; behaving as unbounded",
                );
                hydrate_replay_store(&mux_id, options.replay_store.as_ref())
            }
        };
        Self {
            mux_id,
            agent_cwd: options.agent_cwd,
            subscribers: HashMap::new(),
            next_mux_id: FIRST_MUX_ID,
            pending: HashMap::new(),
            initialize_cache: None,
            session_new_cache: None,
            canonical_session_id: None,
            replay_log,
            replay_store,
            next_replay_seq,
            replay_tag: 0,
            prompt_in_flight: None,
            client_tool_policy: options.client_tool_policy,
            agent_pending: HashMap::new(),
            pending_permissions: Vec::new(),
            agent_outbox: Vec::new(),
            self_tx,
        }
    }

    fn attach(
        &mut self,
        ext: &mut dyn MuxExtension,
        subscriber: Subscriber,
    ) -> Result<(), AttachError> {
        if self.subscribers.contains_key(&subscriber.peer_id) {
            return Err(AttachError::PeerIdInUse);
        }
        let suppress_legacy_replay = subscriber.suppress_legacy_replay;
        let snapshot: Vec<ReplayEntry> = if suppress_legacy_replay {
            Vec::new()
        } else {
            self.replay_log
                .as_ref()
                .map(|log| log.iter().cloned().collect())
                .unwrap_or_default()
        };
        let peer_id = subscriber.peer_id.clone();
        ext.on_subscriber_attaching(&mut MuxCtx::new(self), &subscriber);
        tracing::info!(
            mux = %self.mux_id,
            %peer_id,
            replay_frames = snapshot.len(),
            suppress_legacy_replay,
            "subscriber joined mux",
        );
        self.subscribers.insert(peer_id.clone(), subscriber);
        ext.on_subscriber_attached(&mut MuxCtx::new(self), &peer_id);
        if let Some(outbound) = self
            .subscribers
            .get(&peer_id)
            .map(|sub| sub.outbound.clone())
        {
            for entry in snapshot {
                let Some(frame) = ext.replay_frame(
                    &mut MuxCtx::new(self),
                    ReplayView {
                        seq: entry.seq,
                        ext_tag: entry.ext_tag,
                        recorded_at: entry.recorded_at.as_str(),
                        frame: &entry.frame,
                    },
                ) else {
                    continue;
                };
                if outbound.send(OutMsg::Frame(frame)).is_err() {
                    tracing::debug!(%peer_id, "newcomer dropped during replay");
                    break;
                }
            }
        }
        Ok(())
    }

    fn detach(&mut self, ext: &mut dyn MuxExtension, peer_id: &str) {
        if self.subscribers.remove(peer_id).is_some() {
            tracing::info!(mux = %self.mux_id, %peer_id, "subscriber detached");
            ext.on_subscriber_detached(&mut MuxCtx::new(self), peer_id);
        }
    }

    fn build_snapshot(&mut self, ext: &dyn MuxExtension, ttl_pending: bool) -> MuxSnapshot {
        let subscribers = self
            .subscribers
            .values()
            .map(|s| SubscriberSnapshot {
                peer_id: s.peer_id.clone(),
                peer_name: s.peer_name.clone(),
                role: s.role.clone(),
            })
            .collect();
        let cached_session_id = self.session_new_cache.as_ref().and_then(|v| {
            v.get("sessionId")
                .and_then(Value::as_str)
                .map(str::to_string)
        });
        let extra = match ext.debug_snapshot(&MuxCtx::new(self)) {
            Value::Object(map) => map,
            _ => Map::new(),
        };
        MuxSnapshot {
            mux_id: self.mux_id.clone(),
            agent_cwd: self.agent_cwd.clone(),
            subscribers,
            pending_request_count: self.pending.len(),
            initialize_cached: self.initialize_cache.is_some(),
            cached_session_id,
            canonical_session_id: self.canonical_session_id.clone(),
            prompt_in_flight: self.prompt_in_flight,
            subprocess_dead: false,
            ttl_pending,
            replay_log_len: self.replay_log.as_ref().map(VecDeque::len),
            next_mux_id: self.next_mux_id,
            pending_agent_request_count: self
                .agent_pending
                .values()
                .filter(|state| matches!(state, AgentReqState::InFlight))
                .count(),
            pending_permission_count: self.pending_permissions.len(),
            extra,
        }
    }

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

    fn handle_inbound(&mut self, ext: &mut dyn MuxExtension, peer_id: &str, bytes: Vec<u8>) {
        let frame = match Incoming::parse(&bytes) {
            Ok(f) => f,
            Err(err) => {
                tracing::warn!(
                    mux = %self.mux_id,
                    %peer_id,
                    error = %err,
                    "invalid JSON-RPC frame from subscriber; dropping",
                );
                return;
            }
        };
        match frame {
            Incoming::Notification(notif) => {
                self.handle_subscriber_notification(ext, peer_id, notif, bytes)
            }
            Incoming::Response(resp) => self.gate_subscriber_response(ext, peer_id, resp, bytes),
            Incoming::Request(req) => self.translate_outbound_request(ext, peer_id, req),
        };
    }

    fn handle_subscriber_notification(
        &mut self,
        ext: &mut dyn MuxExtension,
        peer_id: &str,
        notif: IncomingNotification,
        bytes: Vec<u8>,
    ) {
        match notif.method.as_str() {
            CANCEL_REQUEST_METHOD => {
                if let Some(bytes) = self.handle_subscriber_cancel(peer_id, notif) {
                    self.agent_outbox.push(bytes);
                }
            }
            _ => {
                let disposition =
                    ext.on_subscriber_notification(&mut MuxCtx::new(self), peer_id, &notif);
                if matches!(disposition, NotifyDisposition::Passthrough) {
                    self.agent_outbox.push(bytes);
                }
            }
        }
    }

    fn handle_subscriber_cancel(
        &mut self,
        peer_id: &str,
        notif: IncomingNotification,
    ) -> Option<Vec<u8>> {
        let original_id = match parse_cancel_request_id(notif.params.as_ref()) {
            Some(id) => id,
            None => {
                tracing::debug!(mux = %self.mux_id, %peer_id, "invalid/null cancel id; dropping");
                return None;
            }
        };
        let Some(mux_id) = self.find_pending_mux_id(peer_id, &original_id) else {
            tracing::debug!(
                mux = %self.mux_id,
                %peer_id,
                id = ?original_id,
                "subscriber cancel for unknown id; dropping",
            );
            return None;
        };
        Some(build_cancel_request(Id::Number(mux_id as i64)))
    }

    fn gate_subscriber_response(
        &mut self,
        ext: &mut dyn MuxExtension,
        peer_id: &str,
        resp: IncomingResponse,
        bytes: Vec<u8>,
    ) {
        let decision = match self.agent_pending.get_mut(&resp.id) {
            Some(state @ AgentReqState::InFlight) => {
                *state = AgentReqState::Consumed;
                self.pending_permissions.retain(|(id, _)| id != &resp.id);
                Some(true)
            }
            Some(AgentReqState::Consumed) => Some(false),
            None => None,
        };
        match decision {
            Some(true) => {
                tracing::debug!(
                    mux = %self.mux_id,
                    %peer_id,
                    id = ?resp.id,
                    "first reply to agent-initiated request; forwarding to agent",
                );
                ext.on_agent_request_resolved(
                    &mut MuxCtx::new(self),
                    &resp.id,
                    ResolvedBy::Peer(peer_id.to_string()),
                    Some(&resp),
                );
                self.agent_outbox.push(bytes);
            }
            Some(false) => {
                tracing::debug!(
                    mux = %self.mux_id,
                    %peer_id,
                    id = ?resp.id,
                    "duplicate reply to agent-initiated request; dropping",
                );
            }
            None => self.agent_outbox.push(bytes),
        }
    }

    fn translate_outbound_request(
        &mut self,
        ext: &mut dyn MuxExtension,
        peer_id: &str,
        mut req: IncomingRequest,
    ) {
        match req.method.as_str() {
            attach::METHOD_ATTACH => {
                self.handle_attach_request(ext, peer_id, req);
                return;
            }
            attach::METHOD_DETACH => {
                self.handle_detach_request(ext, peer_id, req);
                return;
            }
            "initialize" => {
                if let Some(result) = self.initialize_cache.clone() {
                    self.send_result_response(peer_id, req.id, result);
                    return;
                }
            }
            "session/new" => {
                if let Some(result) = self.session_new_cache.clone() {
                    self.send_result_response(peer_id, req.id, result);
                    return;
                }
            }
            _ => {}
        }

        match ext.on_subscriber_request(&mut MuxCtx::new(self), peer_id, &mut req) {
            Disposition::Handled => return,
            Disposition::Reject { code, message } => {
                self.send_error_response(peer_id, req.id, code, &message);
                return;
            }
            Disposition::Forward => {}
        }

        if req.method == "session/prompt" && self.prompt_in_flight.is_some() {
            self.send_error_response(peer_id, req.id, SESSION_BUSY_ERROR_CODE, "session busy");
            return;
        }

        if req.method == "initialize" {
            sanitize_initialize_client_capabilities(&mut req);
        }

        let mux_id = self.next_mux_id;
        self.next_mux_id = self.next_mux_id.saturating_add(1);
        let original_id = std::mem::replace(&mut req.id, Id::Number(mux_id as i64));
        let handshake = handshake_kind(&req);
        let is_prompt = req.method == "session/prompt";
        ext.on_request_translating(&mut MuxCtx::new(self), peer_id, mux_id, &mut req);
        let bytes = Incoming::Request(req.clone())
            .to_vec()
            .unwrap_or_else(|err| {
                tracing::error!(error = %err, "failed to serialize outbound request");
                Vec::new()
            });
        if bytes.is_empty() {
            return;
        }
        self.pending.insert(
            mux_id,
            PendingRequest {
                peer_id: peer_id.to_string(),
                original_id,
                handshake,
                deliver_response: true,
            },
        );
        if is_prompt {
            self.prompt_in_flight = Some(mux_id);
        }
        self.agent_outbox.push(bytes);
        ext.on_request_forwarded(&mut MuxCtx::new(self), peer_id, mux_id, &req);
    }

    fn handle_agent_line(&mut self, ext: &mut dyn MuxExtension, line: Vec<u8>) {
        let frame = match Incoming::parse(&line) {
            Ok(f) => f,
            Err(err) => {
                tracing::warn!(
                    mux = %self.mux_id,
                    error = %err,
                    "invalid JSON-RPC frame from agent; broadcasting raw",
                );
                self.broadcast(Bytes::from(line));
                return;
            }
        };
        match frame {
            Incoming::Notification(notif) => {
                if notif.method == CANCEL_REQUEST_METHOD {
                    self.handle_agent_cancel(ext, notif, line);
                } else {
                    ext.on_agent_notification(&mut MuxCtx::new(self), &notif);
                    self.broadcast(Bytes::from(line));
                }
            }
            Incoming::Request(req) => self.handle_agent_request(ext, req, line),
            Incoming::Response(resp) => {
                self.route_agent_response(ext, resp);
            }
        }
    }

    fn handle_agent_request(
        &mut self,
        ext: &mut dyn MuxExtension,
        req: IncomingRequest,
        line: Vec<u8>,
    ) {
        if let Some(mode) = self.client_tool_policy.mode_for_method(&req.method)
            && mode == ClientToolMode::Block
        {
            tracing::warn!(
                mux = %self.mux_id,
                method = %req.method,
                id = ?req.id,
                "blocking agent-initiated client-tool request by policy",
            );
            self.agent_outbox
                .push(build_client_tool_blocked_response(req.id, &req.method));
            return;
        }

        self.agent_pending
            .entry(req.id.clone())
            .or_insert(AgentReqState::InFlight);
        let bytes = Bytes::from(line);
        if req.method == "session/request_permission" {
            self.pending_permissions
                .push((req.id.clone(), bytes.clone()));
        }
        ext.on_agent_request(&mut MuxCtx::new(self), &req.id, &req);
        self.fanout(bytes);
    }

    fn handle_agent_cancel(
        &mut self,
        ext: &mut dyn MuxExtension,
        notif: IncomingNotification,
        line: Vec<u8>,
    ) {
        let Some(request_id) = parse_cancel_request_id(notif.params.as_ref()) else {
            tracing::debug!(mux = %self.mux_id, "agent cancel with invalid/null requestId; dropping");
            return;
        };
        if let Some(state @ AgentReqState::InFlight) = self.agent_pending.get_mut(&request_id) {
            *state = AgentReqState::Consumed;
            self.pending_permissions
                .retain(|(pending_id, _)| pending_id != &request_id);
        }
        self.broadcast(Bytes::from(line));
        ext.on_agent_request_resolved(
            &mut MuxCtx::new(self),
            &request_id,
            ResolvedBy::AgentCancelled,
            None,
        );
    }

    fn route_agent_response(&mut self, ext: &mut dyn MuxExtension, mut resp: IncomingResponse) {
        let mux_id = match resp.id {
            Id::Number(n) if n >= 0 => n as u64,
            ref other => {
                tracing::debug!(mux = %self.mux_id, id = ?other, "agent response id is not a mux id; dropping");
                return;
            }
        };
        let Some(pending) = self.pending.remove(&mux_id) else {
            tracing::debug!(mux = %self.mux_id, mux_id, "response for unknown mux id; dropping");
            return;
        };
        resp.id = pending.original_id;

        if self.prompt_in_flight == Some(mux_id) {
            self.prompt_in_flight = None;
            self.sweep_stale_agent_pending(ext);
            ext.on_prompt_settled(&mut MuxCtx::new(self), mux_id, &resp);
        }

        if resp.error.is_none()
            && let Some(handshake) = pending.handshake.as_ref()
        {
            self.apply_successful_handshake(ext, handshake, resp.result.as_ref());
        }

        ext.on_agent_response(&mut MuxCtx::new(self), mux_id, &mut resp);

        if pending.deliver_response {
            let bytes = serde_json::to_vec(&resp).unwrap_or_else(|err| {
                tracing::error!(error = %err, "failed to serialize restored response");
                Vec::new()
            });
            if !bytes.is_empty() {
                self.send_to(&pending.peer_id, Bytes::from(bytes));
            }
        }
    }

    fn apply_successful_handshake(
        &mut self,
        ext: &mut dyn MuxExtension,
        handshake: &HandshakeKind,
        result: Option<&Value>,
    ) {
        match handshake {
            HandshakeKind::Initialize => {
                self.initialize_cache = Some(result.cloned().unwrap_or(Value::Null));
            }
            HandshakeKind::SessionNew => {
                let result = result.cloned().unwrap_or(Value::Null);
                if let Some(session_id) = result.get("sessionId").and_then(Value::as_str) {
                    self.set_canonical_session_id(ext, session_id, false);
                }
                self.session_new_cache = Some(result);
            }
            HandshakeKind::SessionLoad { loaded_session_id } => {
                self.set_canonical_session_id(ext, loaded_session_id, true);
                let result = result
                    .cloned()
                    .unwrap_or_else(|| json!({ "sessionId": loaded_session_id }));
                self.session_new_cache = Some(result);
            }
        }
    }

    fn set_canonical_session_id(
        &mut self,
        ext: &mut dyn MuxExtension,
        session_id: &str,
        via_load: bool,
    ) {
        let old = self.canonical_session_id.clone();
        if old.as_deref() == Some(session_id) {
            if via_load {
                ext.on_canonical_session_id(
                    &mut MuxCtx::new(self),
                    old.as_deref(),
                    session_id,
                    via_load,
                );
            }
            return;
        }
        self.canonical_session_id = Some(session_id.to_string());
        ext.on_canonical_session_id(&mut MuxCtx::new(self), old.as_deref(), session_id, via_load);
    }

    fn sweep_stale_agent_pending(&mut self, ext: &mut dyn MuxExtension) {
        let stale_ids: Vec<Id> = self
            .agent_pending
            .iter()
            .filter(|(_, state)| matches!(state, AgentReqState::InFlight))
            .map(|(id, _)| id.clone())
            .collect();
        for id in stale_ids {
            if let Some(state) = self.agent_pending.get_mut(&id) {
                *state = AgentReqState::Consumed;
            }
            self.pending_permissions
                .retain(|(pending_id, _)| pending_id != &id);
            ext.on_agent_request_resolved(&mut MuxCtx::new(self), &id, ResolvedBy::TurnEnded, None);
        }
    }

    fn handle_attach_request(
        &mut self,
        ext: &mut dyn MuxExtension,
        peer_id: &str,
        req: IncomingRequest,
    ) {
        let params: AttachParams = req
            .params
            .as_ref()
            .map(|v| serde_json::from_value(v.clone()).unwrap_or_default())
            .unwrap_or_default();
        let requested_policy = params.history_policy.unwrap_or_default();
        let effective_policy = match requested_policy {
            HistoryPolicy::AfterMessage => {
                tracing::debug!(
                    mux = %self.mux_id,
                    %peer_id,
                    after_message_id = ?params.after_message_id,
                    "session/attach after_message requested; falling back to full",
                );
                HistoryPolicy::Full
            }
            other => other,
        };
        let resolved_session_id = self
            .canonical_session_id
            .clone()
            .or_else(|| params.session_id.clone().filter(|id| !id.is_empty()))
            .unwrap_or_else(|| self.mux_id.clone());
        if let Some(requested) = params.session_id.as_deref()
            && !requested.is_empty()
            && requested != resolved_session_id
            && requested != self.mux_id
        {
            self.send_error_response(
                peer_id,
                req.id,
                attach::ATTACH_ERR_NOT_FOUND,
                "session not found",
            );
            return;
        }
        let connected_clients = self.connected_clients();
        let history = match effective_policy {
            HistoryPolicy::None => None,
            HistoryPolicy::Full | HistoryPolicy::FullLineage | HistoryPolicy::AfterMessage => {
                Some(self.history_full())
            }
            HistoryPolicy::PendingOnly => Some(self.history_pending_only()),
        };
        let mut result = AttachResult {
            session_id: resolved_session_id,
            client_id: params
                .client_id
                .clone()
                .unwrap_or_else(|| peer_id.to_string()),
            connected_clients,
            history_policy: effective_policy,
            history,
            extra: Default::default(),
        };
        ext.on_attach(&mut MuxCtx::new(self), peer_id, &params, &mut result);
        match serde_json::to_value(result) {
            Ok(value) => self.send_result_response(peer_id, req.id, value),
            Err(err) => {
                tracing::error!(error = %err, "failed to serialize session/attach result");
                self.send_error_response(
                    peer_id,
                    req.id,
                    attach::ATTACH_ERR_UNSUPPORTED,
                    "session/attach serialization failed",
                );
            }
        }
    }

    fn handle_detach_request(
        &mut self,
        ext: &mut dyn MuxExtension,
        peer_id: &str,
        req: IncomingRequest,
    ) {
        let params: DetachParams = req
            .params
            .as_ref()
            .map(|v| serde_json::from_value(v.clone()).unwrap_or_default())
            .unwrap_or_default();
        let resolved_session_id = self
            .canonical_session_id
            .clone()
            .unwrap_or_else(|| self.mux_id.clone());
        if let Some(requested) = params.session_id.as_deref()
            && !requested.is_empty()
            && requested != resolved_session_id
            && requested != self.mux_id
        {
            self.send_error_response(
                peer_id,
                req.id,
                attach::ATTACH_ERR_NOT_FOUND,
                "session not found",
            );
            return;
        }
        let result = DetachResult {
            session_id: resolved_session_id,
            status: "detached",
        };
        match serde_json::to_value(result) {
            Ok(value) => {
                self.send_result_response(peer_id, req.id, value);
                self.detach(ext, peer_id);
            }
            Err(err) => {
                tracing::error!(error = %err, "failed to serialize session/detach result");
                self.send_error_response(
                    peer_id,
                    req.id,
                    attach::ATTACH_ERR_UNSUPPORTED,
                    "session/detach serialization failed",
                );
            }
        }
    }

    fn connected_clients(&self) -> Vec<ConnectedClient> {
        self.subscribers
            .values()
            .map(|s| ConnectedClient {
                client_id: s.peer_id.clone(),
                name: s.peer_name.clone(),
            })
            .collect()
    }

    fn history_full(&self) -> Vec<HistoryEntry> {
        self.replay_log
            .as_ref()
            .into_iter()
            .flat_map(|log| log.iter())
            .filter_map(|entry| history_entry_from_frame(&entry.frame))
            .collect()
    }

    fn history_pending_only(&self) -> Vec<HistoryEntry> {
        self.pending_permissions
            .iter()
            .filter_map(|(_, frame)| history_entry_from_frame(frame))
            .collect()
    }

    fn find_pending_mux_id(&self, peer_id: &str, original_id: &Id) -> Option<u64> {
        self.pending
            .iter()
            .find(|(_, pr)| pr.peer_id == peer_id && &pr.original_id == original_id)
            .map(|(mux_id, _)| *mux_id)
    }

    pub(crate) fn broadcast(&mut self, frame: Bytes) -> bool {
        if let Some(log) = self.replay_log.as_mut() {
            let recorded_at = utc_rfc3339_now();
            let seq = self.next_replay_seq;
            let ext_tag = self.replay_tag;
            log.push_back(ReplayEntry {
                frame: frame.clone(),
                recorded_at: recorded_at.clone(),
                seq,
                ext_tag,
            });
            self.next_replay_seq = self.next_replay_seq.saturating_add(1);
            if let Some(store) = &self.replay_store
                && let Err(err) = store.append(seq, ext_tag, &recorded_at, &frame)
            {
                tracing::warn!(error = %err, "replay store: append failed; frame not persisted");
            }
        }
        for (peer_id, sub) in &self.subscribers {
            if sub.outbound.send(OutMsg::Frame(frame.clone())).is_err() {
                tracing::debug!(%peer_id, "subscriber dropped during broadcast");
            }
        }
        self.subscribers.is_empty()
    }

    fn fanout(&mut self, frame: Bytes) {
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

    pub(crate) fn send_to(&self, peer_id: &str, frame: Bytes) {
        let Some(sub) = self.subscribers.get(peer_id) else {
            tracing::debug!(%peer_id, "target subscriber absent; dropping frame");
            return;
        };
        if sub.outbound.send(OutMsg::Frame(frame)).is_err() {
            tracing::debug!(%peer_id, "target subscriber dropped");
        }
    }

    fn send_result_response(&self, peer_id: &str, id: Id, result: Value) {
        let resp = IncomingResponse {
            jsonrpc: JsonRpcVersion,
            id,
            result: Some(result),
            error: None,
        };
        if let Ok(bytes) = serde_json::to_vec(&resp) {
            self.send_to(peer_id, Bytes::from(bytes));
        }
    }

    fn send_error_response(&self, peer_id: &str, id: Id, code: i64, message: &str) {
        let resp = IncomingResponse {
            jsonrpc: JsonRpcVersion,
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.to_string(),
                data: None,
            }),
        };
        if let Ok(bytes) = serde_json::to_vec(&resp) {
            self.send_to(peer_id, Bytes::from(bytes));
        }
    }

    pub(crate) fn submit_prompt(
        &mut self,
        peer_id: &str,
        params: Value,
        deliver_response: bool,
    ) -> Option<u64> {
        if self.prompt_in_flight.is_some() {
            tracing::debug!(
                mux = %self.mux_id,
                %peer_id,
                "extension submit_prompt while prompt is in flight; dropping",
            );
            return None;
        }
        let mux_id = self.next_mux_id;
        self.next_mux_id = self.next_mux_id.saturating_add(1);
        let req = IncomingRequest {
            jsonrpc: JsonRpcVersion,
            id: Id::Number(mux_id as i64),
            method: "session/prompt".to_string(),
            params: Some(params),
        };
        let bytes = Incoming::Request(req).to_vec().unwrap_or_else(|err| {
            tracing::error!(error = %err, "failed to serialize extension prompt");
            Vec::new()
        });
        if bytes.is_empty() {
            return None;
        }
        self.pending.insert(
            mux_id,
            PendingRequest {
                peer_id: peer_id.to_string(),
                original_id: Id::Number(mux_id as i64),
                handshake: None,
                deliver_response,
            },
        );
        self.prompt_in_flight = Some(mux_id);
        self.agent_outbox.push(bytes);
        Some(mux_id)
    }
}

pub fn spawn_mux(
    subscriber: Subscriber,
    agent: AgentProcess,
    mux_id: String,
    options: MuxOptions,
) -> (MuxHandle, JoinHandle<()>) {
    spawn_mux_with_extension(subscriber, agent, mux_id, options, Box::new(NoopExtension))
}

pub fn spawn_mux_with_extension(
    subscriber: Subscriber,
    mut agent: AgentProcess,
    mux_id: String,
    options: MuxOptions,
    ext: Box<dyn MuxExtension>,
) -> (MuxHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(MUX_QUEUE_CAPACITY);
    if let Some(mut stdout_rx) = agent.take_stdout_rx() {
        let tx_stdout = tx.clone();
        tokio::spawn(async move {
            while let Some(line) = stdout_rx.recv().await {
                if tx_stdout.send(MuxMsg::AgentStdoutLine(line)).await.is_err() {
                    return;
                }
            }
            let _ = tx_stdout.send(MuxMsg::AgentDied).await;
        });
    }
    if let Some(mut stderr_rx) = agent.take_stderr_rx() {
        let tx_stderr = tx.clone();
        tokio::spawn(async move {
            while let Some(line) = stderr_rx.recv().await {
                if tx_stderr.send(MuxMsg::AgentStderrLine(line)).await.is_err() {
                    return;
                }
            }
        });
    }

    let handle = MuxHandle { tx: tx.clone() };
    let actor = tokio::spawn(async move {
        run_mux(subscriber, agent, mux_id, options, ext, tx, rx).await;
    });
    (handle, actor)
}

async fn run_mux(
    subscriber: Subscriber,
    mut agent: AgentProcess,
    mux_id: String,
    options: MuxOptions,
    ext: Box<dyn MuxExtension>,
    tx: mpsc::Sender<MuxMsg>,
    mut rx: mpsc::Receiver<MuxMsg>,
) {
    let mux_ttl = options.mux_ttl;
    let mut mux = Mux {
        core: MuxCore::new(mux_id, options, tx),
        ext,
    };
    if let Err(err) = mux.core.attach(&mut *mux.ext, subscriber) {
        tracing::error!(error = ?err, "failed to attach initial subscriber");
        return;
    }

    let parked_deadline = Instant::now() + Duration::from_secs(365 * 24 * 60 * 60);
    let ttl_sleep = sleep_until(parked_deadline);
    tokio::pin!(ttl_sleep);
    let mut ttl_active = false;

    loop {
        if mux.core.subscribers.is_empty() && !ttl_active {
            ttl_sleep.as_mut().reset(Instant::now() + mux_ttl);
            ttl_active = true;
        } else if !mux.core.subscribers.is_empty() && ttl_active {
            ttl_sleep.as_mut().reset(parked_deadline);
            ttl_active = false;
        }

        tokio::select! {
            _ = &mut ttl_sleep, if ttl_active => {
                tracing::info!(mux = %mux.core.mux_id, "mux ttl expired; shutting down");
                break;
            }
            msg = rx.recv() => {
                let Some(msg) = msg else {
                    tracing::debug!(mux = %mux.core.mux_id, "mux channel closed");
                    break;
                };
                match msg {
                    MuxMsg::Attach { subscriber, ack } => {
                        let result = mux.core.attach(&mut *mux.ext, subscriber);
                        let _ = ack.send(result);
                    }
                    MuxMsg::Detach { peer_id } => mux.core.detach(&mut *mux.ext, &peer_id),
                    MuxMsg::InboundFromSubscriber { peer_id, bytes } => {
                        mux.core.handle_inbound(&mut *mux.ext, &peer_id, bytes);
                    }
                    MuxMsg::AgentStdoutLine(line) => {
                        mux.core.handle_agent_line(&mut *mux.ext, line);
                    }
                    MuxMsg::AgentStderrLine(line) => {
                        let text = String::from_utf8_lossy(&line);
                        tracing::debug!(target: "agent_stderr", mux = %mux.core.mux_id, line = %text);
                    }
                    MuxMsg::AgentDied => {
                        tracing::warn!(mux = %mux.core.mux_id, "agent subprocess exited");
                        mux.core.close_all_subscribers(WS_CLOSE_AGENT_DEAD, "agent subprocess exited");
                        break;
                    }
                    MuxMsg::ExtensionWake(payload) => {
                        mux.ext.on_wake(&mut MuxCtx::new(&mut mux.core), payload);
                    }
                    MuxMsg::Snapshot { ack } => {
                        let _ = ack.send(mux.core.build_snapshot(&*mux.ext, ttl_active));
                    }
                }
                if let Err(err) = drain_agent_outbox(&mut mux.core, &mut agent).await {
                    tracing::error!(mux = %mux.core.mux_id, error = %err, "agent stdin write failed");
                    mux.core.close_all_subscribers(WS_CLOSE_AGENT_DEAD, "agent stdin write failed");
                    return;
                }
            }
        }
    }

    if let Err(err) = agent.shutdown(SHUTDOWN_TIMEOUT).await {
        tracing::warn!(error = %err, "agent shutdown failed");
    }
}

async fn drain_agent_outbox(core: &mut MuxCore, agent: &mut AgentProcess) -> anyhow::Result<()> {
    let writes = std::mem::take(&mut core.agent_outbox);
    for frame in writes {
        agent.send(&frame).await?;
    }
    Ok(())
}

fn handshake_kind(req: &IncomingRequest) -> Option<HandshakeKind> {
    match req.method.as_str() {
        "initialize" => Some(HandshakeKind::Initialize),
        "session/new" => Some(HandshakeKind::SessionNew),
        "session/load" => req
            .params
            .as_ref()
            .and_then(|params| params.get("sessionId"))
            .and_then(Value::as_str)
            .map(|loaded_session_id| HandshakeKind::SessionLoad {
                loaded_session_id: loaded_session_id.to_string(),
            }),
        _ => None,
    }
}

fn hydrate_replay_store(
    mux_id: &str,
    replay_store: Option<&Arc<ReplayStore>>,
) -> (Option<VecDeque<ReplayEntry>>, Option<RoomReplayStore>, u64) {
    let Some(store) = replay_store else {
        return (Some(VecDeque::new()), None, 1);
    };
    let mut room_store = match store.open_room(mux_id) {
        Ok(store) => store,
        Err(err) => {
            tracing::warn!(
                mux = %mux_id,
                error = %err,
                "replay store: open failed; continuing with in-memory replay only",
            );
            return (Some(VecDeque::new()), None, 1);
        }
    };
    let loaded = room_store.take_loaded();
    let next_replay_seq = loaded
        .iter()
        .map(|record| record.seq)
        .max()
        .unwrap_or(0)
        .saturating_add(1);
    let replay_log = loaded
        .into_iter()
        .map(|record| ReplayEntry {
            frame: record.frame_bytes(),
            recorded_at: record.recorded_at,
            seq: record.seq,
            ext_tag: record.segment_id,
        })
        .collect();
    (Some(replay_log), Some(room_store), next_replay_seq)
}

fn parse_cancel_request_id(params: Option<&Value>) -> Option<Id> {
    let id_value = params.and_then(|v| v.get("requestId"))?.clone();
    let id: Id = serde_json::from_value(id_value).ok()?;
    match id {
        Id::Null => None,
        other => Some(other),
    }
}

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

fn build_client_tool_blocked_response(id: Id, method: &str) -> Vec<u8> {
    let resp = IncomingResponse {
        jsonrpc: JsonRpcVersion,
        id,
        result: None,
        error: Some(JsonRpcError {
            code: CLIENT_TOOL_BLOCKED_ERROR_CODE,
            message: format!("client tool request blocked by acp-mux policy: {method}"),
            data: Some(json!({
                "reason": "client_tool_blocked",
                "method": method,
                "policy": "block",
            })),
        }),
    };
    serde_json::to_vec(&resp).expect("client-tool blocked response is always serializable")
}

fn sanitize_initialize_client_capabilities(req: &mut IncomingRequest) {
    let Some(Value::Object(params)) = req.params.as_mut() else {
        return;
    };
    let Some(Value::Object(client_capabilities)) = params.get_mut("clientCapabilities") else {
        return;
    };
    client_capabilities.remove("fs");
    client_capabilities.remove("terminal");
}

fn history_entry_from_frame(frame: &Bytes) -> Option<HistoryEntry> {
    let value: Value = serde_json::from_slice(frame).ok()?;
    let object = value.as_object()?;
    let method = object.get("method")?.as_str()?.to_string();
    let params = object.get("params").cloned();
    Some(HistoryEntry { method, params })
}

fn utc_rfc3339_now() -> String {
    system_time_to_rfc3339_utc(SystemTime::now())
}

fn system_time_to_rfc3339_utc(time: SystemTime) -> String {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let total_secs = duration.as_secs() as i64;
    let days = total_secs.div_euclid(86_400);
    let secs_of_day = total_secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3_600;
    let minute = (secs_of_day % 3_600) / 60;
    let second = secs_of_day % 60;
    format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{nanos:09}Z",
        nanos = duration.subsec_nanos(),
    )
}

// Howard Hinnant's civil-from-days algorithm, with day 0 = 1970-01-01.
fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    (year, month as u32, day as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_core() -> (MuxCore, Box<dyn MuxExtension>) {
        let (tx, _rx) = mpsc::channel(1);
        (
            MuxCore::new(
                "mux1".to_string(),
                MuxOptions {
                    replay_policy: ReplayTurns::Unbounded,
                    mux_ttl: Duration::from_secs(60),
                    client_tool_policy: ClientToolPolicy::default(),
                    agent_cwd: "/tmp".to_string(),
                    replay_store: None,
                },
                tx,
            ),
            Box::new(NoopExtension),
        )
    }

    fn test_subscriber(peer_id: &str) -> (Subscriber, mpsc::UnboundedReceiver<OutMsg>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Subscriber::new(peer_id.to_string(), None, None, false, tx),
            rx,
        )
    }

    fn recv_json(rx: &mut mpsc::UnboundedReceiver<OutMsg>) -> Value {
        let msg = rx.try_recv().expect("outbound frame");
        let OutMsg::Frame(bytes) = msg else {
            panic!("expected frame");
        };
        serde_json::from_slice(&bytes).expect("json frame")
    }

    #[test]
    fn attach_returns_core_roster_without_extension_meta() {
        let (mut inner, mut ext) = test_core();
        let (sub, mut rx) = test_subscriber("p1");
        inner.attach(&mut *ext, sub).unwrap();

        inner.handle_inbound(
            &mut *ext,
            "p1",
            br#"{"jsonrpc":"2.0","id":1,"method":"session/attach","params":{"clientId":"client-1","historyPolicy":"full"}}"#
                .to_vec(),
        );
        assert!(inner.agent_outbox.is_empty());

        let response = recv_json(&mut rx);
        assert_eq!(response["id"], 1);
        let result = response.get("result").expect("attach result");
        assert_eq!(result["sessionId"], "mux1");
        assert_eq!(result["clientId"], "client-1");
        assert_eq!(result["connectedClients"][0]["clientId"], "p1");
        assert!(result.get("_meta").is_none());
    }

    #[test]
    fn agent_initiated_request_is_first_writer_wins() {
        let (mut inner, mut ext) = test_core();
        let (sub1, mut rx1) = test_subscriber("p1");
        let (sub2, mut rx2) = test_subscriber("p2");
        inner.attach(&mut *ext, sub1).unwrap();
        inner.attach(&mut *ext, sub2).unwrap();

        inner.handle_agent_line(
            &mut *ext,
            br#"{"jsonrpc":"2.0","id":"perm-1","method":"session/request_permission","params":{"toolName":"edit"}}"#
                .to_vec(),
        );
        let _ = recv_json(&mut rx1);
        let _ = recv_json(&mut rx2);

        inner.handle_inbound(
            &mut *ext,
            "p1",
            br#"{"jsonrpc":"2.0","id":"perm-1","result":{"outcome":"approved"}}"#.to_vec(),
        );
        assert_eq!(inner.agent_outbox.len(), 1);

        inner.agent_outbox.clear();
        inner.handle_inbound(
            &mut *ext,
            "p2",
            br#"{"jsonrpc":"2.0","id":"perm-1","result":{"outcome":"approved"}}"#.to_vec(),
        );
        assert!(inner.agent_outbox.is_empty());
    }

    #[test]
    fn concurrent_prompt_is_rejected_locally() {
        let (mut inner, mut ext) = test_core();
        let (sub1, _rx1) = test_subscriber("p1");
        let (sub2, mut rx2) = test_subscriber("p2");
        inner.attach(&mut *ext, sub1).unwrap();
        inner.attach(&mut *ext, sub2).unwrap();

        inner.handle_inbound(
            &mut *ext,
            "p1",
            br#"{"jsonrpc":"2.0","id":10,"method":"session/prompt","params":{"sessionId":"s1","prompt":[{"type":"text","text":"hi"}]}}"#
                .to_vec(),
        );
        assert_eq!(inner.agent_outbox.len(), 1);
        inner.agent_outbox.clear();

        inner.handle_inbound(
            &mut *ext,
            "p2",
            br#"{"jsonrpc":"2.0","id":11,"method":"session/prompt","params":{"sessionId":"s1","prompt":[{"type":"text","text":"again"}]}}"#
                .to_vec(),
        );
        assert!(inner.agent_outbox.is_empty());

        let response = recv_json(&mut rx2);
        assert_eq!(response["id"], 11);
        assert_eq!(response["error"]["code"], SESSION_BUSY_ERROR_CODE);
    }
}
