mod attach_views;
mod permissions;
mod presence;
mod queue;
mod segments;
mod turns;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use acp_mux::attach::{AttachParams, AttachResult, HistoryEntry, HistoryPolicy};
use acp_mux::extension::{Disposition, MuxCtx, MuxExtension, NotifyDisposition, ResolvedBy};
use acp_mux::jsonrpc::{
    Id, IncomingNotification, IncomingRequest, IncomingResponse, JsonRpcError, JsonRpcVersion,
};
use acp_mux::mux::ReplayView;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::protocol::rooms::{self, RoomsTurnId, EndReason, SegmentId};

use attach_views::{
    attach_meta_str, inject_replay_metadata, newest_turn_first_history, replay_stream_phases,
    schedule_wake_payload,
};
use queue::{
    RequestTrace, build_session_cancel, inject_request_trace_metadata, text_from_text_only_prompt,
};

#[derive(Debug, Clone)]
pub struct RoomsOptions {
    pub meta_propagate: bool,
    pub emit_segment_frames: bool,
}

impl Default for RoomsOptions {
    fn default() -> Self {
        Self {
            meta_propagate: false,
            emit_segment_frames: true,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RoomsRequestData {
    pub(crate) rooms_turn_id: Option<RoomsTurnId>,
    pub(crate) queue_item_id: Option<String>,
    pub(crate) decorate_session_list: bool,
}

#[derive(Debug, Clone)]
pub(crate) enum QueuedPromptKind {
    Prompt,
    Queue,
    HardSteer { supersedes_turn_id: RoomsTurnId },
}

#[derive(Debug, Clone)]
pub(crate) struct QueuedPrompt {
    pub(crate) queue_item_id: Option<String>,
    pub(crate) peer_id: String,
    pub(crate) session_id: String,
    pub(crate) prompt_text: String,
    pub(crate) kind: QueuedPromptKind,
}

#[derive(Debug, Default)]
pub struct SessionListMetadataIndex {
    inner: std::sync::RwLock<HashMap<String, SessionListRoomsMetadata>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionListRoomsMetadata {
    pub room_id: String,
    pub subscriber_count: usize,
    pub driving_subscriber: Option<String>,
}

impl SessionListMetadataIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, acp_session_id: &str) -> Option<SessionListRoomsMetadata> {
        self.inner
            .read()
            .expect("session list metadata index poisoned")
            .get(acp_session_id)
            .cloned()
    }

    fn upsert(&self, acp_session_id: &str, metadata: SessionListRoomsMetadata) {
        self.inner
            .write()
            .expect("session list metadata index poisoned")
            .insert(acp_session_id.to_string(), metadata);
    }

    fn remove_if_room(&self, acp_session_id: &str, room_id: &str) {
        let mut index = self
            .inner
            .write()
            .expect("session list metadata index poisoned");
        if index
            .get(acp_session_id)
            .is_some_and(|meta| meta.room_id == room_id)
        {
            index.remove(acp_session_id);
        }
    }
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
pub struct Segment {
    pub id: SegmentId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acp_session_id: Option<String>,
    pub opened_at: String,
    pub opened_replay_seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed_replay_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_reason: Option<EndReason>,
}

impl Segment {
    fn open(id: SegmentId, acp_session_id: Option<String>, opened_replay_seq: u64) -> Self {
        Self {
            id,
            acp_session_id,
            opened_at: utc_rfc3339_now(),
            opened_replay_seq,
            closed_at: None,
            closed_replay_seq: None,
            end_reason: None,
        }
    }
}

pub struct RoomsExtension {
    pub(crate) options: RoomsOptions,
    pub(crate) active_rooms_turn_id: Option<RoomsTurnId>,
    pub(crate) active_turn_session_id: Option<String>,
    pub(crate) active_turn_prompt_text: Option<String>,
    pub(crate) next_rooms_turn_id: u64,
    pub(crate) per_request: HashMap<u64, RoomsRequestData>,
    pub(crate) queued_prompts: VecDeque<QueuedPrompt>,
    pub(crate) next_queue_item_id: u64,
    pub(crate) active_segment_id: Option<SegmentId>,
    pub(crate) segments: Vec<Segment>,
    pub(crate) next_segment_id: u64,
    pub(crate) replay_generation: u64,
    pub(crate) last_replay_reset: Option<ReplayResetSnapshot>,
    pub(crate) driving_subscriber_peer_id: Option<String>,
    pub(crate) session_list_index: Arc<SessionListMetadataIndex>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum WakePayload {
    ReplayStream {
        phases: Vec<WakeReplayPhase>,
    },
    PendingPermissions {
        peer_id: String,
        frames: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct WakeReplayPhase {
    pub(super) peer_id: String,
    pub(super) phase: String,
    pub(super) replay_order: String,
    pub(super) replay_generation: u64,
    pub(super) replay_boundary_seq: u64,
    pub(super) frames: Vec<String>,
}

impl RoomsExtension {
    pub fn new(options: RoomsOptions, session_list_index: Arc<SessionListMetadataIndex>) -> Self {
        Self {
            options,
            active_rooms_turn_id: None,
            active_turn_session_id: None,
            active_turn_prompt_text: None,
            next_rooms_turn_id: 1,
            per_request: HashMap::new(),
            queued_prompts: VecDeque::new(),
            next_queue_item_id: 1,
            active_segment_id: None,
            segments: Vec::new(),
            next_segment_id: 1,
            replay_generation: 0,
            last_replay_reset: None,
            driving_subscriber_peer_id: None,
            session_list_index,
        }
    }
}

pub(super) const MAX_MUX_QUEUE_PROMPTS: usize = 6;
const SESSION_BUSY_ERROR_CODE: i64 = -32001;
pub(super) const NO_ACTIVE_TURN_ERROR_CODE: i64 = -32002;
pub(super) const QUEUE_FULL_ERROR_CODE: i64 = -32003;
pub(super) const QUEUE_ITEM_NOT_FOUND_ERROR_CODE: i64 = -32004;
pub(super) const INVALID_PARAMS_ERROR_CODE: i64 = -32602;
pub(super) const SESSION_CANCEL_METHOD: &str = "session/cancel";

impl RoomsExtension {
    fn room_id<'a>(&self, ctx: &'a MuxCtx<'_>) -> &'a str {
        ctx.mux_id()
    }

    pub(super) fn send_error_response(
        &self,
        ctx: &mut MuxCtx,
        peer_id: &str,
        id: Id,
        code: i64,
        message: &str,
    ) {
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
            ctx.send_to(peer_id, Bytes::from(bytes));
        }
    }

    pub(super) fn send_result_response(&self, ctx: &mut MuxCtx, peer_id: &str, id: Id, result: Value) {
        let resp = IncomingResponse {
            jsonrpc: JsonRpcVersion,
            id,
            result: Some(result),
            error: None,
        };
        if let Ok(bytes) = serde_json::to_vec(&resp) {
            ctx.send_to(peer_id, Bytes::from(bytes));
        }
    }
}

impl MuxExtension for RoomsExtension {
    fn on_subscriber_request(
        &mut self,
        ctx: &mut MuxCtx,
        peer_id: &str,
        req: &mut IncomingRequest,
    ) -> Disposition {
        match req.method.as_str() {
            rooms::METHOD_STEER_ACTIVE_TURN => self.handle_rooms_steer_request(ctx, peer_id, req),
            rooms::METHOD_QUEUE_PROMPT => self.handle_rooms_queue_prompt_request(ctx, peer_id, req),
            rooms::METHOD_UNQUEUE_PROMPT => {
                self.handle_rooms_unqueue_prompt_request(ctx, peer_id, req)
            }
            "session/prompt" if ctx.prompt_in_flight().is_some() => {
                let held_by = ctx
                    .prompt_in_flight()
                    .and_then(|mux_id| ctx.pending_peer(mux_id));
                let room_id = ctx.mux_id().to_string();
                ctx.broadcast(rooms::session_busy(&room_id, true, held_by));
                Disposition::Reject {
                    code: SESSION_BUSY_ERROR_CODE,
                    message: "session busy: another turn is in flight".to_string(),
                }
            }
            _ => {
                if req.method != "initialize" {
                    self.note_driving_subscriber(ctx, peer_id);
                }
                Disposition::Forward
            }
        }
    }

    fn on_request_translating(
        &mut self,
        ctx: &mut MuxCtx,
        peer_id: &str,
        mux_id: u64,
        req: &mut IncomingRequest,
    ) {
        let is_prompt = req.method == "session/prompt";
        let turn_id = if is_prompt {
            let turn_id = RoomsTurnId(self.next_rooms_turn_id);
            self.next_rooms_turn_id += 1;
            Some(turn_id)
        } else {
            None
        };
        self.per_request.insert(
            mux_id,
            RoomsRequestData {
                rooms_turn_id: turn_id,
                queue_item_id: None,
                decorate_session_list: req.method == "session/list",
            },
        );
        if self.options.meta_propagate {
            let (peer_name, role) = ctx
                .subscriber(peer_id)
                .map(|s| (s.peer_name.as_deref(), s.role.as_deref()))
                .unwrap_or((None, None));
            inject_request_trace_metadata(
                req,
                RequestTrace {
                    peer_id,
                    peer_name,
                    role,
                    mux_id,
                    rooms_turn_id: turn_id,
                },
            );
        }
    }

    fn on_request_forwarded(
        &mut self,
        ctx: &mut MuxCtx,
        peer_id: &str,
        mux_id: u64,
        req: &IncomingRequest,
    ) {
        let Some(data) = self.per_request.get(&mux_id) else {
            return;
        };
        let Some(turn_id) = data.rooms_turn_id else {
            return;
        };
        self.active_rooms_turn_id = Some(turn_id);
        self.active_turn_session_id = req
            .params
            .as_ref()
            .and_then(|p| p.get("sessionId"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| ctx.canonical_session_id().map(str::to_string));
        self.active_turn_prompt_text = req
            .params
            .as_ref()
            .and_then(|p| p.get("prompt"))
            .and_then(text_from_text_only_prompt);
        self.emit_turn_started(ctx, peer_id, turn_id, req.params.as_ref(), None);
    }

    fn on_subscriber_notification(
        &mut self,
        ctx: &mut MuxCtx,
        peer_id: &str,
        notif: &IncomingNotification,
    ) -> NotifyDisposition {
        if notif.method != rooms::METHOD_CANCEL_ACTIVE_TURN {
            return NotifyDisposition::Passthrough;
        }
        let Some(rooms_turn_id) = self.active_rooms_turn_id else {
            return NotifyDisposition::Handled;
        };
        let Some(active_session_id) = self.active_turn_session_id.clone() else {
            return NotifyDisposition::Handled;
        };
        let original_driver = ctx
            .prompt_in_flight()
            .and_then(|mux_id| ctx.pending_peer(mux_id).map(str::to_string))
            .unwrap_or_else(|| peer_id.to_string());
        let reason = notif
            .params
            .as_ref()
            .and_then(|v| v.get("reason"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let room_id = ctx.mux_id().to_string();
        ctx.broadcast(rooms::turn_cancelled(
            &room_id,
            rooms_turn_id,
            peer_id,
            &original_driver,
            reason.as_deref(),
        ));
        ctx.send_to_agent(build_session_cancel(&active_session_id));
        NotifyDisposition::Handled
    }

    fn on_agent_notification(&mut self, ctx: &mut MuxCtx, notif: &IncomingNotification) {
        let Some(params) = notif.params.as_ref() else {
            return;
        };
        let Some(acp_session_id) = params.get("sessionId").and_then(Value::as_str) else {
            return;
        };
        let Some(active) = self.active_segment() else {
            return;
        };
        if active.acp_session_id.as_deref() == Some(acp_session_id) {
            return;
        }
        self.rotate_segment(
            ctx,
            Some(acp_session_id.to_string()),
            EndReason::AcpSessionIdChanged,
        );
    }

    fn on_agent_request(&mut self, ctx: &mut MuxCtx, id: &Id, req: &IncomingRequest) {
        let request_id_value = serde_json::to_value(id).unwrap_or(Value::Null);
        let room_id = ctx.mux_id().to_string();
        ctx.broadcast(rooms::agent_request_opened(
            &room_id,
            &request_id_value,
            &req.method,
            req.params.as_ref(),
            self.active_rooms_turn_id,
        ));
    }

    fn on_agent_response(&mut self, _ctx: &mut MuxCtx, mux_id: u64, resp: &mut IncomingResponse) {
        if self
            .per_request
            .get(&mux_id)
            .is_some_and(|data| data.decorate_session_list)
        {
            self.decorate_session_list_response(resp);
        }
    }

    fn on_prompt_settled(&mut self, ctx: &mut MuxCtx, mux_id: u64, resp: &IncomingResponse) {
        let data = self.per_request.remove(&mux_id);
        self.active_turn_session_id = None;
        self.active_turn_prompt_text = None;
        let turn_id = self
            .active_rooms_turn_id
            .take()
            .or(data.as_ref().and_then(|d| d.rooms_turn_id));
        if let Some(turn_id) = turn_id {
            self.emit_turn_complete(ctx, turn_id, resp.result.as_ref());
            if let Some(queue_item_id) = data.and_then(|d| d.queue_item_id) {
                let stop_reason = resp
                    .result
                    .as_ref()
                    .and_then(|r| r.get("stopReason"))
                    .cloned()
                    .unwrap_or(Value::Null);
                let room_id = ctx.mux_id().to_string();
                ctx.broadcast(rooms::queue_item_completed(
                    &room_id,
                    &queue_item_id,
                    turn_id,
                    &stop_reason,
                ));
            }
        }
        self.submit_next_queued_prompt(ctx);
    }

    fn on_agent_request_resolved(
        &mut self,
        ctx: &mut MuxCtx,
        id: &Id,
        by: ResolvedBy,
        resp: Option<&IncomingResponse>,
    ) {
        let request_id_value = serde_json::to_value(id).unwrap_or(Value::Null);
        let resolved_by = match by {
            ResolvedBy::Peer(peer_id) => peer_id,
            ResolvedBy::AgentCancelled => rooms::RESOLVED_BY_AGENT_CANCELLED.to_string(),
            ResolvedBy::TurnEnded => "mux:turn-ended".to_string(),
        };
        let error_value = resp
            .and_then(|r| r.error.as_ref())
            .and_then(|e| serde_json::to_value(e).ok());
        let result = resp.and_then(|r| r.result.as_ref());
        let room_id = ctx.mux_id().to_string();
        ctx.broadcast(rooms::agent_request_resolved(
            &room_id,
            &request_id_value,
            &resolved_by,
            result,
            error_value.as_ref(),
        ));
    }

    fn on_canonical_session_id(
        &mut self,
        ctx: &mut MuxCtx,
        _old: Option<&str>,
        new: &str,
        via_load: bool,
    ) {
        let active_already_matches = self
            .active_segment()
            .and_then(|segment| segment.acp_session_id.as_deref())
            == Some(new);
        if !active_already_matches {
            self.rotate_segment(
                ctx,
                Some(new.to_string()),
                if via_load {
                    EndReason::SessionLoad
                } else {
                    EndReason::AcpSessionIdChanged
                },
            );
        }
        if via_load {
            self.replay_generation += 1;
            let (dropped_frame_count, retained_frame_count) = self.replay_retention_counts(ctx);
            self.last_replay_reset = Some(ReplayResetSnapshot {
                loaded_session_id: new.to_string(),
                replay_generation: self.replay_generation,
                dropped_frame_count,
                retained_frame_count,
            });
        }
        self.publish_session_list_metadata(ctx);
    }

    fn on_subscriber_attaching(
        &mut self,
        ctx: &mut MuxCtx,
        newcomer: &acp_mux::subscriber::Subscriber,
    ) {
        let room_id = ctx.mux_id().to_string();
        ctx.broadcast(rooms::peer_joined(
            &room_id,
            &newcomer.peer_id,
            newcomer.peer_name.as_deref(),
            newcomer.role.as_deref(),
        ));
    }

    fn on_subscriber_attached(&mut self, ctx: &mut MuxCtx, peer_id: &str) {
        let room_id = ctx.mux_id().to_string();
        ctx.send_to(
            peer_id,
            Bytes::from(rooms::session_context(&room_id, ctx.agent_cwd())),
        );
        self.publish_session_list_metadata(ctx);
    }

    fn on_subscriber_detached(&mut self, ctx: &mut MuxCtx, peer_id: &str) {
        let room_id = ctx.mux_id().to_string();
        ctx.broadcast(rooms::peer_left(&room_id, peer_id));
        let orphaned_queue_item_ids: Vec<String> = self
            .queued_prompts
            .iter()
            .filter(|item| item.peer_id == peer_id)
            .filter_map(|item| item.queue_item_id.clone())
            .collect();
        for queue_item_id in orphaned_queue_item_ids {
            ctx.broadcast(rooms::queue_item_orphaned(&room_id, &queue_item_id, peer_id));
        }
        if self.driving_subscriber_peer_id.as_deref() == Some(peer_id) {
            self.driving_subscriber_peer_id = None;
        }
        if ctx.subscribers().next().is_none() {
            self.clear_session_list_metadata(ctx);
        } else {
            self.publish_session_list_metadata(ctx);
        }
    }

    fn on_attach(
        &mut self,
        ctx: &mut MuxCtx,
        peer_id: &str,
        params: &AttachParams,
        result: &mut AttachResult,
    ) {
        let replay_order = attach_meta_str(params, "replayOrder").unwrap_or("chronological");
        let requested_delivery = attach_meta_str(params, "historyDelivery").unwrap_or("response");
        let applied_delivery = if matches!(requested_delivery, "stream")
            && !matches!(result.history_policy, acp_mux::attach::HistoryPolicy::None)
        {
            "stream"
        } else {
            "response"
        };
        let connected_clients = std::mem::take(&mut result.connected_clients);
        let replay_boundary_seq = ctx
            .replay_entries()
            .last()
            .map(|entry| entry.seq)
            .unwrap_or(0);
        let self_peer = connected_clients
            .iter()
            .find(|client| client.client_id == peer_id)
            .cloned()
            .unwrap_or(acp_mux::attach::ConnectedClient {
                client_id: peer_id.to_string(),
                name: ctx.subscriber(peer_id).and_then(|s| s.peer_name.clone()),
            });
        let pending_permissions: Vec<Value> = ctx
            .pending_permissions()
            .iter()
            .filter_map(|(id, frame)| {
                let request_id = serde_json::to_value(id).ok()?;
                let value: Value = serde_json::from_slice(frame).ok()?;
                Some(json!({
                    "requestId": request_id,
                    "toolName": value.pointer("/params/toolCall/title").cloned(),
                    "summary": value.pointer("/params/toolCall/title").cloned(),
                }))
            })
            .collect();
        let snapshot = json!({
            "connectedClients": connected_clients,
            "selfPeer": self_peer,
            "activeTurn": self.active_rooms_turn_id.and_then(|turn_id| {
                ctx.prompt_in_flight().and_then(|mux_id| {
                    ctx.pending_peer(mux_id).map(|peer_id| json!({
                        "roomsTurnId": turn_id.formatted(),
                        "peerId": peer_id,
                    }))
                })
            }),
            "queue": self.queued_prompts.iter().map(|item| json!({
                "queueItemId": item.queue_item_id,
                "peerId": item.peer_id,
                "kind": match item.kind {
                    QueuedPromptKind::Prompt => "prompt",
                    QueuedPromptKind::Queue => "queue",
                    QueuedPromptKind::HardSteer { .. } => "hard_steer",
                },
                "status": "queued",
            })).collect::<Vec<_>>(),
            "pendingPermissions": pending_permissions,
            "replayBoundarySeq": replay_boundary_seq,
            "replayGeneration": self.replay_generation,
            "segments": self.segments,
            "activeSegmentId": self.active_segment_id,
        });
        result.extra.insert(
            "_meta".to_string(),
            json!({
                "rooms": {
                    "connectedClients": snapshot["connectedClients"].clone(),
                    "appliedReplayOrder": replay_order,
                    "appliedHistoryDelivery": applied_delivery,
                    "snapshot": snapshot,
                }
            }),
        );

        match result.history_policy {
            HistoryPolicy::Full => {
                result.history = Some(self.replay_history(ctx, false));
            }
            HistoryPolicy::FullLineage => {
                result.history = Some(self.replay_history(ctx, true));
            }
            HistoryPolicy::PendingOnly | HistoryPolicy::None | HistoryPolicy::AfterMessage => {}
        }

        if let Some(history) = result.history.as_mut()
            && replay_order == "newest_turn_first"
        {
            *history = newest_turn_first_history(std::mem::take(history));
        }

        if applied_delivery == "stream" {
            let history = result.history.take().unwrap_or_default();
            let phases = replay_stream_phases(
                peer_id,
                replay_order,
                self.replay_generation,
                replay_boundary_seq,
                history,
            );
            schedule_wake_payload(
                ctx,
                std::time::Duration::from_millis(1),
                WakePayload::ReplayStream { phases },
            );
        }

        if matches!(result.history_policy, HistoryPolicy::PendingOnly) {
            let frames = ctx
                .pending_permissions()
                .iter()
                .filter_map(|(_, frame)| std::str::from_utf8(frame).ok().map(str::to_string))
                .collect::<Vec<_>>();
            if !frames.is_empty() {
                schedule_wake_payload(
                    ctx,
                    std::time::Duration::from_millis(1),
                    WakePayload::PendingPermissions {
                        peer_id: peer_id.to_string(),
                        frames,
                    },
                );
            }
        }
    }

    fn replay_frame(&mut self, ctx: &mut MuxCtx, entry: ReplayView<'_>) -> Option<Bytes> {
        if !self.should_include_replay_entry(ctx, entry.clone()) {
            return None;
        }
        Some(inject_replay_metadata(
            entry.frame,
            entry.recorded_at,
            entry.seq,
        ))
    }

    fn on_wake(&mut self, ctx: &mut MuxCtx, payload: Vec<u8>) {
        let Ok(plan) = serde_json::from_slice::<WakePayload>(&payload) else {
            return;
        };
        match plan {
            WakePayload::ReplayStream { mut phases } => {
                let Some(phase) = phases.first().cloned() else {
                    return;
                };
                phases.remove(0);
                self.send_replay_phase(ctx, &phase);
                if !phases.is_empty() {
                    schedule_wake_payload(
                        ctx,
                        std::time::Duration::from_millis(50),
                        WakePayload::ReplayStream { phases },
                    );
                }
            }
            WakePayload::PendingPermissions { peer_id, frames } => {
                for frame in frames {
                    ctx.send_to(&peer_id, Bytes::from(frame));
                }
            }
        }
    }

    fn debug_snapshot(&self, ctx: &MuxCtx) -> Value {
        json!({
            "activeRoomsTurnId": self.active_rooms_turn_id.map(|t| t.formatted()),
            "drivingSubscriber": self.driving_subscriber_peer_id,
            "replayGeneration": self.replay_generation,
            "lastReplayReset": self.last_replay_reset,
            "replayLogUpdateFramesByAcpSessionId": self.replay_update_counts_by_session(ctx),
            "nextRoomsTurnId": self.next_rooms_turn_id,
            "segments": self.segments,
            "activeSegmentId": self.active_segment_id,
        })
    }
}

pub(super) fn object_params(req: &mut IncomingRequest) -> Option<&mut Map<String, Value>> {
    let params = req.params.get_or_insert_with(|| Value::Object(Map::new()));
    match params {
        Value::Object(map) => Some(map),
        _ => None,
    }
}

pub(super) fn object_field<'a>(
    object: &'a mut Map<String, Value>,
    key: &str,
) -> Option<&'a mut Map<String, Value>> {
    let value = object
        .entry(key.to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    match value {
        Value::Object(map) => Some(map),
        _ => None,
    }
}

pub(super) fn next_replay_seq(ctx: &MuxCtx) -> u64 {
    ctx.replay_entries()
        .last()
        .map(|entry| entry.seq.saturating_add(1))
        .unwrap_or(1)
}

pub(super) fn utc_rfc3339_now() -> String {
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
