use serde_json::{Map, Value};

use crate::transport::{ATTACH_REQUEST_ID, InboundMessage};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConnectionStatus {
    #[default]
    Disconnected,
    Connecting,
    Attached,
    Replaying,
    Live,
    Closed,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RoomState {
    pub connection_status: ConnectionStatus,
    pub room_id: Option<String>,
    pub session_id: Option<String>,
    pub active_segment_id: Option<String>,
    pub peers: Vec<Peer>,
    pub transcript: Vec<TranscriptItem>,
    pub active_turn: Option<ActiveTurn>,
    pub queue: Vec<QueueItem>,
    pub pending_permissions: Vec<PermissionRequest>,
    pub replay: Option<ReplayStatus>,
    pub debug_frames: Vec<DebugFrame>,
    pub errors: Vec<String>,
    pub busy: bool,
    pub busy_held_by: Option<String>,
}

impl RoomState {
    pub fn set_connection_status(&mut self, status: ConnectionStatus) {
        self.connection_status = status;
    }

    pub fn apply_inbound(&mut self, message: &InboundMessage) -> Result<(), String> {
        match message {
            InboundMessage::Frame { raw, .. } => self.apply_frame(raw),
            InboundMessage::Error(error) => {
                self.errors.push(error.clone());
                self.connection_status = ConnectionStatus::Error;
                Ok(())
            }
            InboundMessage::Closed => {
                self.connection_status = ConnectionStatus::Closed;
                Ok(())
            }
        }
    }

    pub fn apply_frame(&mut self, frame: &Value) -> Result<(), String> {
        self.apply_frame_with_origin(frame, false)
    }

    fn apply_frame_with_origin(&mut self, frame: &Value, replayed: bool) -> Result<(), String> {
        let object = frame
            .as_object()
            .ok_or_else(|| "JSON-RPC frame must be an object".to_string())?;

        if let Some(error) = object.get("error") {
            self.errors.push(error.to_string());
            if is_attach_response_object(object) {
                self.connection_status = ConnectionStatus::Error;
            }
            return Ok(());
        }

        if let Some(result) = object.get("result") {
            if is_attach_response_object(object) && looks_like_attach_result(result) {
                self.apply_attach_result(result)?;
            }
            return Ok(());
        }

        let method = object
            .get("method")
            .and_then(Value::as_str)
            .ok_or_else(|| "JSON-RPC frame is missing method".to_string())?;
        let params = object.get("params").unwrap_or(&Value::Null);
        let frame_replayed = replayed
            || (self.replay.is_some()
                && !matches!(method, "rooms/replay_started" | "rooms/replay_complete"));

        match method {
            "rooms/replay_started" => self.apply_replay_started(params),
            "rooms/replay_complete" => self.apply_replay_complete(params),
            "rooms/turn_started" => {
                self.apply_turn_started(params, frame_replayed, !frame_replayed)
            }
            "session/update" => self.apply_session_update(params, frame_replayed, !frame_replayed),
            "session/request_permission" => {
                self.apply_permission_request(frame, params, !frame_replayed)
            }
            _ if frame_replayed => self.debug_frames.push(DebugFrame {
                method: method.to_string(),
                replayed: true,
            }),
            "rooms/peer_joined" => self.apply_peer_joined(params),
            "rooms/peer_left" => self.apply_peer_left(params),
            "rooms/turn_complete" => self.apply_turn_complete(params),
            "rooms/turn_cancelled" => self.apply_turn_cancelled(params),
            "rooms/session_busy" => self.apply_session_busy(params),
            "rooms/queue_item_added" => self.apply_queue_item_added(params),
            "rooms/queue_item_submitted" => {
                self.apply_queue_transition(params, QueueItemStatus::Submitted)
            }
            "rooms/queue_item_completed" => {
                self.apply_queue_transition(params, QueueItemStatus::Completed)
            }
            "rooms/queue_item_removed" => {
                self.apply_queue_transition(params, QueueItemStatus::Removed)
            }
            "rooms/queue_item_orphaned" => {
                self.apply_queue_transition(params, QueueItemStatus::Orphaned)
            }
            "rooms/segment_started" => self.apply_segment_started(params),
            "rooms/agent_request_opened" => self.apply_agent_request_opened(params),
            "rooms/agent_request_resolved" => self.apply_agent_request_resolved(params),
            _ => self.debug_frames.push(DebugFrame {
                method: method.to_string(),
                replayed: false,
            }),
        }

        Ok(())
    }

    fn apply_attach_result(&mut self, result: &Value) -> Result<(), String> {
        if let Some(session_id) = optional_string_field(result, &["sessionId", "session_id"]) {
            self.session_id = Some(session_id);
        }

        if let Some(rooms) = result.get("_meta").and_then(|meta| meta.get("rooms")) {
            if let Some(peers) = rooms.get("connectedClients").and_then(Value::as_array) {
                self.peers.clear();
                for peer in peers {
                    self.upsert_peer(peer_from_value(peer));
                }
            }
            if let Some(snapshot) = rooms.get("snapshot") {
                self.apply_attach_snapshot(snapshot);
            }
        }

        if let Some(history) = result.get("history").and_then(Value::as_array) {
            for frame in history {
                self.apply_frame_with_origin(frame, true)?;
            }
        }

        self.connection_status = ConnectionStatus::Attached;
        Ok(())
    }

    fn apply_attach_snapshot(&mut self, snapshot: &Value) {
        self.update_room_id(snapshot);
        self.active_segment_id =
            optional_string_field(snapshot, &["activeSegmentId", "active_segment_id"])
                .or_else(|| self.active_segment_id.clone());

        if let Some(peers) = snapshot.get("connectedClients").and_then(Value::as_array) {
            self.peers.clear();
            for peer in peers {
                self.upsert_peer(peer_from_value(peer));
            }
        }
        if let Some(self_peer) = snapshot.get("selfPeer") {
            self.upsert_peer(peer_from_value(self_peer));
        }

        if let Some(active_turn) = snapshot.get("activeTurn").filter(|value| value.is_object()) {
            let peer_id = string_field(active_turn, &["peerId", "peer_id"]);
            self.active_turn = Some(ActiveTurn {
                turn_id: string_field(active_turn, &["roomsTurnId", "rooms_turn_id"]),
                peer_name: self.peer_name(&peer_id),
                peer_id,
                text: text_from_params(active_turn),
                cancelled: false,
            });
        } else {
            self.active_turn = None;
        }

        if let Some(queue) = snapshot.get("queue").and_then(Value::as_array) {
            self.queue.clear();
            for item in queue {
                let queue_item_id = string_field(item, &["queueItemId", "queue_item_id"]);
                if queue_item_id.is_empty() {
                    continue;
                }
                let peer_id = string_field(item, &["peerId", "peer_id"]);
                self.queue.push(QueueItem {
                    queue_item_id,
                    room_id: self.room_id.clone(),
                    peer_name: self.peer_name(&peer_id),
                    peer_id,
                    text: text_from_params(item),
                    status: queue_status_from_params(item),
                    turn_id: optional_string_field(item, &["roomsTurnId", "rooms_turn_id"]),
                });
            }
        }

        if let Some(permissions) = snapshot.get("pendingPermissions").and_then(Value::as_array) {
            self.pending_permissions.clear();
            for permission in permissions {
                let request_id = permission
                    .get("requestId")
                    .map(json_id_to_string)
                    .unwrap_or_default();
                self.upsert_permission(PermissionRequest {
                    response_id: permission
                        .get("requestId")
                        .cloned()
                        .unwrap_or_else(|| Value::String(request_id.clone())),
                    request_id,
                    session_id: optional_string_field(permission, &["sessionId", "session_id"]),
                    title: permission_title(permission),
                    options: permission_options(permission),
                    actionable: false,
                });
            }
        }
    }

    fn apply_peer_joined(&mut self, params: &Value) {
        self.update_room_id(params);
        self.upsert_peer(Peer {
            peer_id: string_field(params, &["peerId", "peer_id"]),
            peer_name: optional_string_field(params, &["peerName", "peer_name"]),
            role: optional_string_field(params, &["role"]),
        });
    }

    fn apply_peer_left(&mut self, params: &Value) {
        self.update_room_id(params);
        let peer_id = string_field(params, &["peerId", "peer_id"]);
        self.peers.retain(|peer| peer.peer_id != peer_id);
    }

    fn apply_turn_started(&mut self, params: &Value, replayed: bool, mutate_current: bool) {
        self.update_room_id(params);
        let turn_id = string_field(params, &["roomsTurnId", "rooms_turn_id"]);
        let peer_id = string_field(params, &["peerId", "peer_id"]);
        let peer_name = optional_string_field(params, &["peerName", "peer_name"]);
        let text = text_from_params(params);

        if mutate_current {
            self.upsert_peer(Peer {
                peer_id: peer_id.clone(),
                peer_name: peer_name.clone(),
                role: optional_string_field(params, &["role"]),
            });
            self.active_turn = Some(ActiveTurn {
                turn_id: turn_id.clone(),
                peer_id: peer_id.clone(),
                peer_name: peer_name.clone(),
                text: text.clone(),
                cancelled: false,
            });
        }
        self.transcript.push(TranscriptItem {
            kind: TranscriptKind::Prompt,
            method: "rooms/turn_started".to_string(),
            room_id: self.room_id.clone(),
            session_id: self.session_id.clone(),
            turn_id: Some(turn_id),
            peer_id: Some(peer_id),
            peer_name,
            text,
            replayed,
        });
    }

    fn apply_turn_complete(&mut self, params: &Value) {
        self.update_room_id(params);
        let turn_id = string_field(params, &["roomsTurnId", "rooms_turn_id"]);
        if self
            .active_turn
            .as_ref()
            .is_some_and(|turn| turn.turn_id == turn_id)
        {
            self.active_turn = None;
        }
        self.busy = false;
        self.busy_held_by = None;
    }

    fn apply_turn_cancelled(&mut self, params: &Value) {
        self.update_room_id(params);
        let turn_id = string_field(params, &["roomsTurnId", "rooms_turn_id"]);
        if let Some(turn) = &mut self.active_turn
            && turn.turn_id == turn_id
        {
            turn.cancelled = true;
        }
    }

    fn apply_session_busy(&mut self, params: &Value) {
        self.update_room_id(params);
        self.busy = params.get("busy").and_then(Value::as_bool).unwrap_or(true);
        self.busy_held_by = optional_string_field(params, &["heldBy", "held_by"]);
    }

    fn apply_queue_item_added(&mut self, params: &Value) {
        self.update_room_id(params);
        let peer_id = string_field(params, &["peerId", "peer_id", "queuedBy", "queued_by"]);
        let peer_name = optional_string_field(params, &["peerName", "peer_name"]);
        self.upsert_peer(Peer {
            peer_id: peer_id.clone(),
            peer_name: peer_name.clone(),
            role: optional_string_field(params, &["role"]),
        });
        self.upsert_queue_item(QueueItem {
            queue_item_id: string_field(params, &["queueItemId", "queue_item_id"]),
            room_id: self.room_id.clone(),
            peer_id,
            peer_name,
            text: text_from_params(params),
            status: queue_status_from_params(params),
            turn_id: optional_string_field(params, &["roomsTurnId", "rooms_turn_id"]),
        });
    }

    fn apply_queue_transition(&mut self, params: &Value, status: QueueItemStatus) {
        self.update_room_id(params);
        let queue_item_id = string_field(params, &["queueItemId", "queue_item_id"]);
        let turn_id = optional_string_field(params, &["roomsTurnId", "rooms_turn_id"]);
        if let Some(item) = self
            .queue
            .iter_mut()
            .find(|item| item.queue_item_id == queue_item_id)
        {
            item.status = status;
            if turn_id.is_some() {
                item.turn_id = turn_id;
            }
        } else if !queue_item_id.is_empty() {
            self.queue.push(QueueItem {
                queue_item_id,
                room_id: self.room_id.clone(),
                peer_id: String::new(),
                peer_name: None,
                text: String::new(),
                status,
                turn_id,
            });
        }
    }

    fn apply_replay_started(&mut self, params: &Value) {
        self.update_room_id(params);
        self.replay = Some(ReplayStatus {
            phase: string_field(params, &["phase"]),
            replay_order: string_field(params, &["replayOrder", "replay_order"]),
            generation: u64_field(params, &["generation"]),
            replay_boundary_seq: u64_field(params, &["replayBoundarySeq", "replay_boundary_seq"]),
            frame_count: usize_field(params, &["frameCount", "frame_count"]),
        });
        self.connection_status = ConnectionStatus::Replaying;
    }

    fn apply_replay_complete(&mut self, params: &Value) {
        self.update_room_id(params);
        self.replay = None;
        self.connection_status = ConnectionStatus::Live;
    }

    fn apply_segment_started(&mut self, params: &Value) {
        self.update_room_id(params);
        if let Some(segment_id) = optional_string_field(params, &["segmentId", "segment_id"]) {
            self.active_segment_id = Some(segment_id);
        }
        if let Some(session_id) = optional_string_field(params, &["acpSessionId", "acp_session_id"])
        {
            self.session_id = Some(session_id);
        }
    }

    fn apply_agent_request_opened(&mut self, params: &Value) {
        self.update_room_id(params);
        if optional_string_field(params, &["requestMethod", "request_method"]).as_deref()
            != Some("session/request_permission")
        {
            return;
        }
        let request_id = params
            .get("requestId")
            .map(json_id_to_string)
            .unwrap_or_default();
        let response_id = params
            .get("requestId")
            .cloned()
            .unwrap_or_else(|| Value::String(request_id.clone()));
        let request_params = params.get("requestParams").unwrap_or(&Value::Null);
        self.upsert_permission(PermissionRequest {
            response_id,
            request_id,
            session_id: optional_string_field(request_params, &["sessionId", "session_id"]),
            title: permission_title(request_params),
            options: permission_options(request_params),
            actionable: false,
        });
    }

    fn apply_agent_request_resolved(&mut self, params: &Value) {
        self.update_room_id(params);
        let request_id = params.get("requestId").cloned().unwrap_or(Value::Null);
        self.pending_permissions
            .retain(|permission| permission.response_id != request_id);
    }

    fn apply_permission_request(&mut self, frame: &Value, params: &Value, actionable: bool) {
        self.update_room_id(params);
        let request_id = frame.get("id").map(json_id_to_string).unwrap_or_default();
        self.upsert_permission(PermissionRequest {
            response_id: frame
                .get("id")
                .cloned()
                .unwrap_or_else(|| Value::String(request_id.clone())),
            request_id,
            session_id: optional_string_field(params, &["sessionId", "session_id"]),
            title: permission_title(params),
            options: permission_options(params),
            actionable,
        });
    }

    fn apply_session_update(&mut self, params: &Value, replayed: bool, mutate_current: bool) {
        let frame_session_id = optional_string_field(params, &["sessionId", "session_id"]);
        if mutate_current && let Some(session_id) = &frame_session_id {
            self.session_id = Some(session_id.clone());
        }
        self.transcript.push(TranscriptItem {
            kind: TranscriptKind::AgentUpdate,
            method: "session/update".to_string(),
            room_id: self.room_id.clone(),
            session_id: frame_session_id.or_else(|| self.session_id.clone()),
            turn_id: if mutate_current {
                self.active_turn.as_ref().map(|turn| turn.turn_id.clone())
            } else {
                None
            },
            peer_id: None,
            peer_name: None,
            text: text_from_params(params),
            replayed,
        });
    }

    fn update_room_id(&mut self, value: &Value) {
        if let Some(room_id) = optional_string_field(value, &["roomId", "room_id", "room"]) {
            self.room_id = Some(room_id);
        }
    }

    fn peer_name(&self, peer_id: &str) -> Option<String> {
        self.peers
            .iter()
            .find(|peer| peer.peer_id == peer_id)
            .and_then(|peer| peer.peer_name.clone())
    }

    fn upsert_peer(&mut self, peer: Peer) {
        if peer.peer_id.is_empty() {
            return;
        }
        if let Some(existing) = self
            .peers
            .iter_mut()
            .find(|existing| existing.peer_id == peer.peer_id)
        {
            if peer.peer_name.is_some() {
                existing.peer_name = peer.peer_name;
            }
            if peer.role.is_some() {
                existing.role = peer.role;
            }
        } else {
            self.peers.push(peer);
        }
    }

    fn upsert_queue_item(&mut self, item: QueueItem) {
        if let Some(existing) = self
            .queue
            .iter_mut()
            .find(|existing| existing.queue_item_id == item.queue_item_id)
        {
            *existing = item;
        } else if !item.queue_item_id.is_empty() {
            self.queue.push(item);
        }
    }

    fn upsert_permission(&mut self, permission: PermissionRequest) {
        if let Some(existing) = self
            .pending_permissions
            .iter_mut()
            .find(|existing| existing.response_id == permission.response_id)
        {
            if permission.actionable || !existing.actionable {
                *existing = permission;
            }
        } else if !permission.request_id.is_empty() {
            self.pending_permissions.push(permission);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub peer_id: String,
    pub peer_name: Option<String>,
    pub role: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranscriptKind {
    Prompt,
    AgentUpdate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptItem {
    pub kind: TranscriptKind,
    pub method: String,
    pub room_id: Option<String>,
    pub session_id: Option<String>,
    pub turn_id: Option<String>,
    pub peer_id: Option<String>,
    pub peer_name: Option<String>,
    pub text: String,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveTurn {
    pub turn_id: String,
    pub peer_id: String,
    pub peer_name: Option<String>,
    pub text: String,
    pub cancelled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueItemStatus {
    Queued,
    Submitted,
    Completed,
    Removed,
    Orphaned,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueItem {
    pub queue_item_id: String,
    pub room_id: Option<String>,
    pub peer_id: String,
    pub peer_name: Option<String>,
    pub text: String,
    pub status: QueueItemStatus,
    pub turn_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionRequest {
    pub request_id: String,
    pub response_id: Value,
    pub session_id: Option<String>,
    pub title: Option<String>,
    pub options: Vec<String>,
    pub actionable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayStatus {
    pub phase: String,
    pub replay_order: String,
    pub generation: u64,
    pub replay_boundary_seq: u64,
    pub frame_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DebugFrame {
    pub method: String,
    pub replayed: bool,
}

fn peer_from_value(value: &Value) -> Peer {
    Peer {
        peer_id: string_field(value, &["peerId", "peer_id", "clientId", "client_id"]),
        peer_name: optional_string_field(value, &["peerName", "peer_name", "name"]),
        role: optional_string_field(value, &["role"]),
    }
}

fn is_attach_response_object(object: &Map<String, Value>) -> bool {
    object.get("id").and_then(Value::as_str) == Some(ATTACH_REQUEST_ID)
}

fn looks_like_attach_result(result: &Value) -> bool {
    result.get("sessionId").is_some()
        || result.get("session_id").is_some()
        || result.get("clientId").is_some()
        || result.get("client_id").is_some()
        || result.get("history").is_some()
        || result
            .get("_meta")
            .and_then(|meta| meta.get("rooms"))
            .is_some()
}

fn string_field(value: &Value, names: &[&str]) -> String {
    optional_string_field(value, names).unwrap_or_default()
}

fn optional_string_field(value: &Value, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(Value::as_str))
        .map(ToString::to_string)
}

fn u64_field(value: &Value, names: &[&str]) -> u64 {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(Value::as_u64))
        .unwrap_or_default()
}

fn usize_field(value: &Value, names: &[&str]) -> usize {
    u64_field(value, names) as usize
}

fn text_from_params(params: &Value) -> String {
    if let Some(text) = optional_string_field(params, &["text", "prompt"]) {
        return text;
    }
    for key in ["content", "prompt"] {
        if let Some(text) = text_from_content(params.get(key)) {
            return text;
        }
    }
    if let Some(update) = params.get("update") {
        if let Some(text) = optional_string_field(update, &["text"]) {
            return text;
        }
        if let Some(text) = text_from_content(update.get("content")) {
            return text;
        }
    }
    String::new()
}

fn text_from_content(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::Array(items) => {
            let parts = items
                .iter()
                .filter_map(|item| item.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>();
            if parts.is_empty() {
                None
            } else {
                Some(parts.join("\n"))
            }
        }
        Value::Object(object) => object
            .get("text")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        _ => None,
    }
}

fn queue_status_from_params(params: &Value) -> QueueItemStatus {
    match optional_string_field(params, &["status"]).as_deref() {
        Some("submitted") => QueueItemStatus::Submitted,
        Some("completed") => QueueItemStatus::Completed,
        Some("removed") => QueueItemStatus::Removed,
        Some("orphaned") => QueueItemStatus::Orphaned,
        _ => QueueItemStatus::Queued,
    }
}

fn permission_title(params: &Value) -> Option<String> {
    params
        .get("toolCall")
        .and_then(|tool| optional_string_field(tool, &["title", "name", "kind"]))
        .or_else(|| {
            optional_string_field(
                params,
                &["summary", "title", "name", "toolName", "tool_name"],
            )
        })
}

fn permission_options(params: &Value) -> Vec<String> {
    params
        .get("options")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|option| optional_string_field(option, &["optionId", "option_id", "id"]))
        .collect()
}

fn json_id_to_string(value: &Value) -> String {
    if let Some(text) = value.as_str() {
        text.to_string()
    } else {
        value.to_string()
    }
}
