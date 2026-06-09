//! RFD #533-inspired `session/attach` and `session/detach` request/response shapes.
//!
//! rooms handles these proxy-local methods itself. They are logical
//! ACP handshakes layered on top of the existing WebSocket attach query:
//! the transport peer already exists, and `session/attach` returns optional
//! replay history shaped by `historyPolicy` and `_meta.rooms.replayOrder`.
//! rooms-specific peer metadata lives
//! under `result._meta.rooms` so the top-level result remains standards-shaped.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::protocol::rooms::{EndReason, SegmentId};

pub const METHOD_ATTACH: &str = "session/attach";
pub const METHOD_DETACH: &str = "session/detach";

pub const ATTACH_ERR_NOT_FOUND: i64 = -32001;
pub const ATTACH_ERR_UNSUPPORTED: i64 = -32003;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HistoryPolicy {
    /// Frames recorded in the current segment plus pre-segment
    /// bootstrap. May also carry `rooms/turn_started`,
    /// `rooms/turn_complete`, and `rooms/turn_cancelled` frames from a
    /// prior segment when their `roomsTurnId` brackets the active
    /// segment — that keeps mid-rotation turns properly bracketed for
    /// clients without leaking pre-rotation agent chunks. Preserves
    /// the v0.1.x default for clients that haven't opted into lineage.
    #[default]
    Full,
    /// All segments' frames concatenated in `replaySeq` order.
    /// Surfaces earlier segment history that `Full` would hide.
    FullLineage,
    PendingOnly,
    None,
    /// Depends on stable ACP message IDs; rooms currently accepts it and
    /// falls back to `Full` when it cannot resolve `afterMessageId`.
    AfterMessage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ReplayOrder {
    #[default]
    Chronological,
    NewestTurnFirst,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HistoryDelivery {
    #[default]
    Response,
    Stream,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AttachParams {
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub history_policy: Option<HistoryPolicy>,
    #[serde(default)]
    pub after_message_id: Option<String>,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub client_info: Option<ClientInfo>,
    #[serde(default, rename = "_meta")]
    pub meta: Option<AttachParamsMeta>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AttachParamsMeta {
    #[serde(default)]
    pub rooms: Option<AttachParamsRoomsMeta>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AttachParamsRoomsMeta {
    #[serde(default)]
    pub replay_order: Option<ReplayOrder>,
    #[serde(default)]
    pub history_delivery: Option<HistoryDelivery>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientInfo {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachResult {
    pub session_id: String,
    pub client_id: String,
    pub history_policy: HistoryPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history: Option<Vec<HistoryEntry>>,
    #[serde(rename = "_meta")]
    pub meta: AttachMeta,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachMeta {
    pub rooms: AttachRoomsMeta,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachRoomsMeta {
    pub connected_clients: Vec<ConnectedClient>,
    pub applied_replay_order: ReplayOrder,
    pub applied_history_delivery: HistoryDelivery,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<AttachSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectedClient {
    pub client_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachSnapshot {
    pub connected_clients: Vec<ConnectedClient>,
    pub self_peer: ConnectedClient,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_turn: Option<AttachActiveTurn>,
    pub queue: Vec<AttachQueueItem>,
    pub pending_permissions: Vec<AttachPendingPermission>,
    pub replay_boundary_seq: u64,
    pub replay_generation: u64,
    /// Lineage summary. Always populated so even `historyPolicy: full`
    /// (current-segment-only) clients can tell that earlier segments
    /// exist and request a richer history if desired.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub segments: Vec<SegmentSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_segment_id: Option<SegmentId>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SegmentSummary {
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

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachActiveTurn {
    pub rooms_turn_id: String,
    pub peer_id: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachQueueItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_item_id: Option<String>,
    pub peer_id: String,
    pub kind: String,
    pub status: &'static str,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachPendingPermission {
    pub request_id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HistoryEntry {
    pub method: String,
    pub params: Value,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DetachParams {
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DetachResult {
    pub session_id: String,
    pub status: &'static str,
}
