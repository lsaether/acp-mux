//! RFD #533-inspired `session/attach` and `session/detach` request/response shapes.
//!
//! amux handles these proxy-local methods itself. They are logical
//! ACP handshakes layered on top of the existing WebSocket attach query:
//! the transport peer already exists, and `session/attach` returns optional
//! replay history shaped by `historyPolicy` and `_meta.amux.replayOrder`.
//! amux-specific peer metadata lives
//! under `result._meta.amux` so the top-level result remains standards-shaped.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::protocol::amux::{EndReason, HermesProvenance, SegmentId};

pub const METHOD_ATTACH: &str = "session/attach";
pub const METHOD_DETACH: &str = "session/detach";

pub const ATTACH_ERR_NOT_FOUND: i64 = -32001;
pub const ATTACH_ERR_UNSUPPORTED: i64 = -32003;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HistoryPolicy {
    /// Frames recorded in the current segment plus pre-segment
    /// bootstrap. May also carry `amux/turn_started`,
    /// `amux/turn_complete`, and `amux/turn_cancelled` frames from a
    /// prior segment when their `amuxTurnId` brackets the active
    /// segment — that keeps mid-rotation turns properly bracketed for
    /// clients without leaking pre-rotation agent chunks. Preserves
    /// the v0.1.x default for clients that haven't opted into lineage.
    #[default]
    Full,
    /// All segments' frames concatenated in `replaySeq` order.
    /// Surfaces pre-compaction history that `Full` would hide.
    FullLineage,
    PendingOnly,
    None,
    /// Depends on stable ACP message IDs; amux currently accepts it and
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
    pub amux: Option<AttachParamsAmuxMeta>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AttachParamsAmuxMeta {
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
    pub amux: AttachAmuxMeta,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachAmuxMeta {
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
    /// Mux-observed Hermes compactions for this room. Always present so
    /// clients can tell `0` (no compaction yet) from a missing field.
    pub compression_count: u64,
    /// Most recent compaction lifecycle, if any compaction has been
    /// observed. Omitted on a fresh room.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction: Option<AttachCompactionSummary>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachCompactionSummary {
    pub active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_completed_at: Option<String>,
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
    #[serde(skip_serializing_if = "HermesProvenance::is_empty")]
    pub provenance: HermesProvenance,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachActiveTurn {
    pub amux_turn_id: String,
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
