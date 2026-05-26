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

pub const METHOD_ATTACH: &str = "session/attach";
pub const METHOD_DETACH: &str = "session/detach";

pub const ATTACH_ERR_NOT_FOUND: i64 = -32001;
pub const ATTACH_ERR_UNSUPPORTED: i64 = -32003;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HistoryPolicy {
    #[default]
    Full,
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
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectedClient {
    pub client_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
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
