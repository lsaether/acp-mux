//! RFD #533 `session/attach` and `session/detach` request/response shapes.
//!
//! The proxy intercepts these methods locally — they never reach the
//! agent subprocess. `session/attach` is a logical-attach handshake on
//! top of the underlying WebSocket attach (the transport-level join
//! that already happened during the WS upgrade with peer_id/peer_name
//! query params). It returns the peer roster and optionally a history
//! snapshot shaped by `historyPolicy`.
//!
//! `session/detach` triggers a graceful subscriber close — the WS-out
//! task is signalled to send the response back and then close the
//! socket.
//!
//! The proxy still emits `amux/*` metadata and the dual `session/update`
//! variants for all peers (`prompt_received`, `turn_complete`,
//! `permission_resolved`, `client_disconnected`) regardless of whether
//! a client used `session/attach` or relied purely on the WS query
//! params — the handshake is informational, not gating.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const METHOD_ATTACH: &str = "session/attach";
pub const METHOD_DETACH: &str = "session/detach";

/// RFD-defined error codes returned by attach/detach handling.
/// `-32001` is reused for session-not-found *and* turn busy in amux —
/// the two never apply to the same method, so the overlap is safe.
pub const ATTACH_ERR_NOT_FOUND: i64 = -32001;
pub const ATTACH_ERR_UNAUTHORIZED: i64 = -32002;
pub const ATTACH_ERR_UNSUPPORTED: i64 = -32003;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HistoryPolicy {
    #[default]
    Full,
    PendingOnly,
    None,
    /// Requires the Message ID RFD; amux falls back to Full when used.
    AfterMessage,
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
    /// Self-assigned client identifier. amux already has `peer_id` from
    /// the WS query; when both are present we prefer `peer_id` (the
    /// transport-level identifier the rest of the proxy is keyed on)
    /// and echo this value back unchanged for clients that track it.
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub client_info: Option<ClientInfo>,
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
    pub connected_clients: Vec<ConnectedClient>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history: Option<Vec<HistoryEntry>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectedClient {
    pub client_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// History entry — a raw JSON-RPC frame the proxy previously broadcast.
/// amux is envelope-only and does not translate broadcast frames into
/// the RFD's typed entries (`{type: "prompt", ...}`, `{type:
/// "permission_request", ...}`); each entry instead carries the
/// notification's `method` and `params` verbatim. RFD-aware clients
/// can interpret `session/update` and `session/request_permission`
/// frames using the standard ACP shape.
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
