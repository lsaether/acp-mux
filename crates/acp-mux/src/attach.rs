use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub const METHOD_ATTACH: &str = "session/attach";
pub const METHOD_DETACH: &str = "session/detach";

pub const ATTACH_ERR_NOT_FOUND: i64 = -32001;
pub const ATTACH_ERR_UNSUPPORTED: i64 = -32003;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HistoryPolicy {
    #[default]
    Full,
    FullLineage,
    PendingOnly,
    None,
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
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub client_info: Option<ClientInfo>,
    #[serde(default, rename = "_meta")]
    pub meta: Option<Value>,
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
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub connected_clients: Vec<ConnectedClient>,
    pub history_policy: HistoryPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history: Option<Vec<HistoryEntry>>,
    #[serde(flatten, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
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
