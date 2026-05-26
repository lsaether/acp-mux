//! RFD #533 proxy-owned `session/update` variants.
//!
//! These frames are emitted alongside the existing `amux/*` notifications so
//! ACP-aware clients can consume standard `session/update` siblings while
//! existing amux clients keep routing on the amux namespace.

use serde::Serialize;
use serde_json::Value;

const METHOD_SESSION_UPDATE: &str = "session/update";

const UPDATE_PROMPT_RECEIVED: &str = "prompt_received";
const UPDATE_TURN_COMPLETE: &str = "turn_complete";
const UPDATE_PERMISSION_RESOLVED: &str = "permission_resolved";
const UPDATE_CLIENT_DISCONNECTED: &str = "client_disconnected";

#[derive(Serialize)]
struct Frame<'a, P: Serialize> {
    jsonrpc: &'a str,
    method: &'a str,
    params: P,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Params<'a, U: Serialize> {
    session_id: &'a str,
    update: U,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientRef<'a> {
    pub client_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PromptReceived<'a> {
    #[serde(rename = "type")]
    ty: &'a str,
    prompt: &'a Value,
    sent_by: ClientRef<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TurnComplete<'a> {
    #[serde(rename = "type")]
    ty: &'a str,
    stop_reason: &'a Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PermissionResolved<'a> {
    #[serde(rename = "type")]
    ty: &'a str,
    request_id: &'a Value,
    resolved_by: ClientRef<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    chosen_option_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<&'a Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a Value>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClientDisconnected<'a> {
    #[serde(rename = "type")]
    ty: &'a str,
    client: ClientRef<'a>,
}

fn encode<P: Serialize>(params: P) -> Vec<u8> {
    serde_json::to_vec(&Frame {
        jsonrpc: "2.0",
        method: METHOD_SESSION_UPDATE,
        params,
    })
    .expect("session/update frame is serializable")
}

pub fn prompt_received(
    session_id: &str,
    prompt: &Value,
    client_id: &str,
    client_name: Option<&str>,
) -> Vec<u8> {
    encode(Params {
        session_id,
        update: PromptReceived {
            ty: UPDATE_PROMPT_RECEIVED,
            prompt,
            sent_by: ClientRef {
                client_id,
                name: client_name,
            },
        },
    })
}

pub fn turn_complete(session_id: &str, stop_reason: &Value) -> Vec<u8> {
    encode(Params {
        session_id,
        update: TurnComplete {
            ty: UPDATE_TURN_COMPLETE,
            stop_reason,
        },
    })
}

pub fn permission_resolved(
    session_id: &str,
    request_id: &Value,
    resolved_by_id: &str,
    resolved_by_name: Option<&str>,
    chosen_option_id: Option<&str>,
    result: Option<&Value>,
    error: Option<&Value>,
) -> Vec<u8> {
    encode(Params {
        session_id,
        update: PermissionResolved {
            ty: UPDATE_PERMISSION_RESOLVED,
            request_id,
            resolved_by: ClientRef {
                client_id: resolved_by_id,
                name: resolved_by_name,
            },
            chosen_option_id,
            result,
            error,
        },
    })
}

pub fn client_disconnected(
    session_id: &str,
    client_id: &str,
    client_name: Option<&str>,
) -> Vec<u8> {
    encode(Params {
        session_id,
        update: ClientDisconnected {
            ty: UPDATE_CLIENT_DISCONNECTED,
            client: ClientRef {
                client_id,
                name: client_name,
            },
        },
    })
}
