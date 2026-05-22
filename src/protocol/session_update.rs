//! RFD #533 `session/update` variants emitted by the proxy.
//!
//! These are the in-band, ACP-namespace siblings of the `amux/*`
//! out-of-band frames in [`crate::protocol::amux`]. Both are emitted
//! together: amux clients route off `amux/*`; RFD-aware clients route
//! off `session/update`. Either is sufficient on its own.
//!
//! The agent itself also emits `session/update` frames (for streaming
//! agent-message chunks, tool calls, etc.) — the `update.type` field
//! values defined here (`prompt_received`, `turn_complete`,
//! `permission_resolved`, `client_disconnected`) are new, proxy-only
//! variants that do not collide with anything an unwrapped agent
//! emits.
//!
//! `sessionId` in these frames is the ACP session id (from the cached
//! `session/new` response), not the amux proxy session id used in the
//! `amux/*` namespace. Builders return `None` when no ACP session id
//! is available yet (e.g. before `session/new` has resolved).

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
    .expect("session/update frame is always serializable")
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

/// `chosen_option_id` is extracted from `result.outcome.optionId` when the
/// winning reply is an ACP permission selection; callers pass `None` if
/// the result shape doesn't match (the proxy stays envelope-only and
/// doesn't enforce permission semantics).
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn parse(bytes: &[u8]) -> Value {
        serde_json::from_slice(bytes).expect("frame is JSON")
    }

    #[test]
    fn prompt_received_shape() {
        let prompt = json!([{"type": "text", "text": "hi"}]);
        let v = parse(&prompt_received(
            "sess-abc",
            &prompt,
            "phone-1",
            Some("phone"),
        ));
        assert_eq!(v["method"], json!("session/update"));
        assert_eq!(v["params"]["sessionId"], json!("sess-abc"));
        assert_eq!(v["params"]["update"]["type"], json!("prompt_received"));
        assert_eq!(v["params"]["update"]["prompt"], prompt);
        assert_eq!(
            v["params"]["update"]["sentBy"]["clientId"],
            json!("phone-1")
        );
        assert_eq!(v["params"]["update"]["sentBy"]["name"], json!("phone"));
    }

    #[test]
    fn turn_complete_shape() {
        let reason = json!("end_turn");
        let v = parse(&turn_complete("sess-abc", &reason));
        assert_eq!(v["params"]["update"]["type"], json!("turn_complete"));
        assert_eq!(v["params"]["update"]["stopReason"], json!("end_turn"));
    }

    #[test]
    fn permission_resolved_shape() {
        let req_id = json!(10001);
        let result = json!({"outcome": {"outcome": "selected", "optionId": "allow_once"}});
        let v = parse(&permission_resolved(
            "sess-abc",
            &req_id,
            "alice",
            Some("Alice"),
            Some("allow_once"),
            Some(&result),
            None,
        ));
        assert_eq!(v["params"]["update"]["type"], json!("permission_resolved"));
        assert_eq!(v["params"]["update"]["requestId"], req_id);
        assert_eq!(
            v["params"]["update"]["resolvedBy"]["clientId"],
            json!("alice")
        );
        assert_eq!(v["params"]["update"]["chosenOptionId"], json!("allow_once"));
    }

    #[test]
    fn client_disconnected_shape() {
        let v = parse(&client_disconnected("sess-abc", "phone-1", Some("phone")));
        assert_eq!(v["params"]["update"]["type"], json!("client_disconnected"));
        assert_eq!(
            v["params"]["update"]["client"]["clientId"],
            json!("phone-1")
        );
        assert_eq!(v["params"]["update"]["client"]["name"], json!("phone"));
    }
}
