//! JSON-RPC 2.0 envelope types.
//!
//! Per the multiplex design contract, only the envelope is parsed; everything
//! past `{id, method, params, result, error}` flows as `serde_json::Value`.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// JSON-RPC 2.0 request id. The spec permits number, string, or null
/// (null is discouraged but accepted). Notifications omit the field entirely;
/// this type only models the field when it is present.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Id {
    Number(i64),
    String(String),
    Null,
}

/// A frame received from either a subscriber or the agent subprocess.
///
/// Discrimination is by field presence:
/// - `id` + `method`  → request
/// - `method` only    → notification
/// - `id` + (`result` | `error`) → response
#[derive(Debug, Clone, PartialEq)]
pub enum Incoming {
    Request(IncomingRequest),
    Notification(IncomingNotification),
    Response(IncomingResponse),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IncomingRequest {
    pub jsonrpc: JsonRpcVersion,
    pub id: Id,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IncomingNotification {
    pub jsonrpc: JsonRpcVersion,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IncomingResponse {
    pub jsonrpc: JsonRpcVersion,
    pub id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("jsonrpc error {code}: {message}")]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// Marker type that enforces the literal `"2.0"` value during ser/de.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct JsonRpcVersion;

impl Serialize for JsonRpcVersion {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str("2.0")
    }
}

impl<'de> Deserialize<'de> for JsonRpcVersion {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = String::deserialize(d)?;
        if v == "2.0" {
            Ok(JsonRpcVersion)
        } else {
            Err(serde::de::Error::custom(format!(
                "unsupported jsonrpc version {v:?}, expected \"2.0\""
            )))
        }
    }
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("json decode: {0}")]
    Json(#[from] serde_json::Error),
    #[error("frame is not a JSON object")]
    NotAnObject,
    #[error("frame matches no JSON-RPC envelope shape: {0}")]
    Ambiguous(String),
}

impl Incoming {
    /// Parse a single JSON-RPC frame from raw bytes.
    pub fn parse(bytes: &[u8]) -> Result<Self, ParseError> {
        let v: Value = serde_json::from_slice(bytes)?;
        Self::from_value(v)
    }

    /// Parse from an already-decoded `serde_json::Value`.
    pub fn from_value(v: Value) -> Result<Self, ParseError> {
        let obj = match &v {
            Value::Object(o) => o,
            _ => return Err(ParseError::NotAnObject),
        };

        let has_id = obj.contains_key("id");
        let has_method = obj.contains_key("method");
        let has_result = obj.contains_key("result");
        let has_error = obj.contains_key("error");

        match (has_id, has_method, has_result || has_error) {
            (true, true, _) => Ok(Incoming::Request(serde_json::from_value(v)?)),
            (false, true, _) => Ok(Incoming::Notification(serde_json::from_value(v)?)),
            (true, false, true) => Ok(Incoming::Response(serde_json::from_value(v)?)),
            _ => Err(ParseError::Ambiguous(format!(
                "id={has_id} method={has_method} result_or_error={}",
                has_result || has_error
            ))),
        }
    }

    /// Serialize back to JSON bytes (NDJSON-friendly: no trailing newline).
    pub fn to_vec(&self) -> Result<Vec<u8>, serde_json::Error> {
        match self {
            Incoming::Request(r) => serde_json::to_vec(r),
            Incoming::Notification(n) => serde_json::to_vec(n),
            Incoming::Response(r) => serde_json::to_vec(r),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn round_trip(v: Value) -> Value {
        let inc = Incoming::from_value(v.clone()).expect("parse");
        let bytes = inc.to_vec().expect("serialize");
        serde_json::from_slice::<Value>(&bytes).expect("re-decode")
    }

    #[test]
    fn request_with_numeric_id() {
        let v = json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "session/prompt",
            "params": { "prompt": [{ "type": "text", "text": "hi" }] }
        });
        let inc = Incoming::from_value(v.clone()).unwrap();
        assert!(matches!(inc, Incoming::Request(_)));
        assert_eq!(round_trip(v.clone()), v);
    }

    #[test]
    fn request_with_string_id() {
        let v = json!({
            "jsonrpc": "2.0",
            "id": "req-42",
            "method": "initialize",
            "params": { "protocolVersion": 1 }
        });
        let inc = Incoming::from_value(v.clone()).unwrap();
        match inc {
            Incoming::Request(r) => assert_eq!(r.id, Id::String("req-42".into())),
            other => panic!("expected request, got {other:?}"),
        }
        assert_eq!(round_trip(v.clone()), v);
    }

    #[test]
    fn notification_no_id() {
        let v = json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": { "sessionId": "s-1", "update": { "kind": "agent_message_chunk" } }
        });
        let inc = Incoming::from_value(v.clone()).unwrap();
        assert!(matches!(inc, Incoming::Notification(_)));
        assert_eq!(round_trip(v.clone()), v);
    }

    #[test]
    fn notification_without_params() {
        let v = json!({ "jsonrpc": "2.0", "method": "bridge/peer_left" });
        let inc = Incoming::from_value(v.clone()).unwrap();
        assert!(matches!(inc, Incoming::Notification(_)));
        // Params absent stays absent (not serialized as null).
        assert_eq!(round_trip(v.clone()), v);
    }

    #[test]
    fn response_with_result() {
        let v = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": { "sessionId": "s-1" }
        });
        let inc = Incoming::from_value(v.clone()).unwrap();
        assert!(matches!(inc, Incoming::Response(_)));
        assert_eq!(round_trip(v.clone()), v);
    }

    #[test]
    fn response_with_error() {
        let v = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": { "code": -32001, "message": "session busy" }
        });
        let inc = Incoming::from_value(v.clone()).unwrap();
        match inc {
            Incoming::Response(r) => {
                let err = r.error.expect("error present");
                assert_eq!(err.code, -32001);
                assert_eq!(err.message, "session busy");
            }
            other => panic!("expected response, got {other:?}"),
        }
        assert_eq!(round_trip(v.clone()), v);
    }

    #[test]
    fn response_with_error_data() {
        let v = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": { "code": -32001, "message": "busy", "data": { "retryAfterMs": 500 } }
        });
        assert_eq!(round_trip(v.clone()), v);
    }

    #[test]
    fn response_with_null_id() {
        // Servers may return id=null when the request id couldn't be parsed.
        let v = json!({
            "jsonrpc": "2.0",
            "id": null,
            "error": { "code": -32700, "message": "Parse error" }
        });
        let inc = Incoming::from_value(v.clone()).unwrap();
        match inc {
            Incoming::Response(r) => assert_eq!(r.id, Id::Null),
            other => panic!("expected response, got {other:?}"),
        }
        assert_eq!(round_trip(v.clone()), v);
    }

    #[test]
    fn rejects_non_object() {
        assert!(matches!(
            Incoming::from_value(json!([])),
            Err(ParseError::NotAnObject)
        ));
        assert!(matches!(
            Incoming::from_value(json!("hi")),
            Err(ParseError::NotAnObject)
        ));
    }

    #[test]
    fn rejects_ambiguous_envelope() {
        // No method, no result, no error — not a valid envelope.
        let v = json!({ "jsonrpc": "2.0", "id": 1 });
        assert!(matches!(
            Incoming::from_value(v),
            Err(ParseError::Ambiguous(_))
        ));
    }

    #[test]
    fn rejects_wrong_version() {
        let v = json!({
            "jsonrpc": "1.0",
            "id": 1,
            "method": "foo"
        });
        let err = Incoming::from_value(v).unwrap_err();
        // Wrong-version frame fails inside the typed deserializer.
        assert!(matches!(err, ParseError::Json(_)), "got {err:?}");
    }

    #[test]
    fn parse_from_bytes() {
        let bytes = br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
        let inc = Incoming::parse(bytes).unwrap();
        assert!(matches!(inc, Incoming::Request(_)));
    }
}
