//! `amux/*` namespace frames — out-of-band metadata emitted by the
//! multiplexer (peer presence, turn boundaries, busy state).
//!
//! Each builder returns a complete JSON-RPC notification frame as NDJSON-
//! ready bytes (no trailing newline; the WS-out / replay-log path adds
//! framing as needed). The shapes match `docs/design/amux-namespace.md`.
//!
//! `amuxTurnId` is formatted `at-<u64>` with a monotonic per-session
//! counter; the prefix exists so a token shows its origin in logs.

use serde::Serialize;

const METHOD_PEER_JOINED: &str = "amux/peer_joined";
const METHOD_PEER_LEFT: &str = "amux/peer_left";
const METHOD_TURN_STARTED: &str = "amux/turn_started";
const METHOD_TURN_COMPLETE: &str = "amux/turn_complete";
const METHOD_SESSION_BUSY: &str = "amux/session_busy";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AmuxTurnId(pub u64);

impl AmuxTurnId {
    pub fn formatted(self) -> String {
        format!("at-{}", self.0)
    }
}

#[derive(Serialize)]
struct Frame<'a, P: Serialize> {
    jsonrpc: &'a str,
    method: &'a str,
    params: P,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PeerJoinedParams<'a> {
    session_id: &'a str,
    peer_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PeerLeftParams<'a> {
    session_id: &'a str,
    peer_id: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TurnStartedParams<'a> {
    session_id: &'a str,
    amux_turn_id: &'a str,
    peer_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<&'a str>,
    content: &'a serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TurnCompleteParams<'a> {
    session_id: &'a str,
    amux_turn_id: &'a str,
    stop_reason: &'a serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionBusyParams<'a> {
    session_id: &'a str,
    busy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    held_by: Option<&'a str>,
}

fn encode<P: Serialize>(method: &'static str, params: P) -> Vec<u8> {
    serde_json::to_vec(&Frame {
        jsonrpc: "2.0",
        method,
        params,
    })
    .expect("amux frame is always serializable")
}

pub fn peer_joined(
    session_id: &str,
    peer_id: &str,
    peer_name: Option<&str>,
    role: Option<&str>,
) -> Vec<u8> {
    encode(
        METHOD_PEER_JOINED,
        PeerJoinedParams {
            session_id,
            peer_id,
            peer_name,
            role,
        },
    )
}

pub fn peer_left(session_id: &str, peer_id: &str) -> Vec<u8> {
    encode(
        METHOD_PEER_LEFT,
        PeerLeftParams {
            session_id,
            peer_id,
        },
    )
}

pub fn turn_started(
    session_id: &str,
    amux_turn_id: AmuxTurnId,
    peer_id: &str,
    peer_name: Option<&str>,
    role: Option<&str>,
    content: &serde_json::Value,
) -> Vec<u8> {
    let id = amux_turn_id.formatted();
    encode(
        METHOD_TURN_STARTED,
        TurnStartedParams {
            session_id,
            amux_turn_id: &id,
            peer_id,
            peer_name,
            role,
            content,
        },
    )
}

pub fn turn_complete(
    session_id: &str,
    amux_turn_id: AmuxTurnId,
    stop_reason: &serde_json::Value,
) -> Vec<u8> {
    let id = amux_turn_id.formatted();
    encode(
        METHOD_TURN_COMPLETE,
        TurnCompleteParams {
            session_id,
            amux_turn_id: &id,
            stop_reason,
        },
    )
}

pub fn session_busy(session_id: &str, busy: bool, held_by: Option<&str>) -> Vec<u8> {
    encode(
        METHOD_SESSION_BUSY,
        SessionBusyParams {
            session_id,
            busy,
            held_by,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn parse(bytes: &[u8]) -> Value {
        serde_json::from_slice(bytes).expect("frame is JSON")
    }

    #[test]
    fn turn_id_format() {
        assert_eq!(AmuxTurnId(42).formatted(), "at-42");
        assert_eq!(AmuxTurnId(0).formatted(), "at-0");
    }

    #[test]
    fn peer_joined_includes_optional_fields() {
        let bytes = peer_joined("work", "phone-1", Some("phone"), Some("default"));
        let v = parse(&bytes);
        assert_eq!(v["jsonrpc"], json!("2.0"));
        assert_eq!(v["method"], json!("amux/peer_joined"));
        assert_eq!(v["params"]["sessionId"], json!("work"));
        assert_eq!(v["params"]["peerId"], json!("phone-1"));
        assert_eq!(v["params"]["peerName"], json!("phone"));
        assert_eq!(v["params"]["role"], json!("default"));
    }

    #[test]
    fn peer_joined_omits_missing_optionals() {
        let bytes = peer_joined("work", "p1", None, None);
        let v = parse(&bytes);
        assert!(v["params"].get("peerName").is_none());
        assert!(v["params"].get("role").is_none());
    }

    #[test]
    fn peer_left_shape() {
        let v = parse(&peer_left("work", "p1"));
        assert_eq!(v["method"], json!("amux/peer_left"));
        assert_eq!(v["params"]["sessionId"], json!("work"));
        assert_eq!(v["params"]["peerId"], json!("p1"));
        assert!(v.get("id").is_none());
    }

    #[test]
    fn turn_started_carries_content() {
        let content = json!([{"type": "text", "text": "hi"}]);
        let v = parse(&turn_started(
            "work",
            AmuxTurnId(7),
            "phone-1",
            Some("phone"),
            None,
            &content,
        ));
        assert_eq!(v["method"], json!("amux/turn_started"));
        assert_eq!(v["params"]["amuxTurnId"], json!("at-7"));
        assert_eq!(v["params"]["peerId"], json!("phone-1"));
        assert_eq!(v["params"]["peerName"], json!("phone"));
        assert!(v["params"].get("role").is_none());
        assert_eq!(v["params"]["content"], content);
    }

    #[test]
    fn turn_complete_shape() {
        let reason = json!("end_turn");
        let v = parse(&turn_complete("work", AmuxTurnId(7), &reason));
        assert_eq!(v["method"], json!("amux/turn_complete"));
        assert_eq!(v["params"]["amuxTurnId"], json!("at-7"));
        assert_eq!(v["params"]["stopReason"], json!("end_turn"));
    }

    #[test]
    fn session_busy_shape() {
        let v = parse(&session_busy("work", true, Some("desktop-1")));
        assert_eq!(v["method"], json!("amux/session_busy"));
        assert_eq!(v["params"]["sessionId"], json!("work"));
        assert_eq!(v["params"]["busy"], json!(true));
        assert_eq!(v["params"]["heldBy"], json!("desktop-1"));

        let v = parse(&session_busy("work", false, None));
        assert!(v["params"].get("heldBy").is_none());
    }
}
