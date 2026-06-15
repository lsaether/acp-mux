use serde_json::json;

use rooms_client::protocol::{
    build_attach, build_cancel_active_turn, build_initialize, build_permission_response,
    build_queue_prompt, build_session_prompt, build_steer_active_turn, build_unqueue_prompt,
};

#[test]
fn attach_bootstrap_requests_full_lineage_streamed_rooms_history() {
    let frame = build_attach(
        "rt-2",
        Some("sess-123"),
        Some("desktop"),
        Some("Desktop UI"),
    );

    assert_eq!(
        frame,
        json!({
            "jsonrpc": "2.0",
            "id": "rt-2",
            "method": "session/attach",
            "params": {
                "sessionId": "sess-123",
                "clientId": "desktop",
                "clientInfo": { "name": "Desktop UI" },
                "historyPolicy": "full_lineage",
                "_meta": {
                    "rooms": {
                        "replayOrder": "newest_turn_first",
                        "historyDelivery": "stream"
                    }
                }
            }
        })
    );
}

#[test]
fn control_builders_use_rooms_namespace_and_trim_text() {
    assert_eq!(
        build_queue_prompt("rt-3", "  do this next  ", Some("sess-123")),
        json!({
            "jsonrpc": "2.0",
            "id": "rt-3",
            "method": "rooms/queue_prompt",
            "params": { "sessionId": "sess-123", "text": "do this next" }
        })
    );

    assert_eq!(
        build_steer_active_turn("rt-4", "  revise now  ", None),
        json!({
            "jsonrpc": "2.0",
            "id": "rt-4",
            "method": "rooms/steer_active_turn",
            "params": { "text": "revise now" }
        })
    );

    assert_eq!(
        build_cancel_active_turn("rt-5", Some("  user clicked stop  ")),
        json!({
            "jsonrpc": "2.0",
            "id": "rt-5",
            "method": "rooms/cancel_active_turn",
            "params": { "reason": "user clicked stop" }
        })
    );

    assert_eq!(
        build_unqueue_prompt("rt-6", "  aq-1  "),
        json!({
            "jsonrpc": "2.0",
            "id": "rt-6",
            "method": "rooms/unqueue_prompt",
            "params": { "queueItemId": "aq-1" }
        })
    );
}

#[test]
fn permission_response_builder_selects_trimmed_option_id() {
    assert_eq!(
        build_permission_response(json!(10001), "  deny  "),
        json!({
            "jsonrpc": "2.0",
            "id": 10001,
            "result": { "outcome": { "outcome": "selected", "optionId": "deny" } }
        })
    );
}

#[test]
fn acp_builders_create_initialize_and_prompt_frames() {
    assert_eq!(
        build_initialize("rt-1"),
        json!({
            "jsonrpc": "2.0",
            "id": "rt-1",
            "method": "initialize",
            "params": { "protocolVersion": 1, "clientCapabilities": {} }
        })
    );

    assert_eq!(
        build_session_prompt("rt-7", "sess-123", "hello room"),
        json!({
            "jsonrpc": "2.0",
            "id": "rt-7",
            "method": "session/prompt",
            "params": {
                "sessionId": "sess-123",
                "prompt": [{ "type": "text", "text": "hello room" }]
            }
        })
    );
}
