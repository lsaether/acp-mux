use serde_json::json;

use rooms_client::{Event, event_from_value};

#[test]
fn parses_rooms_turn_started_into_attributed_text_event() {
    let frame = json!({
        "jsonrpc": "2.0",
        "method": "rooms/turn_started",
        "params": {
            "roomId": "demo",
            "roomsTurnId": "at-7",
            "peerId": "phone",
            "peerName": "Phone",
            "content": [{ "type": "text", "text": "ship it" }]
        }
    });

    assert_eq!(
        event_from_value(&frame).unwrap(),
        Event::TurnStarted {
            room_id: "demo".into(),
            turn_id: "at-7".into(),
            peer_id: "phone".into(),
            peer_name: Some("Phone".into()),
            text: "ship it".into(),
        }
    );
}

#[test]
fn parses_queue_item_added_with_text_from_prompt_array() {
    let frame = json!({
        "jsonrpc": "2.0",
        "method": "rooms/queue_item_added",
        "params": {
            "roomId": "demo",
            "queueItemId": "aq-3",
            "peerId": "desktop",
            "prompt": [{ "type": "text", "text": "next please" }]
        }
    });

    assert_eq!(
        event_from_value(&frame).unwrap(),
        Event::QueueItemAdded {
            room_id: "demo".into(),
            queue_item_id: "aq-3".into(),
            peer_id: "desktop".into(),
            text: "next please".into(),
        }
    );
}

#[test]
fn parses_permission_request_as_actionable_event() {
    let frame = json!({
        "jsonrpc": "2.0",
        "id": "perm-1",
        "method": "session/request_permission",
        "params": {
            "sessionId": "sess-123",
            "toolCall": { "title": "Edit src/lib.rs", "kind": "edit" },
            "options": [
                { "optionId": "allow", "name": "Allow" },
                { "optionId": "deny", "name": "Deny" }
            ]
        }
    });

    assert_eq!(
        event_from_value(&frame).unwrap(),
        Event::PermissionRequested {
            request_id: "perm-1".into(),
            session_id: Some("sess-123".into()),
            title: Some("Edit src/lib.rs".into()),
            options: vec!["allow".into(), "deny".into()],
        }
    );
}

#[test]
fn unknown_frames_keep_method_for_debug_rendering() {
    let frame = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": { "sessionId": "sess-123" }
    });

    assert_eq!(
        event_from_value(&frame).unwrap(),
        Event::Unknown {
            method: "session/update".into(),
        }
    );
}
