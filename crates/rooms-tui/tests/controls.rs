use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use rooms_client::{AttachConfig, ClientCommand, InboundMessage};
use rooms_tui::ui::{UiModel, should_quit_key};
use serde_json::{Value, json};

fn connected_model() -> UiModel {
    let mut model = UiModel::new(
        AttachConfig {
            url: "ws://127.0.0.1:8765/acp".into(),
            room: "demo".into(),
            peer_id: "desktop".into(),
            peer_name: Some("Desk".into()),
        },
        "ws://127.0.0.1:8765/acp?room=demo&peer_id=desktop&replay=skip".into(),
    );
    model
        .apply_inbound(InboundMessage::Frame {
            raw: json!({
                "jsonrpc": "2.0",
                "id": "rooms-client.attach",
                "result": {
                    "sessionId": "sess-1",
                    "_meta": {
                        "rooms": {
                            "snapshot": {
                                "roomId": "demo",
                                "connectedClients": [{ "clientId": "desktop", "name": "Desk" }],
                                "selfPeer": { "clientId": "desktop", "name": "Desk" },
                                "activeTurn": null,
                                "queue": [],
                                "pendingPermissions": []
                            }
                        }
                    }
                }
            }),
            event: None,
        })
        .unwrap();
    model
}

fn frame(command: Option<ClientCommand>) -> Value {
    match command.expect("expected command") {
        ClientCommand::SendFrame(frame) => frame,
        other => panic!("expected SendFrame, got {other:?}"),
    }
}

#[test]
fn enter_submits_prompt_when_idle_and_queues_when_busy() {
    let mut model = connected_model();
    model.set_draft("  ship it  ");

    let prompt = frame(model.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)));
    assert_eq!(prompt["id"], json!("rooms-tui.prompt.1"));
    assert_eq!(prompt["method"], json!("session/prompt"));
    assert_eq!(prompt["params"]["sessionId"], json!("sess-1"));
    assert_eq!(prompt["params"]["prompt"][0]["text"], json!("ship it"));
    assert_eq!(model.draft(), "");

    model
        .apply_inbound(InboundMessage::Frame {
            raw: json!({
                "jsonrpc": "2.0",
                "method": "rooms/session_busy",
                "params": { "roomId": "demo", "busy": true, "heldBy": "desktop" }
            }),
            event: None,
        })
        .unwrap();
    model.set_draft("next task");

    let queued = frame(model.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)));
    assert_eq!(queued["id"], json!("rooms-tui.queue.2"));
    assert_eq!(queued["method"], json!("rooms/queue_prompt"));
    assert_eq!(queued["params"]["sessionId"], json!("sess-1"));
    assert_eq!(queued["params"]["text"], json!("next task"));
}

#[test]
fn keyboard_text_editing_and_selection_are_predictable() {
    let mut model = connected_model();
    assert!(
        model
            .handle_key(KeyEvent::new(KeyCode::Char('h'), KeyModifiers::NONE))
            .is_none()
    );
    assert!(
        model
            .handle_key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE))
            .is_none()
    );
    assert!(
        model
            .handle_key(KeyEvent::new(KeyCode::Char('u'), KeyModifiers::NONE))
            .is_none()
    );
    assert!(
        model
            .handle_key(KeyEvent::new(KeyCode::Char('i'), KeyModifiers::NONE))
            .is_none()
    );
    assert_eq!(model.draft(), "hqui");
    assert!(
        model
            .handle_key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE))
            .is_none()
    );
    assert_eq!(model.draft(), "hqu");

    assert!(!should_quit_key(KeyEvent::new(
        KeyCode::Char('q'),
        KeyModifiers::NONE
    )));
    assert!(should_quit_key(KeyEvent::new(
        KeyCode::Char('q'),
        KeyModifiers::CONTROL
    )));
    assert!(should_quit_key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE
    )));

    model
        .apply_inbound(InboundMessage::Frame {
            raw: json!({
                "jsonrpc": "2.0",
                "method": "rooms/queue_item_added",
                "params": { "roomId": "demo", "queueItemId": "q-1", "peerId": "phone", "text": "one" }
            }),
            event: None,
        })
        .unwrap();
    model
        .apply_inbound(InboundMessage::Frame {
            raw: json!({
                "jsonrpc": "2.0",
                "method": "rooms/queue_item_added",
                "params": { "roomId": "demo", "queueItemId": "q-2", "peerId": "phone", "text": "two" }
            }),
            event: None,
        })
        .unwrap();

    model.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
    assert_eq!(model.selected_queue_index(), Some(1));
    model.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
    assert_eq!(model.selected_queue_index(), Some(0));
}

#[test]
fn unqueue_skips_non_pending_queue_items() {
    let mut model = connected_model();
    model
        .apply_inbound(InboundMessage::Frame {
            raw: json!({
                "jsonrpc": "2.0",
                "method": "rooms/queue_item_added",
                "params": { "roomId": "demo", "queueItemId": "q-submitted", "peerId": "phone", "text": "already submitted" }
            }),
            event: None,
        })
        .unwrap();
    model
        .apply_inbound(InboundMessage::Frame {
            raw: json!({
                "jsonrpc": "2.0",
                "method": "rooms/queue_item_submitted",
                "params": { "roomId": "demo", "queueItemId": "q-submitted", "roomsTurnId": "at-old" }
            }),
            event: None,
        })
        .unwrap();

    assert!(model.unqueue_selected_command().is_none());

    model
        .apply_inbound(InboundMessage::Frame {
            raw: json!({
                "jsonrpc": "2.0",
                "method": "rooms/queue_item_added",
                "params": { "roomId": "demo", "queueItemId": "q-pending", "peerId": "phone", "text": "still pending" }
            }),
            event: None,
        })
        .unwrap();

    let unqueue = frame(model.unqueue_selected_command());
    assert_eq!(unqueue["method"], json!("rooms/unqueue_prompt"));
    assert_eq!(unqueue["params"]["queueItemId"], json!("q-pending"));
}

#[test]
fn steer_cancel_and_unqueue_controls_route_through_rooms_client_builders() {
    let mut model = connected_model();
    model
        .apply_inbound(InboundMessage::Frame {
            raw: json!({
                "jsonrpc": "2.0",
                "method": "rooms/turn_started",
                "params": {
                    "roomId": "demo",
                    "roomsTurnId": "at-1",
                    "peerId": "desktop",
                    "peerName": "Desk",
                    "content": [{ "type": "text", "text": "working" }]
                }
            }),
            event: None,
        })
        .unwrap();
    model
        .apply_inbound(InboundMessage::Frame {
            raw: json!({
                "jsonrpc": "2.0",
                "method": "rooms/queue_item_added",
                "params": { "roomId": "demo", "queueItemId": "q-1", "peerId": "phone", "text": "queued" }
            }),
            event: None,
        })
        .unwrap();

    model.set_draft("revise plan");
    let steer = frame(model.handle_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL)));
    assert_eq!(steer["id"], json!("rooms-tui.steer.1"));
    assert_eq!(steer["method"], json!("rooms/steer_active_turn"));
    assert_eq!(steer["params"]["sessionId"], json!("sess-1"));
    assert_eq!(steer["params"]["text"], json!("revise plan"));

    let cancel = frame(model.handle_key(KeyEvent::new(KeyCode::Char('x'), KeyModifiers::CONTROL)));
    assert_eq!(cancel["id"], json!("rooms-tui.cancel.2"));
    assert_eq!(cancel["method"], json!("rooms/cancel_active_turn"));
    assert_eq!(
        cancel["params"]["reason"],
        json!("operator requested cancel")
    );

    let unqueue = frame(model.handle_key(KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL)));
    assert_eq!(unqueue["id"], json!("rooms-tui.unqueue.3"));
    assert_eq!(unqueue["method"], json!("rooms/unqueue_prompt"));
    assert_eq!(unqueue["params"]["queueItemId"], json!("q-1"));
}
