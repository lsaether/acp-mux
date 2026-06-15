use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use rooms_client::{AttachConfig, ClientCommand, InboundMessage};
use rooms_tui::ui::UiModel;
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

fn permission_frame(id: &str, title: &str, options: &[&str]) -> InboundMessage {
    permission_frame_with_id(json!(id), title, options)
}

fn permission_frame_with_id(id: Value, title: &str, options: &[&str]) -> InboundMessage {
    InboundMessage::Frame {
        raw: json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "session/request_permission",
            "params": {
                "sessionId": "sess-1",
                "toolCall": { "title": title },
                "options": options
                    .iter()
                    .map(|option| json!({ "optionId": option }))
                    .collect::<Vec<_>>()
            }
        }),
        event: None,
    }
}

fn frame(command: Option<ClientCommand>) -> Value {
    match command.expect("expected command") {
        ClientCommand::SendFrame(frame) => frame,
        other => panic!("expected SendFrame, got {other:?}"),
    }
}

#[test]
fn permission_requests_render_detail_and_custom_option_selection_replies_once() {
    let mut model = connected_model();
    model
        .apply_inbound(permission_frame(
            "perm-1",
            "Write src/lib.rs",
            &["allow_once", "deny", "escalate"],
        ))
        .unwrap();

    assert_eq!(model.selected_permission_index(), Some(0));
    assert_eq!(model.selected_permission_option_index(), Some(0));
    let snapshot = model.snapshot_text();
    assert!(snapshot.contains("perm-1: Write src/lib.rs"));
    assert!(snapshot.contains("[allow_once], deny, escalate"));

    model.handle_key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE));
    model.handle_key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE));
    assert_eq!(model.selected_permission_option_index(), Some(2));

    let response =
        frame(model.handle_key(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::CONTROL)));
    assert_eq!(response["id"], json!("perm-1"));
    assert_eq!(
        response["result"]["outcome"],
        json!({ "outcome": "selected", "optionId": "escalate" })
    );
    assert!(model.state().pending_permissions.is_empty());
    assert!(
        model
            .handle_key(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::CONTROL))
            .is_none(),
        "same permission should not be answered twice after local removal"
    );
}

#[test]
fn permission_allow_and_deny_shortcuts_choose_matching_options() {
    let mut allow_model = connected_model();
    allow_model
        .apply_inbound(permission_frame(
            "perm-allow",
            "Read file",
            &["deny", "allow_once", "allow_always"],
        ))
        .unwrap();

    let allow =
        frame(allow_model.handle_key(KeyEvent::new(KeyCode::Char('a'), KeyModifiers::CONTROL)));
    assert_eq!(allow["id"], json!("perm-allow"));
    assert_eq!(allow["result"]["outcome"]["optionId"], json!("allow_once"));

    let mut deny_model = connected_model();
    deny_model
        .apply_inbound(permission_frame(
            "perm-deny",
            "Run command",
            &["allow_once", "reject", "deny"],
        ))
        .unwrap();

    let deny =
        frame(deny_model.handle_key(KeyEvent::new(KeyCode::Char('d'), KeyModifiers::CONTROL)));
    assert_eq!(deny["id"], json!("perm-deny"));
    assert_eq!(deny["result"]["outcome"]["optionId"], json!("deny"));
}

#[test]
fn numeric_permission_request_ids_are_preserved_in_responses() {
    let mut model = connected_model();
    model
        .apply_inbound(permission_frame_with_id(
            json!(10001),
            "Approve numeric id",
            &["allow_once", "deny"],
        ))
        .unwrap();

    assert_eq!(model.state().pending_permissions[0].request_id, "10001");
    let response =
        frame(model.handle_key(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::CONTROL)));
    assert_eq!(response["id"], json!(10001));
    assert_eq!(
        response["result"]["outcome"]["optionId"],
        json!("allow_once")
    );
}

#[test]
fn permission_response_removes_only_the_exact_typed_request_id() {
    let mut model = connected_model();
    model
        .apply_inbound(permission_frame_with_id(
            json!(10001),
            "Approve numeric id",
            &["allow_once", "deny"],
        ))
        .unwrap();
    model
        .apply_inbound(permission_frame_with_id(
            json!("10001"),
            "Approve string id",
            &["allow_once", "deny"],
        ))
        .unwrap();

    let snapshot = model.snapshot_text();
    assert_eq!(snapshot.matches("[allow_once]").count(), 1);

    let response =
        frame(model.handle_key(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::CONTROL)));
    assert_eq!(response["id"], json!(10001));
    assert_eq!(model.state().pending_permissions.len(), 1);
    assert_eq!(
        model.state().pending_permissions[0].response_id,
        json!("10001")
    );
}

#[test]
fn permission_selection_cycles_between_requests_and_options() {
    let mut model = connected_model();
    model
        .apply_inbound(permission_frame("perm-1", "Read", &["allow", "deny"]))
        .unwrap();
    model
        .apply_inbound(permission_frame(
            "perm-2",
            "Write",
            &["allow_once", "deny", "ask_later"],
        ))
        .unwrap();

    assert_eq!(model.selected_permission_index(), Some(0));
    model.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
    assert_eq!(model.selected_permission_index(), Some(1));
    assert_eq!(model.selected_permission_option_index(), Some(0));

    model.handle_key(KeyEvent::new(KeyCode::Left, KeyModifiers::NONE));
    assert_eq!(model.selected_permission_option_index(), Some(2));
    model.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
    assert_eq!(model.selected_permission_index(), Some(0));
    assert_eq!(model.selected_permission_option_index(), Some(0));
}
