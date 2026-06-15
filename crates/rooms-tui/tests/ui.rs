use rooms_client::{AttachConfig, ConnectionStatus, InboundMessage, TranscriptKind};
use rooms_tui::ui::UiModel;
use serde_json::json;

fn model() -> UiModel {
    UiModel::new(
        AttachConfig {
            url: "ws://127.0.0.1:8765/acp".into(),
            room: "demo".into(),
            peer_id: "desktop".into(),
            peer_name: Some("Desk".into()),
        },
        "ws://127.0.0.1:8765/acp?room=demo&peer_id=desktop&replay=skip".into(),
    )
}

#[test]
fn model_summarizes_room_peer_and_url() {
    let model = model();

    assert_eq!(model.title(), "rooms-tui · demo");
    assert_eq!(model.peer_label(), "desktop (Desk)");
    assert!(model.attach_url().contains("room=demo"));
}

#[test]
fn snapshot_text_redacts_attach_url_query_values() {
    let model = UiModel::new(
        AttachConfig {
            url: "ws://127.0.0.1:8765/acp?auth_token=super-secret".into(),
            room: "demo".into(),
            peer_id: "desktop".into(),
            peer_name: None,
        },
        "ws://127.0.0.1:8765/acp?auth_token=super-secret&room=demo&peer_id=desktop&replay=skip"
            .into(),
    );

    let snapshot = model.snapshot_text();
    assert!(snapshot.contains("attach: ws://127.0.0.1:8765/acp?<redacted-query>"));
    assert!(!snapshot.contains("super-secret"));
}

#[test]
fn malformed_frames_are_recorded_in_snapshot_and_event_log() {
    let mut model = model();
    let err = model
        .apply_inbound(InboundMessage::Frame {
            raw: json!("not an object"),
            event: None,
        })
        .unwrap_err();

    assert!(err.contains("JSON-RPC frame must be an object"));
    assert_eq!(model.state().connection_status, ConnectionStatus::Error);
    assert!(model.state().errors[0].contains("JSON-RPC frame must be an object"));
    assert!(model.event_log().last().unwrap().contains("state error:"));
}

#[test]
fn model_folds_live_inbound_frames_into_reducer_snapshot_and_event_log() {
    let mut model = model();
    model.set_connection_status(ConnectionStatus::Connecting);

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
        .apply_inbound(InboundMessage::Frame {
            raw: json!({
                "jsonrpc": "2.0",
                "method": "rooms/turn_started",
                "params": {
                    "roomId": "demo",
                    "roomsTurnId": "at-1",
                    "peerId": "desktop",
                    "peerName": "Desk",
                    "content": [{ "type": "text", "text": "hello" }]
                }
            }),
            event: None,
        })
        .unwrap();

    assert_eq!(model.state().connection_status, ConnectionStatus::Attached);
    assert_eq!(model.state().session_id.as_deref(), Some("sess-1"));
    assert_eq!(model.state().peers.len(), 1);
    assert_eq!(model.state().transcript[0].kind, TranscriptKind::Prompt);
    assert_eq!(model.state().transcript[0].text, "hello");
    assert!(
        model
            .event_log()
            .iter()
            .any(|line| line.contains("rooms/turn_started"))
    );

    let snapshot = model.snapshot_text();
    assert!(snapshot.contains("status: Attached"));
    assert!(snapshot.contains("session: sess-1"));
    assert!(snapshot.contains("peers: desktop (Desk)"));
    assert!(snapshot.contains("active: desktop"));
}

#[test]
fn transport_errors_are_visible_without_losing_prior_event_context() {
    let mut model = model();
    model.set_connection_status(ConnectionStatus::Live);
    model
        .apply_inbound(InboundMessage::Error("websocket error: refused".into()))
        .unwrap();

    assert_eq!(model.state().connection_status, ConnectionStatus::Error);
    assert_eq!(model.state().errors, vec!["websocket error: refused"]);
    assert_eq!(
        model.event_log().last().unwrap(),
        "transport error: websocket error: refused"
    );
    assert!(
        model
            .snapshot_text()
            .contains("errors: websocket error: refused")
    );
}
