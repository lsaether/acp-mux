use rooms_tui::cli::AttachConfig;
use rooms_tui::ui::UiModel;

#[test]
fn scaffold_model_summarizes_room_peer_and_url() {
    let model = UiModel::new(
        AttachConfig {
            url: "ws://127.0.0.1:8765/acp".into(),
            room: "demo".into(),
            peer_id: "desktop".into(),
            peer_name: Some("Desk".into()),
        },
        "ws://127.0.0.1:8765/acp?room=demo&peer_id=desktop&replay=skip".into(),
    );

    assert_eq!(model.title(), "rooms-tui · demo");
    assert_eq!(model.peer_label(), "desktop (Desk)");
    assert!(model.attach_url().contains("room=demo"));
}
