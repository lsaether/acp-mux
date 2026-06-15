use rooms_client::{AttachConfig, build_attach_url};
use rooms_tui::ui::UiModel;

#[test]
fn tui_model_accepts_shared_rooms_client_attach_config() {
    let attach_config = AttachConfig {
        url: "ws://127.0.0.1:8765/acp".into(),
        room: "tauri-ready".into(),
        peer_id: "tui".into(),
        peer_name: Some("Terminal".into()),
    };
    let attach_url = build_attach_url(&attach_config).unwrap();

    let model = UiModel::new(attach_config, attach_url);

    assert_eq!(model.title(), "rooms-tui · tauri-ready");
    assert_eq!(model.peer_label(), "tui (Terminal)");
    assert!(model.attach_url().contains("room=tauri-ready"));
}
