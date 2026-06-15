use rooms_client::{AttachConfig, build_attach_url};
use url::Url;

#[test]
fn build_attach_url_uses_room_and_replay_skip() {
    let url = build_attach_url(&AttachConfig {
        url: "ws://127.0.0.1:8765/acp".into(),
        room: "shared-room".into(),
        peer_id: "desktop".into(),
        peer_name: Some("Desktop UI".into()),
    })
    .unwrap();

    let parsed = Url::parse(&url).unwrap();
    let pairs = parsed.query_pairs().collect::<Vec<_>>();

    assert_eq!(parsed.scheme(), "ws");
    assert_eq!(parsed.host_str(), Some("127.0.0.1"));
    assert_eq!(parsed.path(), "/acp");
    assert!(
        pairs
            .iter()
            .any(|(key, value)| key == "room" && value == "shared-room")
    );
    assert!(
        pairs
            .iter()
            .any(|(key, value)| key == "peer_id" && value == "desktop")
    );
    assert!(
        pairs
            .iter()
            .any(|(key, value)| key == "peer_name" && value == "Desktop UI")
    );
    assert!(
        pairs
            .iter()
            .any(|(key, value)| key == "replay" && value == "skip")
    );
    assert!(!pairs.iter().any(|(key, _)| key == "session"));
}

#[test]
fn build_attach_url_preserves_existing_query_and_replay_skip() {
    let url = build_attach_url(&AttachConfig {
        url: "wss://mux.example/acp?theme=black&replay=skip".into(),
        room: "shared-room".into(),
        peer_id: "phone".into(),
        peer_name: None,
    })
    .unwrap();

    let parsed = Url::parse(&url).unwrap();
    let pairs = parsed.query_pairs().collect::<Vec<_>>();

    assert!(
        pairs
            .iter()
            .any(|(key, value)| key == "theme" && value == "black")
    );
    assert!(
        pairs
            .iter()
            .any(|(key, value)| key == "room" && value == "shared-room")
    );
    assert!(
        pairs
            .iter()
            .any(|(key, value)| key == "peer_id" && value == "phone")
    );
    assert_eq!(pairs.iter().filter(|(key, _)| key == "replay").count(), 1);
}
