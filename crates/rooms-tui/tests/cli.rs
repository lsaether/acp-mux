use clap::Parser;
use rooms_client::AttachConfig;
use rooms_tui::cli::Args;

#[test]
fn args_parse_into_shared_attach_config() {
    let args = Args::parse_from([
        "rooms-tui",
        "--url",
        "ws://mux.local/acp",
        "--room",
        "demo",
        "--peer-id",
        "desktop",
        "--peer-name",
        "Desk",
        "--print-url",
    ]);

    assert!(args.print_url);
    assert_eq!(
        args.attach_config(),
        AttachConfig {
            url: "ws://mux.local/acp".into(),
            room: "demo".into(),
            peer_id: "desktop".into(),
            peer_name: Some("Desk".into()),
        }
    );
}
