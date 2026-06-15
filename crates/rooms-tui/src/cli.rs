use clap::Parser;
use rooms_client::AttachConfig;

#[derive(Debug, Clone, Parser, PartialEq, Eq)]
#[command(
    name = "rooms-tui",
    version,
    about = "Room-native terminal client for the acp-mux Rooms layer"
)]
pub struct Args {
    /// WebSocket attach endpoint for a running `rooms` server.
    #[arg(long, default_value = "ws://127.0.0.1:8765/acp")]
    pub url: String,

    /// Rooms collaboration id (`?room=` on the websocket URL).
    #[arg(long)]
    pub room: String,

    /// Stable peer id, unique within the room.
    #[arg(long)]
    pub peer_id: String,

    /// Human display name for this peer.
    #[arg(long)]
    pub peer_name: Option<String>,

    /// Print the resolved websocket URL and exit.
    #[arg(long, default_value_t = false)]
    pub print_url: bool,
}

impl Args {
    pub fn attach_config(&self) -> AttachConfig {
        AttachConfig {
            url: self.url.clone(),
            room: self.room.clone(),
            peer_id: self.peer_id.clone(),
            peer_name: self.peer_name.clone(),
        }
    }
}
