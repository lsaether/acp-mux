//! Per-attached-client state.

use tokio::sync::mpsc;

/// A connected WebSocket subscriber. Owned by the session actor; cloned
/// metadata is fine but the `outbound` sender is move-only per subscriber
/// — drop it to signal the WS-out task to shut down.
#[derive(Debug)]
pub struct Subscriber {
    pub peer_id: String,
    pub peer_name: Option<String>,
    pub role: Option<String>,
    pub outbound: mpsc::UnboundedSender<Vec<u8>>,
}

impl Subscriber {
    pub fn new(
        peer_id: String,
        peer_name: Option<String>,
        role: Option<String>,
        outbound: mpsc::UnboundedSender<Vec<u8>>,
    ) -> Self {
        Self {
            peer_id,
            peer_name,
            role,
            outbound,
        }
    }
}
