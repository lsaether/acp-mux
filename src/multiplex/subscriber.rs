//! Per-attached-client state.

use bytes::Bytes;
use tokio::sync::mpsc;

/// A connected WebSocket subscriber. Owned by the session actor; cloned
/// metadata is fine but the `outbound` sender is move-only per subscriber
/// — drop it to signal the WS-out task to shut down.
///
/// `outbound` carries `Bytes` (not `Vec<u8>`) so broadcast fan-out is a
/// cheap atomic ref-count bump rather than a full memcpy per recipient.
#[derive(Debug)]
pub struct Subscriber {
    pub peer_id: String,
    pub peer_name: Option<String>,
    pub role: Option<String>,
    pub outbound: mpsc::UnboundedSender<Bytes>,
}

impl Subscriber {
    pub fn new(
        peer_id: String,
        peer_name: Option<String>,
        role: Option<String>,
        outbound: mpsc::UnboundedSender<Bytes>,
    ) -> Self {
        Self {
            peer_id,
            peer_name,
            role,
            outbound,
        }
    }
}
