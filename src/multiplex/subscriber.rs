//! Per-attached-client state.

use bytes::Bytes;
use tokio::sync::mpsc;

/// Per-subscriber outbound message. `Frame` is a JSON-RPC text payload
/// (forwarded as WS Text); `Close` instructs the WS-out task to send a
/// WebSocket close frame with the given application code and reason, then
/// exit. The actor uses `Close` for structured shutdown (e.g., agent
/// subprocess exited → code 1011).
#[derive(Debug, Clone)]
pub enum OutMsg {
    Frame(Bytes),
    Close { code: u16, reason: String },
}

impl From<Bytes> for OutMsg {
    fn from(b: Bytes) -> Self {
        OutMsg::Frame(b)
    }
}

impl From<Vec<u8>> for OutMsg {
    fn from(v: Vec<u8>) -> Self {
        OutMsg::Frame(Bytes::from(v))
    }
}

/// A connected WebSocket subscriber. Owned by the session actor; cloned
/// metadata is fine but the `outbound` sender is move-only per subscriber
/// — drop it to signal the WS-out task to shut down.
#[derive(Debug)]
pub struct Subscriber {
    pub peer_id: String,
    pub peer_name: Option<String>,
    pub role: Option<String>,
    pub outbound: mpsc::UnboundedSender<OutMsg>,
}

impl Subscriber {
    pub fn new(
        peer_id: String,
        peer_name: Option<String>,
        role: Option<String>,
        outbound: mpsc::UnboundedSender<OutMsg>,
    ) -> Self {
        Self {
            peer_id,
            peer_name,
            role,
            outbound,
        }
    }
}
