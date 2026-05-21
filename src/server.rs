//! HTTP + WebSocket surface.
//!
//! Routes:
//! - `GET /healthz` → `200 ok`
//! - `GET /acp`     → WebSocket upgrade
//!
//! Subscriber attach query: `session`, `peer_id`, `peer_name?`, `role?`.
//! `session` is validated against `^[A-Za-z0-9_-]{1,128}$`. Missing required
//! fields or invalid session ids cause the upgraded socket to close with
//! application code 4400. `peer_id` already in use on a session closes with
//! 4409. (Real per-session subscriber tracking arrives in chunk 3; chunk 2
//! ships a placeholder set so the 4409 path is wired end-to-end.)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use axum::{
    Router,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket},
    },
    response::IntoResponse,
    routing::get,
};
use serde::Deserialize;
use tokio::sync::Mutex;

const SESSION_ID_MAX_LEN: usize = 128;

pub const CLOSE_CODE_BAD_QUERY: u16 = 4400;
pub const CLOSE_CODE_PEER_CONFLICT: u16 = 4409;

#[derive(Clone)]
pub struct AppState {
    /// Placeholder for the chunk-3 session registry. Maps session id to the
    /// set of peer ids currently attached.
    peers: Arc<Mutex<HashMap<String, HashSet<String>>>>,
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

impl AppState {
    pub fn new() -> Self {
        Self {
            peers: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct AttachQuery {
    pub session: Option<String>,
    pub peer_id: Option<String>,
    pub peer_name: Option<String>,
    pub role: Option<String>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/acp", get(acp_attach))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok\n"
}

async fn acp_attach(
    State(state): State<AppState>,
    Query(q): Query<AttachQuery>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_attach(state, q, socket))
}

async fn handle_attach(state: AppState, q: AttachQuery, mut socket: WebSocket) {
    let (session, peer_id) = match validate(&q) {
        Ok(ok) => ok,
        Err(reason) => {
            tracing::warn!(reason, "rejecting WS upgrade: bad query");
            close_with(&mut socket, CLOSE_CODE_BAD_QUERY, reason).await;
            return;
        }
    };

    // Reserve peer_id under this session. This is the chunk-3 registry
    // placeholder; replace with SessionRegistry when it lands.
    let reserved = {
        let mut peers = state.peers.lock().await;
        let set = peers.entry(session.clone()).or_default();
        set.insert(peer_id.clone())
    };
    if !reserved {
        tracing::warn!(%session, %peer_id, "peer_id collision");
        close_with(
            &mut socket,
            CLOSE_CODE_PEER_CONFLICT,
            "peer_id already attached to this session",
        )
        .await;
        return;
    }

    tracing::info!(%session, %peer_id, "subscriber attached (scaffold)");

    // Chunk-2 behavior: hold open and drain inbound. No agent wiring yet;
    // every inbound frame is logged at trace level and otherwise ignored.
    while let Some(msg) = socket.recv().await {
        match msg {
            Ok(Message::Close(_)) => break,
            Ok(Message::Text(t)) => tracing::trace!(bytes = t.len(), "ws text in"),
            Ok(Message::Binary(b)) => tracing::trace!(bytes = b.len(), "ws binary in"),
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
            Err(err) => {
                tracing::debug!(error = %err, "ws recv error");
                break;
            }
        }
    }

    // Release peer_id.
    let mut peers = state.peers.lock().await;
    if let Some(set) = peers.get_mut(&session) {
        set.remove(&peer_id);
        if set.is_empty() {
            peers.remove(&session);
        }
    }
}

fn validate(q: &AttachQuery) -> Result<(String, String), &'static str> {
    let session = q.session.as_deref().ok_or("missing ?session")?;
    if !is_valid_session_id(session) {
        return Err("invalid ?session (expect ^[A-Za-z0-9_-]{1,128}$)");
    }
    let peer_id = q.peer_id.as_deref().ok_or("missing ?peer_id")?;
    if peer_id.is_empty() {
        return Err("empty ?peer_id");
    }
    Ok((session.to_string(), peer_id.to_string()))
}

pub fn is_valid_session_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= SESSION_ID_MAX_LEN
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

async fn close_with(socket: &mut WebSocket, code: u16, reason: &str) {
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.to_string().into(),
        })))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{SinkExt, StreamExt};
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio_tungstenite::tungstenite::Message as ClientMsg;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    async fn spawn_server() -> SocketAddr {
        let state = AppState::new();
        let app = router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        // Give the listener a moment to come up.
        tokio::time::sleep(Duration::from_millis(20)).await;
        addr
    }

    #[tokio::test]
    async fn healthz_returns_ok() {
        let addr = spawn_server().await;
        let body = reqwest_get(&format!("http://{addr}/healthz")).await;
        assert_eq!(body, "ok\n");
    }

    #[tokio::test]
    async fn ws_valid_attach_round_trips_close() {
        let addr = spawn_server().await;
        let (mut ws, _resp) =
            tokio_tungstenite::connect_async(format!("ws://{addr}/acp?session=ok&peer_id=p1"))
                .await
                .expect("ws connect");
        ws.send(ClientMsg::Close(None)).await.unwrap();
        // Drain until close.
        while let Some(msg) = ws.next().await {
            if matches!(msg, Ok(ClientMsg::Close(_)) | Err(_)) {
                break;
            }
        }
    }

    #[tokio::test]
    async fn ws_invalid_session_closes_4400() {
        let addr = spawn_server().await;
        // `connect_async` URL-encodes the space, but the server's regex
        // rejects the literal space after percent-decode.
        let url = format!("ws://{addr}/acp?session=bad%20space&peer_id=p1");
        let (mut ws, _resp) = tokio_tungstenite::connect_async(url)
            .await
            .expect("ws connect");
        let close = wait_for_close(&mut ws).await.expect("expected close frame");
        assert_eq!(u16::from(close), CLOSE_CODE_BAD_QUERY);
    }

    #[tokio::test]
    async fn ws_missing_peer_id_closes_4400() {
        let addr = spawn_server().await;
        let url = format!("ws://{addr}/acp?session=ok");
        let (mut ws, _resp) = tokio_tungstenite::connect_async(url)
            .await
            .expect("ws connect");
        let close = wait_for_close(&mut ws).await.expect("expected close frame");
        assert_eq!(u16::from(close), CLOSE_CODE_BAD_QUERY);
    }

    #[tokio::test]
    async fn ws_peer_id_collision_closes_4409() {
        let addr = spawn_server().await;
        let url = format!("ws://{addr}/acp?session=ok&peer_id=p1");
        let (mut ws_a, _) = tokio_tungstenite::connect_async(&url)
            .await
            .expect("connect A");
        // Briefly let A complete its reservation.
        tokio::time::sleep(Duration::from_millis(30)).await;
        let (mut ws_b, _) = tokio_tungstenite::connect_async(&url)
            .await
            .expect("connect B");
        let close = wait_for_close(&mut ws_b)
            .await
            .expect("expected close frame on B");
        assert_eq!(u16::from(close), CLOSE_CODE_PEER_CONFLICT);
        let _ = ws_a.send(ClientMsg::Close(None)).await;
    }

    async fn wait_for_close<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>) -> Option<CloseCode>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        while let Some(msg) = ws.next().await {
            match msg {
                Ok(ClientMsg::Close(Some(cf))) => return Some(cf.code),
                Ok(ClientMsg::Close(None)) => return None,
                Err(_) => return None,
                _ => {}
            }
        }
        None
    }

    /// Tiny GET helper using hyper-via-axum's stack to avoid a reqwest dep.
    async fn reqwest_get(url: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let parsed = url::Url::parse(url).unwrap();
        let host = parsed.host_str().unwrap();
        let port = parsed.port().unwrap();
        let path = if parsed.path().is_empty() {
            "/"
        } else {
            parsed.path()
        };
        let mut stream = tokio::net::TcpStream::connect((host, port)).await.unwrap();
        let req =
            format!("GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf);
        let body = text.split("\r\n\r\n").nth(1).unwrap_or("");
        body.to_string()
    }

    #[test]
    fn session_id_regex_accepts_canonical() {
        assert!(is_valid_session_id("abc"));
        assert!(is_valid_session_id("a_b-C-9"));
        assert!(is_valid_session_id(&"a".repeat(128)));
    }

    #[test]
    fn session_id_regex_rejects_edge_cases() {
        assert!(!is_valid_session_id(""));
        assert!(!is_valid_session_id(&"a".repeat(129)));
        assert!(!is_valid_session_id("has space"));
        assert!(!is_valid_session_id("has/slash"));
        assert!(!is_valid_session_id("dotsbad."));
    }

    #[test]
    fn validate_requires_session_and_peer_id() {
        let q = AttachQuery {
            session: None,
            peer_id: Some("p1".into()),
            peer_name: None,
            role: None,
        };
        assert!(validate(&q).is_err());

        let q = AttachQuery {
            session: Some("s1".into()),
            peer_id: None,
            peer_name: None,
            role: None,
        };
        assert!(validate(&q).is_err());

        let q = AttachQuery {
            session: Some("s1".into()),
            peer_id: Some("".into()),
            peer_name: None,
            role: None,
        };
        assert!(validate(&q).is_err());

        let q = AttachQuery {
            session: Some("bad space".into()),
            peer_id: Some("p1".into()),
            peer_name: None,
            role: None,
        };
        assert!(validate(&q).is_err());

        let q = AttachQuery {
            session: Some("ok".into()),
            peer_id: Some("p1".into()),
            peer_name: None,
            role: None,
        };
        assert_eq!(validate(&q).unwrap(), ("ok".into(), "p1".into()));
    }
}
