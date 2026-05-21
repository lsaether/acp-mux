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
//! 4409. Internal failures (no `--agent-cmd`, agent spawn failure) close
//! with code 1011.

use std::sync::Arc;

use axum::{
    Router,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, Utf8Bytes, WebSocket},
    },
    response::IntoResponse,
    routing::get,
};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::multiplex::subscriber::Subscriber;
use crate::session::registry::{RegistryError, SessionRegistry};
use crate::session::state::SessionMsg;

const SESSION_ID_MAX_LEN: usize = 128;

pub const CLOSE_CODE_BAD_QUERY: u16 = 4400;
pub const CLOSE_CODE_PEER_CONFLICT: u16 = 4409;
/// Standard WS internal-error close code; used when the server can't bring
/// up a session (no `--agent-cmd` configured, agent spawn failure, or the
/// session actor died mid-attach).
pub const CLOSE_CODE_INTERNAL: u16 = 1011;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<SessionRegistry>,
}

impl AppState {
    pub fn new(registry: Arc<SessionRegistry>) -> Self {
        Self { registry }
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
    let validated = match validate(&q) {
        Ok(v) => v,
        Err(reason) => {
            tracing::warn!(reason, "rejecting WS upgrade: bad query");
            close_with(&mut socket, CLOSE_CODE_BAD_QUERY, reason).await;
            return;
        }
    };
    let ValidatedAttach {
        session,
        peer_id,
        peer_name,
        role,
    } = validated;

    let (outbound_tx, outbound_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let subscriber = Subscriber::new(peer_id.clone(), peer_name, role, outbound_tx);

    let handle = match state.registry.attach(&session, subscriber).await {
        Ok(h) => h,
        Err(RegistryError::PeerIdInUse) => {
            tracing::warn!(%session, %peer_id, "peer_id collision");
            close_with(
                &mut socket,
                CLOSE_CODE_PEER_CONFLICT,
                "peer_id already attached to this session",
            )
            .await;
            return;
        }
        Err(RegistryError::AgentCmdMissing) => {
            tracing::error!("attach refused: --agent-cmd not configured");
            close_with(
                &mut socket,
                CLOSE_CODE_INTERNAL,
                "server has no --agent-cmd configured",
            )
            .await;
            return;
        }
        Err(RegistryError::AgentSpawn(err)) => {
            tracing::error!(error = %err, "agent spawn failed");
            close_with(&mut socket, CLOSE_CODE_INTERNAL, "agent spawn failed").await;
            return;
        }
        Err(RegistryError::ActorUnreachable) => {
            tracing::error!("session actor unreachable mid-attach");
            close_with(&mut socket, CLOSE_CODE_INTERNAL, "session unreachable").await;
            return;
        }
    };

    tracing::info!(%session, %peer_id, "subscriber attached");

    let (ws_sink, ws_stream) = socket.split();
    let in_session_tx = handle.tx.clone();
    let in_peer_id = peer_id.clone();
    let out_peer_id = peer_id.clone();
    let in_session = session.clone();
    let out_session = session.clone();

    tokio::select! {
        _ = ws_in_task(ws_stream, in_peer_id, in_session_tx, in_session) => {},
        _ = ws_out_task(ws_sink, outbound_rx, out_peer_id, out_session) => {},
    }

    // Whichever side ended first, make sure the session learns we're gone.
    // Send may fail if the session actor already exited; that's fine.
    let _ = handle
        .tx
        .send(SessionMsg::Detach {
            peer_id: peer_id.clone(),
        })
        .await;
    tracing::debug!(%session, %peer_id, "ws handler exiting");
}

async fn ws_in_task(
    mut ws_stream: SplitStream<WebSocket>,
    peer_id: String,
    session_tx: mpsc::Sender<SessionMsg>,
    session: String,
) {
    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(Message::Text(t)) => {
                let bytes = strip_trailing_newline(t.as_bytes());
                if session_tx
                    .send(SessionMsg::InboundFromSubscriber {
                        peer_id: peer_id.clone(),
                        bytes,
                    })
                    .await
                    .is_err()
                {
                    tracing::debug!(%session, %peer_id, "session actor gone; ws_in exiting");
                    return;
                }
            }
            Ok(Message::Binary(b)) => {
                let bytes = strip_trailing_newline(&b);
                if session_tx
                    .send(SessionMsg::InboundFromSubscriber {
                        peer_id: peer_id.clone(),
                        bytes,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Ok(Message::Close(_)) => {
                tracing::debug!(%session, %peer_id, "ws_in: client close");
                return;
            }
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
            Err(err) => {
                tracing::debug!(%session, %peer_id, error = %err, "ws recv error");
                return;
            }
        }
    }
}

async fn ws_out_task(
    mut ws_sink: SplitSink<WebSocket, Message>,
    mut outbound_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    peer_id: String,
    session: String,
) {
    while let Some(line) = outbound_rx.recv().await {
        match Utf8Bytes::try_from(line) {
            Ok(text) => {
                if ws_sink.send(Message::Text(text)).await.is_err() {
                    tracing::debug!(%session, %peer_id, "ws_out: peer dropped");
                    return;
                }
            }
            Err(err) => {
                tracing::warn!(%session, %peer_id, error = %err, "non-UTF8 agent stdout line; dropped");
            }
        }
    }
    tracing::debug!(%session, %peer_id, "ws_out: outbound channel closed");
    let _ = ws_sink.close().await;
}

struct ValidatedAttach {
    session: String,
    peer_id: String,
    peer_name: Option<String>,
    role: Option<String>,
}

fn validate(q: &AttachQuery) -> Result<ValidatedAttach, &'static str> {
    let session = q.session.as_deref().ok_or("missing ?session")?;
    if !is_valid_session_id(session) {
        return Err("invalid ?session (expect ^[A-Za-z0-9_-]{1,128}$)");
    }
    let peer_id = q.peer_id.as_deref().ok_or("missing ?peer_id")?;
    if peer_id.is_empty() {
        return Err("empty ?peer_id");
    }
    Ok(ValidatedAttach {
        session: session.to_string(),
        peer_id: peer_id.to_string(),
        peer_name: q.peer_name.clone(),
        role: q.role.clone(),
    })
}

/// Strip a single trailing `\n` (and the preceding `\r`, if any) from the
/// payload. The agent NDJSON writer already appends its own `\n`, so a
/// client-supplied newline would otherwise yield an empty second line on
/// the agent's stdin — which strict NDJSON parsers reject.
fn strip_trailing_newline(bytes: &[u8]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    if out.ends_with(b"\n") {
        out.pop();
        if out.ends_with(b"\r") {
            out.pop();
        }
    }
    out
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
    use crate::session::registry::AgentCmd;
    use futures::{SinkExt, StreamExt};
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::time::timeout;
    use tokio_tungstenite::tungstenite::Message as ClientMsg;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    /// Spawn an acp-mux server backed by `cat` as the agent (NDJSON loopback).
    async fn spawn_server_with_cat() -> (SocketAddr, Arc<SessionRegistry>) {
        spawn_server(Some(AgentCmd {
            program: "cat".into(),
            args: vec![],
        }))
        .await
    }

    async fn spawn_server(agent_cmd: Option<AgentCmd>) -> (SocketAddr, Arc<SessionRegistry>) {
        let registry = SessionRegistry::new(agent_cmd);
        let app = router(AppState::new(registry.clone()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        (addr, registry)
    }

    #[tokio::test]
    async fn healthz_returns_ok() {
        let (addr, _) = spawn_server_with_cat().await;
        let body = http_get(&format!("http://{addr}/healthz")).await;
        assert_eq!(body, "ok\n");
    }

    #[tokio::test]
    async fn ws_invalid_session_closes_4400() {
        let (addr, _) = spawn_server_with_cat().await;
        let url = format!("ws://{addr}/acp?session=bad%20space&peer_id=p1");
        let (mut ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .expect("ws connect");
        let close = wait_for_close(&mut ws).await.expect("expected close frame");
        assert_eq!(u16::from(close), CLOSE_CODE_BAD_QUERY);
    }

    #[tokio::test]
    async fn ws_missing_peer_id_closes_4400() {
        let (addr, _) = spawn_server_with_cat().await;
        let url = format!("ws://{addr}/acp?session=ok");
        let (mut ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .expect("ws connect");
        let close = wait_for_close(&mut ws).await.expect("expected close frame");
        assert_eq!(u16::from(close), CLOSE_CODE_BAD_QUERY);
    }

    #[tokio::test]
    async fn ws_no_agent_cmd_closes_1011() {
        let (addr, _) = spawn_server(None).await;
        let url = format!("ws://{addr}/acp?session=ok&peer_id=p1");
        let (mut ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .expect("ws connect");
        let close = wait_for_close(&mut ws).await.expect("expected close frame");
        assert_eq!(u16::from(close), CLOSE_CODE_INTERNAL);
    }

    #[tokio::test]
    async fn ws_loopback_roundtrip_via_cat() {
        let (addr, registry) = spawn_server_with_cat().await;
        let url = format!("ws://{addr}/acp?session=loop&peer_id=p1");
        let (mut ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .expect("ws connect");

        let payload = r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#;
        ws.send(ClientMsg::Text(payload.into())).await.unwrap();

        let received = timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("ws recv timeout")
            .expect("stream ended")
            .expect("recv err");
        match received {
            ClientMsg::Text(t) => assert_eq!(t.as_str(), payload),
            other => panic!("expected text echo, got {other:?}"),
        }

        ws.send(ClientMsg::Close(None)).await.unwrap();
        drain_until_close(&mut ws).await;

        // Last subscriber gone → session should terminate.
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(40)).await;
            if registry.live_session_count().await == 0 {
                return;
            }
        }
        panic!("session did not tear down after last subscriber");
    }

    #[tokio::test]
    async fn ws_two_subscribers_see_naive_fanout() {
        let (addr, _) = spawn_server_with_cat().await;
        let url_a = format!("ws://{addr}/acp?session=share&peer_id=A");
        let url_b = format!("ws://{addr}/acp?session=share&peer_id=B");

        let (mut ws_a, _) = tokio_tungstenite::connect_async(&url_a).await.unwrap();
        // Give A's attach time to complete before B joins.
        tokio::time::sleep(Duration::from_millis(40)).await;
        let (mut ws_b, _) = tokio_tungstenite::connect_async(&url_b).await.unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;

        let payload = r#"{"jsonrpc":"2.0","method":"session/update"}"#;
        ws_a.send(ClientMsg::Text(payload.into())).await.unwrap();

        // Both subscribers should see the echoed line (naive fan-out).
        let from_a = timeout(Duration::from_secs(2), ws_a.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let from_b = timeout(Duration::from_secs(2), ws_b.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        for m in [from_a, from_b] {
            match m {
                ClientMsg::Text(t) => assert_eq!(t.as_str(), payload),
                other => panic!("expected text, got {other:?}"),
            }
        }

        let _ = ws_a.send(ClientMsg::Close(None)).await;
        let _ = ws_b.send(ClientMsg::Close(None)).await;
    }

    #[tokio::test]
    async fn ws_peer_id_collision_closes_4409() {
        let (addr, _) = spawn_server_with_cat().await;
        let url = format!("ws://{addr}/acp?session=dup&peer_id=p1");
        let (mut ws_a, _) = tokio_tungstenite::connect_async(&url)
            .await
            .expect("connect A");
        tokio::time::sleep(Duration::from_millis(40)).await;
        let (mut ws_b, _) = tokio_tungstenite::connect_async(&url)
            .await
            .expect("connect B");
        let close = wait_for_close(&mut ws_b)
            .await
            .expect("expected close on B");
        assert_eq!(u16::from(close), CLOSE_CODE_PEER_CONFLICT);
        let _ = ws_a.send(ClientMsg::Close(None)).await;
    }

    async fn drain_until_close<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        while let Some(msg) = ws.next().await {
            if matches!(msg, Ok(ClientMsg::Close(_)) | Err(_)) {
                break;
            }
        }
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

    async fn http_get(url: &str) -> String {
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
    fn strip_trailing_newline_variants() {
        assert_eq!(strip_trailing_newline(b"{}"), b"{}");
        assert_eq!(strip_trailing_newline(b"{}\n"), b"{}");
        assert_eq!(strip_trailing_newline(b"{}\r\n"), b"{}");
        assert_eq!(strip_trailing_newline(b"{}\n\n"), b"{}\n");
        assert_eq!(strip_trailing_newline(b""), b"");
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
            session: Some("ok".into()),
            peer_id: Some("p1".into()),
            peer_name: Some("Alice".into()),
            role: Some("driver".into()),
        };
        let v = validate(&q).unwrap();
        assert_eq!(v.session, "ok");
        assert_eq!(v.peer_id, "p1");
        assert_eq!(v.peer_name.as_deref(), Some("Alice"));
        assert_eq!(v.role.as_deref(), Some("driver"));
    }
}
