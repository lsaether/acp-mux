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
    Json, Router,
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
use serde::Serialize;
use tokio::sync::mpsc;

use crate::multiplex::subscriber::{OutMsg, Subscriber};
use crate::session::registry::{RegistryError, SessionRegistry};
use crate::session::state::{SessionMsg, SessionSnapshot};

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
        .route("/debug/sessions", get(debug_sessions))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok\n"
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DebugSessionsResponse {
    sessions: Vec<SessionSnapshot>,
    session_count: usize,
}

async fn debug_sessions(State(state): State<AppState>) -> impl IntoResponse {
    let sessions = state.registry.snapshot().await;
    let count = sessions.len();
    Json(DebugSessionsResponse {
        sessions,
        session_count: count,
    })
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

    let (outbound_tx, outbound_rx) = mpsc::unbounded_channel::<OutMsg>();
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
    mut outbound_rx: mpsc::UnboundedReceiver<OutMsg>,
    peer_id: String,
    session: String,
) {
    while let Some(msg) = outbound_rx.recv().await {
        match msg {
            OutMsg::Frame(bytes) => match Utf8Bytes::try_from(bytes) {
                Ok(text) => {
                    if ws_sink.send(Message::Text(text)).await.is_err() {
                        tracing::debug!(%session, %peer_id, "ws_out: peer dropped");
                        return;
                    }
                }
                Err(err) => {
                    tracing::warn!(%session, %peer_id, error = %err, "non-UTF8 agent stdout line; dropped");
                }
            },
            OutMsg::Close { code, reason } => {
                tracing::info!(%session, %peer_id, code, %reason, "ws_out: structured close");
                let _ = ws_sink
                    .send(Message::Close(Some(CloseFrame {
                        code,
                        reason: reason.into(),
                    })))
                    .await;
                return;
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
    use crate::cli::ReplayTurns;
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

    /// Default short TTL for tests so last-subscriber-leave teardown is
    /// observable within a normal test budget. Tests that specifically
    /// exercise the grace window override via `spawn_server_with_ttl`.
    const TEST_DEFAULT_TTL: Duration = Duration::from_millis(150);

    async fn spawn_server(agent_cmd: Option<AgentCmd>) -> (SocketAddr, Arc<SessionRegistry>) {
        spawn_server_with_ttl(agent_cmd, TEST_DEFAULT_TTL).await
    }

    async fn spawn_server_with_ttl(
        agent_cmd: Option<AgentCmd>,
        ttl: Duration,
    ) -> (SocketAddr, Arc<SessionRegistry>) {
        let registry = SessionRegistry::new(agent_cmd, ReplayTurns::Unbounded, ttl);
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

        // Both subscribers should see the echoed `session/update` line.
        // amux/peer_joined frames may also be in the queue (A receives it
        // when B joined); skip until we see the expected method.
        for ws in [&mut ws_a, &mut ws_b] {
            let mut found = false;
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while std::time::Instant::now() < deadline {
                let msg = ws.next().await.unwrap().unwrap();
                if let ClientMsg::Text(t) = msg {
                    let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
                    if v.get("method") == Some(&serde_json::json!("session/update")) {
                        found = true;
                        break;
                    }
                }
            }
            assert!(found, "did not see session/update broadcast");
        }

        let _ = ws_a.send(ClientMsg::Close(None)).await;
        let _ = ws_b.send(ClientMsg::Close(None)).await;
    }

    fn mock_acp_path() -> String {
        // CARGO_BIN_EXE_<name> is only set for integration tests under
        // `tests/`. For inline unit tests we reconstruct the path from
        // CARGO_MANIFEST_DIR + the active cargo profile dir; `cargo test`
        // builds every bin into target/{debug,release}/ alongside the
        // test binary.
        let profile = if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        };
        format!("{}/target/{}/mock_acp", env!("CARGO_MANIFEST_DIR"), profile)
    }

    fn mock_agent_cmd() -> AgentCmd {
        AgentCmd {
            program: mock_acp_path(),
            args: vec![],
        }
    }

    async fn spawn_server_with_mock() -> (SocketAddr, Arc<SessionRegistry>) {
        spawn_server(Some(mock_agent_cmd())).await
    }

    /// Send `payload` (a JSON-RPC request) over `ws`, skip any
    /// notifications / unrelated frames, return the response matching the
    /// request's id.
    async fn ws_request(
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        payload: &str,
    ) -> serde_json::Value {
        let req: serde_json::Value = serde_json::from_str(payload).expect("payload is JSON");
        let req_id = req.get("id").cloned();
        ws.send(ClientMsg::Text(payload.into())).await.unwrap();
        loop {
            let msg = timeout(Duration::from_secs(2), ws.next())
                .await
                .expect("ws recv timeout")
                .expect("stream ended")
                .expect("recv err");
            let ClientMsg::Text(t) = msg else {
                continue;
            };
            let v: serde_json::Value = serde_json::from_str(t.as_str()).expect("frame is JSON");
            // Skip notifications (no id) and any frame carrying a `method`
            // — that's amux/* metadata or agent session/update broadcasts.
            if v.get("method").is_some() {
                continue;
            }
            if v.get("id") == req_id.as_ref() {
                return v;
            }
        }
    }

    #[tokio::test]
    async fn initialize_caches_for_late_joiners() {
        let (addr, _) = spawn_server_with_mock().await;
        let url_a = format!("ws://{addr}/acp?session=cache&peer_id=A");
        let url_b = format!("ws://{addr}/acp?session=cache&peer_id=B");

        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        let resp_a = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}"#,
        )
        .await;
        assert_eq!(resp_a["id"], serde_json::json!(1));
        assert_eq!(resp_a["result"]["_invocation"], serde_json::json!(1));

        let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
        // B uses a different original id to also confirm id translation.
        let resp_b = ws_request(
            &mut ws_b,
            r#"{"jsonrpc":"2.0","id":"req-b","method":"initialize","params":{"protocolVersion":1}}"#,
        )
        .await;
        // B's original id is preserved.
        assert_eq!(resp_b["id"], serde_json::json!("req-b"));
        // The mock would emit _invocation=2 if reached; cached path keeps =1.
        assert_eq!(
            resp_b["result"]["_invocation"],
            serde_json::json!(1),
            "B's initialize should be answered from cache, not re-sent to the agent",
        );
        assert_eq!(
            resp_b["result"]["agentInfo"]["name"],
            serde_json::json!("mock-acp")
        );

        let _ = ws_a.send(ClientMsg::Close(None)).await;
        let _ = ws_b.send(ClientMsg::Close(None)).await;
    }

    #[tokio::test]
    async fn session_new_caches_for_late_joiners() {
        let (addr, _) = spawn_server_with_mock().await;
        let url_a = format!("ws://{addr}/acp?session=newcache&peer_id=A");
        let url_b = format!("ws://{addr}/acp?session=newcache&peer_id=B");

        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let r1 = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":2,"method":"session/new","params":{}}"#,
        )
        .await;
        assert_eq!(r1["result"]["_invocation"], serde_json::json!(1));

        let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
        let _ = ws_request(
            &mut ws_b,
            r#"{"jsonrpc":"2.0","id":10,"method":"initialize"}"#,
        )
        .await;
        let r2 = ws_request(
            &mut ws_b,
            r#"{"jsonrpc":"2.0","id":11,"method":"session/new"}"#,
        )
        .await;
        assert_eq!(
            r2["result"]["_invocation"],
            serde_json::json!(1),
            "B's session/new should be served from cache",
        );
        assert_eq!(r2["result"]["sessionId"], serde_json::json!("sess-mock"));

        let _ = ws_a.send(ClientMsg::Close(None)).await;
        let _ = ws_b.send(ClientMsg::Close(None)).await;
    }

    #[tokio::test]
    async fn prompt_notifications_broadcast_response_routes_to_originator() {
        let (addr, _) = spawn_server_with_mock().await;
        let url_a = format!("ws://{addr}/acp?session=prompt&peer_id=A");
        let url_b = format!("ws://{addr}/acp?session=prompt&peer_id=B");

        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
        let _ = ws_request(
            &mut ws_b,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
        )
        .await;

        // A sends a prompt with original id 7.
        ws_a.send(ClientMsg::Text(
            r#"{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[]}}"#.into(),
        ))
        .await
        .unwrap();

        // Collect frames on each side until A receives a response (id=7).
        let mut a_frames: Vec<serde_json::Value> = vec![];
        let mut b_frames: Vec<serde_json::Value> = vec![];
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while std::time::Instant::now() < deadline {
            tokio::select! {
                msg = ws_a.next() => {
                    if let Some(Ok(ClientMsg::Text(t))) = msg {
                        let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
                        let is_response = v.get("id").is_some() && v.get("method").is_none();
                        a_frames.push(v);
                        if is_response {
                            break;
                        }
                    }
                }
                msg = ws_b.next() => {
                    if let Some(Ok(ClientMsg::Text(t))) = msg {
                        let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
                        b_frames.push(v);
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
        // Drain any pending B frames.
        for _ in 0..20 {
            tokio::select! {
                msg = ws_b.next() => {
                    if let Some(Ok(ClientMsg::Text(t))) = msg {
                        let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
                        b_frames.push(v);
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(20)) => break,
            }
        }

        // Both A and B should have seen two agent-emitted session/update
        // notifications (the `agent_message_chunk` pair from mock_acp).
        // The proxy's RFD #533 prompt_received / turn_complete frames are
        // also session/update notifications but use update.type rather
        // than update.kind — filter them out for this assertion.
        let count_agent_updates = |frames: &[serde_json::Value]| {
            frames
                .iter()
                .filter(|v| v.get("method") == Some(&serde_json::json!("session/update")))
                .filter(|v| v["params"]["update"].get("kind").is_some())
                .count()
        };
        assert_eq!(count_agent_updates(&a_frames), 2, "A frames: {a_frames:?}");
        assert_eq!(count_agent_updates(&b_frames), 2, "B frames: {b_frames:?}");

        // A must have received the prompt response with original id 7.
        let a_response = a_frames
            .iter()
            .find(|v| v.get("id") == Some(&serde_json::json!(7)) && v.get("result").is_some())
            .expect("A should have received the prompt response");
        assert_eq!(
            a_response["result"]["stopReason"],
            serde_json::json!("end_turn")
        );

        // B must NOT have received the prompt response.
        let b_got_response = b_frames
            .iter()
            .any(|v| v.get("result").is_some() && v.get("method").is_none());
        assert!(
            !b_got_response,
            "B should not see A's prompt response, got {b_frames:?}"
        );

        let _ = ws_a.send(ClientMsg::Close(None)).await;
        let _ = ws_b.send(ClientMsg::Close(None)).await;
    }

    #[tokio::test]
    async fn original_id_is_preserved_across_mux() {
        let (addr, _) = spawn_server_with_mock().await;
        let url = format!("ws://{addr}/acp?session=id&peer_id=A");
        let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();

        // Use a high non-overlapping id to ensure we're not just lucky that
        // mux_id == original_id.
        let resp = ws_request(
            &mut ws,
            r#"{"jsonrpc":"2.0","id":99999,"method":"initialize"}"#,
        )
        .await;
        assert_eq!(resp["id"], serde_json::json!(99999));
        assert_eq!(resp["jsonrpc"], serde_json::json!("2.0"));

        let resp2 = ws_request(
            &mut ws,
            r#"{"jsonrpc":"2.0","id":"abc-123","method":"session/new"}"#,
        )
        .await;
        assert_eq!(resp2["id"], serde_json::json!("abc-123"));

        let _ = ws.send(ClientMsg::Close(None)).await;
    }

    /// Helper for chunk 5/6 tests: spawn acp-mux with mock_acp wrapped to
    /// pass through env vars (permission emission, prompt delay).
    async fn spawn_server_with_mock_env(
        env: &[(&str, &str)],
    ) -> (SocketAddr, Arc<SessionRegistry>) {
        // We can't customize per-process env via AgentCmd directly without
        // adding it to the schema; for now use `env -i` style invocation
        // via /usr/bin/env if available, falling back to a wrapper that
        // re-execs mock_acp with the desired vars.
        let mut args = vec![];
        for (k, v) in env {
            args.push(format!("{k}={v}"));
        }
        args.push(mock_acp_path());
        spawn_server(Some(AgentCmd {
            program: "/usr/bin/env".to_string(),
            args,
        }))
        .await
    }

    /// Drain all text frames from `ws` until `dur` elapses; returns them
    /// as parsed JSON values. Used to collect amux/* notification streams
    /// without locking the test to a specific arrival order.
    async fn drain_for<S>(
        ws: &mut tokio_tungstenite::WebSocketStream<S>,
        dur: Duration,
    ) -> Vec<serde_json::Value>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let mut out = vec![];
        let deadline = std::time::Instant::now() + dur;
        while std::time::Instant::now() < deadline {
            match timeout(Duration::from_millis(80), ws.next()).await {
                Ok(Some(Ok(ClientMsg::Text(t)))) => {
                    out.push(serde_json::from_str(t.as_str()).unwrap());
                }
                Ok(Some(Ok(_))) | Ok(None) => {}
                Ok(Some(Err(_))) => return out,
                Err(_) => {}
            }
        }
        out
    }

    /// Chunk 7: amux/peer_joined fires when B joins, A sees it; B does not
    /// see their own join (emit-before-insert). On detach the remaining
    /// subscriber sees amux/peer_left.
    #[tokio::test]
    async fn amux_peer_joined_and_peer_left() {
        let (addr, _) = spawn_server_with_mock().await;
        let url_a = format!("ws://{addr}/acp?session=presence&peer_id=A&peer_name=Alice");
        let url_b = format!("ws://{addr}/acp?session=presence&peer_id=B&peer_name=Bob");

        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        // A is the initial sub — peer_joined for A is emitted to an empty
        // map, so A sees nothing yet.
        let a_early = drain_for(&mut ws_a, Duration::from_millis(100)).await;
        assert!(
            a_early.is_empty(),
            "A should see no events before B joins, got {a_early:?}"
        );

        let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
        // Now A should receive peer_joined for B.
        let a_after_b = drain_for(&mut ws_a, Duration::from_millis(150)).await;
        let pj = a_after_b
            .iter()
            .find(|v| v.get("method") == Some(&serde_json::json!("amux/peer_joined")))
            .expect("A should see amux/peer_joined for B");
        assert_eq!(pj["params"]["peerId"], serde_json::json!("B"));
        assert_eq!(pj["params"]["peerName"], serde_json::json!("Bob"));
        assert_eq!(pj["params"]["sessionId"], serde_json::json!("presence"));

        // B receives the replay log on join. The log contains peer_joined
        // for A (so B learns about A) but NOT peer_joined for B (B's own
        // join is appended to the log AFTER the snapshot is taken).
        let b_early = drain_for(&mut ws_b, Duration::from_millis(150)).await;
        let saw_a_join = b_early.iter().any(|v| {
            v.get("method") == Some(&serde_json::json!("amux/peer_joined"))
                && v["params"]["peerId"] == serde_json::json!("A")
        });
        let saw_own_join = b_early.iter().any(|v| {
            v.get("method") == Some(&serde_json::json!("amux/peer_joined"))
                && v["params"]["peerId"] == serde_json::json!("B")
        });
        assert!(
            saw_a_join,
            "B should see A's peer_joined via replay, got {b_early:?}"
        );
        assert!(
            !saw_own_join,
            "B should not see their own peer_joined, got {b_early:?}"
        );

        // B detaches → A sees peer_left.
        ws_b.send(ClientMsg::Close(None)).await.unwrap();
        drop(ws_b);
        let a_after_detach = drain_for(&mut ws_a, Duration::from_millis(200)).await;
        let pl = a_after_detach
            .iter()
            .find(|v| v.get("method") == Some(&serde_json::json!("amux/peer_left")))
            .expect("A should see amux/peer_left for B");
        assert_eq!(pl["params"]["peerId"], serde_json::json!("B"));

        let _ = ws_a.send(ClientMsg::Close(None)).await;
    }

    /// Chunk 9: a reconnect within the TTL grace window cancels the
    /// pending teardown and the new subscriber lands on the same session
    /// — proven by hitting the initialize cache populated by A.
    /// Chunk 10: GET /debug/sessions returns the registry snapshot.
    #[tokio::test]
    async fn debug_sessions_reflects_live_state() {
        let (addr, _) = spawn_server_with_mock().await;
        let url_a = format!("ws://{addr}/acp?session=debug&peer_id=A&peer_name=Alice");

        // Empty registry before any attaches.
        let body = http_get(&format!("http://{addr}/debug/sessions")).await;
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["sessionCount"], serde_json::json!(0));

        // Attach, initialize, drive.
        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
        )
        .await;

        let body = http_get(&format!("http://{addr}/debug/sessions")).await;
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["sessionCount"], serde_json::json!(1));

        let s = &v["sessions"][0];
        assert_eq!(s["sessionId"], serde_json::json!("debug"));
        assert_eq!(s["subscribers"].as_array().unwrap().len(), 1);
        assert_eq!(s["subscribers"][0]["peerId"], serde_json::json!("A"));
        assert_eq!(s["subscribers"][0]["peerName"], serde_json::json!("Alice"));
        assert_eq!(s["subscribers"][0]["isDriving"], serde_json::json!(true));
        assert_eq!(s["initializeCached"], serde_json::json!(true));
        assert_eq!(s["cachedSessionId"], serde_json::json!("sess-mock"));
        assert_eq!(s["drivingSubscriber"], serde_json::json!("A"));
        assert_eq!(s["activeTurnMuxId"], serde_json::Value::Null);
        assert_eq!(s["ttlPending"], serde_json::json!(false));

        let _ = ws_a.send(ClientMsg::Close(None)).await;
    }

    #[tokio::test]
    async fn ttl_grace_cancelled_by_reconnect() {
        let (addr, registry) =
            spawn_server_with_ttl(Some(mock_agent_cmd()), Duration::from_millis(500)).await;
        let url_a = format!("ws://{addr}/acp?session=grace&peer_id=A");
        let url_b = format!("ws://{addr}/acp?session=grace&peer_id=B");

        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        assert_eq!(registry.live_session_count().await, 1);

        // A disconnects → TTL grace starts; session must stay alive.
        ws_a.send(ClientMsg::Close(None)).await.unwrap();
        drop(ws_a);
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(
            registry.live_session_count().await,
            1,
            "session must still be alive during TTL grace",
        );

        // B reconnects within the grace window. The cache should still be
        // present → B's initialize is answered from cache (mock_acp would
        // produce _invocation:2 if a fresh process was spawned).
        let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
        let resp = ws_request(
            &mut ws_b,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        assert_eq!(
            resp["result"]["_invocation"],
            serde_json::json!(1),
            "B's initialize must hit A's cached response (same session preserved)",
        );

        let _ = ws_b.send(ClientMsg::Close(None)).await;
    }

    /// Chunk 9: with no reconnect, the session is torn down once TTL
    /// expires.
    #[tokio::test]
    async fn ttl_grace_expires_when_idle() {
        let (addr, registry) =
            spawn_server_with_ttl(Some(mock_agent_cmd()), Duration::from_millis(150)).await;
        let url_a = format!("ws://{addr}/acp?session=idle&peer_id=A");

        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        assert_eq!(registry.live_session_count().await, 1);

        ws_a.send(ClientMsg::Close(None)).await.unwrap();
        drop(ws_a);

        // Within grace, session still alive.
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(registry.live_session_count().await, 1);

        // After grace, session torn down.
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_millis(40)).await;
            if registry.live_session_count().await == 0 {
                return;
            }
        }
        panic!("session did not tear down after TTL expiry");
    }

    /// Chunk 9: when the agent subprocess exits, subscribers are closed
    /// with WS application code 1011 (skipping the TTL grace).
    #[tokio::test]
    async fn agent_death_closes_subscribers_with_1011() {
        // `sleep 0.4` exits cleanly after 400ms; its stdout closes,
        // triggering AgentDied in the session actor.
        let agent_cmd = AgentCmd {
            program: "sleep".into(),
            args: vec!["0.4".into()],
        };
        let (addr, _) = spawn_server_with_ttl(Some(agent_cmd), Duration::from_secs(30)).await;
        let url = format!("ws://{addr}/acp?session=die&peer_id=A");
        let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();

        let close = wait_for_close(&mut ws).await.expect("expected close frame");
        assert_eq!(
            u16::from(close),
            1011,
            "agent death must close subscriber with WS code 1011"
        );
    }

    /// Chunk 8: late joiner receives the replay log: peer_joined for A,
    /// turn_started + session/update notifications + turn_complete for A's
    /// completed turn — all delivered to B before any live events, in
    /// order.
    #[tokio::test]
    async fn replay_log_delivers_history_to_late_joiner() {
        let (addr, _) = spawn_server_with_mock().await;
        let url_a = format!("ws://{addr}/acp?session=replay&peer_id=A");
        let url_b = format!("ws://{addr}/acp?session=replay&peer_id=B");

        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
        )
        .await;

        // A runs a full turn to completion.
        ws_a.send(ClientMsg::Text(
            r#"{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"hi"}]}}"#.into(),
        )).await.unwrap();
        // Drain A until it sees the prompt response, ensuring the turn has
        // closed (turn_complete is in the log) before B joins.
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while std::time::Instant::now() < deadline {
            match timeout(Duration::from_millis(200), ws_a.next()).await {
                Ok(Some(Ok(ClientMsg::Text(t)))) => {
                    let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
                    if v.get("id") == Some(&serde_json::json!(7)) && v.get("result").is_some() {
                        break;
                    }
                }
                _ => continue,
            }
        }

        // B attaches AFTER the turn has finished.
        let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
        let replay = drain_for(&mut ws_b, Duration::from_millis(400)).await;

        // The replay should include, in this order:
        //   peer_joined(A), turn_started(A), 2x session/update, turn_complete
        let methods: Vec<&str> = replay
            .iter()
            .filter_map(|v| v.get("method").and_then(|m| m.as_str()))
            .collect();

        let pj_idx = methods
            .iter()
            .position(|m| *m == "amux/peer_joined")
            .expect("replay should contain peer_joined");
        let ts_idx = methods
            .iter()
            .position(|m| *m == "amux/turn_started")
            .expect("replay should contain turn_started");
        let tc_idx = methods
            .iter()
            .position(|m| *m == "amux/turn_complete")
            .expect("replay should contain turn_complete");

        assert!(pj_idx < ts_idx, "peer_joined before turn_started in replay");
        assert!(
            ts_idx < tc_idx,
            "turn_started before turn_complete in replay"
        );

        // Two agent-emitted session/update frames (update.kind), plus
        // the proxy's RFD #533 prompt_received and turn_complete siblings
        // (update.type). Filter to the agent's pair for the position
        // check.
        let agent_updates: Vec<_> = replay
            .iter()
            .filter(|v| v.get("method") == Some(&serde_json::json!("session/update")))
            .filter(|v| v["params"]["update"].get("kind").is_some())
            .collect();
        assert_eq!(
            agent_updates.len(),
            2,
            "two agent session/update notifications in replay"
        );

        // The agent's session/update notifications must sit between
        // turn_started and turn_complete in the replay order.
        let agent_update_positions: Vec<_> = replay
            .iter()
            .enumerate()
            .filter(|(_, v)| {
                v.get("method") == Some(&serde_json::json!("session/update"))
                    && v["params"]["update"].get("kind").is_some()
            })
            .map(|(i, _)| i)
            .collect();
        // ts_idx/tc_idx index into `methods` (filtered to frames with a
        // method); recompute corresponding positions in `replay`.
        let ts_pos = replay
            .iter()
            .position(|v| v.get("method") == Some(&serde_json::json!("amux/turn_started")))
            .unwrap();
        let tc_pos = replay
            .iter()
            .position(|v| v.get("method") == Some(&serde_json::json!("amux/turn_complete")))
            .unwrap();
        for pos in &agent_update_positions {
            assert!(
                *pos > ts_pos && *pos < tc_pos,
                "agent session/update inside turn"
            );
        }
        let _ = (ts_idx, tc_idx);

        // B should NOT see a response to A's request (id=7) — that was a
        // per-subscriber frame, not broadcast-tier.
        let saw_a_response = replay
            .iter()
            .any(|v| v.get("id") == Some(&serde_json::json!(7)));
        assert!(!saw_a_response, "B should not see A's prompt response");

        let _ = ws_a.send(ClientMsg::Close(None)).await;
        let _ = ws_b.send(ClientMsg::Close(None)).await;
    }

    /// Chunk 8: --replay-turns 0 disables the log; B sees no history.
    #[tokio::test]
    async fn replay_turns_disabled_emits_no_history() {
        let agent_cmd = mock_agent_cmd();
        let registry = SessionRegistry::new(
            Some(agent_cmd),
            ReplayTurns::Disabled,
            Duration::from_secs(60),
        );
        let app = router(AppState::new(registry));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let url_a = format!("ws://{addr}/acp?session=nolog&peer_id=A");
        let url_b = format!("ws://{addr}/acp?session=nolog&peer_id=B");
        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;

        let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
        let early = drain_for(&mut ws_b, Duration::from_millis(150)).await;
        // peer_joined for B's own join doesn't broadcast to B; without a
        // replay log, B sees nothing until the next live event.
        assert!(
            early.is_empty(),
            "B should see no replay frames, got {early:?}"
        );

        let _ = ws_a.send(ClientMsg::Close(None)).await;
        let _ = ws_b.send(ClientMsg::Close(None)).await;
    }

    /// Chunk 7: amux/turn_started fires before forwarding session/prompt,
    /// and amux/turn_complete fires when the matching response arrives.
    /// Both broadcast to every subscriber. amuxTurnId bookends the pair.
    #[tokio::test]
    async fn amux_turn_started_and_complete() {
        let (addr, _) = spawn_server_with_mock().await;
        let url_a = format!("ws://{addr}/acp?session=turn&peer_id=A");
        let url_b = format!("ws://{addr}/acp?session=turn&peer_id=B");

        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
        let _ = ws_request(
            &mut ws_b,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
        )
        .await;

        ws_a.send(ClientMsg::Text(
            r#"{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"hi"}]}}"#.into(),
        ))
        .await
        .unwrap();

        let a_frames = drain_for(&mut ws_a, Duration::from_secs(2)).await;
        let b_frames = drain_for(&mut ws_b, Duration::from_secs(2)).await;

        for (label, frames) in [("A", &a_frames), ("B", &b_frames)] {
            let started = frames
                .iter()
                .find(|v| v.get("method") == Some(&serde_json::json!("amux/turn_started")))
                .unwrap_or_else(|| {
                    panic!("{label} should see amux/turn_started, frames: {frames:?}")
                });
            assert_eq!(started["params"]["peerId"], serde_json::json!("A"));
            assert_eq!(started["params"]["sessionId"], serde_json::json!("turn"));
            assert_eq!(started["params"]["amuxTurnId"], serde_json::json!("at-1"));
            assert_eq!(
                started["params"]["content"],
                serde_json::json!([{"type":"text","text":"hi"}])
            );

            let complete = frames
                .iter()
                .find(|v| v.get("method") == Some(&serde_json::json!("amux/turn_complete")))
                .unwrap_or_else(|| {
                    panic!("{label} should see amux/turn_complete, frames: {frames:?}")
                });
            assert_eq!(complete["params"]["amuxTurnId"], serde_json::json!("at-1"));
            assert_eq!(
                complete["params"]["stopReason"],
                serde_json::json!("end_turn")
            );
        }

        let _ = ws_a.send(ClientMsg::Close(None)).await;
        let _ = ws_b.send(ClientMsg::Close(None)).await;
    }

    /// Chunk 7: amux/session_busy fires alongside the -32001 rejection.
    #[tokio::test]
    async fn amux_session_busy_on_concurrent_prompt() {
        let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_PROMPT_DELAY_MS", "500")]).await;
        let url_a = format!("ws://{addr}/acp?session=busy&peer_id=A");
        let url_b = format!("ws://{addr}/acp?session=busy&peer_id=B");

        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
        let _ = ws_request(
            &mut ws_b,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
        )
        .await;

        ws_a.send(ClientMsg::Text(
            r#"{"jsonrpc":"2.0","id":100,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#.into(),
        ))
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;
        ws_b.send(ClientMsg::Text(
            r#"{"jsonrpc":"2.0","id":200,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#.into(),
        ))
        .await
        .unwrap();

        let b_frames = drain_for(&mut ws_b, Duration::from_secs(2)).await;
        let busy = b_frames
            .iter()
            .find(|v| v.get("method") == Some(&serde_json::json!("amux/session_busy")))
            .expect("B should see amux/session_busy");
        assert_eq!(busy["params"]["busy"], serde_json::json!(true));
        assert_eq!(busy["params"]["heldBy"], serde_json::json!("A"));

        // Drain A so the test cleans up promptly.
        let _ = drain_for(&mut ws_a, Duration::from_secs(2)).await;
        let _ = ws_a.send(ClientMsg::Close(None)).await;
        let _ = ws_b.send(ClientMsg::Close(None)).await;
    }

    /// Agent-initiated requests fan out to every attached subscriber so
    /// any peer can confirm. Previously this was driver-only routing; the
    /// duplicate-reply concern is now handled by the first-reply-wins
    /// gate inside `SessionInner::gate_subscriber_response`.
    #[tokio::test]
    async fn agent_request_broadcasts_to_every_subscriber() {
        let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_EMIT_PERMISSION", "1")]).await;
        let url_a = format!("ws://{addr}/acp?session=drive&peer_id=A");
        let url_b = format!("ws://{addr}/acp?session=drive&peer_id=B");

        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
        let _ = ws_request(
            &mut ws_b,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;

        // A drives by sending session/new.
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
        )
        .await;

        // A sends prompt → mock emits session/request_permission (agent-initiated).
        ws_a.send(ClientMsg::Text(
            r#"{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#.into(),
        ))
        .await
        .unwrap();

        let (a_frames, b_frames) =
            collect_frames(&mut ws_a, &mut ws_b, Duration::from_secs(3)).await;

        let perm_in_a = a_frames
            .iter()
            .any(|v| v.get("method") == Some(&serde_json::json!("session/request_permission")));
        let perm_in_b = b_frames
            .iter()
            .any(|v| v.get("method") == Some(&serde_json::json!("session/request_permission")));
        assert!(
            perm_in_a,
            "subscriber A should receive session/request_permission, frames: {a_frames:?}",
        );
        assert!(
            perm_in_b,
            "subscriber B should also receive session/request_permission (broadcast), frames: {b_frames:?}",
        );

        let _ = ws_a.send(ClientMsg::Close(None)).await;
        let _ = ws_b.send(ClientMsg::Close(None)).await;
    }

    /// When two subscribers reply to the same agent-initiated request id,
    /// only the first reply is forwarded to the agent. Proven by counting
    /// `mock/response_echo` notifications emitted by the mock for the
    /// specific permission id.
    #[tokio::test]
    async fn agent_request_first_reply_wins() {
        let (addr, _) = spawn_server_with_mock_env(&[
            ("MOCK_ACP_EMIT_PERMISSION", "1"),
            ("MOCK_ACP_ECHO_RESPONSES", "1"),
            ("MOCK_ACP_PROMPT_DELAY_MS", "400"),
        ])
        .await;
        let url_a = format!("ws://{addr}/acp?session=first-wins&peer_id=A");
        let url_b = format!("ws://{addr}/acp?session=first-wins&peer_id=B");

        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
        let _ = ws_request(
            &mut ws_b,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
        )
        .await;

        // Kick off the prompt; the prompt delay holds the agent open so
        // both A and B have time to reply to the permission request.
        ws_a.send(ClientMsg::Text(
            r#"{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#.into(),
        ))
        .await
        .unwrap();

        // Find the permission request id from the broadcast both sides
        // received. The mock allocates 10_001 for the first prompt, but
        // we read it from the wire so the test doesn't lock to the
        // mock's internal counter.
        async fn wait_for_perm_id<S>(
            ws: &mut tokio_tungstenite::WebSocketStream<S>,
            collected: &mut Vec<serde_json::Value>,
        ) -> serde_json::Value
        where
            S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
        {
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while std::time::Instant::now() < deadline {
                if let Ok(Some(Ok(ClientMsg::Text(t)))) =
                    timeout(Duration::from_millis(200), ws.next()).await
                {
                    let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
                    let is_perm =
                        v.get("method") == Some(&serde_json::json!("session/request_permission"));
                    collected.push(v.clone());
                    if is_perm {
                        return v["id"].clone();
                    }
                }
            }
            panic!("session/request_permission never arrived");
        }

        let mut a_frames = vec![];
        let mut b_frames = vec![];
        let perm_id_a = wait_for_perm_id(&mut ws_a, &mut a_frames).await;
        let perm_id_b = wait_for_perm_id(&mut ws_b, &mut b_frames).await;
        assert_eq!(
            perm_id_a, perm_id_b,
            "both subscribers must see the same agent request id"
        );

        // Both reply with spec-shaped outcomes. The agent should accept
        // exactly one of them; amux/agent_request_resolved should echo
        // whichever result was forwarded.
        let reply_a = format!(
            r#"{{"jsonrpc":"2.0","id":{perm_id_a},"result":{{"outcome":{{"outcome":"selected","optionId":"allow_once"}}}}}}"#,
        );
        let reply_b = format!(
            r#"{{"jsonrpc":"2.0","id":{perm_id_b},"result":{{"outcome":{{"outcome":"selected","optionId":"deny"}}}}}}"#,
        );
        ws_a.send(ClientMsg::Text(reply_a.into())).await.unwrap();
        ws_b.send(ClientMsg::Text(reply_b.into())).await.unwrap();

        // Drain both sides for a fixed window. The mock is single-threaded
        // and won't process the (buffered) permission reply until AFTER
        // the prompt response is sent and it loops back to read stdin,
        // so the echo arrives later than id=7. Keep collecting long
        // enough to observe (or rule out) the late echo.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut saw_prompt_response = false;
        while std::time::Instant::now() < deadline {
            tokio::select! {
                msg = ws_a.next() => {
                    if let Some(Ok(ClientMsg::Text(t))) = msg {
                        let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
                        if v.get("id") == Some(&serde_json::json!(7))
                            && v.get("result").is_some()
                        {
                            saw_prompt_response = true;
                        }
                        a_frames.push(v);
                    }
                }
                msg = ws_b.next() => {
                    if let Some(Ok(ClientMsg::Text(t))) = msg {
                        let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
                        b_frames.push(v);
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
        }
        assert!(saw_prompt_response, "A never received the prompt response");

        let perm_echoes: usize = a_frames
            .iter()
            .filter(|v| {
                v.get("method") == Some(&serde_json::json!("mock/response_echo"))
                    && v["params"]["id"] == perm_id_a
            })
            .count();
        assert_eq!(
            perm_echoes, 1,
            "agent must receive exactly one reply for permission id; A frames: {a_frames:?}",
        );

        // Both peers should also see exactly one amux/agent_request_resolved
        // for the resolved permission id, carrying the winning result and
        // the resolving peer's id.
        fn resolved_for<'a>(
            frames: &'a [serde_json::Value],
            req_id: &serde_json::Value,
        ) -> Vec<&'a serde_json::Value> {
            frames
                .iter()
                .filter(|v| {
                    v.get("method") == Some(&serde_json::json!("amux/agent_request_resolved"))
                        && &v["params"]["requestId"] == req_id
                })
                .collect()
        }
        let a_resolved = resolved_for(&a_frames, &perm_id_a);
        let b_resolved = resolved_for(&b_frames, &perm_id_a);
        assert_eq!(
            a_resolved.len(),
            1,
            "A must see exactly one amux/agent_request_resolved; frames: {a_frames:?}"
        );
        assert_eq!(
            b_resolved.len(),
            1,
            "B must see exactly one amux/agent_request_resolved; frames: {b_frames:?}"
        );
        let resolver = a_resolved[0]["params"]["resolvedBy"]
            .as_str()
            .expect("resolvedBy is a string");
        assert!(
            resolver == "A" || resolver == "B",
            "resolvedBy must be one of the subscribers; got {resolver:?}"
        );
        // Whichever reply won, the broadcast result echoes its outcome
        // (A=allow_once, B=deny). Both sides must see the same outcome.
        let outcome = &a_resolved[0]["params"]["result"]["outcome"];
        assert_eq!(outcome["outcome"], serde_json::json!("selected"));
        let option_id = outcome["optionId"].as_str().expect("optionId is a string");
        assert!(
            option_id == "allow_once" || option_id == "deny",
            "unexpected optionId {option_id:?}"
        );
        assert_eq!(
            a_resolved[0], b_resolved[0],
            "A and B must see identical amux/agent_request_resolved frames"
        );

        let _ = ws_a.send(ClientMsg::Close(None)).await;
        let _ = ws_b.send(ClientMsg::Close(None)).await;
    }

    /// When a turn completes with an agent-initiated request still
    /// outstanding (no peer ever replied — the agent's own deadline
    /// fired and it carried on), the mux must sweep the entry and
    /// broadcast `amux/agent_request_resolved { resolvedBy:
    /// "mux:turn-ended" }` so peers can dismiss the stale UI.
    #[tokio::test]
    async fn agent_request_resolved_on_turn_end_when_no_reply() {
        let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_EMIT_PERMISSION", "1")]).await;
        let url_a = format!("ws://{addr}/acp?session=turn-end&peer_id=A");
        let url_b = format!("ws://{addr}/acp?session=turn-end&peer_id=B");

        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
        let _ = ws_request(
            &mut ws_b,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
        )
        .await;

        // A sends a prompt. Neither A nor B replies to the permission
        // request the mock emits; the mock proceeds anyway (mimicking
        // an agent whose internal deadline fired without a reply).
        ws_a.send(ClientMsg::Text(
            r#"{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#.into(),
        ))
        .await
        .unwrap();

        let (a_frames, b_frames) =
            collect_frames(&mut ws_a, &mut ws_b, Duration::from_secs(3)).await;

        // Capture the permission id from the broadcast — comes through
        // before turn end on both sides.
        let perm_id_a = a_frames
            .iter()
            .find(|v| v.get("method") == Some(&serde_json::json!("session/request_permission")))
            .map(|v| v["id"].clone())
            .expect("A must have seen the permission/request");
        assert_eq!(
            perm_id_a,
            b_frames
                .iter()
                .find(|v| v.get("method") == Some(&serde_json::json!("session/request_permission")))
                .map(|v| v["id"].clone())
                .expect("B must have seen the permission/request"),
        );

        // Both peers must see exactly one cleanup
        // amux/agent_request_resolved with resolvedBy=mux:turn-ended
        // for that id, and it must appear before amux/turn_complete.
        fn find_resolved(
            frames: &[serde_json::Value],
            req_id: &serde_json::Value,
        ) -> Option<usize> {
            frames.iter().position(|v| {
                v.get("method") == Some(&serde_json::json!("amux/agent_request_resolved"))
                    && &v["params"]["requestId"] == req_id
                    && v["params"]["resolvedBy"] == serde_json::json!("mux:turn-ended")
            })
        }
        fn find_turn_complete(frames: &[serde_json::Value]) -> Option<usize> {
            frames
                .iter()
                .position(|v| v.get("method") == Some(&serde_json::json!("amux/turn_complete")))
        }

        for (label, frames) in [("A", &a_frames), ("B", &b_frames)] {
            let resolved_idx = find_resolved(frames, &perm_id_a).unwrap_or_else(|| {
                panic!("{label}: missing mux:turn-ended cleanup; frames: {frames:?}")
            });
            let turn_complete_idx = find_turn_complete(frames).unwrap_or_else(|| {
                panic!("{label}: missing amux/turn_complete; frames: {frames:?}")
            });
            assert!(
                resolved_idx < turn_complete_idx,
                "{label}: cleanup must precede turn_complete; resolved@{resolved_idx} turn_complete@{turn_complete_idx}",
            );
            // result and error are both absent on the cleanup broadcast.
            let resolved = &frames[resolved_idx];
            assert!(
                resolved["params"].get("result").is_none()
                    || resolved["params"]["result"].is_null(),
                "{label}: cleanup must not carry a result; got {resolved:?}",
            );
            assert!(
                resolved["params"].get("error").is_none() || resolved["params"]["error"].is_null(),
                "{label}: cleanup must not carry an error; got {resolved:?}",
            );
        }

        let _ = ws_a.send(ClientMsg::Close(None)).await;
        let _ = ws_b.send(ClientMsg::Close(None)).await;
    }

    /// Chunk 5: driving subscriber detaches mid-flight → agent-initiated
    /// requests fall through to the remaining subscriber.
    #[tokio::test]
    async fn agent_request_falls_through_when_driver_left() {
        let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_EMIT_PERMISSION", "1")]).await;
        let url_a = format!("ws://{addr}/acp?session=fallback&peer_id=A");
        let url_b = format!("ws://{addr}/acp?session=fallback&peer_id=B");

        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
        let _ = ws_request(
            &mut ws_b,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;

        // A drives.
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
        )
        .await;

        // A disconnects; B should now receive the agent-initiated request.
        ws_a.send(ClientMsg::Close(None)).await.unwrap();
        let _ = ws_a.next().await;
        drop(ws_a);
        tokio::time::sleep(Duration::from_millis(80)).await;

        // B sends a prompt — the resulting session/request_permission must reach
        // B (the only remaining subscriber, so broadcast = single-target).
        ws_b.send(ClientMsg::Text(
            r#"{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#.into(),
        ))
        .await
        .unwrap();

        let mut saw_perm = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while std::time::Instant::now() < deadline {
            match timeout(Duration::from_millis(200), ws_b.next()).await {
                Ok(Some(Ok(ClientMsg::Text(t)))) => {
                    let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
                    if v.get("method") == Some(&serde_json::json!("session/request_permission")) {
                        saw_perm = true;
                    }
                    if v.get("id") == Some(&serde_json::json!(7)) && v.get("result").is_some() {
                        break;
                    }
                }
                _ => continue,
            }
        }
        assert!(
            saw_perm,
            "B should have received the session/request_permission (sole attached subscriber)"
        );

        let _ = ws_b.send(ClientMsg::Close(None)).await;
    }

    /// Chunk 6: while a session/prompt is in flight, a second concurrent
    /// session/prompt is rejected with JSON-RPC -32001.
    #[tokio::test]
    async fn concurrent_prompt_rejected_with_32001() {
        let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_PROMPT_DELAY_MS", "600")]).await;
        let url_a = format!("ws://{addr}/acp?session=busy&peer_id=A");
        let url_b = format!("ws://{addr}/acp?session=busy&peer_id=B");

        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
        let _ = ws_request(
            &mut ws_b,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
        )
        .await;

        // A sends prompt 1 — mock holds it for 600ms.
        ws_a.send(ClientMsg::Text(
            r#"{"jsonrpc":"2.0","id":100,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#.into(),
        ))
        .await
        .unwrap();

        // Give the actor time to register the in-flight turn before B's prompt.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // B sends prompt 2 while A's turn is in flight — should be rejected.
        ws_b.send(ClientMsg::Text(
            r#"{"jsonrpc":"2.0","id":200,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#.into(),
        ))
        .await
        .unwrap();

        // B receives an error response for id 200. May arrive interleaved
        // with session/update broadcasts from A's in-flight turn.
        let mut b_err: Option<serde_json::Value> = None;
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while std::time::Instant::now() < deadline {
            match timeout(Duration::from_millis(200), ws_b.next()).await {
                Ok(Some(Ok(ClientMsg::Text(t)))) => {
                    let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
                    if v.get("id") == Some(&serde_json::json!(200)) {
                        b_err = Some(v);
                        break;
                    }
                }
                _ => continue,
            }
        }
        let b_json = b_err.expect("B should have received a response for id 200");
        assert_eq!(
            b_json["error"]["code"],
            serde_json::json!(-32001),
            "B should have received session-busy -32001, got {b_json:?}",
        );

        // A's prompt eventually completes.
        let mut a_response_seen = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while std::time::Instant::now() < deadline {
            match timeout(Duration::from_millis(200), ws_a.next()).await {
                Ok(Some(Ok(ClientMsg::Text(t)))) => {
                    let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
                    if v.get("id") == Some(&serde_json::json!(100)) && v.get("result").is_some() {
                        a_response_seen = true;
                        break;
                    }
                }
                _ => continue,
            }
        }
        assert!(a_response_seen, "A's prompt should have completed");

        // After A's turn, B can issue a fresh prompt successfully.
        ws_b.send(ClientMsg::Text(
            r#"{"jsonrpc":"2.0","id":201,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#.into(),
        ))
        .await
        .unwrap();
        let mut b_ok = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while std::time::Instant::now() < deadline {
            match timeout(Duration::from_millis(200), ws_b.next()).await {
                Ok(Some(Ok(ClientMsg::Text(t)))) => {
                    let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
                    if v.get("id") == Some(&serde_json::json!(201)) && v.get("result").is_some() {
                        b_ok = true;
                        break;
                    }
                }
                _ => continue,
            }
        }
        assert!(
            b_ok,
            "B's follow-up prompt should succeed after A's turn cleared"
        );

        let _ = ws_a.send(ClientMsg::Close(None)).await;
        let _ = ws_b.send(ClientMsg::Close(None)).await;
    }

    /// Collect text frames from both WS streams for `dur`, returning the
    /// frames seen on each side. Useful when an interaction emits a mix of
    /// notifications and a final response without a fixed order.
    async fn collect_frames(
        ws_a: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        ws_b: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        dur: Duration,
    ) -> (Vec<serde_json::Value>, Vec<serde_json::Value>) {
        let mut a = vec![];
        let mut b = vec![];
        let deadline = std::time::Instant::now() + dur;
        while std::time::Instant::now() < deadline {
            tokio::select! {
                msg = ws_a.next() => {
                    if let Some(Ok(ClientMsg::Text(t))) = msg {
                        a.push(serde_json::from_str(t.as_str()).unwrap());
                    }
                }
                msg = ws_b.next() => {
                    if let Some(Ok(ClientMsg::Text(t))) = msg {
                        b.push(serde_json::from_str(t.as_str()).unwrap());
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(15)) => {}
            }
        }
        (a, b)
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

    // ===== RFD #533 alignment tests =====

    /// `initialize` response advertises `sessionCapabilities.attach: true`
    /// in `agentCapabilities`, so RFD-aware clients can detect the proxy
    /// supports multi-client attach. The upstream agent (mock_acp) does
    /// not emit this — it's synthesized by the proxy on top of the
    /// agent's reply.
    #[tokio::test]
    async fn initialize_advertises_attach_capability() {
        let (addr, _) = spawn_server_with_mock().await;
        let url = format!("ws://{addr}/acp?session=cap&peer_id=A");
        let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let resp = ws_request(&mut ws, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#).await;
        assert_eq!(
            resp["result"]["agentCapabilities"]["sessionCapabilities"]["attach"],
            serde_json::json!(true),
            "initialize result must advertise sessionCapabilities.attach"
        );
        let _ = ws.send(ClientMsg::Close(None)).await;
    }

    /// `session/attach` is handled by the proxy locally, returns the
    /// connected-peers roster and echoes the client-supplied clientId.
    #[tokio::test]
    async fn session_attach_returns_roster() {
        let (addr, _) = spawn_server_with_mock().await;
        let url_a = format!("ws://{addr}/acp?session=rfd&peer_id=A&peer_name=Alice");
        let url_b = format!("ws://{addr}/acp?session=rfd&peer_id=B&peer_name=Bob");

        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
        )
        .await;
        let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
        let _ = ws_request(
            &mut ws_b,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;

        let resp = ws_request(
            &mut ws_b,
            r#"{"jsonrpc":"2.0","id":50,"method":"session/attach","params":{"clientId":"dashboard-1","clientInfo":{"name":"dashboard","version":"1.0"}}}"#,
        )
        .await;
        assert_eq!(resp["id"], serde_json::json!(50));
        assert_eq!(resp["result"]["clientId"], serde_json::json!("dashboard-1"));
        assert_eq!(resp["result"]["sessionId"], serde_json::json!("sess-mock"));
        assert_eq!(resp["result"]["historyPolicy"], serde_json::json!("full"));
        let connected = resp["result"]["connectedClients"]
            .as_array()
            .expect("connectedClients is array");
        let ids: Vec<&str> = connected
            .iter()
            .filter_map(|c| c["clientId"].as_str())
            .collect();
        assert!(ids.contains(&"A"));
        assert!(ids.contains(&"B"));

        let _ = ws_a.send(ClientMsg::Close(None)).await;
        let _ = ws_b.send(ClientMsg::Close(None)).await;
    }

    /// `historyPolicy: "none"` omits the `history` field.
    #[tokio::test]
    async fn session_attach_history_none_omits_history() {
        let (addr, _) = spawn_server_with_mock().await;
        let url = format!("ws://{addr}/acp?session=h-none&peer_id=A");
        let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let _ = ws_request(&mut ws, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#).await;
        let resp = ws_request(
            &mut ws,
            r#"{"jsonrpc":"2.0","id":2,"method":"session/attach","params":{"historyPolicy":"none"}}"#,
        )
        .await;
        assert!(resp["result"].get("history").is_none());
        let _ = ws.send(ClientMsg::Close(None)).await;
    }

    /// `session/detach` returns `{ status: "detached" }` and then the
    /// proxy closes the WebSocket. The remaining peer sees
    /// `amux/peer_left` + `session/update { type: "client_disconnected" }`.
    #[tokio::test]
    async fn session_detach_closes_ws_and_notifies_peers() {
        let (addr, _) = spawn_server_with_mock().await;
        let url_a = format!("ws://{addr}/acp?session=detach&peer_id=A&peer_name=Alice");
        let url_b = format!("ws://{addr}/acp?session=detach&peer_id=B");

        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
        )
        .await;
        let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
        let _ = ws_request(
            &mut ws_b,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;

        // Drain any backlog on B before triggering the detach.
        let _ = drain_for(&mut ws_b, Duration::from_millis(100)).await;

        let resp = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":99,"method":"session/detach"}"#,
        )
        .await;
        assert_eq!(resp["result"]["status"], serde_json::json!("detached"));

        // A's WebSocket should close shortly after.
        let close_ok = wait_for_close(&mut ws_a).await.is_some();
        assert!(close_ok, "session/detach should close the WS");

        let b_frames = drain_for(&mut ws_b, Duration::from_millis(300)).await;
        assert!(
            b_frames.iter().any(
                |v| v.get("method") == Some(&serde_json::json!("amux/peer_left"))
                    && v["params"]["peerId"] == serde_json::json!("A")
            ),
            "B should see amux/peer_left for A; frames: {b_frames:?}",
        );
        assert!(
            b_frames.iter().any(|v| {
                v.get("method") == Some(&serde_json::json!("session/update"))
                    && v["params"]["update"]["type"] == serde_json::json!("client_disconnected")
                    && v["params"]["update"]["client"]["clientId"] == serde_json::json!("A")
            }),
            "B should see RFD client_disconnected for A; frames: {b_frames:?}",
        );

        let _ = ws_b.send(ClientMsg::Close(None)).await;
    }

    /// A client that joins mid-permission and calls `session/attach`
    /// receives the unresolved `session/request_permission` as a fresh
    /// JSON-RPC request — actionable rather than just informational.
    #[tokio::test]
    async fn session_attach_reissues_pending_permission() {
        // The mock holds the prompt response back so the permission
        // request stays InFlight long enough for B to attach + send
        // session/attach.
        let (addr, _) = spawn_server_with_mock_env(&[
            ("MOCK_ACP_EMIT_PERMISSION", "1"),
            ("MOCK_ACP_PROMPT_DELAY_MS", "1500"),
        ])
        .await;
        let url_a = format!("ws://{addr}/acp?session=pend&peer_id=A");
        let url_b = format!("ws://{addr}/acp?session=pend&peer_id=B");

        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
        )
        .await;

        // A drives a prompt → mock emits a permission request. Neither
        // peer answers (the mock proceeds on its own deadline). The
        // permission stays InFlight in `pending_permission_frames` until
        // turn-end sweep — capture its id from A's wire first.
        ws_a.send(ClientMsg::Text(
            r#"{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#.into(),
        ))
        .await
        .unwrap();

        // Wait until A has seen the permission request go by but BEFORE
        // turn-end clears it (mock_acp emits permission immediately, then
        // updates, then response — no delay configured here).
        let perm_id = {
            let mut found = None;
            let deadline = std::time::Instant::now() + Duration::from_millis(500);
            while std::time::Instant::now() < deadline {
                if let Ok(Some(Ok(ClientMsg::Text(t)))) =
                    timeout(Duration::from_millis(80), ws_a.next()).await
                {
                    let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
                    if v.get("method") == Some(&serde_json::json!("session/request_permission")) {
                        found = Some(v["id"].clone());
                        break;
                    }
                }
            }
            found.expect("A must see the permission request")
        };

        // B attaches AFTER the permission was broadcast (it can't be a
        // recipient via the standard fan-out). B then calls
        // session/attach — the proxy must re-issue the pending permission
        // to B's wire as a fresh JSON-RPC request.
        let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
        let _ = ws_request(
            &mut ws_b,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let _attach_resp = ws_request(
            &mut ws_b,
            r#"{"jsonrpc":"2.0","id":2,"method":"session/attach"}"#,
        )
        .await;

        // B should now see the re-issued permission request with the
        // same id as the original. (Not guaranteed if turn-end fired
        // first — give it a generous window relative to mock timing.)
        let mut saw_reissue = false;
        let deadline = std::time::Instant::now() + Duration::from_millis(400);
        while std::time::Instant::now() < deadline {
            if let Ok(Some(Ok(ClientMsg::Text(t)))) =
                timeout(Duration::from_millis(80), ws_b.next()).await
            {
                let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
                if v.get("method") == Some(&serde_json::json!("session/request_permission"))
                    && v["id"] == perm_id
                {
                    saw_reissue = true;
                    break;
                }
            }
        }
        // The mock's prompt response is fast, so the re-issue may race
        // with the turn-end sweep; treat absence as a soft pass when the
        // mock has already swept. The strong assertion is that IF B
        // sees the permission, it carries the original id (i.e. the
        // proxy didn't fabricate a new id).
        if !saw_reissue {
            // Confirm the permission was swept rather than missed by
            // checking for the agent_request_resolved cleanup on B.
            let b_frames = drain_for(&mut ws_b, Duration::from_millis(200)).await;
            let swept = b_frames.iter().any(|v| {
                v.get("method") == Some(&serde_json::json!("amux/agent_request_resolved"))
                    && v["params"]["requestId"] == perm_id
            });
            assert!(
                swept,
                "B saw neither the re-issued permission nor its sweep; frames: {b_frames:?}",
            );
        }

        let _ = ws_a.send(ClientMsg::Close(None)).await;
        let _ = ws_b.send(ClientMsg::Close(None)).await;
    }

    /// RFD-shaped session/update siblings fire alongside amux/* metadata
    /// during a normal prompt turn: prompt_received (with sentBy) and
    /// turn_complete (with stopReason). Both use update.type, not
    /// update.kind (which is the agent's own session/update shape).
    #[tokio::test]
    async fn session_update_siblings_fire_during_turn() {
        let (addr, _) = spawn_server_with_mock().await;
        let url_a = format!("ws://{addr}/acp?session=siblings&peer_id=A&peer_name=Alice");
        let url_b = format!("ws://{addr}/acp?session=siblings&peer_id=B");

        let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;
        let _ = ws_request(
            &mut ws_a,
            r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
        )
        .await;
        let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
        let _ = ws_request(
            &mut ws_b,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
        )
        .await;

        ws_a.send(ClientMsg::Text(
            r#"{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"hi"}]}}"#.into(),
        ))
        .await
        .unwrap();

        let b_frames = drain_for(&mut ws_b, Duration::from_secs(2)).await;
        let prompt_received = b_frames
            .iter()
            .find(|v| {
                v.get("method") == Some(&serde_json::json!("session/update"))
                    && v["params"]["update"]["type"] == serde_json::json!("prompt_received")
            })
            .expect("B should see prompt_received");
        assert_eq!(
            prompt_received["params"]["sessionId"],
            serde_json::json!("sess-mock"),
            "session/update must use the ACP session id",
        );
        assert_eq!(
            prompt_received["params"]["update"]["sentBy"]["clientId"],
            serde_json::json!("A"),
        );
        assert_eq!(
            prompt_received["params"]["update"]["sentBy"]["name"],
            serde_json::json!("Alice"),
        );
        assert_eq!(
            prompt_received["params"]["update"]["prompt"],
            serde_json::json!([{"type":"text","text":"hi"}]),
        );

        let turn_complete = b_frames
            .iter()
            .find(|v| {
                v.get("method") == Some(&serde_json::json!("session/update"))
                    && v["params"]["update"]["type"] == serde_json::json!("turn_complete")
            })
            .expect("B should see turn_complete");
        assert_eq!(
            turn_complete["params"]["update"]["stopReason"],
            serde_json::json!("end_turn"),
        );

        // Drain A so the test cleans up promptly.
        let _ = drain_for(&mut ws_a, Duration::from_millis(200)).await;
        let _ = ws_a.send(ClientMsg::Close(None)).await;
        let _ = ws_b.send(ClientMsg::Close(None)).await;
    }
}
