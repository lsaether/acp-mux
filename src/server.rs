//! HTTP + WebSocket surface.
//!
//! Routes:
//! - `GET /healthz` → `200 ok`
//! - `GET /acp`     → WebSocket upgrade
//! - `GET /acp/sessions?cwd=...` → cold-start `session/list` query
//! - `GET /debug/sessions` → live mux session snapshot
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
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::multiplex::subscriber::{OutMsg, ReplayOrder, Subscriber};
use crate::session::registry::{ControlPlaneSessionListError, RegistryError, SessionRegistry};
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
    pub replay_order: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SessionDiscoveryQuery {
    pub cwd: Option<String>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/acp", get(acp_attach))
        .route("/acp/sessions", get(acp_sessions))
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

#[derive(Serialize)]
struct ErrorResponse {
    error: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<String>,
}

async fn acp_sessions(
    State(state): State<AppState>,
    Query(q): Query<SessionDiscoveryQuery>,
) -> Response {
    match state.registry.list_sessions_control_plane(q.cwd).await {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(err) => control_plane_error_response(err),
    }
}

fn control_plane_error_response(err: ControlPlaneSessionListError) -> Response {
    let (status, error, details) = match err {
        ControlPlaneSessionListError::AgentCmdMissing => (
            StatusCode::SERVICE_UNAVAILABLE,
            "agent command not configured",
            None,
        ),
        other => (
            StatusCode::BAD_GATEWAY,
            "agent session/list failed",
            Some(other.to_string()),
        ),
    };
    (status, Json(ErrorResponse { error, details })).into_response()
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
        replay_order,
    } = validated;

    let (outbound_tx, outbound_rx) = mpsc::unbounded_channel::<OutMsg>();
    let subscriber = Subscriber::new(peer_id.clone(), peer_name, role, replay_order, outbound_tx);

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
    replay_order: ReplayOrder,
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
    let replay_order = parse_replay_order(q.replay_order.as_deref())?;
    Ok(ValidatedAttach {
        session: session.to_string(),
        peer_id: peer_id.to_string(),
        peer_name: q.peer_name.clone(),
        role: q.role.clone(),
        replay_order,
    })
}

fn parse_replay_order(value: Option<&str>) -> Result<ReplayOrder, &'static str> {
    match value.unwrap_or("chronological") {
        "" | "chronological" => Ok(ReplayOrder::Chronological),
        "newest_turn_first" => Ok(ReplayOrder::NewestTurnFirst),
        _ => Err("invalid ?replay_order (expected chronological or newest_turn_first)"),
    }
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
            replay_order: None,
        };
        assert!(validate(&q).is_err());

        let q = AttachQuery {
            session: Some("ok".into()),
            peer_id: Some("p1".into()),
            peer_name: Some("Alice".into()),
            role: Some("driver".into()),
            replay_order: Some("newest_turn_first".into()),
        };
        let v = validate(&q).unwrap();
        assert_eq!(v.session, "ok");
        assert_eq!(v.peer_id, "p1");
        assert_eq!(v.peer_name.as_deref(), Some("Alice"));
        assert_eq!(v.role.as_deref(), Some("driver"));
        assert_eq!(v.replay_order, ReplayOrder::NewestTurnFirst);
    }
}
