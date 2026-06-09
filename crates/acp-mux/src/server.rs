//! HTTP + WebSocket surface for the standalone ACP mux.

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

use crate::mux::registry::{ControlPlaneSessionListError, MuxRegistry, RegistryError};
use crate::mux::{MuxMsg, MuxSnapshot};
use crate::subscriber::{OutMsg, Subscriber};

const MUX_ID_MAX_LEN: usize = 128;

pub const CLOSE_CODE_BAD_QUERY: u16 = 4400;
pub const CLOSE_CODE_PEER_CONFLICT: u16 = 4409;
pub const CLOSE_CODE_INTERNAL: u16 = 1011;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<MuxRegistry>,
}

impl AppState {
    pub fn new(registry: Arc<MuxRegistry>) -> Self {
        Self { registry }
    }
}

#[derive(Debug, Deserialize)]
pub struct AttachQuery {
    pub mux: Option<String>,
    pub peer_id: Option<String>,
    pub peer_name: Option<String>,
    pub role: Option<String>,
    pub replay: Option<String>,
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
    muxes: Vec<MuxSnapshot>,
    mux_count: usize,
}

async fn debug_sessions(State(state): State<AppState>) -> impl IntoResponse {
    let muxes = state.registry.snapshot().await;
    let count = muxes.len();
    Json(DebugSessionsResponse {
        muxes,
        mux_count: count,
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
        mux,
        peer_id,
        peer_name,
        role,
        skip_legacy_replay,
    } = validated;

    let (outbound_tx, outbound_rx) = mpsc::unbounded_channel::<OutMsg>();
    let subscriber = Subscriber::new(
        peer_id.clone(),
        peer_name,
        role,
        skip_legacy_replay,
        outbound_tx,
    );

    let handle = match state.registry.attach(&mux, subscriber).await {
        Ok(h) => h,
        Err(RegistryError::PeerIdInUse) => {
            tracing::warn!(%mux, %peer_id, "peer_id collision");
            close_with(
                &mut socket,
                CLOSE_CODE_PEER_CONFLICT,
                "peer_id already attached to this mux",
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
            tracing::error!("mux actor unreachable mid-attach");
            close_with(&mut socket, CLOSE_CODE_INTERNAL, "mux unreachable").await;
            return;
        }
    };

    tracing::info!(%mux, %peer_id, "subscriber attached");

    let (ws_sink, ws_stream) = socket.split();
    let in_mux_tx = handle.tx.clone();
    let in_peer_id = peer_id.clone();
    let out_peer_id = peer_id.clone();
    let in_mux = mux.clone();
    let out_mux = mux.clone();

    tokio::select! {
        _ = ws_in_task(ws_stream, in_peer_id, in_mux_tx, in_mux) => {},
        _ = ws_out_task(ws_sink, outbound_rx, out_peer_id, out_mux) => {},
    }

    let _ = handle
        .tx
        .send(MuxMsg::Detach {
            peer_id: peer_id.clone(),
        })
        .await;
    tracing::debug!(%mux, %peer_id, "ws handler exiting");
}

async fn ws_in_task(
    mut ws_stream: SplitStream<WebSocket>,
    peer_id: String,
    mux_tx: mpsc::Sender<MuxMsg>,
    mux: String,
) {
    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(Message::Text(t)) => {
                let bytes = strip_trailing_newline(t.as_bytes());
                if mux_tx
                    .send(MuxMsg::InboundFromSubscriber {
                        peer_id: peer_id.clone(),
                        bytes,
                    })
                    .await
                    .is_err()
                {
                    tracing::debug!(%mux, %peer_id, "mux actor gone; ws_in exiting");
                    return;
                }
            }
            Ok(Message::Binary(b)) => {
                let bytes = strip_trailing_newline(&b);
                if mux_tx
                    .send(MuxMsg::InboundFromSubscriber {
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
                tracing::debug!(%mux, %peer_id, "ws_in: client close");
                return;
            }
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
            Err(err) => {
                tracing::debug!(%mux, %peer_id, error = %err, "ws recv error");
                return;
            }
        }
    }
}

async fn ws_out_task(
    mut ws_sink: SplitSink<WebSocket, Message>,
    mut outbound_rx: mpsc::UnboundedReceiver<OutMsg>,
    peer_id: String,
    mux: String,
) {
    while let Some(msg) = outbound_rx.recv().await {
        match msg {
            OutMsg::Frame(bytes) => match Utf8Bytes::try_from(bytes) {
                Ok(text) => {
                    if ws_sink.send(Message::Text(text)).await.is_err() {
                        tracing::debug!(%mux, %peer_id, "ws_out: peer dropped");
                        return;
                    }
                }
                Err(err) => {
                    tracing::warn!(%mux, %peer_id, error = %err, "non-UTF8 agent stdout line; dropped");
                }
            },
            OutMsg::Close { code, reason } => {
                tracing::info!(%mux, %peer_id, code, %reason, "ws_out: structured close");
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
    tracing::debug!(%mux, %peer_id, "ws_out: outbound channel closed");
    let _ = ws_sink.close().await;
}

struct ValidatedAttach {
    mux: String,
    peer_id: String,
    peer_name: Option<String>,
    role: Option<String>,
    skip_legacy_replay: bool,
}

fn validate(q: &AttachQuery) -> Result<ValidatedAttach, &'static str> {
    let mux = q.mux.as_deref().ok_or("missing ?mux")?;
    if !is_valid_mux_id(mux) {
        return Err("invalid ?mux (expect ^[A-Za-z0-9_-]{1,128}$)");
    }
    let peer_id = q.peer_id.as_deref().ok_or("missing ?peer_id")?;
    if peer_id.is_empty() {
        return Err("empty ?peer_id");
    }
    let skip_legacy_replay = match q.replay.as_deref() {
        None => false,
        Some("skip") => true,
        Some(_) => return Err("invalid ?replay (expected 'skip')"),
    };
    Ok(ValidatedAttach {
        mux: mux.to_string(),
        peer_id: peer_id.to_string(),
        peer_name: q.peer_name.clone(),
        role: q.role.clone(),
        skip_legacy_replay,
    })
}

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

pub fn is_valid_mux_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= MUX_ID_MAX_LEN
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
