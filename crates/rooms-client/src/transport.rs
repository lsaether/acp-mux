use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use url::Url;

use crate::connection::{AttachConfig, build_attach_url};
use crate::events::{Event, event_from_value};
use crate::protocol::{build_attach, build_initialize};

pub const DEFAULT_CHANNEL_CAPACITY: usize = 64;
pub const INITIALIZE_REQUEST_ID: &str = "rooms-client.initialize";
pub const ATTACH_REQUEST_ID: &str = "rooms-client.attach";

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("failed to build rooms attach URL: {0}")]
    AttachUrl(#[from] url::ParseError),
    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ClientCommand {
    SendFrame(Value),
    Shutdown,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InboundMessage {
    Frame { raw: Value, event: Option<Event> },
    Error(String),
    Closed,
}

pub fn connect_error_hint(error: &str, attach_url: &str) -> String {
    let lower = error.to_ascii_lowercase();
    let endpoint = endpoint_summary(attach_url);
    let peer_id = peer_id_from_url(attach_url).unwrap_or_else(|| "this peer".to_string());

    if lower.contains("peer_id")
        || lower.contains("peer-id")
        || lower.contains("peer id")
        || lower.contains("4409")
        || lower.contains("409 conflict")
    {
        return format!(
            "peer-id collision for {peer_id}; choose a different --peer-id or close the existing client before reconnecting ({error})"
        );
    }

    if lower.contains("404") || lower.contains("not found") {
        return format!(
            "wrong endpoint {endpoint}; rooms-tui expects the websocket attach path /acp on the rooms server ({error})"
        );
    }

    if lower.contains("connection refused")
        || lower.contains("os error 111")
        || lower.contains("failed to connect")
    {
        return format!(
            "could not reach rooms server at {endpoint}; start the rooms server or check the --url host/port ({error})"
        );
    }

    if lower.contains("timed out") || lower.contains("timeout") {
        return format!(
            "websocket attach timed out at {endpoint}; check sleep/network recovery, server availability, and the --url port ({error})"
        );
    }

    format!(
        "websocket attach failed at {endpoint}; check rooms server availability, the --url port, and the /acp endpoint ({error})"
    )
}

pub struct Transport {
    pub inbound: mpsc::Receiver<InboundMessage>,
    pub outbound: mpsc::Sender<ClientCommand>,
    pub task: JoinHandle<Result<(), TransportError>>,
}

pub async fn connect(config: AttachConfig) -> Result<Transport, TransportError> {
    let attach_url = build_attach_url(&config)?;
    let (mut ws, _) = connect_async(&attach_url).await?;

    let initialize = build_initialize(INITIALIZE_REQUEST_ID);
    ws.send(Message::Text(initialize.to_string().into()))
        .await?;

    let attach = build_attach(
        ATTACH_REQUEST_ID,
        None,
        Some(&config.peer_id),
        config.peer_name.as_deref(),
    );
    ws.send(Message::Text(attach.to_string().into())).await?;

    let (mut write, mut read) = ws.split();
    let (inbound_tx, inbound_rx) = mpsc::channel(DEFAULT_CHANNEL_CAPACITY);
    let (outbound_tx, mut outbound_rx) = mpsc::channel(DEFAULT_CHANNEL_CAPACITY);
    let task_attach_url = attach_url.clone();

    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                command = outbound_rx.recv() => {
                    match command {
                        Some(ClientCommand::SendFrame(frame)) => {
                            write.send(Message::Text(frame.to_string().into())).await?;
                        }
                        Some(ClientCommand::Shutdown) | None => {
                            write.send(Message::Close(None)).await?;
                            return Ok(());
                        }
                    }
                }
                message = read.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) => {
                            match serde_json::from_str::<Value>(text.as_str()) {
                                Ok(raw) => {
                                    let event = parse_optional_event(&raw, &inbound_tx).await;
                                    if inbound_tx.send(InboundMessage::Frame { raw, event }).await.is_err() {
                                        return Ok(());
                                    }
                                }
                                Err(err) => {
                                    if inbound_tx.send(InboundMessage::Error(format!("invalid JSON-RPC frame JSON: {err}"))).await.is_err() {
                                        return Ok(());
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Close(frame))) => {
                            if let Some(frame) = frame
                                && let Some(hint) = close_frame_hint(&frame, &task_attach_url)
                            {
                                let _ = inbound_tx.send(InboundMessage::Error(hint)).await;
                            }
                            let _ = inbound_tx.send(InboundMessage::Closed).await;
                            return Ok(());
                        }
                        Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Binary(_))) => {
                            if inbound_tx.send(InboundMessage::Error("ignored non-text websocket frame".to_string())).await.is_err() {
                                return Ok(());
                            }
                        }
                        Some(Ok(Message::Frame(_))) => {}
                        Some(Err(err)) => {
                            let error = err.to_string();
                            let _ = inbound_tx
                                .send(InboundMessage::Error(format!("websocket error: {error}")))
                                .await;
                            return Err(err.into());
                        }
                        None => {
                            let _ = inbound_tx.send(InboundMessage::Closed).await;
                            return Ok(());
                        }
                    }
                }
            }
        }
    });

    Ok(Transport {
        inbound: inbound_rx,
        outbound: outbound_tx,
        task,
    })
}

async fn parse_optional_event(
    raw: &Value,
    inbound_tx: &mpsc::Sender<InboundMessage>,
) -> Option<Event> {
    raw.get("method")?;
    match event_from_value(raw) {
        Ok(event) => Some(event),
        Err(err) => {
            let _ = inbound_tx
                .send(InboundMessage::Error(format!(
                    "could not parse JSON-RPC event: {err}"
                )))
                .await;
            None
        }
    }
}

fn close_frame_hint(frame: &CloseFrame, attach_url: &str) -> Option<String> {
    if matches!(frame.code, CloseCode::Normal | CloseCode::Away) && frame.reason.is_empty() {
        return None;
    }
    Some(connect_error_hint(
        &format!("websocket closed: {:?}: {}", frame.code, frame.reason),
        attach_url,
    ))
}

fn endpoint_summary(attach_url: &str) -> String {
    let Ok(url) = Url::parse(attach_url) else {
        return attach_url.to_string();
    };
    let host = url.host_str().unwrap_or("<unknown-host>");
    let host_port = match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    };
    format!("{}{}", host_port, url.path())
}

fn peer_id_from_url(attach_url: &str) -> Option<String> {
    let url = Url::parse(attach_url).ok()?;
    url.query_pairs()
        .find(|(key, _)| key == "peer_id")
        .map(|(_, value)| value.into_owned())
}
