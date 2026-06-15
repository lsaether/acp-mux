use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use rooms_client::transport::{ClientCommand, InboundMessage, connect, connect_error_hint};
use rooms_client::{AttachConfig, Event};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn connect_bootstraps_initialize_then_attach_and_streams_typed_events() {
    let (listener, addr) = server_listener().await;
    let seen_uri = Arc::new(Mutex::new(None::<String>));
    let seen_uri_for_server = seen_uri.clone();
    let (ready_tx, ready_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws =
            tokio_tungstenite::accept_hdr_async(stream, |req: &Request, response: Response| {
                *seen_uri_for_server.lock().unwrap() = Some(req.uri().to_string());
                Ok(response)
            })
            .await
            .unwrap();

        let first = read_json(&mut ws).await;
        assert_eq!(first["method"], json!("initialize"));

        let second = read_json(&mut ws).await;
        assert_eq!(second["method"], json!("session/attach"));
        assert_eq!(second["params"]["clientId"], json!("desktop"));
        assert_eq!(second["params"]["clientInfo"]["name"], json!("Desktop"));
        assert_eq!(second["params"]["historyPolicy"], json!("full_lineage"));
        assert_eq!(
            second["params"]["_meta"]["rooms"]["historyDelivery"],
            json!("stream")
        );

        ready_tx.send(()).unwrap();

        ws.send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "method": "rooms/turn_started",
                "params": {
                    "roomId": "demo",
                    "roomsTurnId": "at-1",
                    "peerId": "desktop",
                    "peerName": "Desktop",
                    "content": [{ "type": "text", "text": "hello" }]
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();

        let close = ws.next().await.unwrap().unwrap();
        assert!(matches!(close, Message::Close(_)));
    });

    let mut transport = connect(AttachConfig {
        url: format!("ws://{addr}/acp?theme=black"),
        room: "demo".into(),
        peer_id: "desktop".into(),
        peer_name: Some("Desktop".into()),
    })
    .await
    .unwrap();

    ready_rx.await.unwrap();
    assert_eq!(
        seen_uri.lock().unwrap().as_deref(),
        Some("/acp?theme=black&room=demo&peer_id=desktop&peer_name=Desktop&replay=skip")
    );

    let received = tokio::time::timeout(Duration::from_secs(2), transport.inbound.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        received,
        InboundMessage::Frame {
            raw: json!({
                "jsonrpc": "2.0",
                "method": "rooms/turn_started",
                "params": {
                    "roomId": "demo",
                    "roomsTurnId": "at-1",
                    "peerId": "desktop",
                    "peerName": "Desktop",
                    "content": [{ "type": "text", "text": "hello" }]
                }
            }),
            event: Some(Event::TurnStarted {
                room_id: "demo".into(),
                turn_id: "at-1".into(),
                peer_id: "desktop".into(),
                peer_name: Some("Desktop".into()),
                text: "hello".into(),
            }),
        }
    );

    transport
        .outbound
        .send(ClientCommand::Shutdown)
        .await
        .unwrap();
    transport.task.await.unwrap().unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn outbound_channel_sends_json_frames_to_the_websocket() {
    let (listener, addr) = server_listener().await;
    let (bootstrapped_tx, bootstrapped_rx) = oneshot::channel();
    let (third_frame_tx, third_frame_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

        let _initialize = read_json(&mut ws).await;
        let _attach = read_json(&mut ws).await;
        bootstrapped_tx.send(()).unwrap();

        let third = read_json(&mut ws).await;
        third_frame_tx.send(third).unwrap();

        let close = ws.next().await.unwrap().unwrap();
        assert!(matches!(close, Message::Close(_)));
    });

    let transport = connect(AttachConfig {
        url: format!("ws://{addr}/acp"),
        room: "demo".into(),
        peer_id: "desktop".into(),
        peer_name: None,
    })
    .await
    .unwrap();

    bootstrapped_rx.await.unwrap();
    transport
        .outbound
        .send(ClientCommand::SendFrame(json!({
            "jsonrpc": "2.0",
            "id": "q-1",
            "method": "rooms/queue_prompt",
            "params": { "text": "next" }
        })))
        .await
        .unwrap();

    let third = third_frame_rx.await.unwrap();
    assert_eq!(third["method"], json!("rooms/queue_prompt"));
    assert_eq!(third["params"]["text"], json!("next"));

    transport
        .outbound
        .send(ClientCommand::Shutdown)
        .await
        .unwrap();
    transport.task.await.unwrap().unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn outbound_channel_drop_closes_the_websocket_task() {
    let (listener, addr) = server_listener().await;
    let (bootstrapped_tx, bootstrapped_rx) = oneshot::channel();
    let (close_tx, close_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

        let _initialize = read_json(&mut ws).await;
        let _attach = read_json(&mut ws).await;
        bootstrapped_tx.send(()).unwrap();

        let close = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("client should close when the outbound channel is dropped")
            .unwrap()
            .unwrap();
        close_tx.send(matches!(close, Message::Close(_))).unwrap();
    });

    let transport = connect(AttachConfig {
        url: format!("ws://{addr}/acp"),
        room: "demo".into(),
        peer_id: "desktop".into(),
        peer_name: None,
    })
    .await
    .unwrap();

    bootstrapped_rx.await.unwrap();
    let task = transport.task;
    drop(transport.outbound);
    drop(transport.inbound);

    task.await.unwrap().unwrap();
    assert!(close_rx.await.unwrap());
    server.await.unwrap();
}

#[test]
fn connect_error_hints_are_actionable_for_common_operator_mistakes() {
    let wrong_port = connect_error_hint(
        "IO error: Connection refused (os error 111)",
        "ws://127.0.0.1:8765/acp?room=demo&peer_id=desktop&replay=skip",
    );
    assert!(wrong_port.contains("rooms server"));
    assert!(wrong_port.contains("127.0.0.1:8765"));

    let wrong_endpoint = connect_error_hint(
        "HTTP error: 404 Not Found",
        "ws://127.0.0.1:8765/wrong?room=demo&peer_id=desktop&replay=skip",
    );
    assert!(wrong_endpoint.contains("wrong endpoint"));
    assert!(wrong_endpoint.contains("/acp"));

    let peer_collision = connect_error_hint(
        "HTTP error: 409 Conflict: peer_id already connected",
        "ws://127.0.0.1:8765/acp?room=demo&peer_id=desktop&replay=skip",
    );
    assert!(peer_collision.contains("peer-id collision"));
    assert!(peer_collision.contains("desktop"));
}

#[tokio::test]
async fn peer_id_collision_close_is_reported_as_actionable_error() {
    let (listener, addr) = server_listener().await;

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

        let _initialize = read_json(&mut ws).await;
        let _attach = read_json(&mut ws).await;
        ws.send(Message::Close(Some(CloseFrame {
            code: CloseCode::Library(4409),
            reason: "already connected".into(),
        })))
        .await
        .unwrap();
    });

    let mut transport = connect(AttachConfig {
        url: format!("ws://{addr}/acp"),
        room: "demo".into(),
        peer_id: "desktop".into(),
        peer_name: None,
    })
    .await
    .unwrap();

    let error = tokio::time::timeout(Duration::from_secs(2), transport.inbound.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        error,
        InboundMessage::Error(ref message)
            if message.contains("peer-id collision") && message.contains("desktop")
    ));

    let closed = tokio::time::timeout(Duration::from_secs(2), transport.inbound.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(closed, InboundMessage::Closed);

    transport.task.await.unwrap().unwrap();
    server.await.unwrap();
}

async fn server_listener() -> (TcpListener, std::net::SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (listener, addr)
}

async fn read_json<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let msg = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .expect("websocket read timed out")
        .expect("websocket ended")
        .expect("websocket read failed");
    let Message::Text(text) = msg else {
        panic!("expected text websocket message, got {msg:?}");
    };
    serde_json::from_str(text.as_str()).unwrap()
}
