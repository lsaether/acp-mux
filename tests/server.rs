//! Integration tests for the WebSocket / HTTP surface.
//!
//! These spawn a real `acp-mux` server, drive it over WebSocket, and
//! use the `mock_acp` binary as the agent subprocess. Lives under
//! `tests/` so `CARGO_BIN_EXE_mock_acp` is set automatically by Cargo
//! and the mock binary is built as a dependency of this test crate —
//! no separate CI step needed.
//!
//! Pure unit tests for private server helpers (`strip_trailing_newline`,
//! `validate`, `is_valid_session_id`) stay in `src/server.rs`.

use std::sync::Arc;

use amux::cli::{ClientToolPolicy, ReplayTurns};
use amux::server::{
    AppState, CLOSE_CODE_BAD_QUERY, CLOSE_CODE_INTERNAL, CLOSE_CODE_PEER_CONFLICT, router,
};
use amux::session::registry::{AgentCmd, SessionRegistry};
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

async fn spawn_server_with_meta_propagation(
    agent_cmd: Option<AgentCmd>,
    enabled: bool,
) -> (SocketAddr, Arc<SessionRegistry>) {
    let registry = SessionRegistry::new_with_meta_propagation(
        agent_cmd,
        ReplayTurns::Unbounded,
        TEST_DEFAULT_TTL,
        enabled,
    );
    let app = router(AppState::new(registry.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    (addr, registry)
}

async fn spawn_server_with_client_tool_policy(
    agent_cmd: Option<AgentCmd>,
    client_tool_policy: ClientToolPolicy,
) -> (SocketAddr, Arc<SessionRegistry>) {
    let registry = SessionRegistry::new_with_client_tool_policy(
        agent_cmd,
        ReplayTurns::Unbounded,
        TEST_DEFAULT_TTL,
        false,
        client_tool_policy,
    );
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
    let url = format!("ws://{addr}/acp?session=***");
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

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut saw_opened = false;
    let mut saw_echo = false;
    while std::time::Instant::now() < deadline {
        let received = timeout(Duration::from_millis(100), ws.next())
            .await
            .expect("ws recv timeout")
            .expect("stream ended")
            .expect("recv err");
        let ClientMsg::Text(t) = received else {
            continue;
        };
        if t.as_str() == payload {
            assert!(
                saw_opened,
                "agent request echoes should be preceded by inert amux/agent_request_opened metadata"
            );
            saw_echo = true;
            break;
        }
        let v: serde_json::Value = serde_json::from_str(t.as_str()).expect("frame is JSON");
        if v.get("method") == Some(&serde_json::json!("amux/session_context")) {
            continue;
        }
        if v.get("method") == Some(&serde_json::json!("amux/agent_request_opened")) {
            saw_opened = true;
            continue;
        }
        panic!("expected text echo or amux/agent_request_opened, got {v:?}");
    }
    assert!(
        saw_opened,
        "expected amux/agent_request_opened before raw echo"
    );
    assert!(
        saw_echo,
        "expected raw text echo after amux/agent_request_opened"
    );

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
async fn subscriber_receives_agent_context_cwd_on_attach() {
    let (addr, _) = spawn_server_with_cat().await;
    let expected_cwd = std::env::current_dir()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let url = format!("ws://{addr}/acp?session=ctx&peer_id=p1");
    let (mut ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .expect("ws connect");

    let context = ws_next_method(&mut ws, "amux/session_context").await;

    assert_eq!(context["params"]["sessionId"], serde_json::json!("ctx"));
    assert_eq!(context["params"]["cwd"], serde_json::json!(expected_cwd));

    let _ = ws.send(ClientMsg::Close(None)).await;
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
    // Cargo sets `CARGO_BIN_EXE_<name>` automatically for integration
    // tests under `tests/`, and builds the bin as a dependency. No
    // hardcoded `target/<profile>/` path, no CI special-casing.
    env!("CARGO_BIN_EXE_mock_acp").to_string()
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

async fn ws_next_method<S>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    method: &str,
) -> serde_json::Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        let Ok(Some(Ok(ClientMsg::Text(t)))) = timeout(Duration::from_millis(100), ws.next()).await
        else {
            continue;
        };
        let v: serde_json::Value = serde_json::from_str(t.as_str()).expect("frame is JSON");
        if v.get("method") == Some(&serde_json::json!(method)) {
            return v;
        }
    }
    panic!("timed out waiting for method {method}");
}

async fn ws_next_session_update_type<S>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    update_type: &str,
) -> serde_json::Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        let Ok(Some(Ok(ClientMsg::Text(t)))) = timeout(Duration::from_millis(100), ws.next()).await
        else {
            continue;
        };
        let v: serde_json::Value = serde_json::from_str(t.as_str()).expect("frame is JSON");
        if v.get("method") == Some(&serde_json::json!("session/update"))
            && v["params"]["update"]["type"] == serde_json::json!(update_type)
        {
            return v;
        }
    }
    panic!("timed out waiting for session/update type {update_type}");
}

// ===== RFD #533 multi-client attach facade =====

#[tokio::test]
async fn rfd533_attach_returns_roster_and_history_policy() {
    let (addr, _) = spawn_server_with_mock().await;
    let url = format!("ws://{addr}/acp?session=rfd533&peer_id=A&peer_name=Alice");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let init = ws_request(&mut ws, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#).await;
    assert!(
        init["result"]["agentCapabilities"]["sessionCapabilities"]
            .get("attach")
            .is_none(),
        "amux should not inject attach capability into upstream initialize responses: {init:?}",
    );
    let _ = ws_request(
        &mut ws,
        r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
    )
    .await;

    // Seed replay history so `after_message` fallback has something visible.
    let _ = ws_request(
        &mut ws,
        r#"{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"seed"}]}}"#,
    )
    .await;

    let none = ws_request(
        &mut ws,
        r#"{"jsonrpc":"2.0","id":4,"method":"session/attach","params":{"sessionId":"sess-mock","historyPolicy":"none","clientId":"client-A","clientInfo":{"name":"dash","version":"1.0"}}}"#,
    )
    .await;
    assert_eq!(none["result"]["sessionId"], serde_json::json!("sess-mock"));
    assert_eq!(none["result"]["clientId"], serde_json::json!("client-A"));
    assert_eq!(none["result"]["historyPolicy"], serde_json::json!("none"));
    assert!(
        none["result"].get("history").is_none(),
        "historyPolicy none must omit history: {none:?}",
    );
    assert!(
        none["result"]["connectedClients"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["clientId"] == serde_json::json!("A")
                && c["name"] == serde_json::json!("Alice")),
        "attach result should expose current connected peer roster: {none:?}",
    );

    let after_message = ws_request(
        &mut ws,
        r#"{"jsonrpc":"2.0","id":5,"method":"session/attach","params":{"sessionId":"sess-mock","historyPolicy":"after_message","afterMessageId":"ea87d0e7-beb8-484a-a404-94a30b78a5a8"}}"#,
    )
    .await;
    assert_eq!(
        after_message["result"]["historyPolicy"],
        serde_json::json!("full"),
        "until ACP messageId is end-to-end available, after_message should fall back to full",
    );
    let history = after_message["result"]["history"].as_array().unwrap();
    assert!(
        history
            .iter()
            .any(|entry| entry["method"] == serde_json::json!("session/update")),
        "full fallback history should include replayed broadcast frames: {after_message:?}",
    );

    let _ = ws.send(ClientMsg::Close(None)).await;
}

#[tokio::test]
async fn rfd533_attach_pending_only_reissues_permission_and_resolved_update() {
    let (addr, _) = spawn_server_with_mock_env(&[
        ("MOCK_ACP_EMIT_PERMISSION", "1"),
        ("MOCK_ACP_PROMPT_DELAY_MS", "2000"),
    ])
    .await;
    let url_a = format!("ws://{addr}/acp?session=rfd533&peer_id=A&peer_name=Alice");
    let url_b = format!("ws://{addr}/acp?session=rfd533&peer_id=B&peer_name=Bob");

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
    ws_a.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"needs approval"}]}}"#.into(),
    ))
    .await
    .unwrap();
    let permission_a = ws_next_method(&mut ws_a, "session/request_permission").await;
    let permission_id = permission_a["id"].clone();

    let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
    let _ = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":10,"method":"initialize"}"#,
    )
    .await;
    let attach = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":11,"method":"session/attach","params":{"sessionId":"sess-mock","historyPolicy":"pending_only"}}"#,
    )
    .await;
    assert_eq!(
        attach["result"]["historyPolicy"],
        serde_json::json!("pending_only")
    );
    let history = attach["result"]["history"].as_array().unwrap();
    assert_eq!(history.len(), 1, "pending_only history: {attach:?}");
    assert_eq!(
        history[0]["method"],
        serde_json::json!("session/request_permission")
    );
    assert_eq!(
        history[0]["params"]["toolCall"]["status"],
        serde_json::json!("pending")
    );

    let reissued = ws_next_method(&mut ws_b, "session/request_permission").await;
    assert_eq!(
        reissued["id"], permission_id,
        "pending permission must be re-issued as the original actionable JSON-RPC request",
    );
    ws_b.send(ClientMsg::Text(
        serde_json::to_string(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": permission_id,
            "result": { "outcome": { "outcome": "selected", "optionId": "allow_once" } }
        }))
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let resolved = ws_next_session_update_type(&mut ws_a, "permission_resolved").await;
    assert_eq!(
        resolved["params"]["sessionId"],
        serde_json::json!("sess-mock")
    );
    assert_eq!(
        resolved["params"]["update"]["requestId"],
        permission_a["id"]
    );
    assert_eq!(
        resolved["params"]["update"]["chosenOptionId"],
        serde_json::json!("allow_once")
    );
    assert_eq!(
        resolved["params"]["update"]["resolvedBy"]["clientId"],
        serde_json::json!("B")
    );
    assert_eq!(
        resolved["params"]["update"]["resolvedBy"]["name"],
        serde_json::json!("Bob")
    );

    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
}

#[tokio::test]
async fn rfd533_prompt_turn_and_disconnect_session_updates() {
    let (addr, _) = spawn_server_with_mock().await;
    let url_a = format!("ws://{addr}/acp?session=rfd533&peer_id=A&peer_name=Alice");
    let url_b = format!("ws://{addr}/acp?session=rfd533&peer_id=B&peer_name=Bob");

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
        r#"{"jsonrpc":"2.0","id":10,"method":"initialize"}"#,
    )
    .await;
    let _ = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":11,"method":"session/attach","params":{"sessionId":"sess-mock","historyPolicy":"none"}}"#,
    )
    .await;

    ws_a.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"hello from A"}]}}"#.into(),
    ))
    .await
    .unwrap();

    let prompt_received = ws_next_session_update_type(&mut ws_b, "prompt_received").await;
    assert_eq!(
        prompt_received["params"]["sessionId"],
        serde_json::json!("sess-mock")
    );
    assert_eq!(
        prompt_received["params"]["update"]["sentBy"]["clientId"],
        serde_json::json!("A")
    );
    assert_eq!(
        prompt_received["params"]["update"]["prompt"][0]["text"],
        serde_json::json!("hello from A")
    );

    let turn_complete = ws_next_session_update_type(&mut ws_b, "turn_complete").await;
    assert_eq!(
        turn_complete["params"]["update"]["stopReason"],
        serde_json::json!("end_turn")
    );

    let detached = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":12,"method":"session/detach","params":{"sessionId":"sess-mock"}}"#,
    )
    .await;
    assert_eq!(detached["result"]["status"], serde_json::json!("detached"));
    assert_eq!(
        detached["result"]["sessionId"],
        serde_json::json!("sess-mock")
    );

    let disconnected = ws_next_session_update_type(&mut ws_a, "client_disconnected").await;
    assert_eq!(
        disconnected["params"]["update"]["client"]["clientId"],
        serde_json::json!("B")
    );
    assert_eq!(
        disconnected["params"]["update"]["client"]["name"],
        serde_json::json!("Bob")
    );

    let _ = ws_a.send(ClientMsg::Close(None)).await;
}

// ===== _meta.amux request trace propagation (issue #6) =====

#[tokio::test]
async fn meta_propagate_default_off_leaves_outbound_request_meta_unchanged() {
    let (addr, _) = spawn_server_with_cat().await;
    let url = format!("ws://{addr}/acp?session=meta-default&peer_id=A&peer_name=Alice&role=driver");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();

    ws.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","id":77,"method":"session/list","params":{"cwd":"/tmp","_meta":{"traceparent":"00-abc"}}}"#.into(),
    ))
    .await
    .unwrap();

    let echoed = ws_next_method(&mut ws, "session/list").await;
    assert_eq!(echoed["id"], serde_json::json!(1));
    assert_eq!(
        echoed["params"]["_meta"]["traceparent"],
        serde_json::json!("00-abc")
    );
    assert!(
        echoed["params"]["_meta"].get("amux").is_none(),
        "default-off meta propagation must not add _meta.amux: {echoed:?}",
    );

    let _ = ws.send(ClientMsg::Close(None)).await;
}

#[tokio::test]
async fn meta_propagate_opt_in_adds_peer_trace_to_outbound_requests() {
    let (addr, _) = spawn_server_with_meta_propagation(
        Some(AgentCmd {
            program: "cat".into(),
            args: vec![],
        }),
        true,
    )
    .await;
    let url = format!("ws://{addr}/acp?session=meta-on&peer_id=A&peer_name=Alice&role=driver");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();

    ws.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","id":77,"method":"session/list","params":{"cwd":"/tmp","_meta":{"traceparent":"00-abc","client.example/debug":true,"amux":{"clientTrace":"keep"}}}}"#.into(),
    ))
    .await
    .unwrap();

    let echoed = ws_next_method(&mut ws, "session/list").await;
    assert_eq!(echoed["id"], serde_json::json!(1));
    assert_eq!(
        echoed["params"]["_meta"]["traceparent"],
        serde_json::json!("00-abc")
    );
    assert_eq!(
        echoed["params"]["_meta"]["client.example/debug"],
        serde_json::json!(true),
        "existing non-amux metadata must be preserved",
    );

    let amux = &echoed["params"]["_meta"]["amux"];
    assert_eq!(amux["peerId"], serde_json::json!("A"));
    assert_eq!(amux["peerName"], serde_json::json!("Alice"));
    assert_eq!(amux["role"], serde_json::json!("driver"));
    assert_eq!(amux["muxId"], serde_json::json!(1));
    assert_eq!(amux["clientTrace"], serde_json::json!("keep"));
    assert!(
        amux.get("amuxTurnId").is_none(),
        "non-prompt requests should not carry a turn id: {echoed:?}",
    );

    let _ = ws.send(ClientMsg::Close(None)).await;
}

#[tokio::test]
async fn meta_propagate_prompt_includes_amux_turn_id() {
    let (addr, _) = spawn_server_with_meta_propagation(
        Some(AgentCmd {
            program: "cat".into(),
            args: vec![],
        }),
        true,
    )
    .await;
    let url = format!("ws://{addr}/acp?session=meta-prompt&peer_id=A&peer_name=Alice&role=driver");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();

    ws.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","id":42,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"hi"}]}}"#.into(),
    ))
    .await
    .unwrap();

    let echoed = ws_next_method(&mut ws, "session/prompt").await;
    let amux = &echoed["params"]["_meta"]["amux"];
    assert_eq!(amux["peerId"], serde_json::json!("A"));
    assert_eq!(amux["muxId"], serde_json::json!(1));
    assert_eq!(amux["amuxTurnId"], serde_json::json!("at-1"));

    let _ = ws.send(ClientMsg::Close(None)).await;
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

    // Both A and B should have seen the two agent chunks plus the two
    // proxy-owned RFD #533 session/update siblings.
    let count_updates = |frames: &[serde_json::Value]| {
        frames
            .iter()
            .filter(|v| v.get("method") == Some(&serde_json::json!("session/update")))
            .count()
    };
    let count_agent_updates = |frames: &[serde_json::Value]| {
        frames
            .iter()
            .filter(|v| v.get("method") == Some(&serde_json::json!("session/update")))
            .filter(|v| v["params"]["update"].get("kind").is_some())
            .count()
    };
    assert_eq!(count_updates(&a_frames), 4, "A frames: {a_frames:?}");
    assert_eq!(count_updates(&b_frames), 4, "B frames: {b_frames:?}");
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
async fn spawn_server_with_mock_env(env: &[(&str, &str)]) -> (SocketAddr, Arc<SessionRegistry>) {
    spawn_server(Some(mock_agent_cmd_with_env(env))).await
}

async fn spawn_server_with_mock_env_and_client_tool_policy(
    env: &[(&str, &str)],
    client_tool_policy: ClientToolPolicy,
) -> (SocketAddr, Arc<SessionRegistry>) {
    spawn_server_with_client_tool_policy(Some(mock_agent_cmd_with_env(env)), client_tool_policy)
        .await
}

fn mock_agent_cmd_with_env(env: &[(&str, &str)]) -> AgentCmd {
    // We can't customize per-process env via AgentCmd directly without
    // adding it to the schema; for now use `env -i` style invocation
    // via /usr/bin/env if available, falling back to a wrapper that
    // re-execs mock_acp with the desired vars.
    let mut args = vec![];
    for (k, v) in env {
        args.push(format!("{k}={v}"));
    }
    args.push(mock_acp_path());
    AgentCmd {
        program: "/usr/bin/env".to_string(),
        args,
    }
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
    // map, so A sees only its direct session_context before B joins.
    let a_early = drain_for(&mut ws_a, Duration::from_millis(100)).await;
    assert!(
        a_early
            .iter()
            .any(|v| v.get("method") == Some(&serde_json::json!("amux/session_context"))),
        "A should receive direct session_context on attach, got {a_early:?}"
    );
    assert!(
        a_early
            .iter()
            .all(|v| v.get("method") == Some(&serde_json::json!("amux/session_context"))),
        "A should see no peer/presence events before B joins, got {a_early:?}"
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
    //   peer_joined(A), turn_started(A), prompt_received sibling,
    //   2x agent session/update, turn_complete, turn_complete sibling
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

    let updates: Vec<_> = replay
        .iter()
        .enumerate()
        .filter(|(_, v)| v.get("method") == Some(&serde_json::json!("session/update")))
        .collect();
    assert_eq!(
        updates.len(),
        4,
        "two agent updates plus two RFD #533 siblings in replay"
    );

    // Agent-owned session/update chunks must sit between turn_started
    // and turn_complete in the replay order. Proxy-owned siblings bookend
    // the same turn for RFD #533 clients.
    let agent_update_positions: Vec<_> = replay
        .iter()
        .enumerate()
        .filter(|(_, v)| v.get("method") == Some(&serde_json::json!("session/update")))
        .filter(|(_, v)| v["params"]["update"].get("kind").is_some())
        .map(|(i, _)| i)
        .collect();
    assert_eq!(agent_update_positions.len(), 2);
    for pos in &agent_update_positions {
        assert!(*pos > ts_idx && *pos < tc_idx, "session/update inside turn");
    }
    let proxy_update_types: Vec<_> = replay
        .iter()
        .filter(|v| v.get("method") == Some(&serde_json::json!("session/update")))
        .filter_map(|v| v["params"]["update"]["type"].as_str())
        .collect();
    assert_eq!(proxy_update_types, vec!["prompt_received", "turn_complete"]);

    // B should NOT see a response to A's request (id=7) — that was a
    // per-subscriber frame, not broadcast-tier.
    let saw_a_response = replay
        .iter()
        .any(|v| v.get("id") == Some(&serde_json::json!(7)));
    assert!(!saw_a_response, "B should not see A's prompt response");

    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
}

#[tokio::test]
async fn replay_log_adds_mux_record_metadata_to_late_join_frames() {
    let (addr, _) = spawn_server_with_mock().await;
    let url_a = format!("ws://{addr}/acp?session=replay-meta&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=replay-meta&peer_id=B");

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

    ws_a.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"hi"}]}}"#.into(),
    )).await.unwrap();
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

    let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
    let replay = drain_for(&mut ws_b, Duration::from_millis(400)).await;
    let session_updates: Vec<_> = replay
        .iter()
        .filter(|v| v.get("method") == Some(&serde_json::json!("session/update")))
        .collect();
    assert_eq!(
        session_updates.len(),
        4,
        "expected two agent updates plus two proxy siblings: {replay:?}"
    );

    let mut seqs = Vec::new();
    let mut recorded_ats = Vec::new();
    let mut agent_update_count = 0;
    let mut proxy_update_types = Vec::new();
    for update in &session_updates {
        // ACP payload remains where clients expect it; agent-owned updates
        // retain their original `kind` discriminator while proxy-owned RFD
        // #533 siblings use `type`.
        assert_eq!(
            update["params"]["sessionId"],
            serde_json::json!("sess-mock")
        );
        if update["params"]["update"].get("kind").is_some() {
            agent_update_count += 1;
        }
        if let Some(ty) = update["params"]["update"]["type"].as_str() {
            proxy_update_types.push(ty.to_string());
        }

        let amux = &update["params"]["_meta"]["amux"];
        let recorded_at = amux["recordedAt"]
            .as_str()
            .expect("replay metadata should include recordedAt");
        assert!(
            recorded_at.ends_with('Z') && recorded_at.contains('T'),
            "recordedAt should be an RFC3339-ish UTC timestamp, got {recorded_at:?}"
        );
        let seq = amux["replaySeq"]
            .as_u64()
            .expect("replay metadata should include numeric replaySeq");
        recorded_ats.push(recorded_at.to_string());
        seqs.push(seq);
    }

    assert_eq!(agent_update_count, 2, "agent chunks should remain intact");
    assert_eq!(
        proxy_update_types,
        vec!["prompt_received".to_string(), "turn_complete".to_string()]
    );
    assert!(
        seqs.windows(2).all(|w| w[0] < w[1]),
        "replaySeq should be monotonic: {seqs:?}"
    );
    assert_eq!(
        recorded_ats.len(),
        4,
        "each replayed update should carry its original record timestamp"
    );

    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
}

#[tokio::test]
async fn replay_log_merges_amux_metadata_without_clobbering_existing_meta() {
    let (addr, _) = spawn_server_with_cat().await;
    let url_a = format!("ws://{addr}/acp?session=replay-meta-merge&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=replay-meta-merge&peer_id=B");

    let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
    let payload = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess-meta","update":{"kind":"tool_call","toolCallId":"tool-1"},"_meta":{"traceparent":"00-abc","zed.dev/debugMode":true}}}"#;
    ws_a.send(ClientMsg::Text(payload.into())).await.unwrap();

    let live = drain_for(&mut ws_a, Duration::from_millis(200)).await;
    let live_update = live
        .iter()
        .find(|v| v.get("method") == Some(&serde_json::json!("session/update")))
        .expect("A should see the live echoed session/update");
    assert_eq!(
        live_update["params"]
            .get("_meta")
            .and_then(|m| m.get("amux")),
        None,
        "live fan-out should not gain replay-only amux metadata"
    );

    let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
    let replay = drain_for(&mut ws_b, Duration::from_millis(300)).await;
    let replay_update = replay
        .iter()
        .find(|v| v.get("method") == Some(&serde_json::json!("session/update")))
        .expect("B should see replayed session/update");

    assert_eq!(
        replay_update["params"]["_meta"]["traceparent"],
        serde_json::json!("00-abc"),
        "replay metadata injection should preserve existing _meta keys"
    );
    assert_eq!(
        replay_update["params"]["_meta"]["zed.dev/debugMode"],
        serde_json::json!(true),
        "replay metadata injection should preserve implementation-specific keys"
    );
    assert!(
        replay_update["params"]["_meta"]["amux"]["recordedAt"].is_string(),
        "replay metadata should add _meta.amux.recordedAt"
    );
    assert!(
        replay_update["params"]["_meta"]["amux"]["replaySeq"].is_u64(),
        "replay metadata should add _meta.amux.replaySeq"
    );

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
    // replay log, B sees only the per-attach session_context until the
    // next live event.
    assert!(
        early
            .iter()
            .any(|v| v.get("method") == Some(&serde_json::json!("amux/session_context"))),
        "B should receive direct session_context on attach, got {early:?}"
    );
    assert!(
        early
            .iter()
            .all(|v| v.get("method") == Some(&serde_json::json!("amux/session_context"))),
        "B should see no replay frames beyond session_context, got {early:?}"
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
            .unwrap_or_else(|| panic!("{label} should see amux/turn_started, frames: {frames:?}"));
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
            .unwrap_or_else(|| panic!("{label} should see amux/turn_complete, frames: {frames:?}"));
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

#[tokio::test]
async fn busy_session_prompt_steer_text_is_rejected() {
    assert_busy_session_prompt_rejected("/steer revise the approach").await;
}

#[tokio::test]
async fn busy_session_prompt_queue_text_is_rejected() {
    assert_busy_session_prompt_rejected("/queue do this next").await;
}

async fn assert_busy_session_prompt_rejected(control_prompt: &str) {
    let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_PROMPT_DELAY_MS", "300")]).await;
    let url_a = format!("ws://{addr}/acp?session=busy-control&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=busy-control&peer_id=B");

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
        r#"{"jsonrpc":"2.0","id":100,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"hi"}]}}"#.into(),
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let control_text = serde_json::to_string(control_prompt).unwrap();
    let control_request = format!(
        r#"{{"jsonrpc":"2.0","id":200,"method":"session/prompt","params":{{"sessionId":"sess-mock","prompt":[{{"type":"text","text":{control_text}}}]}}}}"#,
    );
    ws_b.send(ClientMsg::Text(control_request.into()))
        .await
        .unwrap();

    let b_frames = drain_for(&mut ws_b, Duration::from_secs(1)).await;
    let rejection = b_frames
        .iter()
        .find(|v| v.get("id") == Some(&serde_json::json!(200)))
        .unwrap_or_else(|| panic!("B should receive a busy rejection response: {b_frames:?}"));
    assert_eq!(
        rejection["error"]["code"],
        serde_json::json!(-32001),
        "plain session/prompt slash commands must not bypass mux turn serialization"
    );
    assert!(
        b_frames
            .iter()
            .any(|v| v.get("method") == Some(&serde_json::json!("amux/session_busy"))),
        "plain session/prompt slash commands should still emit amux/session_busy: {b_frames:?}"
    );

    let _ = drain_for(&mut ws_a, Duration::from_secs(1)).await;
    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
}

#[tokio::test]
async fn amux_steer_active_turn_hard_replaces_after_cancel() {
    let (addr, _) = spawn_server_with_mock_env(&[
        ("MOCK_ACP_PROMPT_DELAY_MS", "120"),
        ("MOCK_ACP_ECHO_SESSION_CANCELS", "1"),
    ])
    .await;
    let url_a = format!("ws://{addr}/acp?session=hard-steer&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=hard-steer&peer_id=B");

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
        r#"{"jsonrpc":"2.0","id":100,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"original mission"}]}}"#.into(),
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    ws_b.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","id":200,"method":"amux/steer_active_turn","params":{"sessionId":"sess-mock","text":"revise the approach"}}"#.into(),
    ))
    .await
    .unwrap();

    let (a_frames, b_frames) = collect_frames(&mut ws_a, &mut ws_b, Duration::from_secs(3)).await;
    let control_response = b_frames
        .iter()
        .find(|v| v.get("id") == Some(&serde_json::json!(200)))
        .unwrap_or_else(|| panic!("B should receive hard-steer acceptance response: {b_frames:?}"));
    assert_eq!(
        control_response["result"]["mode"],
        serde_json::json!("hard")
    );
    assert_eq!(
        control_response["result"]["supersedesTurnId"],
        serde_json::json!("at-1")
    );

    for (label, frames) in [("A", &a_frames), ("B", &b_frames)] {
        assert!(
            frames
                .iter()
                .any(|v| v.get("method") == Some(&serde_json::json!("mock/session_cancel_echo"))),
            "{label} should observe ACP-native session/cancel for hard steer: {frames:?}"
        );
        let cancelled = frames
            .iter()
            .find(|v| v.get("method") == Some(&serde_json::json!("amux/turn_cancelled")))
            .unwrap_or_else(|| panic!("{label} should see cancelled original turn: {frames:?}"));
        assert_eq!(cancelled["params"]["cancelledBy"], serde_json::json!("B"));
        assert_eq!(
            cancelled["params"]["reason"],
            serde_json::json!("hard_steer")
        );

        let control = frames
            .iter()
            .find(|v| v.get("method") == Some(&serde_json::json!("amux/control_submitted")))
            .unwrap_or_else(|| panic!("{label} should see hard-steer control event: {frames:?}"));
        assert_eq!(control["params"]["kind"], serde_json::json!("steer"));
        assert_eq!(control["params"]["mode"], serde_json::json!("hard"));

        let turn_started: Vec<_> = frames
            .iter()
            .filter(|v| v.get("method") == Some(&serde_json::json!("amux/turn_started")))
            .collect();
        assert_eq!(
            turn_started.len(),
            2,
            "{label} should see original plus replacement turns: {frames:?}"
        );
        assert_eq!(turn_started[0]["params"]["peerId"], serde_json::json!("A"));
        assert_eq!(turn_started[1]["params"]["peerId"], serde_json::json!("B"));
        assert_eq!(
            turn_started[1]["params"]["supersedesTurnId"],
            serde_json::json!("at-1")
        );
        let replacement_text = turn_started[1]["params"]["content"][0]["text"]
            .as_str()
            .expect("replacement prompt is text");
        assert!(
            replacement_text.starts_with("Active turn steered by peer `B` (supersedes at-1)."),
            "replacement prompt should use compact mux prompt-injection context: {replacement_text}"
        );
        assert!(
            !replacement_text.contains("Previous active turn was interrupted"),
            "replacement prompt should avoid the older verbose preamble: {replacement_text}"
        );
        assert!(replacement_text.contains("Original:\noriginal mission"));
        assert!(replacement_text.contains("Steer:\nrevise the approach"));

        let turn_complete_count = frames
            .iter()
            .filter(|v| v.get("method") == Some(&serde_json::json!("amux/turn_complete")))
            .count();
        assert_eq!(
            turn_complete_count, 2,
            "{label} should see completion for cancelled turn settlement and replacement: {frames:?}"
        );
    }

    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
}

#[tokio::test]
async fn amux_steer_active_turn_without_active_turn_submits_prompt() {
    let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_PROMPT_DELAY_MS", "10")]).await;
    let url_a = format!("ws://{addr}/acp?session=idle-steer&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=idle-steer&peer_id=B");

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

    ws_b.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","id":200,"method":"amux/steer_active_turn","params":{"sessionId":"sess-mock","text":"start from idle steer"}}"#.into(),
    ))
    .await
    .unwrap();

    let (a_frames, b_frames) = collect_frames(&mut ws_a, &mut ws_b, Duration::from_secs(3)).await;
    let control_response = b_frames
        .iter()
        .find(|v| v.get("id") == Some(&serde_json::json!(200)))
        .unwrap_or_else(|| panic!("B should receive idle-steer submission ack: {b_frames:?}"));
    assert_eq!(
        control_response["result"]["accepted"],
        serde_json::json!(true)
    );
    assert_eq!(
        control_response["result"]["mode"],
        serde_json::json!("prompt")
    );
    assert_eq!(
        control_response["result"]["status"],
        serde_json::json!("submitted")
    );
    assert_eq!(
        control_response["result"]["amuxTurnId"],
        serde_json::json!("at-1")
    );
    assert!(
        control_response["result"].get("supersedesTurnId").is_none(),
        "idle steer should not claim to supersede a turn: {control_response:?}"
    );

    for (label, frames) in [("A", &a_frames), ("B", &b_frames)] {
        let control = frames
            .iter()
            .find(|v| v.get("method") == Some(&serde_json::json!("amux/control_submitted")))
            .unwrap_or_else(|| panic!("{label} should see idle steer control event: {frames:?}"));
        assert_eq!(control["params"]["kind"], serde_json::json!("steer"));
        assert_eq!(control["params"]["mode"], serde_json::json!("prompt"));
        assert_eq!(control["params"]["amuxTurnId"], serde_json::json!("at-1"));
        assert_eq!(
            control["params"]["text"],
            serde_json::json!("start from idle steer")
        );

        assert!(
            frames
                .iter()
                .all(|v| v.get("method") != Some(&serde_json::json!("amux/turn_cancelled"))),
            "idle steer must not emit cancellation: {frames:?}"
        );
        assert!(
            frames
                .iter()
                .all(|v| v.get("method") != Some(&serde_json::json!("mock/session_cancel_echo"))),
            "idle steer must not send ACP session/cancel: {frames:?}"
        );
        assert!(
            frames.iter().all(|v| {
                v.get("method") != Some(&serde_json::json!("amux/queue_item_added"))
                    && v.get("method") != Some(&serde_json::json!("amux/queue_item_submitted"))
                    && v.get("method") != Some(&serde_json::json!("amux/queue_item_completed"))
            }),
            "idle steer should not use public queue lifecycle: {frames:?}"
        );

        let turn_started = frames
            .iter()
            .find(|v| v.get("method") == Some(&serde_json::json!("amux/turn_started")))
            .unwrap_or_else(|| panic!("{label} should see idle steer turn start: {frames:?}"));
        assert_eq!(
            turn_started["params"]["amuxTurnId"],
            serde_json::json!("at-1")
        );
        assert_eq!(turn_started["params"]["peerId"], serde_json::json!("B"));
        assert_eq!(
            turn_started["params"]["content"],
            serde_json::json!([{ "type": "text", "text": "start from idle steer" }])
        );
        assert!(
            turn_started["params"].get("supersedesTurnId").is_none(),
            "idle steer turn should not include supersedesTurnId: {turn_started:?}"
        );

        let completed = frames
            .iter()
            .find(|v| v.get("method") == Some(&serde_json::json!("amux/turn_complete")))
            .unwrap_or_else(|| panic!("{label} should see idle steer completion: {frames:?}"));
        assert_eq!(completed["params"]["amuxTurnId"], serde_json::json!("at-1"));
    }

    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
}

#[tokio::test]
async fn amux_steer_active_turn_rejects_second_pending_hard_steer_until_replacement_pops() {
    let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_PROMPT_DELAY_MS", "350")]).await;
    let url_a = format!("ws://{addr}/acp?session=busy-fixes&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=busy-fixes&peer_id=B");

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
        r#"{"jsonrpc":"2.0","id":100,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"original"}]}}"#.into(),
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    let first = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":200,"method":"amux/steer_active_turn","params":{"sessionId":"sess-mock","text":"first steer"}}"#,
    )
    .await;
    assert_eq!(first["result"]["mode"], serde_json::json!("hard"));
    assert_eq!(
        first["result"]["supersedesTurnId"],
        serde_json::json!("at-1")
    );

    let second = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":201,"method":"amux/steer_active_turn","params":{"sessionId":"sess-mock","text":"second steer too soon"}}"#,
    )
    .await;
    assert_eq!(second["error"]["code"], serde_json::json!(-32002));
    assert_eq!(
        second["error"]["message"],
        serde_json::json!("a hard steer is already pending for this turn")
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for hard-steer replacement turn to pop"
        );
        let started = ws_next_method(&mut ws_b, "amux/turn_started").await;
        if started["params"]["amuxTurnId"] == serde_json::json!("at-2") {
            break;
        }
    }

    let third = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":202,"method":"amux/steer_active_turn","params":{"sessionId":"sess-mock","text":"steer replacement"}}"#,
    )
    .await;
    assert_eq!(third["result"]["mode"], serde_json::json!("hard"));
    assert_eq!(
        third["result"]["supersedesTurnId"],
        serde_json::json!("at-2"),
        "pending-hard-steer guard should clear when the replacement prompt pops"
    );

    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
}

#[tokio::test]
async fn amux_queue_prompt_is_mux_owned_and_replays_lifecycle() {
    let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_PROMPT_DELAY_MS", "120")]).await;
    let url_a = format!("ws://{addr}/acp?session=mux-owned-queue&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=mux-owned-queue&peer_id=B");
    let url_c = format!("ws://{addr}/acp?session=mux-owned-queue&peer_id=C");

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
        r#"{"jsonrpc":"2.0","id":100,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"hi"}]}}"#.into(),
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    ws_b.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","id":200,"method":"amux/queue_prompt","params":{"sessionId":"sess-mock","text":"do this next"}}"#.into(),
    ))
    .await
    .unwrap();

    let (a_frames, b_frames) = collect_frames(&mut ws_a, &mut ws_b, Duration::from_secs(3)).await;
    let control_response = b_frames
        .iter()
        .find(|v| v.get("id") == Some(&serde_json::json!(200)))
        .unwrap_or_else(|| panic!("B should receive mux queue acceptance response: {b_frames:?}"));
    assert_eq!(
        control_response["result"]["queueItemId"],
        serde_json::json!("q-1")
    );
    assert_eq!(
        control_response["result"]["status"],
        serde_json::json!("queued")
    );

    for (label, frames) in [("A", &a_frames), ("B", &b_frames)] {
        let added = frames
            .iter()
            .find(|v| v.get("method") == Some(&serde_json::json!("amux/queue_item_added")))
            .unwrap_or_else(|| panic!("{label} should see queue item added: {frames:?}"));
        assert_eq!(added["params"]["queueItemId"], serde_json::json!("q-1"));
        assert_eq!(added["params"]["peerId"], serde_json::json!("B"));
        assert_eq!(added["params"]["text"], serde_json::json!("do this next"));

        let submitted = frames
            .iter()
            .find(|v| v.get("method") == Some(&serde_json::json!("amux/queue_item_submitted")))
            .unwrap_or_else(|| panic!("{label} should see queue item submitted: {frames:?}"));
        assert_eq!(submitted["params"]["queueItemId"], serde_json::json!("q-1"));
        assert_eq!(submitted["params"]["amuxTurnId"], serde_json::json!("at-2"));

        let completed = frames
            .iter()
            .find(|v| v.get("method") == Some(&serde_json::json!("amux/queue_item_completed")))
            .unwrap_or_else(|| panic!("{label} should see queue item completed: {frames:?}"));
        assert_eq!(completed["params"]["queueItemId"], serde_json::json!("q-1"));
        assert_eq!(completed["params"]["amuxTurnId"], serde_json::json!("at-2"));

        let turn_started: Vec<_> = frames
            .iter()
            .filter(|v| v.get("method") == Some(&serde_json::json!("amux/turn_started")))
            .collect();
        assert_eq!(
            turn_started.len(),
            2,
            "{label} should see original and queued turns: {frames:?}"
        );
        assert_eq!(turn_started[1]["params"]["peerId"], serde_json::json!("B"));
        assert_eq!(
            turn_started[1]["params"]["content"],
            serde_json::json!([{ "type": "text", "text": "do this next" }])
        );
    }

    let (mut ws_c, _) = tokio_tungstenite::connect_async(url_c).await.unwrap();
    let replay = drain_for(&mut ws_c, Duration::from_millis(500)).await;
    let methods: Vec<&str> = replay
        .iter()
        .filter_map(|v| v.get("method").and_then(|m| m.as_str()))
        .collect();

    for expected in [
        "amux/queue_item_added",
        "amux/queue_item_submitted",
        "amux/queue_item_completed",
    ] {
        assert!(
            methods.contains(&expected),
            "late joiner should replay {expected}: {replay:?}"
        );
    }
    assert_eq!(
        methods
            .iter()
            .filter(|method| **method == "amux/turn_started")
            .count(),
        2,
        "late joiner should replay original and queued turn starts: {replay:?}"
    );
    assert_eq!(
        methods
            .iter()
            .filter(|method| **method == "amux/turn_complete")
            .count(),
        2,
        "late joiner should replay original and queued turn completions: {replay:?}"
    );
    assert!(
        replay
            .iter()
            .all(|v| v.get("id") != Some(&serde_json::json!(200))),
        "late joiner must not replay B's per-subscriber queue response: {replay:?}"
    );

    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
    let _ = ws_c.send(ClientMsg::Close(None)).await;
}

#[tokio::test]
async fn amux_queue_prompt_without_active_turn_submits_immediately() {
    let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_PROMPT_DELAY_MS", "10")]).await;
    let url_a = format!("ws://{addr}/acp?session=queue_idle&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=queue_idle&peer_id=B");

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

    ws_b.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","id":200,"method":"amux/queue_prompt","params":{"sessionId":"sess-mock","text":"start from idle"}}"#.into(),
    ))
    .await
    .unwrap();

    let (a_frames, b_frames) = collect_frames(&mut ws_a, &mut ws_b, Duration::from_secs(3)).await;
    let control_response = b_frames
        .iter()
        .find(|v| v.get("id") == Some(&serde_json::json!(200)))
        .unwrap_or_else(|| panic!("B should receive immediate queue submission ack: {b_frames:?}"));
    assert_eq!(
        control_response["result"]["queueItemId"],
        serde_json::json!("q-1")
    );
    assert_eq!(
        control_response["result"]["status"],
        serde_json::json!("submitted")
    );

    for (label, frames) in [("A", &a_frames), ("B", &b_frames)] {
        let added = frames
            .iter()
            .find(|v| v.get("method") == Some(&serde_json::json!("amux/queue_item_added")))
            .unwrap_or_else(|| panic!("{label} should see queue item added: {frames:?}"));
        assert_eq!(added["params"]["queueItemId"], serde_json::json!("q-1"));
        assert_eq!(added["params"]["peerId"], serde_json::json!("B"));
        assert_eq!(
            added["params"]["text"],
            serde_json::json!("start from idle")
        );

        let turn_started = frames
            .iter()
            .find(|v| v.get("method") == Some(&serde_json::json!("amux/turn_started")))
            .unwrap_or_else(|| {
                panic!("{label} should see immediate queued turn start: {frames:?}")
            });
        assert_eq!(
            turn_started["params"]["amuxTurnId"],
            serde_json::json!("at-1")
        );
        assert_eq!(turn_started["params"]["peerId"], serde_json::json!("B"));
        assert_eq!(
            turn_started["params"]["content"],
            serde_json::json!([{ "type": "text", "text": "start from idle" }])
        );

        let submitted = frames
            .iter()
            .find(|v| v.get("method") == Some(&serde_json::json!("amux/queue_item_submitted")))
            .unwrap_or_else(|| panic!("{label} should see queue item submitted: {frames:?}"));
        assert_eq!(submitted["params"]["queueItemId"], serde_json::json!("q-1"));
        assert_eq!(submitted["params"]["amuxTurnId"], serde_json::json!("at-1"));

        let completed = frames
            .iter()
            .find(|v| v.get("method") == Some(&serde_json::json!("amux/queue_item_completed")))
            .unwrap_or_else(|| panic!("{label} should see queue item completed: {frames:?}"));
        assert_eq!(completed["params"]["queueItemId"], serde_json::json!("q-1"));
        assert_eq!(completed["params"]["amuxTurnId"], serde_json::json!("at-1"));
    }

    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
}

#[tokio::test]
async fn amux_queue_prompt_rejects_seventh_pending_item() {
    let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_PROMPT_DELAY_MS", "2000")]).await;
    let url_a = format!("ws://{addr}/acp?session=busy-fixes&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=busy-fixes&peer_id=B");

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
        r#"{"jsonrpc":"2.0","id":100,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"hold active"}]}}"#.into(),
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    for i in 1..=6 {
        let payload = format!(
            r#"{{"jsonrpc":"2.0","id":{},"method":"amux/queue_prompt","params":{{"sessionId":"sess-mock","text":"queued {i}"}}}}"#,
            200 + i
        );
        let response = ws_request(&mut ws_b, &payload).await;
        assert_eq!(response["result"]["status"], serde_json::json!("queued"));
        assert_eq!(
            response["result"]["queueItemId"],
            serde_json::json!(format!("q-{i}"))
        );
    }

    let seventh = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":300,"method":"amux/queue_prompt","params":{"sessionId":"sess-mock","text":"queued 7"}}"#,
    )
    .await;
    assert_eq!(seventh["error"]["code"], serde_json::json!(-32003));
    assert_eq!(seventh["error"]["message"], serde_json::json!("queue full"));

    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
}

#[tokio::test]
async fn amux_unqueue_prompt_removes_pending_item_and_replays_removal() {
    let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_PROMPT_DELAY_MS", "600")]).await;
    let url_a = format!("ws://{addr}/acp?session=busy-fixes&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=busy-fixes&peer_id=B");
    let url_c = format!("ws://{addr}/acp?session=busy-fixes&peer_id=C");

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
        r#"{"jsonrpc":"2.0","id":100,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"active"}]}}"#.into(),
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    let queued = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":200,"method":"amux/queue_prompt","params":{"sessionId":"sess-mock","text":"do not run"}}"#,
    )
    .await;
    assert_eq!(queued["result"]["queueItemId"], serde_json::json!("q-1"));

    ws_a.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","id":300,"method":"amux/unqueue_prompt","params":{"queueItemId":"q-1"}}"#.into(),
    ))
    .await
    .unwrap();

    let (a_frames, b_frames) =
        collect_frames(&mut ws_a, &mut ws_b, Duration::from_millis(900)).await;
    let removed = a_frames
        .iter()
        .find(|v| v.get("id") == Some(&serde_json::json!(300)))
        .unwrap_or_else(|| panic!("A should receive unqueue response: {a_frames:?}"));
    assert_eq!(removed["result"]["queueItemId"], serde_json::json!("q-1"));
    assert_eq!(removed["result"]["status"], serde_json::json!("removed"));

    for (label, frames) in [("A", &a_frames), ("B", &b_frames)] {
        assert!(
            frames.iter().any(|v| {
                v.get("method") == Some(&serde_json::json!("amux/queue_item_removed"))
                    && v["params"]["queueItemId"] == serde_json::json!("q-1")
                    && v["params"]["removedBy"] == serde_json::json!("A")
            }),
            "{label} should see queue item removal: {frames:?}"
        );
        assert!(
            frames.iter().all(|v| {
                !(v.get("method") == Some(&serde_json::json!("amux/queue_item_submitted"))
                    && v["params"]["queueItemId"] == serde_json::json!("q-1"))
            }),
            "removed queue item must not submit: {frames:?}"
        );
    }

    let (mut ws_c, _) = tokio_tungstenite::connect_async(url_c).await.unwrap();
    let replay = drain_for(&mut ws_c, Duration::from_millis(400)).await;
    assert!(
        replay.iter().any(|v| {
            v.get("method") == Some(&serde_json::json!("amux/queue_item_removed"))
                && v["params"]["queueItemId"] == serde_json::json!("q-1")
        }),
        "late joiner should replay queue removal: {replay:?}"
    );
    assert!(
        replay.iter().all(|v| {
            !(v.get("method") == Some(&serde_json::json!("amux/queue_item_submitted"))
                && v["params"]["queueItemId"] == serde_json::json!("q-1"))
        }),
        "late replay must not include submission for removed queue item: {replay:?}"
    );

    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
    let _ = ws_c.send(ClientMsg::Close(None)).await;
}

#[tokio::test]
async fn amux_disconnected_queue_owner_persists_without_becoming_driver() {
    let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_PROMPT_DELAY_MS", "500")]).await;
    let url_a = format!("ws://{addr}/acp?session=busy-fixes&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=busy-fixes&peer_id=B");

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
        r#"{"jsonrpc":"2.0","id":100,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"active"}]}}"#.into(),
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    let queued = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":200,"method":"amux/queue_prompt","params":{"sessionId":"sess-mock","text":"queued after disconnect"}}"#,
    )
    .await;
    assert_eq!(queued["result"]["queueItemId"], serde_json::json!("q-1"));

    ws_b.send(ClientMsg::Close(None)).await.unwrap();
    drop(ws_b);
    let after_detach = drain_for(&mut ws_a, Duration::from_millis(200)).await;
    assert!(
        after_detach.iter().any(|v| {
            v.get("method") == Some(&serde_json::json!("amux/queue_item_orphaned"))
                && v["params"]["queueItemId"] == serde_json::json!("q-1")
                && v["params"]["peerId"] == serde_json::json!("B")
        }),
        "remaining peer should see orphaned queued item when owner disconnects: {after_detach:?}"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for disconnected owner's queued prompt to submit"
        );
        let started = ws_next_method(&mut ws_a, "amux/turn_started").await;
        if started["params"]["amuxTurnId"] == serde_json::json!("at-2") {
            assert_eq!(started["params"]["peerId"], serde_json::json!("B"));
            break;
        }
    }

    let body = http_get(&format!("http://{addr}/debug/sessions")).await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let session = &v["sessions"][0];
    assert_ne!(
        session["drivingSubscriber"],
        serde_json::json!("B"),
        "disconnected queued owner must not become the driving subscriber ghost: {session:?}"
    );
    assert!(
        session["subscribers"]
            .as_array()
            .unwrap()
            .iter()
            .all(|sub| sub["peerId"] != serde_json::json!("B")),
        "B should be detached in debug state: {session:?}"
    );

    let _ = ws_a.send(ClientMsg::Close(None)).await;
}

#[tokio::test]
async fn busy_multimodal_control_prompt_still_rejected() {
    let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_PROMPT_DELAY_MS", "300")]).await;
    let url_a = format!("ws://{addr}/acp?session=busy-control&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=busy-control&peer_id=B");

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
        r#"{"jsonrpc":"2.0","id":100,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"hi"}]}}"#.into(),
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    ws_b.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","id":200,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"/steer look here"},{"type":"image","url":"file:///tmp/nope.png"}]}}"#.into(),
    ))
    .await
    .unwrap();

    let b_frames = drain_for(&mut ws_b, Duration::from_secs(1)).await;
    let b_json = b_frames
        .iter()
        .find(|v| v.get("id") == Some(&serde_json::json!(200)))
        .unwrap_or_else(|| panic!("B should receive a rejection response, frames: {b_frames:?}"));
    assert_eq!(
        b_json["error"]["code"],
        serde_json::json!(-32001),
        "non-text busy control prompts must not bypass turn serialization"
    );
    assert!(
        b_frames
            .iter()
            .any(|v| v.get("method") == Some(&serde_json::json!("amux/session_busy"))),
        "non-text busy control prompts should still emit amux/session_busy: {b_frames:?}"
    );

    let _ = drain_for(&mut ws_a, Duration::from_secs(1)).await;
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
        r#"{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#
            .into(),
    ))
    .await
    .unwrap();

    let (a_frames, b_frames) = collect_frames(&mut ws_a, &mut ws_b, Duration::from_secs(3)).await;

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

#[tokio::test]
async fn agent_fs_read_request_blocked_by_default_and_not_broadcast() {
    assert_agent_client_tool_blocked_by_default("fs/read_text_file").await;
}

#[tokio::test]
async fn agent_fs_write_request_blocked_by_default_and_not_broadcast() {
    assert_agent_client_tool_blocked_by_default("fs/write_text_file").await;
}

#[tokio::test]
async fn agent_terminal_create_request_blocked_by_default_and_not_broadcast() {
    assert_agent_client_tool_blocked_by_default("terminal/create").await;
}

#[tokio::test]
async fn unsafe_debug_client_tool_broadcast_preserves_raw_fanout() {
    let (a_frames, b_frames) = drive_agent_client_tool_prompt_with_policy(
        "fs/read_text_file",
        ClientToolPolicy::unsafe_debug_broadcast(),
    )
    .await;

    assert!(
        frames_contain_method(&a_frames, "fs/read_text_file"),
        "unsafe debug should preserve raw fs request fanout to A; frames: {a_frames:?}",
    );
    assert!(
        frames_contain_method(&b_frames, "fs/read_text_file"),
        "unsafe debug should preserve raw fs request fanout to B; frames: {b_frames:?}",
    );
    assert!(
        find_client_tool_block_echo(&a_frames, "fs/read_text_file").is_none(),
        "unsafe debug should not synthesize a blocked error; frames: {a_frames:?}",
    );

    // Permission prompts remain the collaborative broadcast path; this
    // test covers only the scary client-tool escape hatch.
}

#[tokio::test]
async fn initialize_strips_blocked_client_tool_capabilities_by_default() {
    let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_ECHO_INITIALIZE_PARAMS", "1")]).await;
    let url = format!("ws://{addr}/acp?session=clienttool&peer_id=A");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();

    let resp = ws_request(
        &mut ws,
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{"fs":{"readTextFile":true,"writeTextFile":true},"terminal":true,"session":{"load":{}},"customTool":{"enabled":true}}}}"#,
    )
    .await;

    let caps = resp["result"]["_seenInitializeParams"]["clientCapabilities"]
        .as_object()
        .expect("mock should echo clientCapabilities object");
    assert!(
        !caps.contains_key("fs"),
        "blocked default policy must not advertise fs capability to the agent: {caps:?}",
    );
    assert!(
        !caps.contains_key("terminal"),
        "blocked default policy must not advertise terminal capability to the agent: {caps:?}",
    );
    assert_eq!(
        caps.get("session"),
        Some(&serde_json::json!({"load": {}})),
        "non-client-tool capabilities should be preserved",
    );
    assert_eq!(
        caps.get("customTool"),
        Some(&serde_json::json!({"enabled": true})),
        "unknown client capabilities should not be stripped in v1",
    );

    let _ = ws.send(ClientMsg::Close(None)).await;
}

async fn assert_agent_client_tool_blocked_by_default(method: &str) {
    let (a_frames, b_frames) = drive_agent_client_tool_prompt(method).await;

    assert!(
        !frames_contain_method(&a_frames, method),
        "blocked {method} must not be delivered to subscriber A; frames: {a_frames:?}",
    );
    assert!(
        !frames_contain_method(&b_frames, method),
        "blocked {method} must not be delivered to subscriber B; frames: {b_frames:?}",
    );

    let echo = find_client_tool_block_echo(&a_frames, method).unwrap_or_else(|| {
        panic!("blocked {method} should produce a structured error response to the agent; frames: {a_frames:?}")
    });
    assert_eq!(echo["params"]["error"]["code"], serde_json::json!(-32000));
    assert_eq!(
        echo["params"]["error"]["data"]["reason"],
        serde_json::json!("client_tool_blocked"),
    );
    assert_eq!(
        echo["params"]["error"]["data"]["method"],
        serde_json::json!(method),
    );
}

async fn drive_agent_client_tool_prompt(
    method: &str,
) -> (Vec<serde_json::Value>, Vec<serde_json::Value>) {
    let (addr, _) = spawn_server_with_mock_env(&[
        ("MOCK_ACP_EMIT_CLIENT_TOOL", method),
        ("MOCK_ACP_ECHO_RESPONSES", "1"),
    ])
    .await;
    drive_prompt_and_collect(addr).await
}

async fn drive_agent_client_tool_prompt_with_policy(
    method: &str,
    client_tool_policy: ClientToolPolicy,
) -> (Vec<serde_json::Value>, Vec<serde_json::Value>) {
    let (addr, _) = spawn_server_with_mock_env_and_client_tool_policy(
        &[
            ("MOCK_ACP_EMIT_CLIENT_TOOL", method),
            ("MOCK_ACP_ECHO_RESPONSES", "1"),
        ],
        client_tool_policy,
    )
    .await;
    drive_prompt_and_collect(addr).await
}

async fn drive_prompt_and_collect(
    addr: SocketAddr,
) -> (Vec<serde_json::Value>, Vec<serde_json::Value>) {
    let url_a = format!("ws://{addr}/acp?session=clienttool&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=clienttool&peer_id=B");

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
        r#"{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#
            .into(),
    ))
    .await
    .unwrap();

    let frames = collect_frames(&mut ws_a, &mut ws_b, Duration::from_secs(3)).await;
    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
    frames
}

fn frames_contain_method(frames: &[serde_json::Value], method: &str) -> bool {
    frames
        .iter()
        .any(|v| v.get("method") == Some(&serde_json::json!(method)))
}

fn find_client_tool_block_echo<'a>(
    frames: &'a [serde_json::Value],
    method: &str,
) -> Option<&'a serde_json::Value> {
    frames.iter().find(|v| {
        v.get("method") == Some(&serde_json::json!("mock/response_echo"))
            && v["params"]["error"]["data"]["reason"] == serde_json::json!("client_tool_blocked")
            && v["params"]["error"]["data"]["method"] == serde_json::json!(method)
    })
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
        r#"{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#
            .into(),
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
        r#"{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#
            .into(),
    ))
    .await
    .unwrap();

    let (a_frames, b_frames) = collect_frames(&mut ws_a, &mut ws_b, Duration::from_secs(3)).await;

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

    // Both peers must see the inert, replayable
    // amux/agent_request_opened before the cleanup resolution. The raw
    // session/request_permission stays live-only; the opened sibling is
    // the durable audit context for replay clients.
    fn find_opened(frames: &[serde_json::Value], req_id: &serde_json::Value) -> Option<usize> {
        frames.iter().position(|v| {
            v.get("method") == Some(&serde_json::json!("amux/agent_request_opened"))
                && &v["params"]["requestId"] == req_id
        })
    }

    // Both peers must see exactly one cleanup
    // amux/agent_request_resolved with resolvedBy=mux:turn-ended
    // for that id, and it must appear after opened but before
    // amux/turn_complete.
    fn find_resolved(frames: &[serde_json::Value], req_id: &serde_json::Value) -> Option<usize> {
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
        let opened_idx = find_opened(frames, &perm_id_a).unwrap_or_else(|| {
            panic!("{label}: missing amux/agent_request_opened; frames: {frames:?}")
        });
        let resolved_idx = find_resolved(frames, &perm_id_a).unwrap_or_else(|| {
            panic!("{label}: missing mux:turn-ended cleanup; frames: {frames:?}")
        });
        let turn_complete_idx = find_turn_complete(frames)
            .unwrap_or_else(|| panic!("{label}: missing amux/turn_complete; frames: {frames:?}"));
        assert!(
            opened_idx < resolved_idx,
            "{label}: opened must precede cleanup; opened@{opened_idx} resolved@{resolved_idx}",
        );
        assert!(
            resolved_idx < turn_complete_idx,
            "{label}: cleanup must precede turn_complete; resolved@{resolved_idx} turn_complete@{turn_complete_idx}",
        );
        let opened = &frames[opened_idx];
        assert_eq!(
            opened["params"]["requestMethod"],
            serde_json::json!("session/request_permission"),
            "{label}: opened should name the original agent request method"
        );
        assert_eq!(
            opened["params"]["requestParams"]["options"][0]["optionId"],
            serde_json::json!("allow_once"),
            "{label}: opened should carry enough original request context for replay UI"
        );
        assert!(
            opened["params"].get("amuxTurnId").is_some(),
            "{label}: opened should be associated with the active amux turn"
        );
        // result and error are both absent on the cleanup broadcast.
        let resolved = &frames[resolved_idx];
        assert!(
            resolved["params"].get("result").is_none() || resolved["params"]["result"].is_null(),
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

/// Resolved agent-initiated requests should replay as an inert
/// amux/agent_request_opened + amux/agent_request_resolved lifecycle pair,
/// not as a stale actionable JSON-RPC request. Live subscribers still see
/// and answer the raw session/request_permission exactly once.
#[tokio::test]
async fn agent_request_opened_replayed_to_late_joiner_without_actionable_request() {
    let (addr, _) = spawn_server_with_mock_env(&[
        ("MOCK_ACP_EMIT_PERMISSION", "1"),
        ("MOCK_ACP_ECHO_RESPONSES", "1"),
        ("MOCK_ACP_PROMPT_DELAY_MS", "200"),
    ])
    .await;
    let url_a = format!("ws://{addr}/acp?session=agent-request-opened-replay&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=agent-request-opened-replay&peer_id=B");

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

    ws_a.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#
            .into(),
    ))
    .await
    .unwrap();

    let mut live_frames = vec![];
    let perm_id = loop {
        let msg = timeout(Duration::from_secs(3), ws_a.next())
            .await
            .expect("permission wait timed out")
            .expect("A stream ended")
            .expect("A recv error");
        if let ClientMsg::Text(t) = msg {
            let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
            let is_perm = v.get("method") == Some(&serde_json::json!("session/request_permission"));
            live_frames.push(v.clone());
            if is_perm {
                break v["id"].clone();
            }
        }
    };

    let reply = format!(
        r#"{{"jsonrpc":"2.0","id":{perm_id},"result":{{"outcome":{{"outcome":"selected","optionId":"allow_once"}}}}}}"#,
    );
    ws_a.send(ClientMsg::Text(reply.into())).await.unwrap();
    live_frames.extend(drain_for(&mut ws_a, Duration::from_secs(2)).await);

    assert!(
        live_frames.iter().any(|v| {
            v.get("method") == Some(&serde_json::json!("mock/response_echo"))
                && v["params"]["id"] == perm_id
        }),
        "agent must receive the live subscriber permission reply; frames: {live_frames:?}"
    );

    let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
    let replay_frames = drain_for(&mut ws_b, Duration::from_millis(500)).await;

    let opened_idx = replay_frames
        .iter()
        .position(|v| {
            v.get("method") == Some(&serde_json::json!("amux/agent_request_opened"))
                && v["params"]["requestId"] == perm_id
        })
        .unwrap_or_else(|| {
            panic!("late joiner must replay amux/agent_request_opened; frames: {replay_frames:?}")
        });
    let resolved_idx = replay_frames
        .iter()
        .position(|v| {
            v.get("method") == Some(&serde_json::json!("amux/agent_request_resolved"))
                && v["params"]["requestId"] == perm_id
        })
        .unwrap_or_else(|| {
            panic!("late joiner must replay amux/agent_request_resolved; frames: {replay_frames:?}")
        });
    assert!(
        opened_idx < resolved_idx,
        "late replay must present opened before resolved; opened@{opened_idx} resolved@{resolved_idx}"
    );
    let opened = &replay_frames[opened_idx];
    assert_eq!(
        opened["params"]["requestMethod"],
        serde_json::json!("session/request_permission")
    );
    assert_eq!(opened["params"]["requestId"], perm_id);
    let expected_tool_call_id = format!(
        "mock-tool-{}",
        perm_id.as_u64().expect("numeric request id")
    );
    assert_eq!(
        opened["params"]["requestParams"]["toolCall"]["toolCallId"],
        serde_json::json!(expected_tool_call_id)
    );
    assert!(
        replay_frames
            .iter()
            .all(|v| v.get("method") != Some(&serde_json::json!("session/request_permission"))),
        "late replay must not include the stale actionable request; frames: {replay_frames:?}"
    );

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
        r#"{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#
            .into(),
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
    http_get_response(url).await.1
}

async fn http_get_response(url: &str) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let parsed = url::Url::parse(url).unwrap();
    let host = parsed.host_str().unwrap();
    let port = parsed.port().unwrap();
    let mut path = if parsed.path().is_empty() {
        "/"
    } else {
        parsed.path()
    }
    .to_string();
    if let Some(query) = parsed.query() {
        path.push('?');
        path.push_str(query);
    }
    let mut stream = tokio::net::TcpStream::connect((host, port)).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf);
    let status = text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .expect("HTTP response status");
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("");
    (status, body.to_string())
}

// ===== Cancellation tests (issue #4 / $/cancel_request RFD) =====

/// Subscriber-driven `$/cancel_request` for the subscriber's own
/// in-flight `session/prompt` is translated by id and forwarded to the
/// agent. `MOCK_ACP_ECHO_CANCELS=1` makes the mock emit
/// `mock/cancel_echo` whenever it receives a cancellation, carrying
/// the translated `requestId` — we assert that against the expected
/// `mux_id`.
#[tokio::test]
async fn subscriber_cancels_own_prompt_translated_to_agent() {
    // Long prompt delay so the cancel arrives while the turn is still
    // in flight.
    let (addr, _) = spawn_server_with_mock_env(&[
        ("MOCK_ACP_ECHO_CANCELS", "1"),
        ("MOCK_ACP_PROMPT_DELAY_MS", "1500"),
    ])
    .await;
    let url = format!("ws://{addr}/acp?session=cancel&peer_id=A");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();

    let _ = ws_request(&mut ws, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#).await;
    let _ = ws_request(
        &mut ws,
        r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
    )
    .await;

    // Send the prompt. id=42 is the subscriber's id; amux rewrites to a
    // mux_id internally. Don't await its response — we want to cancel
    // while it's in flight.
    ws.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","id":42,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#
            .into(),
    ))
    .await
    .unwrap();

    // Give the proxy a moment to forward the prompt and register it in
    // `pending` before we cancel.
    tokio::time::sleep(Duration::from_millis(80)).await;

    ws.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","method":"$/cancel_request","params":{"requestId":42}}"#.into(),
    ))
    .await
    .unwrap();

    // Look for the mock's cancel echo. The agent should see the cancel
    // with the *translated* mux_id (not the subscriber's 42).
    let frames = drain_for(&mut ws, Duration::from_secs(3)).await;
    let cancel_echo = frames
        .iter()
        .find(|v| v.get("method") == Some(&serde_json::json!("mock/cancel_echo")))
        .unwrap_or_else(|| panic!("agent should have received the cancel; frames: {frames:?}"));
    // mux_id allocation is sequential from 1; initialize=1, session/new=2,
    // prompt=3. The cancel should carry requestId=3.
    assert_eq!(
        cancel_echo["params"]["requestId"],
        serde_json::json!(3),
        "cancel must carry the mux_id, not the subscriber's original id"
    );

    let _ = ws.send(ClientMsg::Close(None)).await;
}

/// A subscriber that sends `$/cancel_request` for a `requestId` that
/// doesn't match any of its own pending requests gets the cancel
/// dropped silently — no agent traffic.
#[tokio::test]
async fn subscriber_cancel_unknown_id_dropped() {
    let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_ECHO_CANCELS", "1")]).await;
    let url = format!("ws://{addr}/acp?session=cu&peer_id=A");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();

    let _ = ws_request(&mut ws, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#).await;

    ws.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","method":"$/cancel_request","params":{"requestId":9999}}"#.into(),
    ))
    .await
    .unwrap();

    let frames = drain_for(&mut ws, Duration::from_millis(400)).await;
    let any_cancel_echo = frames
        .iter()
        .any(|v| v.get("method") == Some(&serde_json::json!("mock/cancel_echo")));
    assert!(
        !any_cancel_echo,
        "cancel for unknown id must not reach the agent; frames: {frames:?}"
    );

    let _ = ws.send(ClientMsg::Close(None)).await;
}

/// Subscriber B sending `$/cancel_request` with subscriber A's
/// original id finds no pending entry under (B, A's id) and is dropped
/// silently. A's request continues uninterrupted. (B should use
/// `amux/cancel_active_turn` for cross-peer cancel.)
#[tokio::test]
async fn subscriber_cannot_cancel_another_subscribers_request() {
    let (addr, _) = spawn_server_with_mock_env(&[
        ("MOCK_ACP_ECHO_CANCELS", "1"),
        ("MOCK_ACP_PROMPT_DELAY_MS", "800"),
    ])
    .await;
    let url_a = format!("ws://{addr}/acp?session=xpeer&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=xpeer&peer_id=B");

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

    // A drives a prompt with id=42.
    ws_a.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","id":42,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#
            .into(),
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    // B tries to cancel using A's id. Should be dropped — B has no
    // pending entry under that id.
    ws_b.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","method":"$/cancel_request","params":{"requestId":42}}"#.into(),
    ))
    .await
    .unwrap();

    let a_frames = drain_for(&mut ws_a, Duration::from_secs(2)).await;
    let b_frames = drain_for(&mut ws_b, Duration::from_secs(2)).await;

    // No cancel echo on either side — the cancel never reached the
    // agent.
    let saw_cancel_echo = a_frames
        .iter()
        .chain(b_frames.iter())
        .any(|v| v.get("method") == Some(&serde_json::json!("mock/cancel_echo")));
    assert!(
        !saw_cancel_echo,
        "B should not have been able to cancel A's request"
    );
    // A's prompt should still have completed normally.
    let saw_a_response = a_frames
        .iter()
        .any(|v| v.get("id") == Some(&serde_json::json!(42)) && v.get("result").is_some());
    assert!(saw_a_response, "A's prompt should have completed normally");

    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
}

/// Agent-emitted `$/cancel_request` for an in-flight agent-initiated
/// request (e.g. `session/request_permission`) is preceded by inert
/// `amux/agent_request_opened`, forwarded to every subscriber, and
/// mirrored by `amux/agent_request_resolved { resolvedBy:
/// "agent:cancelled" }`. Subsequent subscriber replies for the same id
/// are dropped via the first-writer-wins gate.
#[tokio::test]
async fn agent_cancels_permission_request_fans_out() {
    let (addr, _) = spawn_server_with_mock_env(&[
        ("MOCK_ACP_EMIT_PERMISSION", "1"),
        ("MOCK_ACP_CANCEL_PERMISSION", "1"),
        ("MOCK_ACP_PROMPT_DELAY_MS", "500"),
    ])
    .await;
    let url_a = format!("ws://{addr}/acp?session=agcancel&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=agcancel&peer_id=B");

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
        r#"{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#
            .into(),
    ))
    .await
    .unwrap();

    let a_frames = drain_for(&mut ws_a, Duration::from_secs(2)).await;
    let b_frames = drain_for(&mut ws_b, Duration::from_secs(2)).await;

    for (label, frames) in [("A", &a_frames), ("B", &b_frames)] {
        let perm_idx = frames
            .iter()
            .position(|v| v.get("method") == Some(&serde_json::json!("session/request_permission")))
            .unwrap_or_else(|| panic!("{label}: must see permission request; got {frames:?}"));
        let perm = &frames[perm_idx];
        let perm_id = &perm["id"];

        let opened_idx = frames
            .iter()
            .position(|v| {
                v.get("method") == Some(&serde_json::json!("amux/agent_request_opened"))
                    && &v["params"]["requestId"] == perm_id
            })
            .unwrap_or_else(|| panic!("{label}: must see amux/agent_request_opened"));
        let opened = &frames[opened_idx];
        assert!(
            opened_idx < perm_idx,
            "{label}: opened must precede raw permission; opened@{opened_idx} permission@{perm_idx}",
        );
        assert_eq!(
            opened["params"]["requestMethod"],
            serde_json::json!("session/request_permission"),
            "{label}: opened should name the cancelled request method"
        );
        assert_eq!(
            opened["params"]["requestParams"]["sessionId"],
            serde_json::json!("sess-mock"),
            "{label}: opened should retain request context for replay"
        );

        let cancel_idx = frames
            .iter()
            .position(|v| {
                v.get("method") == Some(&serde_json::json!("$/cancel_request"))
                    && &v["params"]["requestId"] == perm_id
            })
            .unwrap_or_else(|| panic!("{label}: must see $/cancel_request for permission id"));
        let cancel = &frames[cancel_idx];
        assert_eq!(cancel["params"]["requestId"], *perm_id);
        assert!(
            opened_idx < cancel_idx,
            "{label}: opened must precede agent cancellation; opened@{opened_idx} cancel@{cancel_idx}",
        );

        let resolved_idx = frames
            .iter()
            .position(|v| {
                v.get("method") == Some(&serde_json::json!("amux/agent_request_resolved"))
                    && &v["params"]["requestId"] == perm_id
            })
            .unwrap_or_else(|| panic!("{label}: must see amux/agent_request_resolved"));
        let resolved = &frames[resolved_idx];
        assert!(
            opened_idx < resolved_idx,
            "{label}: opened must precede agent-cancelled resolution; opened@{opened_idx} resolved@{resolved_idx}",
        );
        assert_eq!(
            resolved["params"]["resolvedBy"],
            serde_json::json!("agent:cancelled")
        );
    }

    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
}

/// `amux/cancel_active_turn` from a non-driver peer broadcasts
/// `amux/turn_cancelled` to every peer AND sends ACP-native
/// `session/cancel` to the agent using the active turn's `sessionId`.
#[tokio::test]
async fn amux_cancel_active_turn_by_non_driver() {
    let (addr, _) = spawn_server_with_mock_env(&[
        ("MOCK_ACP_ECHO_CANCELS", "1"),
        ("MOCK_ACP_ECHO_SESSION_CANCELS", "1"),
        ("MOCK_ACP_PROMPT_DELAY_MS", "1500"),
    ])
    .await;
    let url_a = format!("ws://{addr}/acp?session=cact&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=cact&peer_id=B");

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

    // A drives.
    ws_a.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","id":99,"method":"session/prompt","params":{"sessionId":"sess-mock"}}"#
            .into(),
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    // B clicks stop.
    ws_b.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","method":"amux/cancel_active_turn","params":{"reason":"user clicked stop"}}"#.into(),
    ))
    .await
    .unwrap();

    let a_frames = drain_for(&mut ws_a, Duration::from_secs(3)).await;
    let b_frames = drain_for(&mut ws_b, Duration::from_secs(3)).await;

    for (label, frames) in [("A", &a_frames), ("B", &b_frames)] {
        let cancelled = frames
            .iter()
            .find(|v| v.get("method") == Some(&serde_json::json!("amux/turn_cancelled")))
            .unwrap_or_else(|| panic!("{label}: must see amux/turn_cancelled; got {frames:?}"));
        assert_eq!(cancelled["params"]["cancelledBy"], serde_json::json!("B"));
        assert_eq!(
            cancelled["params"]["originalDriver"],
            serde_json::json!("A")
        );
        assert_eq!(
            cancelled["params"]["reason"],
            serde_json::json!("user clicked stop")
        );
    }

    // The agent should have received ACP-native session/cancel for the
    // active prompt's upstream ACP session id, not a request-id cancel.
    let session_cancel_echo = a_frames
        .iter()
        .chain(b_frames.iter())
        .find(|v| v.get("method") == Some(&serde_json::json!("mock/session_cancel_echo")))
        .expect("agent should have received session/cancel for the active turn");
    assert_eq!(
        session_cancel_echo["params"]["sessionId"],
        serde_json::json!("sess-mock")
    );
    let saw_request_cancel_echo = a_frames
        .iter()
        .chain(b_frames.iter())
        .any(|v| v.get("method") == Some(&serde_json::json!("mock/cancel_echo")));
    assert!(
        !saw_request_cancel_echo,
        "amux/cancel_active_turn must not use $/cancel_request for active prompts"
    );

    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
}

/// `amux/cancel_active_turn` with no active turn is dropped silently —
/// no broadcast, no agent traffic.
#[tokio::test]
async fn amux_cancel_active_turn_with_no_active_turn_dropped() {
    let (addr, _) = spawn_server_with_mock_env(&[
        ("MOCK_ACP_ECHO_CANCELS", "1"),
        ("MOCK_ACP_ECHO_SESSION_CANCELS", "1"),
    ])
    .await;
    let url = format!("ws://{addr}/acp?session=nt&peer_id=A");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let _ = ws_request(&mut ws, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#).await;

    ws.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","method":"amux/cancel_active_turn"}"#.into(),
    ))
    .await
    .unwrap();

    let frames = drain_for(&mut ws, Duration::from_millis(400)).await;
    let saw_cancelled = frames
        .iter()
        .any(|v| v.get("method") == Some(&serde_json::json!("amux/turn_cancelled")));
    let saw_cancel_echo = frames
        .iter()
        .any(|v| v.get("method") == Some(&serde_json::json!("mock/cancel_echo")));
    let saw_session_cancel_echo = frames
        .iter()
        .any(|v| v.get("method") == Some(&serde_json::json!("mock/session_cancel_echo")));
    assert!(
        !saw_cancelled,
        "should not broadcast turn_cancelled when no turn active"
    );
    assert!(
        !saw_cancel_echo && !saw_session_cancel_echo,
        "should not forward cancel to agent when no turn active"
    );

    let _ = ws.send(ClientMsg::Close(None)).await;
}

// ===== session/list tests (issue #5 / session-list RFD) =====

/// When the agent advertises `sessionCapabilities.list` in its
/// `initialize` response, amux passes the capability through to the
/// client verbatim. No injection by amux — the agent owns this
/// capability.
#[tokio::test]
async fn session_list_capability_propagates_from_agent() {
    let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_SESSION_LIST", "1")]).await;
    let url = format!("ws://{addr}/acp?session=cap&peer_id=A");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let resp = ws_request(&mut ws, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#).await;
    assert_eq!(
        resp["result"]["agentCapabilities"]["sessionCapabilities"]["list"],
        serde_json::json!({}),
        "agent's sessionCapabilities.list must reach the client unchanged"
    );
    let _ = ws.send(ClientMsg::Close(None)).await;
}

/// When the agent does *not* advertise the capability, amux must not
/// fabricate it — clients that probe see nothing.
#[tokio::test]
async fn session_list_capability_absent_when_agent_doesnt_advertise() {
    let (addr, _) = spawn_server_with_mock().await;
    let url = format!("ws://{addr}/acp?session=nocap&peer_id=A");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let resp = ws_request(&mut ws, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#).await;
    assert!(
        resp["result"]["agentCapabilities"]["sessionCapabilities"]
            .get("list")
            .is_none(),
        "amux must not synthesize sessionCapabilities.list when the agent doesn't advertise it",
    );
    let _ = ws.send(ClientMsg::Close(None)).await;
}

/// End-to-end: client sends `session/list`, amux forwards to the
/// agent via the normal request path (id translation), agent
/// responds, amux returns the response with the client's original id
/// restored. The session list flows through unmodified.
#[tokio::test]
async fn session_list_request_forwards_through_amux() {
    let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_SESSION_LIST", "1")]).await;
    let url = format!("ws://{addr}/acp?session=list&peer_id=A");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let _ = ws_request(&mut ws, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#).await;

    // Use a string id to also confirm amux's id translation round-trip
    // works for non-numeric ids on this path.
    let resp = ws_request(
        &mut ws,
        r#"{"jsonrpc":"2.0","id":"list-1","method":"session/list","params":{}}"#,
    )
    .await;
    assert_eq!(resp["id"], serde_json::json!("list-1"));
    let sessions = resp["result"]["sessions"]
        .as_array()
        .expect("sessions array");
    assert_eq!(sessions.len(), 3, "all three canned sessions should arrive");
    let ids: Vec<&str> = sessions
        .iter()
        .filter_map(|s| s["sessionId"].as_str())
        .collect();
    assert!(ids.contains(&"sess-mock"));
    assert!(ids.contains(&"sess-archive-001"));
    assert!(ids.contains(&"sess-archive-002"));

    let _ = ws.send(ClientMsg::Close(None)).await;
}

/// Once a room has established its upstream ACP session id via
/// `session/new`, `session/list` decorates only the matching live entry
/// under `_meta.amux`, preserving agent-owned `_meta` keys.
#[tokio::test]
async fn session_list_decorates_live_entry_with_amux_metadata() {
    let (addr, _) = spawn_server_with_mock_env(&[
        ("MOCK_ACP_SESSION_LIST", "1"),
        ("MOCK_ACP_SESSION_LIST_META", "1"),
    ])
    .await;
    let url_a = format!("ws://{addr}/acp?session=live-room&peer_id=A&peer_name=Alice&role=driver");
    let url_b = format!("ws://{addr}/acp?session=live-room&peer_id=B&peer_name=Bob&role=observer");

    let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
    let _ = ws_request(
        &mut ws_a,
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
    )
    .await;
    let r_new = ws_request(
        &mut ws_a,
        r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
    )
    .await;
    assert_eq!(r_new["result"]["sessionId"], serde_json::json!("sess-mock"));

    let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let resp = ws_request(
        &mut ws_a,
        r#"{"jsonrpc":"2.0","id":"list-live","method":"session/list","params":{}}"#,
    )
    .await;
    let sessions = resp["result"]["sessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 3, "all canned sessions should arrive");

    let current = sessions
        .iter()
        .find(|s| s["sessionId"] == serde_json::json!("sess-mock"))
        .expect("current session entry");
    assert_eq!(current["_meta"]["agentKey"], serde_json::json!("preserved"));
    assert_eq!(
        current["_meta"]["amux"]["agentAmuxKey"],
        serde_json::json!("preserved")
    );
    assert_eq!(
        current["_meta"]["amux"]["proxySessionId"],
        serde_json::json!("live-room")
    );
    assert_eq!(
        current["_meta"]["amux"]["subscriberCount"],
        serde_json::json!(2)
    );
    assert_eq!(
        current["_meta"]["amux"]["drivingSubscriber"],
        serde_json::json!("A")
    );

    for archive_id in ["sess-archive-001", "sess-archive-002"] {
        let archived = sessions
            .iter()
            .find(|s| s["sessionId"] == serde_json::json!(archive_id))
            .expect("archive session entry");
        assert!(
            archived.get("_meta").and_then(|m| m.get("amux")).is_none(),
            "non-live session {archive_id} must not receive amux metadata"
        );
    }

    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
}

/// `session/list` with a `cwd` filter is forwarded with the filter
/// intact — amux doesn't interpret the params, the agent does. The
/// mock filters by exact match on `cwd`.
#[tokio::test]
async fn session_list_with_cwd_filter_forwarded_unmodified() {
    let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_SESSION_LIST", "1")]).await;
    let url = format!("ws://{addr}/acp?session=lfilter&peer_id=A");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let _ = ws_request(&mut ws, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#).await;

    let resp = ws_request(
        &mut ws,
        r#"{"jsonrpc":"2.0","id":2,"method":"session/list","params":{"cwd":"/tmp/other"}}"#,
    )
    .await;
    let sessions = resp["result"]["sessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 1, "filter should narrow to one session");
    assert_eq!(
        sessions[0]["sessionId"],
        serde_json::json!("sess-archive-002")
    );
    assert_eq!(sessions[0]["cwd"], serde_json::json!("/tmp/other"));

    let _ = ws.send(ClientMsg::Close(None)).await;
}

/// Cold-start discovery: clients can list the upstream agent's persisted
/// sessions before committing to any `?session=` WebSocket room. The
/// query spawns a transient agent process, asks for `session/list`, then
/// tears it down without adding live mux state.
#[tokio::test]
async fn control_plane_sessions_lists_without_ws_attach() {
    let (addr, registry) = spawn_server_with_mock_env(&[("MOCK_ACP_SESSION_LIST", "1")]).await;

    let (status, body) = http_get_response(&format!("http://{addr}/acp/sessions")).await;

    assert_eq!(status, 200, "body: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let sessions = v["sessions"].as_array().expect("sessions array");
    assert_eq!(sessions.len(), 3, "all canned sessions should arrive");
    let ids: Vec<&str> = sessions
        .iter()
        .filter_map(|s| s["sessionId"].as_str())
        .collect();
    assert!(ids.contains(&"sess-mock"));
    assert!(ids.contains(&"sess-archive-001"));
    assert!(ids.contains(&"sess-archive-002"));
    assert_eq!(
        registry.live_session_count().await,
        0,
        "control-plane listing must not create a live mux session"
    );
}

/// The HTTP control-plane surface mirrors the `session/list` params that
/// make sense for a GET endpoint. `cwd` is forwarded to the transient
/// agent query, so clients can cold-start directly into a filtered view.
#[tokio::test]
async fn control_plane_sessions_forwards_cwd_filter() {
    let (addr, registry) = spawn_server_with_mock_env(&[("MOCK_ACP_SESSION_LIST", "1")]).await;

    let (status, body) =
        http_get_response(&format!("http://{addr}/acp/sessions?cwd=%2Ftmp%2Fother")).await;

    assert_eq!(status, 200, "body: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let sessions = v["sessions"].as_array().expect("sessions array");
    assert_eq!(sessions.len(), 1, "filter should narrow to one session");
    assert_eq!(
        sessions[0]["sessionId"],
        serde_json::json!("sess-archive-002")
    );
    assert_eq!(sessions[0]["cwd"], serde_json::json!("/tmp/other"));
    assert_eq!(registry.live_session_count().await, 0);
}

#[tokio::test]
async fn control_plane_sessions_without_agent_cmd_returns_503() {
    let (addr, registry) = spawn_server(None).await;

    let (status, body) = http_get_response(&format!("http://{addr}/acp/sessions")).await;

    assert_eq!(status, 503, "body: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        v["error"],
        serde_json::json!("agent command not configured")
    );
    assert_eq!(registry.live_session_count().await, 0);
}

// ===== session/load canonical rebinding (issue #12) =====

/// A successful `session/load` rebinds the room's cached session id
/// so that late joiners' `session/new` returns the loaded session
/// rather than the originally-created one.
#[tokio::test]
async fn session_load_rebinds_canonical_session_for_late_joiners() {
    let (addr, _) = spawn_server_with_mock().await;
    let url_a = format!("ws://{addr}/acp?session=load&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=load&peer_id=B");

    let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
    let _ = ws_request(
        &mut ws_a,
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
    )
    .await;
    let r_new = ws_request(
        &mut ws_a,
        r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
    )
    .await;
    assert_eq!(r_new["result"]["sessionId"], serde_json::json!("sess-mock"));

    // A loads a different session id. Mock_acp echoes the requested
    // id back as the loaded session id with `_loaded: true`.
    let r_load = ws_request(
        &mut ws_a,
        r#"{"jsonrpc":"2.0","id":3,"method":"session/load","params":{"sessionId":"sess-loaded-xyz","cwd":"/tmp"}}"#,
    )
    .await;
    assert_eq!(
        r_load["result"]["sessionId"],
        serde_json::json!("sess-loaded-xyz")
    );

    // Late joiner attaches. Their session/new should see the loaded
    // session id, not the original `sess-mock`.
    let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
    let _ = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
    )
    .await;
    let r_b_new = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
    )
    .await;
    assert_eq!(
        r_b_new["result"]["sessionId"],
        serde_json::json!("sess-loaded-xyz"),
        "late joiner must see the loaded session, not the original",
    );

    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
}

/// A successful `session/load` starts a new replay generation. Late
/// joiners should receive coherent replay for the loaded ACP session,
/// not stale `session/update` frames from the previous upstream session.
#[tokio::test]
async fn session_load_replay_generation_excludes_previous_session_updates_for_late_joiners() {
    let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_EMIT_LOAD_HISTORY", "1")]).await;
    let url_a = format!("ws://{addr}/acp?session=load-replay&peer_id=A&peer_name=Alice");
    let url_b = format!("ws://{addr}/acp?session=load-replay&peer_id=B&peer_name=Bob");

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

    // Seed the replay log with old-session turn history.
    let _ = ws_request(
        &mut ws_a,
        r#"{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"old"}]}}"#,
    )
    .await;

    // Loading emits two load-time history chunks for the new session before
    // the response. Those chunks should survive the replay reset.
    let _ = ws_request(
        &mut ws_a,
        r#"{"jsonrpc":"2.0","id":8,"method":"session/load","params":{"sessionId":"sess-loaded-xyz","cwd":"/tmp"}}"#,
    )
    .await;

    let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
    let replay = drain_for(&mut ws_b, Duration::from_millis(400)).await;
    let update_session_ids: Vec<_> = replay
        .iter()
        .filter(|v| v.get("method") == Some(&serde_json::json!("session/update")))
        .filter_map(|v| v["params"]["sessionId"].as_str())
        .collect();

    assert!(
        update_session_ids.contains(&"sess-loaded-xyz"),
        "late replay should retain load-time history for loaded session, got {replay:?}",
    );
    assert!(
        !update_session_ids.contains(&"sess-mock"),
        "late replay must not contain stale updates from prior ACP session, got {replay:?}",
    );
    assert!(
        replay.iter().any(|v| {
            v.get("method") == Some(&serde_json::json!("amux/peer_joined"))
                && v["params"]["peerId"] == serde_json::json!("A")
        }),
        "replay reset should still teach late joiners about existing peers, got {replay:?}",
    );

    let _ = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
    )
    .await;
    let r_b_new = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
    )
    .await;
    assert_eq!(
        r_b_new["result"]["sessionId"],
        serde_json::json!("sess-loaded-xyz")
    );

    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
}

/// `/debug/sessions` exposes enough replay-generation metadata to see
/// which ACP session ids are present in the late-join replay snapshot.
#[tokio::test]
async fn session_load_debug_sessions_exposes_replay_generation_and_acp_update_counts() {
    let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_EMIT_LOAD_HISTORY", "1")]).await;
    let url = format!("ws://{addr}/acp?session=load-debug&peer_id=A");

    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let _ = ws_request(&mut ws, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#).await;
    let _ = ws_request(
        &mut ws,
        r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
    )
    .await;
    let _ = ws_request(
        &mut ws,
        r#"{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"old"}]}}"#,
    )
    .await;
    let _ = ws_request(
        &mut ws,
        r#"{"jsonrpc":"2.0","id":4,"method":"session/load","params":{"sessionId":"loaded-debug","cwd":"/tmp"}}"#,
    )
    .await;

    let body = http_get(&format!("http://{addr}/debug/sessions")).await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let s = &v["sessions"][0];
    assert_eq!(s["cachedSessionId"], serde_json::json!("loaded-debug"));
    assert_eq!(s["replayGeneration"], serde_json::json!(1));
    assert_eq!(
        s["lastReplayReset"]["loadedSessionId"],
        serde_json::json!("loaded-debug")
    );
    assert!(
        s["lastReplayReset"]["droppedFrameCount"]
            .as_u64()
            .unwrap_or(0)
            > 0,
        "debug metadata should report truncated old-generation frames: {s:?}",
    );
    assert!(
        s["lastReplayReset"]["retainedFrameCount"]
            .as_u64()
            .unwrap_or(0)
            >= 2,
        "debug metadata should report retained load-history frames: {s:?}",
    );
    assert_eq!(
        s["replayLogUpdateFramesByAcpSessionId"]["loaded-debug"],
        serde_json::json!(2),
    );
    assert!(
        s["replayLogUpdateFramesByAcpSessionId"]
            .get("sess-mock")
            .is_none(),
        "debug counts should not include old-session updates after replay reset: {s:?}",
    );

    let _ = ws.send(ClientMsg::Close(None)).await;
}

/// `session/load` issued *without* a prior `session/new` populates
/// the room's canonical session cache from scratch. Late joiners get
/// a synthesized session/new response carrying just the loaded id.
#[tokio::test]
async fn session_load_without_prior_new_populates_cache() {
    let (addr, _) = spawn_server_with_mock().await;
    let url_a = format!("ws://{addr}/acp?session=loadfirst&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=loadfirst&peer_id=B");

    let (mut ws_a, _) = tokio_tungstenite::connect_async(url_a).await.unwrap();
    let _ = ws_request(
        &mut ws_a,
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
    )
    .await;
    let _ = ws_request(
        &mut ws_a,
        r#"{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"sess-from-load","cwd":"/tmp"}}"#,
    )
    .await;

    let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
    let _ = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
    )
    .await;
    let r_b_new = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
    )
    .await;
    assert_eq!(
        r_b_new["result"]["sessionId"],
        serde_json::json!("sess-from-load"),
        "late joiner must see the loaded session even though there was never a session/new",
    );

    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
}

/// A **failed** `session/load` (error response from agent) must not
/// touch the existing canonical session cache. Late joiners still see
/// the original session.
#[tokio::test]
async fn failed_session_load_leaves_cache_untouched() {
    let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_FAIL_LOAD", "1")]).await;
    let url_a = format!("ws://{addr}/acp?session=loadfail&peer_id=A");
    let url_b = format!("ws://{addr}/acp?session=loadfail&peer_id=B");

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
    let r_load = ws_request(
        &mut ws_a,
        r#"{"jsonrpc":"2.0","id":3,"method":"session/load","params":{"sessionId":"nope","cwd":"/tmp"}}"#,
    )
    .await;
    assert!(
        r_load.get("error").is_some(),
        "load should have failed: {r_load:?}"
    );

    let (mut ws_b, _) = tokio_tungstenite::connect_async(url_b).await.unwrap();
    let _ = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
    )
    .await;
    let r_b_new = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
    )
    .await;
    assert_eq!(
        r_b_new["result"]["sessionId"],
        serde_json::json!("sess-mock"),
        "failed load must not rebind the cache",
    );

    let _ = ws_a.send(ClientMsg::Close(None)).await;
    let _ = ws_b.send(ClientMsg::Close(None)).await;
}

/// `/debug/sessions` reflects the loaded session id after a
/// successful `session/load`, so operators can verify the rebinding.
#[tokio::test]
async fn debug_sessions_shows_loaded_session_id() {
    let (addr, _) = spawn_server_with_mock().await;
    let url = format!("ws://{addr}/acp?session=debugload&peer_id=A");

    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let _ = ws_request(&mut ws, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#).await;
    let _ = ws_request(
        &mut ws,
        r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#,
    )
    .await;
    let _ = ws_request(
        &mut ws,
        r#"{"jsonrpc":"2.0","id":3,"method":"session/load","params":{"sessionId":"loaded-debug","cwd":"/tmp"}}"#,
    )
    .await;

    let body = http_get(&format!("http://{addr}/debug/sessions")).await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let s = &v["sessions"][0];
    assert_eq!(
        s["cachedSessionId"],
        serde_json::json!("loaded-debug"),
        "cached session id should reflect the loaded session"
    );

    let _ = ws.send(ClientMsg::Close(None)).await;
}
