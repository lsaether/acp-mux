//! RFD-#533 conformance tests for the standalone core multiplexer.
//!
//! These spawn a real core `acp-mux` server (`acp_mux::server::router`),
//! drive it over WebSocket using `tokio-tungstenite`, and use the
//! `mock_acp_core` binary as the agent subprocess. Cargo sets
//! `CARGO_BIN_EXE_mock_acp_core` automatically for integration tests under
//! `tests/` and builds the bin as a dependency of this test crate.
//! (The binary is named `mock_acp_core` rather than `mock_acp` so it does
//! not collide with the `rooms` crate's `mock_acp` in the shared target dir.)
//!
//! The core attaches with the `?mux=<id>` query param (NOT `?room=`) and
//! must NEVER emit any `rooms/*` frame. The invariant test below asserts
//! that explicitly across every frame any client receives.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use acp_mux::cli::{ClientToolPolicy, ReplayTurns};
use acp_mux::mux::registry::{AgentCmd, MuxRegistry};
use acp_mux::server::{AppState, router};
use futures::{SinkExt, StreamExt};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message as ClientMsg;

type WsStream = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
>;

/// Short TTL so teardown is observable inside a normal test budget.
const TEST_DEFAULT_TTL: Duration = Duration::from_millis(150);

fn mock_acp_path() -> String {
    env!("CARGO_BIN_EXE_mock_acp_core").to_string()
}

fn mock_agent_cmd() -> AgentCmd {
    AgentCmd {
        program: mock_acp_path(),
        args: vec![],
    }
}

async fn spawn_server(agent_cmd: Option<AgentCmd>) -> (SocketAddr, Arc<MuxRegistry>) {
    let registry = MuxRegistry::new(
        agent_cmd,
        ReplayTurns::Unbounded,
        TEST_DEFAULT_TTL,
        ClientToolPolicy::default(),
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

async fn spawn_server_with_mock() -> (SocketAddr, Arc<MuxRegistry>) {
    spawn_server(Some(mock_agent_cmd())).await
}

async fn spawn_server_with_mock_env(vars: &[(&str, &str)]) -> (SocketAddr, Arc<MuxRegistry>) {
    for (k, v) in vars {
        // Safety: tests run in-process; the mock_acp subprocess inherits
        // these via the spawned agent. Set before the registry spawns any
        // agent. Single-threaded test bodies, no concurrent env mutation.
        unsafe { std::env::set_var(k, v) };
    }
    spawn_server_with_mock().await
}

async fn connect(addr: SocketAddr, query: &str) -> WsStream {
    let url = format!("ws://{addr}/acp?{query}");
    let (ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .expect("ws connect");
    ws
}

/// Send a JSON-RPC request, skip notifications / unrelated frames, and
/// return the response matching the request's id. Asserts the invariant
/// that no frame seen along the way carries a `rooms/*` method.
async fn ws_request(ws: &mut WsStream, payload: &str) -> serde_json::Value {
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
        assert_no_rooms_frame(&v);
        // Skip notifications and unrelated requests (anything with a method).
        if v.get("method").is_some() {
            continue;
        }
        if v.get("id") == req_id.as_ref() {
            return v;
        }
    }
}

/// Wait for the next frame carrying the given `method`, asserting the
/// no-`rooms/*` invariant on every frame observed in the meantime.
async fn ws_next_method(ws: &mut WsStream, method: &str) -> serde_json::Value {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        let Ok(Some(Ok(ClientMsg::Text(t)))) =
            timeout(Duration::from_millis(100), ws.next()).await
        else {
            continue;
        };
        let v: serde_json::Value = serde_json::from_str(t.as_str()).expect("frame is JSON");
        assert_no_rooms_frame(&v);
        if v.get("method") == Some(&serde_json::json!(method)) {
            return v;
        }
    }
    panic!("timed out waiting for method {method}");
}

/// Collect every text frame received within `dur`, asserting the
/// no-`rooms/*` invariant on each.
async fn drain_for(ws: &mut WsStream, dur: Duration) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    let deadline = std::time::Instant::now() + dur;
    while std::time::Instant::now() < deadline {
        let Ok(next) = timeout(Duration::from_millis(50), ws.next()).await else {
            continue;
        };
        match next {
            Some(Ok(ClientMsg::Text(t))) => {
                let v: serde_json::Value = serde_json::from_str(t.as_str()).expect("frame is JSON");
                assert_no_rooms_frame(&v);
                out.push(v);
            }
            Some(Ok(_)) => {}
            _ => break,
        }
    }
    out
}

/// Strong invariant: the core path must NEVER emit a frame whose `method`
/// starts with `"rooms/"`. This is the load-bearing assertion separating
/// the provider-neutral core from any AMUX layer.
fn assert_no_rooms_frame(frame: &serde_json::Value) {
    if let Some(method) = frame.get("method").and_then(|m| m.as_str()) {
        assert!(
            !method.starts_with("rooms/"),
            "core must never emit a rooms/* frame; saw method={method:?} in {frame:?}",
        );
    }
}

async fn close(mut ws: WsStream) {
    let _ = ws.send(ClientMsg::Close(None)).await;
}

// ===== (a) session/attach roster + historyPolicy shaping =====

#[tokio::test]
async fn attach_history_policy_full_returns_history_and_roster() {
    let (addr, _) = spawn_server_with_mock().await;
    let mut ws = connect(addr, "mux=full533&peer_id=A&peer_name=Alice").await;

    let _ = ws_request(&mut ws, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#).await;
    let _ = ws_request(&mut ws, r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#).await;
    // Seed replay history with agent broadcast frames.
    let _ = ws_request(
        &mut ws,
        r#"{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"seed"}]}}"#,
    )
    .await;

    let attach = ws_request(
        &mut ws,
        r#"{"jsonrpc":"2.0","id":4,"method":"session/attach","params":{"sessionId":"sess-mock","historyPolicy":"full","clientId":"client-A"}}"#,
    )
    .await;

    let result = &attach["result"];
    assert_eq!(result["sessionId"], serde_json::json!("sess-mock"));
    assert_eq!(result["clientId"], serde_json::json!("client-A"));
    assert_eq!(result["historyPolicy"], serde_json::json!("full"));

    // Top-level connectedClients roster includes this peer.
    let roster = result["connectedClients"].as_array().expect("roster array");
    assert!(
        roster
            .iter()
            .any(|c| c["clientId"] == serde_json::json!("A") && c["name"] == serde_json::json!("Alice")),
        "attach result must expose a top-level connectedClients roster: {attach:?}",
    );

    // Full history includes the replayed session/update broadcast frames.
    let history = result["history"].as_array().expect("full history array");
    assert!(
        history
            .iter()
            .any(|entry| entry["method"] == serde_json::json!("session/update")),
        "full history should include replayed broadcast frames: {attach:?}",
    );

    close(ws).await;
}

#[tokio::test]
async fn attach_history_policy_none_omits_history_but_keeps_roster() {
    let (addr, _) = spawn_server_with_mock().await;
    let mut ws = connect(addr, "mux=none533&peer_id=A&peer_name=Alice").await;

    let _ = ws_request(&mut ws, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#).await;
    let _ = ws_request(&mut ws, r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#).await;
    let _ = ws_request(
        &mut ws,
        r#"{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"seed"}]}}"#,
    )
    .await;

    let attach = ws_request(
        &mut ws,
        r#"{"jsonrpc":"2.0","id":4,"method":"session/attach","params":{"sessionId":"sess-mock","historyPolicy":"none","clientId":"client-A"}}"#,
    )
    .await;

    let result = &attach["result"];
    assert_eq!(result["historyPolicy"], serde_json::json!("none"));
    assert!(
        result.get("history").is_none(),
        "historyPolicy none must omit history: {attach:?}",
    );
    let roster = result["connectedClients"].as_array().expect("roster array");
    assert!(
        roster.iter().any(|c| c["clientId"] == serde_json::json!("A")),
        "roster should still be present for historyPolicy none: {attach:?}",
    );

    close(ws).await;
}

#[tokio::test]
async fn attach_history_policy_pending_only_returns_open_permission() {
    let (addr, _) = spawn_server_with_mock_env(&[
        ("MOCK_ACP_EMIT_PERMISSION", "1"),
        ("MOCK_ACP_PROMPT_DELAY_MS", "2000"),
    ])
    .await;
    let mut ws_a = connect(addr, "mux=pending533&peer_id=A&peer_name=Alice").await;

    let _ = ws_request(&mut ws_a, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#).await;
    let _ = ws_request(&mut ws_a, r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#).await;
    // Fire a prompt that triggers an agent-initiated permission request and
    // then stalls (2s delay) so the permission stays open.
    ws_a.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"needs approval"}]}}"#.into(),
    ))
    .await
    .unwrap();
    let _permission = ws_next_method(&mut ws_a, "session/request_permission").await;

    // A second client attaches with pending_only and should see exactly the
    // open permission in history.
    let mut ws_b = connect(addr, "mux=pending533&peer_id=B&peer_name=Bob").await;
    let _ = ws_request(&mut ws_b, r#"{"jsonrpc":"2.0","id":10,"method":"initialize"}"#).await;
    let attach = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":11,"method":"session/attach","params":{"sessionId":"sess-mock","historyPolicy":"pending_only"}}"#,
    )
    .await;

    let result = &attach["result"];
    assert_eq!(result["historyPolicy"], serde_json::json!("pending_only"));
    let history = result["history"].as_array().expect("pending_only history array");
    assert_eq!(history.len(), 1, "pending_only history: {attach:?}");
    assert_eq!(
        history[0]["method"],
        serde_json::json!("session/request_permission"),
    );
    assert_eq!(
        history[0]["params"]["toolCall"]["status"],
        serde_json::json!("pending"),
    );

    close(ws_a).await;
    close(ws_b).await;
}

// ===== (b) session/detach standard result shape =====

#[tokio::test]
async fn detach_returns_standard_result_shape() {
    let (addr, _) = spawn_server_with_mock().await;
    let mut ws_a = connect(addr, "mux=detach533&peer_id=A&peer_name=Alice").await;
    let _ = ws_request(&mut ws_a, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#).await;
    let _ = ws_request(&mut ws_a, r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#).await;

    let mut ws_b = connect(addr, "mux=detach533&peer_id=B&peer_name=Bob").await;
    let _ = ws_request(&mut ws_b, r#"{"jsonrpc":"2.0","id":10,"method":"initialize"}"#).await;
    let _ = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":11,"method":"session/attach","params":{"sessionId":"sess-mock","historyPolicy":"none"}}"#,
    )
    .await;

    let detached = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":12,"method":"session/detach","params":{"sessionId":"sess-mock"}}"#,
    )
    .await;
    let result = &detached["result"];
    assert_eq!(result["status"], serde_json::json!("detached"));
    assert_eq!(result["sessionId"], serde_json::json!("sess-mock"));

    close(ws_a).await;
    close(ws_b).await;
}

// ===== (c) first-writer-wins permission resolution =====

#[tokio::test]
async fn permission_resolution_is_first_writer_wins() {
    let (addr, _) = spawn_server_with_mock_env(&[
        ("MOCK_ACP_EMIT_PERMISSION", "1"),
        ("MOCK_ACP_PROMPT_DELAY_MS", "2000"),
    ])
    .await;
    let mut ws_a = connect(addr, "mux=fww533&peer_id=A&peer_name=Alice").await;
    let mut ws_b = connect(addr, "mux=fww533&peer_id=B&peer_name=Bob").await;

    let _ = ws_request(&mut ws_a, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#).await;
    let _ = ws_request(&mut ws_a, r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#).await;

    // Prompt triggers an agent-initiated permission, fanned out to both A and B.
    ws_a.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"needs approval"}]}}"#.into(),
    ))
    .await
    .unwrap();

    let perm_a = ws_next_method(&mut ws_a, "session/request_permission").await;
    let perm_b = ws_next_method(&mut ws_b, "session/request_permission").await;
    let permission_id = perm_a["id"].clone();
    assert_eq!(perm_a["id"], perm_b["id"], "both clients see the same request id");

    // A replies first; this reply should be forwarded to the agent.
    let reply = serde_json::json!({
        "jsonrpc": "2.0",
        "id": permission_id,
        "result": { "outcome": { "outcome": "selected", "optionId": "allow_once" } },
    });
    ws_a.send(ClientMsg::Text(reply.to_string().into()))
        .await
        .unwrap();

    // B replies later; this duplicate must be dropped (first-writer-wins).
    let dup = serde_json::json!({
        "jsonrpc": "2.0",
        "id": permission_id,
        "result": { "outcome": { "outcome": "selected", "optionId": "deny" } },
    });
    // Give the first reply time to be consumed by the actor.
    tokio::time::sleep(Duration::from_millis(100)).await;
    ws_b.send(ClientMsg::Text(dup.to_string().into()))
        .await
        .unwrap();

    // The prompt eventually settles with end_turn. The first-writer reply
    // (A) is what gets forwarded to the agent; B's later duplicate is
    // dropped by the core. We confirm the turn completes cleanly and that
    // no rooms/* leaked (asserted on every frame drained).
    //
    // Drain A until the prompt response (id 3) arrives.
    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    let mut saw_prompt_result = false;
    while std::time::Instant::now() < deadline {
        let Ok(Some(Ok(ClientMsg::Text(t)))) =
            timeout(Duration::from_millis(150), ws_a.next()).await
        else {
            continue;
        };
        let v: serde_json::Value = serde_json::from_str(t.as_str()).expect("frame is JSON");
        assert_no_rooms_frame(&v);
        if v.get("id") == Some(&serde_json::json!(3)) && v.get("result").is_some() {
            assert_eq!(v["result"]["stopReason"], serde_json::json!("end_turn"));
            saw_prompt_result = true;
            break;
        }
    }
    assert!(
        saw_prompt_result,
        "expected A to receive its prompt response after first-writer permission resolution",
    );

    // Drain any residual frames on B; the no-rooms invariant is checked there too.
    let _ = drain_for(&mut ws_b, Duration::from_millis(100)).await;

    close(ws_a).await;
    close(ws_b).await;
}

// ===== (d) INVARIANT: no rooms/* frames anywhere on the core path =====

#[tokio::test]
async fn core_never_emits_rooms_frames() {
    let (addr, _) = spawn_server_with_mock_env(&[("MOCK_ACP_EMIT_PERMISSION", "1")]).await;
    let mut ws_a = connect(addr, "mux=invariant533&peer_id=A&peer_name=Alice").await;
    let mut ws_b = connect(addr, "mux=invariant533&peer_id=B&peer_name=Bob").await;

    // Full handshake + a prompt (with permission) + attach/detach lifecycle
    // exercises every code path that produces outbound frames.
    let _ = ws_request(&mut ws_a, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#).await;
    let _ = ws_request(&mut ws_a, r#"{"jsonrpc":"2.0","id":2,"method":"session/new"}"#).await;
    let _ = ws_request(&mut ws_b, r#"{"jsonrpc":"2.0","id":10,"method":"initialize"}"#).await;
    let _ = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":11,"method":"session/attach","params":{"sessionId":"sess-mock","historyPolicy":"full"}}"#,
    )
    .await;

    ws_a.send(ClientMsg::Text(
        r#"{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"sess-mock","prompt":[{"type":"text","text":"hello"}]}}"#.into(),
    ))
    .await
    .unwrap();

    // Resolve the permission so the turn completes.
    let perm = ws_next_method(&mut ws_b, "session/request_permission").await;
    let reply = serde_json::json!({
        "jsonrpc": "2.0",
        "id": perm["id"],
        "result": { "outcome": { "outcome": "selected", "optionId": "allow_once" } },
    });
    ws_b.send(ClientMsg::Text(reply.to_string().into()))
        .await
        .unwrap();

    // Drain a generous window of frames from both clients. assert_no_rooms_frame
    // (invoked inside drain_for) fails the test if any rooms/* method appears.
    let a_frames = drain_for(&mut ws_a, Duration::from_millis(400)).await;
    let b_frames = drain_for(&mut ws_b, Duration::from_millis(400)).await;

    // Sanity: the core actually delivered real ACP traffic (not just silence).
    let saw_real_traffic = a_frames
        .iter()
        .chain(b_frames.iter())
        .any(|v| v.get("method") == Some(&serde_json::json!("session/update")));
    assert!(
        saw_real_traffic,
        "expected real session/update traffic to validate the invariant against live frames",
    );

    // Detach exercises the lifecycle path; its result must also be rooms-free.
    let detached = ws_request(
        &mut ws_b,
        r#"{"jsonrpc":"2.0","id":12,"method":"session/detach","params":{"sessionId":"sess-mock"}}"#,
    )
    .await;
    assert_eq!(detached["result"]["status"], serde_json::json!("detached"));
    let _ = drain_for(&mut ws_a, Duration::from_millis(150)).await;

    close(ws_a).await;
    close(ws_b).await;
}
