//! Minimal mock ACP agent used by integration tests.
//!
//! Speaks NDJSON over stdin/stdout. Recognizes a small set of methods:
//!
//! - `initialize` → canned `result` with `protocolVersion: 1`.
//! - `session/new` → canned `sessionId` (configurable via $MOCK_ACP_SESSION_ID).
//! - `session/prompt` → emits two `session/update` notifications referencing
//!   the param `sessionId`, then a response with `stopReason: "end_turn"`.
//! - anything else with an id → empty `result`.
//!
//! Env knobs:
//!
//! - `MOCK_ACP_SESSION_ID` — sessionId returned by `session/new`.
//! - `MOCK_ACP_EMIT_PERMISSION=1` — on `session/prompt`, emit an
//!   agent-initiated `session/request_permission` (id 10000+counter)
//!   before the updates and the response. Uses the canonical ACP wire
//!   shape: `params.options[{optionId, kind, name}]` and expects a reply
//!   of `result.outcome = {outcome: "selected", optionId} | {outcome:
//!   "cancelled"}`. The mock does NOT block on the response; it carries
//!   on so subscriber-side response handling can be tested independently
//!   of agent turn timing.
//! - `MOCK_ACP_PROMPT_DELAY_MS=N` — sleep N ms before responding to
//!   `session/prompt`. Lets the test queue a second concurrent prompt at
//!   acp-mux while the first turn is in flight (chunk 6).
//! - `MOCK_ACP_ECHO_RESPONSES=1` — whenever the mock receives a response
//!   frame (id + result/error, no method), emit an observable
//!   `mock/response_echo` notification carrying the id and a monotonic
//!   counter. Tests use this to confirm exactly one subscriber reply
//!   reaches the agent for any given agent-initiated request id.
//! - `MOCK_ACP_ECHO_CANCELS=1` — whenever the mock receives a
//!   `$/cancel_request` notification, emit an observable
//!   `mock/cancel_echo` notification carrying the `requestId` and a
//!   monotonic counter. Tests use this to confirm the proxy translated
//!   cancellation correctly.
//! - `MOCK_ACP_CANCEL_PERMISSION=1` — on `session/prompt` (with
//!   `MOCK_ACP_EMIT_PERMISSION=1`), after emitting the permission
//!   request the mock immediately emits a `$/cancel_request` for that
//!   permission's id (simulating an agent that gave up on the
//!   permission). Used to test agent → subscribers cancellation.
//! - `MOCK_ACP_SESSION_LIST=1` — advertise `sessionCapabilities.list`
//!   in the `initialize` response and respond to `session/list` with
//!   a small canned set of sessions (the current `sess-mock` plus two
//!   historical entries). Used to test session/list end-to-end
//!   passthrough through amux.
//!
//! Per-line behavior is logged to stderr at info level so tests can grep
//! the output if needed. The process exits when stdin closes.

use std::env;
use std::io::{self, BufRead, BufReader, Write};
use std::thread;
use std::time::Duration;

use serde_json::{Value, json};

fn main() {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    let mut reader = BufReader::new(stdin.lock());

    let session_id = env::var("MOCK_ACP_SESSION_ID").unwrap_or_else(|_| "sess-mock".to_string());
    let emit_permission = env::var("MOCK_ACP_EMIT_PERMISSION")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let prompt_delay_ms = env::var("MOCK_ACP_PROMPT_DELAY_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    let echo_responses = env::var("MOCK_ACP_ECHO_RESPONSES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let echo_cancels = env::var("MOCK_ACP_ECHO_CANCELS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let cancel_permission = env::var("MOCK_ACP_CANCEL_PERMISSION")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let session_list = env::var("MOCK_ACP_SESSION_LIST")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let mut initialize_count: u32 = 0;
    let mut session_new_count: u32 = 0;
    let mut prompt_count: u32 = 0;
    let mut next_permission_id: u64 = 10_000;
    let mut response_echo_count: u32 = 0;
    let mut cancel_echo_count: u32 = 0;

    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(err) => {
                eprintln!("mock_acp: stdin read error: {err}");
                break;
            }
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let frame: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(err) => {
                eprintln!("mock_acp: parse error: {err}: {trimmed}");
                continue;
            }
        };

        let id = frame.get("id").cloned();
        let method = frame.get("method").and_then(Value::as_str).unwrap_or("");

        eprintln!("mock_acp: rx method={method} id={id:?}");

        // Responses from the multiplexer (id + result/error, no method).
        // Optionally echo them as observable notifications so tests can
        // assert exactly one reply reached the agent for a given id.
        if id.is_some() && method.is_empty() {
            if echo_responses {
                response_echo_count += 1;
                let echo = json!({
                    "jsonrpc": "2.0",
                    "method": "mock/response_echo",
                    "params": {
                        "id": id,
                        "seq": response_echo_count,
                    },
                });
                writeln!(stdout, "{echo}").ok();
                stdout.flush().ok();
            }
            continue;
        }

        // Notifications without an id. `$/cancel_request` from the proxy
        // is the only one we care about — optionally echo it so tests
        // can verify cancellation translation.
        if id.is_none() {
            if echo_cancels && method == "$/cancel_request" {
                cancel_echo_count += 1;
                let request_id = frame
                    .get("params")
                    .and_then(|p| p.get("requestId"))
                    .cloned()
                    .unwrap_or(Value::Null);
                let echo = json!({
                    "jsonrpc": "2.0",
                    "method": "mock/cancel_echo",
                    "params": {
                        "requestId": request_id,
                        "seq": cancel_echo_count,
                    },
                });
                writeln!(stdout, "{echo}").ok();
                stdout.flush().ok();
            }
            continue;
        }
        let id = id.expect("checked above");

        match method {
            "initialize" => {
                initialize_count += 1;
                let mut result = json!({
                    "protocolVersion": 1,
                    "agentInfo": { "name": "mock-acp", "version": "0.0.1" },
                    "_invocation": initialize_count,
                });
                if session_list {
                    result["agentCapabilities"] = json!({
                        "sessionCapabilities": { "list": {} },
                    });
                }
                let resp = json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": result,
                });
                writeln!(stdout, "{resp}").ok();
                stdout.flush().ok();
            }
            "session/list" => {
                let result = if session_list {
                    let requested_cwd = frame
                        .get("params")
                        .and_then(|p| p.get("cwd"))
                        .and_then(|v| v.as_str());
                    let sessions = vec![
                        json!({
                            "sessionId": session_id,
                            "cwd": "/tmp/mock",
                            "title": "Current mock session",
                            "updatedAt": "2026-05-22T12:00:00Z",
                        }),
                        json!({
                            "sessionId": "sess-archive-001",
                            "cwd": "/tmp/mock",
                            "title": "Previous run",
                            "updatedAt": "2026-05-21T18:00:00Z",
                        }),
                        json!({
                            "sessionId": "sess-archive-002",
                            "cwd": "/tmp/other",
                            "title": "Different cwd",
                            "updatedAt": "2026-05-20T09:00:00Z",
                        }),
                    ];
                    let filtered: Vec<Value> = sessions
                        .into_iter()
                        .filter(|s| {
                            requested_cwd.is_none_or(|cwd| {
                                s.get("cwd").and_then(|c| c.as_str()) == Some(cwd)
                            })
                        })
                        .collect();
                    json!({ "sessions": filtered })
                } else {
                    // Capability not advertised; the agent should not have
                    // been called. Returning an error so any accidental
                    // call surfaces clearly in tests.
                    json!({ "sessions": [] })
                };
                let resp = json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": result,
                });
                writeln!(stdout, "{resp}").ok();
                stdout.flush().ok();
            }
            "session/new" => {
                session_new_count += 1;
                let resp = json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "sessionId": session_id,
                        "_invocation": session_new_count,
                    },
                });
                writeln!(stdout, "{resp}").ok();
                stdout.flush().ok();
            }
            "session/prompt" => {
                prompt_count += 1;
                let sess = frame
                    .get("params")
                    .and_then(|p| p.get("sessionId"))
                    .cloned()
                    .unwrap_or_else(|| json!(session_id));

                if emit_permission {
                    next_permission_id += 1;
                    let perm = json!({
                        "jsonrpc": "2.0",
                        "id": next_permission_id,
                        "method": "session/request_permission",
                        "params": {
                            "sessionId": sess,
                            "toolCall": {
                                "toolCallId": format!("mock-tool-{next_permission_id}"),
                                "title": "demo_tool",
                                "kind": "execute",
                                "status": "pending",
                            },
                            "options": [
                                { "optionId": "allow_once", "kind": "allow_once", "name": "Allow once" },
                                { "optionId": "deny", "kind": "reject_once", "name": "Deny" },
                            ],
                        },
                    });
                    writeln!(stdout, "{perm}").ok();
                    stdout.flush().ok();

                    if cancel_permission {
                        let cancel = json!({
                            "jsonrpc": "2.0",
                            "method": "$/cancel_request",
                            "params": { "requestId": next_permission_id },
                        });
                        writeln!(stdout, "{cancel}").ok();
                        stdout.flush().ok();
                    }
                }

                // Stream two update notifications.
                for chunk in ["hello ", "world"] {
                    let upd = json!({
                        "jsonrpc": "2.0",
                        "method": "session/update",
                        "params": {
                            "sessionId": sess,
                            "update": {
                                "kind": "agent_message_chunk",
                                "content": { "type": "text", "text": chunk },
                            },
                        },
                    });
                    writeln!(stdout, "{upd}").ok();
                }

                if prompt_delay_ms > 0 {
                    stdout.flush().ok();
                    thread::sleep(Duration::from_millis(prompt_delay_ms));
                }

                let resp = json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "stopReason": "end_turn",
                        "_invocation": prompt_count,
                    },
                });
                writeln!(stdout, "{resp}").ok();
                stdout.flush().ok();
            }
            _ => {
                let resp = json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {},
                });
                writeln!(stdout, "{resp}").ok();
                stdout.flush().ok();
            }
        }
    }
}
