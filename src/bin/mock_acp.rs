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

    let mut initialize_count: u32 = 0;
    let mut session_new_count: u32 = 0;
    let mut prompt_count: u32 = 0;
    let mut next_permission_id: u64 = 10_000;
    let mut response_echo_count: u32 = 0;

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

        // Notifications without an id are dropped silently.
        let Some(id) = id else { continue };

        match method {
            "initialize" => {
                initialize_count += 1;
                let resp = json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "protocolVersion": 1,
                        "agentInfo": { "name": "mock-acp", "version": "0.0.1" },
                        "_invocation": initialize_count,
                    },
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
