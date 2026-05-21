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
//! Per-line behavior is logged to stderr at info level so tests can grep
//! the output if needed. The process exits when stdin closes.

use std::env;
use std::io::{self, BufRead, BufReader, Write};

use serde_json::{Value, json};

fn main() {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    let mut reader = BufReader::new(stdin.lock());

    let session_id = env::var("MOCK_ACP_SESSION_ID").unwrap_or_else(|_| "sess-mock".to_string());

    let mut initialize_count: u32 = 0;
    let mut session_new_count: u32 = 0;
    let mut prompt_count: u32 = 0;

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

        // Responses from the multiplexer (id + result, no method) are
        // ignored by this mock — it never initiates requests in chunk 4.
        if id.is_some() && method.is_empty() {
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
