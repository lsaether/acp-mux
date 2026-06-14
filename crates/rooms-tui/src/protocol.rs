use serde_json::{Map, Value, json};

pub fn build_initialize(request_id: impl Into<Value>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": request_id.into(),
        "method": "initialize",
        "params": {
            "protocolVersion": 1,
            "clientCapabilities": {}
        }
    })
}

pub fn build_attach(
    request_id: impl Into<Value>,
    session_id: Option<&str>,
    client_id: Option<&str>,
    client_name: Option<&str>,
) -> Value {
    let mut params = Map::new();
    params.insert("historyPolicy".to_string(), json!("full_lineage"));
    params.insert(
        "_meta".to_string(),
        json!({
            "rooms": {
                "replayOrder": "newest_turn_first",
                "historyDelivery": "stream"
            }
        }),
    );
    if let Some(session_id) = non_empty(session_id) {
        params.insert("sessionId".to_string(), json!(session_id));
    }
    if let Some(client_id) = non_empty(client_id) {
        params.insert("clientId".to_string(), json!(client_id));
    }
    if let Some(client_name) = non_empty(client_name) {
        params.insert("clientInfo".to_string(), json!({ "name": client_name }));
    }

    json!({
        "jsonrpc": "2.0",
        "id": request_id.into(),
        "method": "session/attach",
        "params": Value::Object(params)
    })
}

pub fn build_session_prompt(request_id: impl Into<Value>, session_id: &str, text: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": request_id.into(),
        "method": "session/prompt",
        "params": {
            "sessionId": session_id,
            "prompt": [{ "type": "text", "text": text }]
        }
    })
}

pub fn build_queue_prompt(
    request_id: impl Into<Value>,
    text: &str,
    session_id: Option<&str>,
) -> Value {
    build_text_control(request_id, "rooms/queue_prompt", text, session_id)
}

pub fn build_steer_active_turn(
    request_id: impl Into<Value>,
    text: &str,
    session_id: Option<&str>,
) -> Value {
    build_text_control(request_id, "rooms/steer_active_turn", text, session_id)
}

pub fn build_cancel_active_turn(request_id: impl Into<Value>, reason: Option<&str>) -> Value {
    let mut params = Map::new();
    if let Some(reason) = non_empty(reason) {
        params.insert("reason".to_string(), json!(reason));
    }

    json!({
        "jsonrpc": "2.0",
        "id": request_id.into(),
        "method": "rooms/cancel_active_turn",
        "params": Value::Object(params)
    })
}

pub fn build_unqueue_prompt(request_id: impl Into<Value>, queue_item_id: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": request_id.into(),
        "method": "rooms/unqueue_prompt",
        "params": { "queueItemId": queue_item_id.trim() }
    })
}

fn build_text_control(
    request_id: impl Into<Value>,
    method: &str,
    text: &str,
    session_id: Option<&str>,
) -> Value {
    let mut params = Map::new();
    params.insert("text".to_string(), json!(text.trim()));
    if let Some(session_id) = non_empty(session_id) {
        params.insert("sessionId".to_string(), json!(session_id));
    }

    json!({
        "jsonrpc": "2.0",
        "id": request_id.into(),
        "method": method,
        "params": Value::Object(params)
    })
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}
