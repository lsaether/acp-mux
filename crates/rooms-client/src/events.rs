use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    TurnStarted {
        room_id: String,
        turn_id: String,
        peer_id: String,
        peer_name: Option<String>,
        text: String,
    },
    QueueItemAdded {
        room_id: String,
        queue_item_id: String,
        peer_id: String,
        text: String,
    },
    PermissionRequested {
        request_id: String,
        session_id: Option<String>,
        title: Option<String>,
        options: Vec<String>,
    },
    Unknown {
        method: String,
    },
}

pub fn event_from_value(frame: &Value) -> Result<Event, String> {
    let object = frame
        .as_object()
        .ok_or_else(|| "JSON-RPC frame must be an object".to_string())?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err("JSON-RPC frame must use jsonrpc='2.0'".to_string());
    }

    let method = object
        .get("method")
        .and_then(Value::as_str)
        .ok_or_else(|| "JSON-RPC frame is missing method".to_string())?;
    let params = object.get("params").unwrap_or(&Value::Null);

    match method {
        "rooms/turn_started" => Ok(Event::TurnStarted {
            room_id: string_field(params, &["roomId", "room_id"]),
            turn_id: string_field(params, &["roomsTurnId", "rooms_turn_id"]),
            peer_id: string_field(params, &["peerId", "peer_id"]),
            peer_name: optional_string_field(params, &["peerName", "peer_name"]),
            text: text_from_params(params),
        }),
        "rooms/queue_item_added" => Ok(Event::QueueItemAdded {
            room_id: string_field(params, &["roomId", "room_id"]),
            queue_item_id: string_field(params, &["queueItemId", "queue_item_id"]),
            peer_id: string_field(params, &["peerId", "peer_id", "queuedBy", "queued_by"]),
            text: text_from_params(params),
        }),
        "session/request_permission" => Ok(Event::PermissionRequested {
            request_id: object.get("id").map(json_id_to_string).unwrap_or_default(),
            session_id: optional_string_field(params, &["sessionId", "session_id"]),
            title: permission_title(params),
            options: permission_options(params),
        }),
        _ => Ok(Event::Unknown {
            method: method.to_string(),
        }),
    }
}

fn string_field(value: &Value, names: &[&str]) -> String {
    optional_string_field(value, names).unwrap_or_default()
}

fn optional_string_field(value: &Value, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(Value::as_str))
        .map(ToString::to_string)
}

fn text_from_params(params: &Value) -> String {
    if let Some(text) = optional_string_field(params, &["text", "prompt"]) {
        return text;
    }
    for key in ["content", "prompt"] {
        if let Some(text) = text_from_content_array(params.get(key)) {
            return text;
        }
    }
    String::new()
}

fn text_from_content_array(value: Option<&Value>) -> Option<String> {
    let array = value?.as_array()?;
    let parts = array
        .iter()
        .filter_map(|item| item.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>();
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
}

fn permission_title(params: &Value) -> Option<String> {
    params
        .get("toolCall")
        .and_then(|tool| optional_string_field(tool, &["title", "name", "kind"]))
        .or_else(|| optional_string_field(params, &["title", "name"]))
}

fn permission_options(params: &Value) -> Vec<String> {
    params
        .get("options")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|option| optional_string_field(option, &["optionId", "option_id", "id"]))
        .collect()
}

fn json_id_to_string(value: &Value) -> String {
    if let Some(text) = value.as_str() {
        text.to_string()
    } else {
        value.to_string()
    }
}
