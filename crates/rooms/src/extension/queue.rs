//! Queue and steering hooks for `RoomsExtension`.

use super::*;

#[derive(Debug)]
struct ActiveControlParams {
    session_id: String,
    text: String,
}

impl RoomsExtension {
    fn parse_rooms_active_turn_control_params(
        &self,
        ctx: &mut MuxCtx,
        peer_id: &str,
        req: &IncomingRequest,
        require_active_turn: bool,
    ) -> Option<ActiveControlParams> {
        if require_active_turn && ctx.prompt_in_flight().is_none() {
            self.send_error_response(
                ctx,
                peer_id,
                req.id.clone(),
                NO_ACTIVE_TURN_ERROR_CODE,
                "rooms active-turn control requires an active turn",
            );
            return None;
        }

        let Some(Value::Object(params)) = req.params.as_ref() else {
            self.send_error_response(
                ctx,
                peer_id,
                req.id.clone(),
                INVALID_PARAMS_ERROR_CODE,
                "rooms control params must be an object",
            );
            return None;
        };

        let text = match params.get("text") {
            Some(Value::String(text)) => text.clone(),
            Some(_) => {
                self.send_error_response(
                    ctx,
                    peer_id,
                    req.id.clone(),
                    INVALID_PARAMS_ERROR_CODE,
                    "rooms control params.text must be a string",
                );
                return None;
            }
            None => match params.get("prompt").and_then(text_from_text_only_prompt) {
                Some(text) => text,
                None => {
                    self.send_error_response(
                        ctx,
                        peer_id,
                        req.id.clone(),
                        INVALID_PARAMS_ERROR_CODE,
                        "rooms control params.text or text-only params.prompt is required",
                    );
                    return None;
                }
            },
        };
        let text = text.trim();
        if text.is_empty() {
            self.send_error_response(
                ctx,
                peer_id,
                req.id.clone(),
                INVALID_PARAMS_ERROR_CODE,
                "rooms control text must be non-empty",
            );
            return None;
        }

        let requested_session_id = match params.get("sessionId") {
            Some(Value::String(session_id)) => Some(session_id.clone()),
            Some(_) => {
                self.send_error_response(
                    ctx,
                    peer_id,
                    req.id.clone(),
                    INVALID_PARAMS_ERROR_CODE,
                    "rooms control params.sessionId must be a string when present",
                );
                return None;
            }
            None => None,
        };
        let active_session_id = self
            .active_turn_session_id
            .clone()
            .or_else(|| ctx.canonical_session_id().map(str::to_string));
        if let (Some(requested), Some(active)) = (&requested_session_id, &active_session_id)
            && requested != active
        {
            self.send_error_response(
                ctx,
                peer_id,
                req.id.clone(),
                INVALID_PARAMS_ERROR_CODE,
                "rooms control params.sessionId must match the active or canonical sessionId",
            );
            return None;
        }
        let Some(session_id) = requested_session_id.or(active_session_id) else {
            self.send_error_response(
                ctx,
                peer_id,
                req.id.clone(),
                INVALID_PARAMS_ERROR_CODE,
                "rooms control could not determine an ACP sessionId",
            );
            return None;
        };

        Some(ActiveControlParams {
            session_id,
            text: text.to_string(),
        })
    }

    fn pending_queue_prompt_count(&self) -> usize {
        self.queued_prompts
            .iter()
            .filter(|item| matches!(item.kind, QueuedPromptKind::Queue))
            .count()
    }

    fn has_pending_hard_steer(&self) -> bool {
        self.queued_prompts
            .iter()
            .any(|item| matches!(item.kind, QueuedPromptKind::HardSteer { .. }))
    }

    pub(super) fn handle_rooms_queue_prompt_request(
        &mut self,
        ctx: &mut MuxCtx,
        peer_id: &str,
        req: &IncomingRequest,
    ) -> Disposition {
        let Some(control) = self.parse_rooms_active_turn_control_params(ctx, peer_id, req, false)
        else {
            return Disposition::Handled;
        };
        if self.pending_queue_prompt_count() >= MAX_MUX_QUEUE_PROMPTS {
            self.send_error_response(
                ctx,
                peer_id,
                req.id.clone(),
                QUEUE_FULL_ERROR_CODE,
                "queue full",
            );
            return Disposition::Handled;
        }
        let submit_immediately = ctx.prompt_in_flight().is_none();
        let queue_item_id = format!("q-{}", self.next_queue_item_id);
        self.next_queue_item_id += 1;
        let (peer_name, role) = ctx
            .subscriber(peer_id)
            .map(|s| (s.peer_name.as_deref(), s.role.as_deref()))
            .unwrap_or((None, None));
        self.queued_prompts.push_back(QueuedPrompt {
            queue_item_id: Some(queue_item_id.clone()),
            peer_id: peer_id.to_string(),
            session_id: control.session_id,
            prompt_text: control.text.clone(),
            kind: QueuedPromptKind::Queue,
        });
        let room_id = ctx.mux_id().to_string();
        ctx.broadcast(rooms::queue_item_added(
            &room_id,
            &queue_item_id,
            peer_id,
            peer_name,
            role,
            &control.text,
        ));
        let submitted = submit_immediately && self.submit_next_queued_prompt(ctx).is_some();
        let status = if submitted { "submitted" } else { "queued" };
        self.send_result_response(
            ctx,
            peer_id,
            req.id.clone(),
            json!({ "queueItemId": queue_item_id, "status": status }),
        );
        Disposition::Handled
    }

    pub(super) fn handle_rooms_unqueue_prompt_request(
        &mut self,
        ctx: &mut MuxCtx,
        peer_id: &str,
        req: &IncomingRequest,
    ) -> Disposition {
        let Some(Value::Object(params)) = req.params.as_ref() else {
            self.send_error_response(
                ctx,
                peer_id,
                req.id.clone(),
                INVALID_PARAMS_ERROR_CODE,
                "rooms/unqueue_prompt params must be an object",
            );
            return Disposition::Handled;
        };
        let Some(queue_item_id) = params.get("queueItemId").and_then(Value::as_str) else {
            self.send_error_response(
                ctx,
                peer_id,
                req.id.clone(),
                INVALID_PARAMS_ERROR_CODE,
                "rooms/unqueue_prompt params.queueItemId must be a string",
            );
            return Disposition::Handled;
        };
        let queue_item_id = queue_item_id.trim().to_string();
        if queue_item_id.is_empty() {
            self.send_error_response(
                ctx,
                peer_id,
                req.id.clone(),
                INVALID_PARAMS_ERROR_CODE,
                "rooms/unqueue_prompt params.queueItemId must be non-empty",
            );
            return Disposition::Handled;
        }
        let Some(position) = self
            .queued_prompts
            .iter()
            .position(|item| item.queue_item_id.as_deref() == Some(queue_item_id.as_str()))
        else {
            self.send_error_response(
                ctx,
                peer_id,
                req.id.clone(),
                QUEUE_ITEM_NOT_FOUND_ERROR_CODE,
                "queue item not found",
            );
            return Disposition::Handled;
        };
        self.queued_prompts.remove(position);
        let room_id = ctx.mux_id().to_string();
        ctx.broadcast(rooms::queue_item_removed(&room_id, &queue_item_id, peer_id));
        self.send_result_response(
            ctx,
            peer_id,
            req.id.clone(),
            json!({ "queueItemId": queue_item_id, "status": "removed" }),
        );
        Disposition::Handled
    }

    pub(super) fn handle_rooms_steer_request(
        &mut self,
        ctx: &mut MuxCtx,
        peer_id: &str,
        req: &IncomingRequest,
    ) -> Disposition {
        let Some(control) = self.parse_rooms_active_turn_control_params(ctx, peer_id, req, false)
        else {
            return Disposition::Handled;
        };
        if ctx.prompt_in_flight().is_none() {
            return self.handle_rooms_idle_steer_request(ctx, peer_id, req.id.clone(), control);
        }
        if self.has_pending_hard_steer() {
            self.send_error_response(
                ctx,
                peer_id,
                req.id.clone(),
                NO_ACTIVE_TURN_ERROR_CODE,
                "a hard steer is already pending for this turn",
            );
            return Disposition::Handled;
        }
        let supersedes_turn_id = match self.active_rooms_turn_id {
            Some(turn_id) => turn_id,
            None => return Disposition::Handled,
        };
        let Some(active_session_id) = self.active_turn_session_id.clone() else {
            self.send_error_response(
                ctx,
                peer_id,
                req.id.clone(),
                INVALID_PARAMS_ERROR_CODE,
                "rooms control could not determine the active ACP sessionId",
            );
            return Disposition::Handled;
        };
        let original_driver = ctx
            .prompt_in_flight()
            .and_then(|mux_id| ctx.pending_peer(mux_id).map(str::to_string))
            .unwrap_or_else(|| peer_id.to_string());
        let original_prompt = self.active_turn_prompt_text.as_deref();
        let replacement_prompt =
            build_hard_steer_prompt(peer_id, supersedes_turn_id, original_prompt, &control.text);
        let (peer_name, role) = ctx
            .subscriber(peer_id)
            .map(|s| (s.peer_name.as_deref(), s.role.as_deref()))
            .unwrap_or((None, None));
        let room_id = ctx.mux_id().to_string();
        ctx.broadcast(rooms::control_submitted(rooms::ControlSubmitted {
            room_id: &room_id,
            kind: "steer",
            mode: "hard",
            peer_id,
            peer_name,
            role,
            rooms_turn_id: Some(supersedes_turn_id),
            text: &control.text,
        }));
        ctx.broadcast(rooms::turn_cancelled(
            &room_id,
            supersedes_turn_id,
            peer_id,
            &original_driver,
            Some("hard_steer"),
        ));
        self.queued_prompts.push_front(QueuedPrompt {
            queue_item_id: None,
            peer_id: peer_id.to_string(),
            session_id: control.session_id,
            prompt_text: replacement_prompt,
            kind: QueuedPromptKind::HardSteer { supersedes_turn_id },
        });
        self.send_result_response(
            ctx,
            peer_id,
            req.id.clone(),
            json!({
                "accepted": true,
                "mode": "hard",
                "supersedesTurnId": supersedes_turn_id.formatted(),
            }),
        );
        ctx.send_to_agent(build_session_cancel(&active_session_id));
        Disposition::Handled
    }

    fn handle_rooms_idle_steer_request(
        &mut self,
        ctx: &mut MuxCtx,
        peer_id: &str,
        req_id: Id,
        control: ActiveControlParams,
    ) -> Disposition {
        let turn_id = RoomsTurnId(self.next_rooms_turn_id);
        let (peer_name, role) = ctx
            .subscriber(peer_id)
            .map(|s| (s.peer_name.as_deref(), s.role.as_deref()))
            .unwrap_or((None, None));
        let room_id = ctx.mux_id().to_string();
        ctx.broadcast(rooms::control_submitted(rooms::ControlSubmitted {
            room_id: &room_id,
            kind: "steer",
            mode: "prompt",
            peer_id,
            peer_name,
            role,
            rooms_turn_id: Some(turn_id),
            text: &control.text,
        }));
        self.queued_prompts.push_front(QueuedPrompt {
            queue_item_id: None,
            peer_id: peer_id.to_string(),
            session_id: control.session_id,
            prompt_text: control.text,
            kind: QueuedPromptKind::Prompt,
        });
        self.submit_next_queued_prompt(ctx);
        self.send_result_response(
            ctx,
            peer_id,
            req_id,
            json!({
                "accepted": true,
                "mode": "prompt",
                "status": "submitted",
                "roomsTurnId": turn_id.formatted(),
            }),
        );
        Disposition::Handled
    }

    pub(super) fn submit_next_queued_prompt(&mut self, ctx: &mut MuxCtx) -> Option<u64> {
        let item = self.queued_prompts.pop_front()?;
        self.note_driving_subscriber(ctx, &item.peer_id);
        let turn_id = RoomsTurnId(self.next_rooms_turn_id);
        self.next_rooms_turn_id += 1;
        let supersedes_turn_id = match item.kind {
            QueuedPromptKind::Prompt | QueuedPromptKind::Queue => None,
            QueuedPromptKind::HardSteer { supersedes_turn_id } => Some(supersedes_turn_id),
        };
        let queue_item_id = item.queue_item_id.clone();
        let params = json!({
            "sessionId": item.session_id,
            "prompt": [{ "type": "text", "text": item.prompt_text }],
        });
        let mux_id = ctx.submit_prompt(&item.peer_id, params.clone(), false);
        if mux_id == 0 {
            return None;
        }
        self.per_request.insert(
            mux_id,
            RoomsRequestData {
                rooms_turn_id: Some(turn_id),
                queue_item_id: queue_item_id.clone(),
                decorate_session_list: false,
            },
        );
        self.active_rooms_turn_id = Some(turn_id);
        self.active_turn_session_id = params
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_string);
        self.active_turn_prompt_text = params.get("prompt").and_then(text_from_text_only_prompt);
        self.emit_turn_started(
            ctx,
            &item.peer_id,
            turn_id,
            Some(&params),
            supersedes_turn_id,
        );
        if let Some(queue_item_id) = queue_item_id.as_deref() {
            let room_id = ctx.mux_id().to_string();
            ctx.broadcast(rooms::queue_item_submitted(&room_id, queue_item_id, turn_id));
        }
        Some(mux_id)
    }
}

pub(super) fn text_from_text_only_prompt(prompt: &Value) -> Option<String> {
    let prompt = prompt.as_array()?;
    if prompt.is_empty() {
        return None;
    }
    let mut text = String::new();
    for block in prompt {
        let block_type = block.get("type").and_then(Value::as_str)?;
        if block_type != "text" {
            return None;
        }
        let block_text = block.get("text").and_then(Value::as_str)?;
        text.push_str(block_text);
    }
    Some(text)
}

fn build_hard_steer_prompt(
    peer_id: &str,
    supersedes_turn_id: RoomsTurnId,
    original_prompt: Option<&str>,
    steering_text: &str,
) -> String {
    let original_prompt = original_prompt.unwrap_or("(unavailable/non-text)");
    format!(
        "Active turn steered by peer `{peer_id}` (supersedes {supersedes}). Use the steer below to answer the original prompt.\n\nOriginal:\n{original_prompt}\n\nSteer:\n{steering_text}",
        supersedes = supersedes_turn_id.formatted(),
    )
}

pub(super) fn build_session_cancel(session_id: &str) -> Vec<u8> {
    #[derive(serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct CancelParams<'a> {
        session_id: &'a str,
    }
    #[derive(serde::Serialize)]
    struct CancelFrame<'a> {
        jsonrpc: &'static str,
        method: &'static str,
        params: CancelParams<'a>,
    }
    serde_json::to_vec(&CancelFrame {
        jsonrpc: "2.0",
        method: SESSION_CANCEL_METHOD,
        params: CancelParams { session_id },
    })
    .expect("session/cancel frame is always serializable")
}

pub(super) struct RequestTrace<'a> {
    pub(super) peer_id: &'a str,
    pub(super) peer_name: Option<&'a str>,
    pub(super) role: Option<&'a str>,
    pub(super) mux_id: u64,
    pub(super) rooms_turn_id: Option<RoomsTurnId>,
}

pub(super) fn inject_request_trace_metadata(req: &mut IncomingRequest, trace: RequestTrace<'_>) {
    let Some(params) = object_params(req) else {
        return;
    };
    let Some(meta) = object_field(params, "_meta") else {
        return;
    };
    let Some(rooms) = object_field(meta, "rooms") else {
        return;
    };
    rooms.insert(
        "peerId".to_string(),
        Value::String(trace.peer_id.to_string()),
    );
    if let Some(peer_name) = trace.peer_name {
        rooms.insert("peerName".to_string(), Value::String(peer_name.to_string()));
    }
    if let Some(role) = trace.role {
        rooms.insert("role".to_string(), Value::String(role.to_string()));
    }
    rooms.insert(
        "muxId".to_string(),
        Value::Number(serde_json::Number::from(trace.mux_id)),
    );
    if let Some(turn_id) = trace.rooms_turn_id {
        rooms.insert("roomsTurnId".to_string(), Value::String(turn_id.formatted()));
    }
}
