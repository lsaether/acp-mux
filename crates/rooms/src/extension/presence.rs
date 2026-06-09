//! Presence and session-list hooks for `RoomsExtension`.

use super::*;

impl RoomsExtension {
    pub(super) fn note_driving_subscriber(&mut self, ctx: &mut MuxCtx, peer_id: &str) {
        if ctx.subscriber(peer_id).is_none() {
            return;
        }
        if self.driving_subscriber_peer_id.as_deref() != Some(peer_id) {
            self.driving_subscriber_peer_id = Some(peer_id.to_string());
            self.publish_session_list_metadata(ctx);
        }
    }

    pub(super) fn publish_session_list_metadata(&self, ctx: &MuxCtx) {
        let Some(acp_session_id) = ctx.canonical_session_id() else {
            return;
        };
        let subscriber_count = ctx.subscribers().count();
        if subscriber_count == 0 {
            self.session_list_index
                .remove_if_room(acp_session_id, ctx.mux_id());
            return;
        }
        self.session_list_index.upsert(
            acp_session_id,
            SessionListRoomsMetadata {
                room_id: ctx.mux_id().to_string(),
                subscriber_count,
                driving_subscriber: self.driving_subscriber_peer_id.clone(),
            },
        );
    }

    pub(super) fn clear_session_list_metadata(&self, ctx: &MuxCtx) {
        if let Some(acp_session_id) = ctx.canonical_session_id() {
            self.session_list_index
                .remove_if_room(acp_session_id, ctx.mux_id());
        }
    }

    pub(super) fn decorate_session_list_response(&self, resp: &mut IncomingResponse) {
        let Some(result) = resp.result.as_mut() else {
            return;
        };
        let Some(sessions) = result.get_mut("sessions").and_then(Value::as_array_mut) else {
            return;
        };
        for session in sessions {
            let Some(acp_session_id) = session
                .get("sessionId")
                .and_then(Value::as_str)
                .map(str::to_string)
            else {
                continue;
            };
            let Some(metadata) = self.session_list_index.get(&acp_session_id) else {
                continue;
            };
            inject_session_list_rooms_metadata(session, &metadata);
        }
    }
}

fn inject_session_list_rooms_metadata(session: &mut Value, metadata: &SessionListRoomsMetadata) {
    let Value::Object(session) = session else {
        return;
    };
    let Some(meta) = object_field(session, "_meta") else {
        return;
    };
    let Some(rooms) = object_field(meta, "rooms") else {
        return;
    };
    rooms.insert(
        "roomId".to_string(),
        Value::String(metadata.room_id.clone()),
    );
    rooms.insert(
        "subscriberCount".to_string(),
        Value::Number(serde_json::Number::from(metadata.subscriber_count)),
    );
    if let Some(driving_subscriber) = metadata.driving_subscriber.as_ref() {
        rooms.insert(
            "drivingSubscriber".to_string(),
            Value::String(driving_subscriber.clone()),
        );
    } else {
        rooms.remove("drivingSubscriber");
    }
}
