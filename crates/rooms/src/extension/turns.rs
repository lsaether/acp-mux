//! Turn lifecycle hooks for `RoomsExtension`.

use super::*;

impl RoomsExtension {
    pub(super) fn emit_turn_started(
        &mut self,
        ctx: &mut MuxCtx,
        peer_id: &str,
        turn_id: RoomsTurnId,
        params: Option<&Value>,
        supersedes_turn_id: Option<RoomsTurnId>,
    ) {
        let null = Value::Null;
        let content = params.and_then(|p| p.get("prompt")).unwrap_or(&null);
        let (peer_name, role) = ctx
            .subscriber(peer_id)
            .map(|s| (s.peer_name.clone(), s.role.clone()))
            .unwrap_or((None, None));
        let room_id = ctx.mux_id().to_string();
        ctx.broadcast(rooms::turn_started(
            &room_id,
            turn_id,
            peer_id,
            peer_name.as_deref(),
            role.as_deref(),
            content,
            supersedes_turn_id,
        ));
    }

    pub(super) fn emit_turn_complete(
        &mut self,
        ctx: &mut MuxCtx,
        turn_id: RoomsTurnId,
        result: Option<&Value>,
    ) {
        let null = Value::Null;
        let stop_reason = result.and_then(|r| r.get("stopReason")).unwrap_or(&null);
        let room_id = ctx.mux_id().to_string();
        ctx.broadcast(rooms::turn_complete(&room_id, turn_id, stop_reason));
    }
}
