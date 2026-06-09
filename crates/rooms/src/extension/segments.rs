//! Segment and replay-generation hooks for `RoomsExtension`.

use super::*;

impl RoomsExtension {
    pub(super) fn rotate_segment(
        &mut self,
        ctx: &mut MuxCtx,
        new_acp_session_id: Option<String>,
        reason: EndReason,
    ) {
        let now = utc_rfc3339_now();
        let room_id = ctx.mux_id().to_string();

        let Some(current_id) = self.active_segment_id else {
            let id = SegmentId(self.next_segment_id);
            self.next_segment_id = self.next_segment_id.saturating_add(1);
            let mut seg = Segment::open(id, new_acp_session_id.clone(), next_replay_seq(ctx));
            seg.opened_at = now.clone();
            self.segments.push(seg);
            self.active_segment_id = Some(id);
            ctx.set_replay_tag(id.0);
            if self.options.emit_segment_frames {
                ctx.broadcast(rooms::segment_started(
                    &room_id,
                    id,
                    new_acp_session_id.as_deref(),
                    &now,
                ));
            }
            return;
        };

        if let Some(seg) = self.segments.iter_mut().find(|s| s.id == current_id) {
            seg.closed_at = Some(now.clone());
            seg.end_reason = Some(reason);
        }
        let new_id = SegmentId(self.next_segment_id);
        self.next_segment_id = self.next_segment_id.saturating_add(1);
        ctx.set_replay_tag(current_id.0);
        if self.options.emit_segment_frames {
            ctx.broadcast(rooms::segment_ended(
                &room_id,
                current_id,
                &now,
                reason,
                Some(new_id),
            ));
        }
        let closed_replay_seq = next_replay_seq(ctx).saturating_sub(1);
        if let Some(seg) = self.segments.iter_mut().find(|s| s.id == current_id) {
            seg.closed_replay_seq = Some(closed_replay_seq);
        }
        for item in &mut self.queued_prompts {
            if let Some(new_acp) = new_acp_session_id.as_deref() {
                item.session_id = new_acp.to_string();
            }
        }
        let mut new_segment =
            Segment::open(new_id, new_acp_session_id.clone(), next_replay_seq(ctx));
        new_segment.opened_at = now.clone();
        self.segments.push(new_segment);
        self.active_segment_id = Some(new_id);
        ctx.set_replay_tag(new_id.0);
        if self.options.emit_segment_frames {
            ctx.broadcast(rooms::segment_started(
                &room_id,
                new_id,
                new_acp_session_id.as_deref(),
                &now,
            ));
        }
    }

    pub(super) fn active_segment(&self) -> Option<&Segment> {
        let id = self.active_segment_id?;
        self.segments.iter().find(|s| s.id == id)
    }
}
