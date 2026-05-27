//! RFD #533-inspired `session/attach` / `session/detach` proxy-local methods
//! and the streaming attach machinery (snapshot, latest-segment, async
//! backfill) that lives on top of the mux's broadcast replay log.
//!
//! All methods here are declared as `impl RoomInner` blocks; the parent
//! `state` module owns the struct, fields, and actor loop. This module is
//! a logical split for review and maintenance, not a behavioral one.

use std::collections::VecDeque;
use std::time::Duration;

use bytes::Bytes;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::multiplex::subscriber::OutMsg;
use crate::protocol::amux;
use crate::protocol::attach::{
    self, AttachActiveTurn, AttachAmuxMeta, AttachMeta, AttachParams, AttachPendingPermission,
    AttachQueueItem, AttachResult, AttachSnapshot, ConnectedClient, DetachParams, DetachResult,
    HistoryDelivery, HistoryEntry, HistoryPolicy, ReplayOrder,
};
use crate::protocol::jsonrpc::IncomingRequest;
use crate::room::state::{PRE_SEGMENT_ID, QueuedPromptKind, ReplayEntry, RoomInner, RoomMsg};

#[derive(Debug)]
pub struct AttachStreamBackfill {
    peer_id: String,
    room_id: String,
    replay_order: ReplayOrder,
    replay_generation: u64,
    replay_boundary_seq: u64,
    frame_count: usize,
    segments: VecDeque<Vec<Bytes>>,
    started: bool,
}

impl RoomInner {
    pub(super) fn handle_attach(&mut self, peer_id: &str, req: IncomingRequest) {
        let params: AttachParams = req
            .params
            .as_ref()
            .map(|v| serde_json::from_value(v.clone()).unwrap_or_default())
            .unwrap_or_default();

        let requested_policy = params.history_policy.unwrap_or_default();
        let requested_replay_order = params
            .meta
            .as_ref()
            .and_then(|meta| meta.amux.as_ref())
            .and_then(|amux| amux.replay_order)
            .unwrap_or_default();
        let requested_history_delivery = params
            .meta
            .as_ref()
            .and_then(|meta| meta.amux.as_ref())
            .and_then(|amux| amux.history_delivery)
            .unwrap_or_default();
        let effective_policy = match requested_policy {
            HistoryPolicy::AfterMessage => {
                tracing::debug!(
                    session = %self.room_id,
                    %peer_id,
                    after_message_id = ?params.after_message_id,
                    "session/attach after_message requested; falling back to full until ACP message IDs are available end-to-end",
                );
                HistoryPolicy::Full
            }
            other => other,
        };

        let resolved_session_id = self
            .acp_session_id()
            .map(str::to_string)
            .unwrap_or_else(|| self.room_id.clone());
        if let Some(requested) = params.session_id.as_deref()
            && !requested.is_empty()
            && requested != resolved_session_id
            && requested != self.room_id
        {
            self.send_error_response(
                peer_id,
                req.id,
                attach::ATTACH_ERR_NOT_FOUND,
                "session not found",
            );
            return;
        }

        let connected_clients: Vec<ConnectedClient> = self
            .subscribers
            .values()
            .map(|s| ConnectedClient {
                client_id: s.peer_id.clone(),
                name: s.peer_name.clone(),
            })
            .collect();
        let applied_history_delivery = match (effective_policy, requested_history_delivery) {
            (HistoryPolicy::Full, HistoryDelivery::Stream)
            | (HistoryPolicy::FullLineage, HistoryDelivery::Stream) => HistoryDelivery::Stream,
            _ => HistoryDelivery::Response,
        };
        let stream_entries: Vec<ReplayEntry> =
            if applied_history_delivery == HistoryDelivery::Stream {
                self.replay_entries_for_policy(effective_policy)
            } else {
                Vec::new()
            };
        let replay_boundary_seq = stream_entries.last().map(|entry| entry.seq).unwrap_or(0);
        let snapshot = if applied_history_delivery == HistoryDelivery::Stream {
            Some(self.attach_snapshot(peer_id, connected_clients.clone(), replay_boundary_seq))
        } else {
            None
        };
        let history = if applied_history_delivery == HistoryDelivery::Stream {
            None
        } else {
            match effective_policy {
                HistoryPolicy::None => None,
                HistoryPolicy::Full => Some(Self::apply_history_replay_order(
                    self.history_full(),
                    requested_replay_order,
                )),
                HistoryPolicy::FullLineage => Some(Self::apply_history_replay_order(
                    self.history_full_lineage(),
                    requested_replay_order,
                )),
                HistoryPolicy::PendingOnly => Some(Self::apply_history_replay_order(
                    self.history_pending_only(),
                    requested_replay_order,
                )),
                HistoryPolicy::AfterMessage => unreachable!("normalized above"),
            }
        };
        let result = AttachResult {
            session_id: resolved_session_id,
            client_id: params.client_id.unwrap_or_else(|| peer_id.to_string()),
            history_policy: effective_policy,
            history,
            meta: AttachMeta {
                amux: AttachAmuxMeta {
                    connected_clients,
                    applied_replay_order: requested_replay_order,
                    applied_history_delivery,
                    snapshot,
                },
            },
        };
        let result = match serde_json::to_value(result) {
            Ok(v) => v,
            Err(err) => {
                tracing::error!(error = %err, "failed to serialize session/attach result");
                self.send_error_response(
                    peer_id,
                    req.id,
                    attach::ATTACH_ERR_UNSUPPORTED,
                    "session/attach serialization failed",
                );
                return;
            }
        };
        self.send_result_response(peer_id, req.id, result);
        if applied_history_delivery == HistoryDelivery::Stream {
            let pending_permission_frames = self
                .pending_permission_frames
                .iter()
                .map(|(_, frame)| frame.clone())
                .collect();
            self.stream_attach_history(
                peer_id,
                stream_entries,
                requested_replay_order,
                replay_boundary_seq,
                pending_permission_frames,
            );
        } else {
            self.reissue_pending_permissions(peer_id);
        }
    }

    fn attach_snapshot(
        &self,
        peer_id: &str,
        connected_clients: Vec<ConnectedClient>,
        replay_boundary_seq: u64,
    ) -> AttachSnapshot {
        let self_peer = connected_clients
            .iter()
            .find(|client| client.client_id == peer_id)
            .cloned()
            .unwrap_or_else(|| ConnectedClient {
                client_id: peer_id.to_string(),
                name: self
                    .subscribers
                    .get(peer_id)
                    .and_then(|s| s.peer_name.clone()),
            });
        let active_turn = self.active_amux_turn_id.and_then(|amux_turn_id| {
            self.active_turn_mux_id
                .and_then(|mux_id| self.pending.get(&mux_id))
                .map(|pending| AttachActiveTurn {
                    amux_turn_id: amux_turn_id.formatted(),
                    peer_id: pending.peer_id.clone(),
                })
        });
        let queue = self
            .queued_prompts
            .iter()
            .map(|item| AttachQueueItem {
                queue_item_id: item.queue_item_id.clone(),
                peer_id: item.peer_id.clone(),
                kind: match &item.kind {
                    QueuedPromptKind::Prompt => "prompt",
                    QueuedPromptKind::Queue => "queue",
                    QueuedPromptKind::HardSteer { .. } => "hard_steer",
                }
                .to_string(),
                status: "queued",
            })
            .collect();
        AttachSnapshot {
            connected_clients,
            self_peer,
            active_turn,
            queue,
            pending_permissions: self.pending_permission_summaries(),
            replay_boundary_seq,
            replay_generation: self.replay_generation,
            segments: self
                .segments
                .iter()
                .map(crate::room::state::segment_summary)
                .collect(),
            active_segment_id: self.active_segment_id,
        }
    }

    fn pending_permission_summaries(&self) -> Vec<AttachPendingPermission> {
        self.pending_permission_frames
            .iter()
            .map(|(id, frame)| {
                let value: Value = serde_json::from_slice(frame).unwrap_or(Value::Null);
                let params = value.get("params");
                let tool_call = params.and_then(|p| p.get("toolCall"));
                AttachPendingPermission {
                    request_id: serde_json::to_value(id).unwrap_or(Value::Null),
                    tool_name: tool_call
                        .and_then(|t| {
                            t.get("title")
                                .or_else(|| t.get("toolName"))
                                .or_else(|| t.get("name"))
                        })
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    summary: tool_call
                        .and_then(|t| t.get("title"))
                        .and_then(Value::as_str)
                        .map(str::to_string),
                }
            })
            .collect()
    }

    fn stream_attach_history(
        &self,
        peer_id: &str,
        entries: Vec<ReplayEntry>,
        replay_order: ReplayOrder,
        replay_boundary_seq: u64,
        pending_permission_frames: Vec<Bytes>,
    ) {
        let Some(sub) = self.subscribers.get(peer_id) else {
            return;
        };
        let outbound = sub.outbound.clone();
        let room_id = self.room_id.clone();
        let replay_generation = self.replay_generation;
        let replay_order_wire = Self::replay_order_wire(replay_order);
        let (latest_segment, backfill_segments) =
            Self::streaming_replay_segments(entries, replay_order);

        if !latest_segment.is_empty() {
            let frame_count = latest_segment.len();
            if outbound
                .send(OutMsg::Frame(Bytes::from(amux::replay_started(
                    &room_id,
                    "latest_segment",
                    replay_order_wire,
                    replay_generation,
                    replay_boundary_seq,
                    frame_count,
                ))))
                .is_err()
            {
                return;
            }
            for entry in latest_segment {
                if outbound
                    .send(OutMsg::Frame(entry.frame_for_replay()))
                    .is_err()
                {
                    return;
                }
            }
            if outbound
                .send(OutMsg::Frame(Bytes::from(amux::replay_complete(
                    &room_id,
                    "latest_segment",
                    replay_order_wire,
                    replay_generation,
                    replay_boundary_seq,
                    frame_count,
                ))))
                .is_err()
            {
                return;
            }
        }

        for frame in pending_permission_frames {
            if outbound.send(OutMsg::Frame(frame)).is_err() {
                return;
            }
        }

        if backfill_segments.is_empty() {
            return;
        }

        let frame_count: usize = backfill_segments.iter().map(Vec::len).sum();
        let plan = AttachStreamBackfill {
            peer_id: peer_id.to_string(),
            room_id,
            replay_order,
            replay_generation,
            replay_boundary_seq,
            frame_count,
            segments: backfill_segments
                .into_iter()
                .map(|segment| {
                    segment
                        .into_iter()
                        .map(|entry| entry.frame_for_replay())
                        .collect()
                })
                .collect(),
            started: false,
        };
        Self::schedule_attach_backfill(self.self_tx.clone(), plan, Duration::from_millis(25));
    }

    pub(super) fn send_attach_backfill_page(&self, mut plan: AttachStreamBackfill) {
        let Some(sub) = self.subscribers.get(&plan.peer_id) else {
            return;
        };
        let replay_order_wire = Self::replay_order_wire(plan.replay_order);
        if !plan.started {
            if sub
                .outbound
                .send(OutMsg::Frame(Bytes::from(amux::replay_started(
                    &plan.room_id,
                    "backfill",
                    replay_order_wire,
                    plan.replay_generation,
                    plan.replay_boundary_seq,
                    plan.frame_count,
                ))))
                .is_err()
            {
                return;
            }
            plan.started = true;
        }

        if let Some(segment) = plan.segments.pop_front() {
            for frame in segment {
                if sub.outbound.send(OutMsg::Frame(frame)).is_err() {
                    return;
                }
            }
        }

        if plan.segments.is_empty() {
            let _ = sub
                .outbound
                .send(OutMsg::Frame(Bytes::from(amux::replay_complete(
                    &plan.room_id,
                    "backfill",
                    replay_order_wire,
                    plan.replay_generation,
                    plan.replay_boundary_seq,
                    plan.frame_count,
                ))));
        } else {
            Self::schedule_attach_backfill(self.self_tx.clone(), plan, Duration::from_millis(1));
        }
    }

    fn schedule_attach_backfill(
        tx: mpsc::Sender<RoomMsg>,
        plan: AttachStreamBackfill,
        delay: Duration,
    ) {
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = tx.send(RoomMsg::AttachStreamBackfill(plan)).await;
        });
    }

    fn streaming_replay_segments(
        entries: Vec<ReplayEntry>,
        replay_order: ReplayOrder,
    ) -> (Vec<ReplayEntry>, Vec<Vec<ReplayEntry>>) {
        let mut ambient = Vec::new();
        let mut turns: Vec<Vec<ReplayEntry>> = Vec::new();
        let mut current_turn: Option<Vec<ReplayEntry>> = None;

        for entry in entries {
            match Self::replay_entry_method(&entry).as_deref() {
                Some("amux/turn_started") => {
                    if let Some(turn) = current_turn.take()
                        && !turn.is_empty()
                    {
                        turns.push(turn);
                    }
                    current_turn = Some(vec![entry]);
                }
                Some("amux/turn_complete") => {
                    if let Some(mut turn) = current_turn.take() {
                        turn.push(entry);
                        turns.push(turn);
                    } else {
                        ambient.push(entry);
                    }
                }
                _ => {
                    if let Some(turn) = current_turn.as_mut() {
                        turn.push(entry);
                    } else {
                        ambient.push(entry);
                    }
                }
            }
        }

        if let Some(turn) = current_turn
            && !turn.is_empty()
        {
            turns.push(turn);
        }

        match replay_order {
            ReplayOrder::Chronological => {
                let mut backfill_segments = Vec::new();
                if !ambient.is_empty() {
                    backfill_segments.push(ambient);
                }
                backfill_segments.extend(turns);
                (Vec::new(), backfill_segments)
            }
            ReplayOrder::NewestTurnFirst => {
                let latest_segment = turns.pop().unwrap_or_default();
                let mut backfill_segments: Vec<Vec<ReplayEntry>> =
                    turns.into_iter().rev().collect();
                if !ambient.is_empty() {
                    backfill_segments.push(ambient);
                }
                (latest_segment, backfill_segments)
            }
        }
    }

    fn replay_entry_method(entry: &ReplayEntry) -> Option<String> {
        let value: Value = serde_json::from_slice(&entry.frame).ok()?;
        value.get("method")?.as_str().map(str::to_string)
    }

    fn replay_order_wire(replay_order: ReplayOrder) -> &'static str {
        match replay_order {
            ReplayOrder::Chronological => "chronological",
            ReplayOrder::NewestTurnFirst => "newest_turn_first",
        }
    }

    /// Current-segment-only history. Includes pre-segment bootstrap
    /// frames (`SegmentId(0)`) so peer-presence emitted before the
    /// canonical ACP id was captured is preserved across the
    /// `historyPolicy: full` view.
    pub(super) fn history_full(&self) -> Vec<HistoryEntry> {
        let Some(log) = self.replay_log.as_ref() else {
            return Vec::new();
        };
        let active = self.active_segment_id;
        log.iter()
            .filter(|entry| entry.segment_id == PRE_SEGMENT_ID || Some(entry.segment_id) == active)
            .filter_map(|entry| Self::history_entry_from_frame(&entry.frame_for_replay()))
            .collect()
    }

    /// Every segment's frames concatenated in `replaySeq` order. The
    /// `historyPolicy: full_lineage` view for clients that want to see
    /// pre-compaction history.
    pub(super) fn history_full_lineage(&self) -> Vec<HistoryEntry> {
        let Some(log) = self.replay_log.as_ref() else {
            return Vec::new();
        };
        log.iter()
            .filter_map(|entry| Self::history_entry_from_frame(&entry.frame_for_replay()))
            .collect()
    }

    /// Replay entries shaped by `historyPolicy`, used by the streaming
    /// delivery path so `stream + full` honours current-segment-only
    /// semantics and never leaks pre-compaction lineage to clients that
    /// didn't opt in.
    pub(crate) fn replay_entries_for_policy(&self, policy: HistoryPolicy) -> Vec<ReplayEntry> {
        let Some(log) = self.replay_log.as_ref() else {
            return Vec::new();
        };
        match policy {
            HistoryPolicy::FullLineage => log.iter().cloned().collect(),
            HistoryPolicy::Full => {
                let active = self.active_segment_id;
                log.iter()
                    .filter(|entry| {
                        entry.segment_id == PRE_SEGMENT_ID || Some(entry.segment_id) == active
                    })
                    .cloned()
                    .collect()
            }
            // Other policies don't use streaming; fall back to current-segment view.
            _ => {
                let active = self.active_segment_id;
                log.iter()
                    .filter(|entry| {
                        entry.segment_id == PRE_SEGMENT_ID || Some(entry.segment_id) == active
                    })
                    .cloned()
                    .collect()
            }
        }
    }

    fn history_pending_only(&self) -> Vec<HistoryEntry> {
        self.pending_permission_frames
            .iter()
            .filter_map(|(_, frame)| Self::history_entry_from_frame(frame))
            .collect()
    }

    fn apply_history_replay_order(
        history: Vec<HistoryEntry>,
        replay_order: ReplayOrder,
    ) -> Vec<HistoryEntry> {
        match replay_order {
            ReplayOrder::Chronological => history,
            ReplayOrder::NewestTurnFirst => Self::newest_turn_first_history(history),
        }
    }

    fn newest_turn_first_history(history: Vec<HistoryEntry>) -> Vec<HistoryEntry> {
        let mut ambient = Vec::new();
        let mut turns: Vec<Vec<HistoryEntry>> = Vec::new();
        let mut current_turn: Option<Vec<HistoryEntry>> = None;

        for entry in history {
            match entry.method.as_str() {
                "amux/turn_started" => {
                    if let Some(turn) = current_turn.take()
                        && !turn.is_empty()
                    {
                        turns.push(turn);
                    }
                    current_turn = Some(vec![entry]);
                }
                "amux/turn_complete" => {
                    if let Some(mut turn) = current_turn.take() {
                        turn.push(entry);
                        turns.push(turn);
                    } else {
                        ambient.push(entry);
                    }
                }
                _ => {
                    if let Some(turn) = current_turn.as_mut() {
                        turn.push(entry);
                    } else {
                        ambient.push(entry);
                    }
                }
            }
        }

        if let Some(turn) = current_turn
            && !turn.is_empty()
        {
            turns.push(turn);
        }

        ambient.extend(turns.into_iter().rev().flatten());
        ambient
    }

    fn history_entry_from_frame(frame: &Bytes) -> Option<HistoryEntry> {
        let value: Value = serde_json::from_slice(frame).ok()?;
        let method = value.get("method")?.as_str()?.to_string();
        let params = value.get("params").cloned().unwrap_or(Value::Null);
        Some(HistoryEntry { method, params })
    }

    fn reissue_pending_permissions(&self, peer_id: &str) {
        if self.pending_permission_frames.is_empty() {
            return;
        }
        let Some(sub) = self.subscribers.get(peer_id) else {
            return;
        };
        for (_, frame) in &self.pending_permission_frames {
            if sub.outbound.send(OutMsg::Frame(frame.clone())).is_err() {
                tracing::debug!(%peer_id, "subscriber dropped during pending permission re-issue");
                return;
            }
        }
    }

    pub(super) fn handle_detach(&mut self, peer_id: &str, req: IncomingRequest) {
        let params: DetachParams = req
            .params
            .as_ref()
            .map(|v| serde_json::from_value(v.clone()).unwrap_or_default())
            .unwrap_or_default();
        let resolved_session_id = self
            .acp_session_id()
            .map(str::to_string)
            .unwrap_or_else(|| self.room_id.clone());
        if let Some(requested) = params.session_id.as_deref()
            && !requested.is_empty()
            && requested != resolved_session_id
            && requested != self.room_id
        {
            self.send_error_response(
                peer_id,
                req.id,
                attach::ATTACH_ERR_NOT_FOUND,
                "session not found",
            );
            return;
        }
        let result = DetachResult {
            session_id: resolved_session_id,
            status: "detached",
        };
        let Ok(result) = serde_json::to_value(result) else {
            self.send_error_response(
                peer_id,
                req.id,
                attach::ATTACH_ERR_UNSUPPORTED,
                "session/detach serialization failed",
            );
            return;
        };
        self.send_result_response(peer_id, req.id, result);
        if let Some(sub) = self.subscribers.get(peer_id) {
            let _ = sub.outbound.send(OutMsg::Close {
                code: 1000,
                reason: "client requested detach".to_string(),
            });
        }
    }
}
