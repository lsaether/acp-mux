//! `session/attach` enrichment hooks for `RoomsExtension`.

use super::*;

impl RoomsExtension {
    pub(super) fn should_include_replay_entry(&self, ctx: &MuxCtx, entry: ReplayView<'_>) -> bool {
        if self.replay_generation == 0 {
            return true;
        }
        if let Some(active_id) = self.active_segment_id
            && entry.ext_tag == active_id.0
        {
            return true;
        }

        let Ok(value) = serde_json::from_slice::<Value>(entry.frame) else {
            return false;
        };
        let Some(method) = value.get("method").and_then(Value::as_str) else {
            return false;
        };
        match method {
            "rooms/peer_joined" => value
                .pointer("/params/peerId")
                .and_then(Value::as_str)
                .is_some_and(|peer_id| ctx.subscriber(peer_id).is_some()),
            "session/update" => {
                let Some(canonical) = ctx.canonical_session_id() else {
                    return false;
                };
                value
                    .pointer("/params/sessionId")
                    .and_then(Value::as_str)
                    .is_some_and(|session_id| session_id == canonical)
            }
            _ => false,
        }
    }

    pub(super) fn replay_history(&self, ctx: &MuxCtx, full_lineage: bool) -> Vec<HistoryEntry> {
        ctx.replay_entries()
            .filter(|entry| full_lineage || self.should_include_replay_entry(ctx, entry.clone()))
            .filter_map(history_entry_from_replay)
            .collect()
    }

    pub(super) fn replay_retention_counts(&self, ctx: &MuxCtx) -> (usize, usize) {
        let mut dropped = 0;
        let mut retained = 0;
        for entry in ctx.replay_entries() {
            if self.should_include_replay_entry(ctx, entry) {
                retained += 1;
            } else {
                dropped += 1;
            }
        }
        (dropped, retained)
    }

    pub(super) fn replay_update_counts_by_session(&self, ctx: &MuxCtx) -> Map<String, Value> {
        let mut counts: HashMap<String, u64> = HashMap::new();
        for entry in ctx.replay_entries() {
            if !self.should_include_replay_entry(ctx, entry.clone()) {
                continue;
            }
            let Ok(value) = serde_json::from_slice::<Value>(entry.frame) else {
                continue;
            };
            if value.get("method").and_then(Value::as_str) != Some("session/update") {
                continue;
            }
            let Some(session_id) = value.pointer("/params/sessionId").and_then(Value::as_str)
            else {
                continue;
            };
            *counts.entry(session_id.to_string()).or_default() += 1;
        }
        counts
            .into_iter()
            .map(|(session_id, count)| (session_id, Value::Number(serde_json::Number::from(count))))
            .collect()
    }

    pub(super) fn send_replay_phase(&self, ctx: &mut MuxCtx, phase: &WakeReplayPhase) {
        let room_id = ctx.mux_id().to_string();
        ctx.send_to(
            &phase.peer_id,
            Bytes::from(rooms::replay_started(
                &room_id,
                &phase.phase,
                &phase.replay_order,
                phase.replay_generation,
                phase.replay_boundary_seq,
                phase.frames.len(),
            )),
        );
        for frame in &phase.frames {
            ctx.send_to(&phase.peer_id, Bytes::from(frame.clone()));
        }
        ctx.send_to(
            &phase.peer_id,
            Bytes::from(rooms::replay_complete(
                &room_id,
                &phase.phase,
                &phase.replay_order,
                phase.replay_generation,
                phase.replay_boundary_seq,
                phase.frames.len(),
            )),
        );
    }
}

pub(super) fn attach_meta_str<'a>(params: &'a AttachParams, key: &str) -> Option<&'a str> {
    params
        .meta
        .as_ref()
        .and_then(|meta| meta.get("rooms"))
        .and_then(|rooms| rooms.get(key))
        .and_then(Value::as_str)
}

fn entry_to_frame(entry: acp_mux::attach::HistoryEntry) -> Value {
    let mut frame = Map::new();
    frame.insert("jsonrpc".to_string(), Value::String("2.0".to_string()));
    frame.insert("method".to_string(), Value::String(entry.method));
    if let Some(params) = entry.params {
        frame.insert("params".to_string(), params);
    }
    Value::Object(frame)
}

fn history_entry_from_replay(entry: ReplayView<'_>) -> Option<HistoryEntry> {
    let frame = inject_replay_metadata(entry.frame, entry.recorded_at, entry.seq);
    let value: Value = serde_json::from_slice(&frame).ok()?;
    let object = value.as_object()?;
    let method = object.get("method")?.as_str()?.to_string();
    let params = object.get("params").cloned();
    Some(HistoryEntry { method, params })
}

pub(super) fn schedule_wake_payload(
    ctx: &mut MuxCtx,
    delay: std::time::Duration,
    payload: WakePayload,
) {
    if let Ok(bytes) = serde_json::to_vec(&payload) {
        ctx.schedule_wake(delay, bytes);
    }
}

pub(super) fn replay_stream_phases(
    peer_id: &str,
    replay_order: &str,
    replay_generation: u64,
    replay_boundary_seq: u64,
    history: Vec<HistoryEntry>,
) -> Vec<WakeReplayPhase> {
    if replay_order != "newest_turn_first" {
        return vec![WakeReplayPhase {
            peer_id: peer_id.to_string(),
            phase: "backfill".to_string(),
            replay_order: replay_order.to_string(),
            replay_generation,
            replay_boundary_seq,
            frames: history_entries_to_frame_strings(history),
        }];
    }

    let (prefix, groups) = turn_groups(history);
    let Some((latest, older)) = groups.split_first() else {
        return vec![WakeReplayPhase {
            peer_id: peer_id.to_string(),
            phase: "latest_segment".to_string(),
            replay_order: replay_order.to_string(),
            replay_generation,
            replay_boundary_seq,
            frames: history_entries_to_frame_strings(prefix),
        }];
    };

    let mut latest_segment = prefix;
    latest_segment.extend(latest.clone());
    let mut phases = vec![WakeReplayPhase {
        peer_id: peer_id.to_string(),
        phase: "latest_segment".to_string(),
        replay_order: replay_order.to_string(),
        replay_generation,
        replay_boundary_seq,
        frames: history_entries_to_frame_strings(latest_segment),
    }];
    let backfill_entries = older
        .iter()
        .flat_map(|group| group.iter().cloned())
        .collect::<Vec<_>>();
    if !backfill_entries.is_empty() {
        phases.push(WakeReplayPhase {
            peer_id: peer_id.to_string(),
            phase: "backfill".to_string(),
            replay_order: replay_order.to_string(),
            replay_generation,
            replay_boundary_seq,
            frames: history_entries_to_frame_strings(backfill_entries),
        });
    }
    phases
}

fn history_entries_to_frame_strings(history: Vec<HistoryEntry>) -> Vec<String> {
    history
        .into_iter()
        .filter_map(|entry| serde_json::to_string(&entry_to_frame(entry)).ok())
        .collect()
}

fn turn_groups(history: Vec<HistoryEntry>) -> (Vec<HistoryEntry>, Vec<Vec<HistoryEntry>>) {
    let mut groups: Vec<Vec<HistoryEntry>> = Vec::new();
    let mut prefix: Vec<HistoryEntry> = Vec::new();
    let mut current: Option<Vec<HistoryEntry>> = None;

    for entry in history {
        if entry.method == "rooms/turn_started" {
            if let Some(group) = current.take() {
                groups.push(group);
            }
            current = Some(vec![entry]);
        } else if let Some(group) = current.as_mut() {
            let closes =
                entry.method == "rooms/turn_complete" || entry.method == "rooms/turn_cancelled";
            group.push(entry);
            if closes && let Some(group) = current.take() {
                groups.push(group);
            }
        } else {
            prefix.push(entry);
        }
    }
    if let Some(group) = current {
        groups.push(group);
    }
    (prefix, groups)
}

pub(super) fn newest_turn_first_history(
    history: Vec<acp_mux::attach::HistoryEntry>,
) -> Vec<acp_mux::attach::HistoryEntry> {
    let mut groups: Vec<Vec<acp_mux::attach::HistoryEntry>> = Vec::new();
    let mut prefix: Vec<acp_mux::attach::HistoryEntry> = Vec::new();
    let mut current: Option<Vec<acp_mux::attach::HistoryEntry>> = None;

    for entry in history {
        if entry.method == "rooms/turn_started" {
            if let Some(group) = current.take() {
                groups.push(group);
            }
            current = Some(vec![entry]);
        } else if let Some(group) = current.as_mut() {
            let closes =
                entry.method == "rooms/turn_complete" || entry.method == "rooms/turn_cancelled";
            group.push(entry);
            if closes && let Some(group) = current.take() {
                groups.push(group);
            }
        } else {
            prefix.push(entry);
        }
    }
    if let Some(group) = current {
        groups.push(group);
    }

    let mut out = prefix;
    for group in groups.into_iter().rev() {
        out.extend(group);
    }
    out
}

pub(super) fn inject_replay_metadata(frame: &Bytes, recorded_at: &str, replay_seq: u64) -> Bytes {
    let Ok(mut value) = serde_json::from_slice::<Value>(frame) else {
        return frame.clone();
    };
    let Value::Object(root) = &mut value else {
        return frame.clone();
    };
    let Some(params) = object_field(root, "params") else {
        return frame.clone();
    };
    let Some(meta) = object_field(params, "_meta") else {
        return frame.clone();
    };
    let Some(rooms) = object_field(meta, "rooms") else {
        return frame.clone();
    };
    rooms.insert(
        "recordedAt".to_string(),
        Value::String(recorded_at.to_string()),
    );
    rooms.insert(
        "replaySeq".to_string(),
        Value::Number(serde_json::Number::from(replay_seq)),
    );
    serde_json::to_vec(&value)
        .map(Bytes::from)
        .unwrap_or_else(|_| frame.clone())
}
