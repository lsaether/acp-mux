//! Per-session state: agent subprocess + attached subscribers + the
//! actor task that serializes all mutations.
//!
//! All state mutation flows through a single tokio task driven by an mpsc
//! `RoomMsg` queue. Subscribers push inbound frames via
//! `InboundFromSubscriber` and detach via `Detach`. The agent's stdout pump
//! task forwards each NDJSON line as `AgentStdoutLine` and signals exit
//! via `AgentDied`.
//!
//! ## Routing contract
//!
//! Inbound (subscriber → agent), per JSON-RPC envelope shape:
//! - `notification` → forward to agent unchanged.
//! - `request` → check the `initialize` / `session/new` response cache; if
//!   present, answer the subscriber locally without touching the agent.
//!   Otherwise allocate a per-session `mux_id`, store the
//!   `(peer_id, original_id)` mapping, rewrite the `id`, and forward.
//!   Substantive (non-`initialize`) requests also mark the sender as the
//!   current "driving subscriber" — surfaced in `amux/turn_started` and
//!   `/debug/sessions`. Agent-initiated requests are broadcast (see the
//!   Inbound (agent → subscribers) `request` arm below), so the driver
//!   no longer has a privileged role at routing time.
//! - `session/prompt` requests participate in turn serialization: while a
//!   prompt is in flight, a second ordinary `session/prompt` is rejected
//!   locally with JSON-RPC error code `-32001` ("session busy"). Active-turn
//!   steering/queueing uses explicit `amux/steer_active_turn` and
//!   `amux/queue_prompt` requests. Hard steer is mux-owned cancel-and-
//!   replace when a turn is active; idle steer becomes an immediate prompt;
//!   queue is mux-owned queued prompt submission. The active turn clears when
//!   the matching response returns from the agent.
//! - `response` → forward unchanged. Subscriber-originated responses only
//!   show up as replies to agent-initiated requests, whose ids belong to
//!   the agent's own id space (never our `mux_id` space), so they round
//!   trip without rewriting.
//!
//! Inbound (agent → subscribers):
//! - `notification` → broadcast to every attached subscriber.
//! - `response` → look up `mux_id`, restore `original_id`, send to the
//!   originator only. If the original request was the first `initialize`
//!   or `session/new`, cache the `result` for later joiners. If it matches
//!   `active_turn_mux_id`, clear the active turn.
//! - `request` → emit inert `amux/agent_request_opened` metadata,
//!   broadcast the raw request to every live attached subscriber, and
//!   record the agent's request id as `InFlight`. Whichever subscriber
//!   replies first gets its response forwarded to the agent; the id
//!   transitions to `Consumed` and any later responses with the same id
//!   are dropped with a debug log. On the InFlight → Consumed transition
//!   the mux also broadcasts `amux/agent_request_resolved { requestId,
//!   resolvedBy, result | error }` so peers that lost the race (or never
//!   replied) can dismiss the request from their UI. Replay clients see
//!   the non-actionable opened/resolved lifecycle, not the stale raw ACP
//!   request. This lets any attached peer (not just the driver) confirm
//!   an agent-initiated request while preserving the JSON-RPC contract
//!   that the agent sees exactly one reply per id.
//!
//! Turn-end cleanup: when the session/prompt response arrives and
//! `active_turn_mux_id` clears, the mux sweeps every `agent_pending`
//! entry still `InFlight`, transitions them to `Consumed`, and
//! broadcasts `amux/agent_request_resolved { resolvedBy:
//! "mux:turn-ended", result: null, error: null }` for each. This catches
//! the case where the agent times out an unanswered permission
//! internally (for example due to an agent-side permission timeout) and proceeds without writing a
//! response frame — without that sweep, TUI clients would be stuck
//! displaying a permission the agent has already abandoned.
//!
//! Frames that fail JSON-RPC envelope parsing fall back to raw broadcast
//! on the agent → subscribers direction (so non-JSON debug output, if any,
//! still reaches clients), and are dropped with a warn on the
//! subscriber → agent direction (we will not feed garbage to a real ACP
//! server).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde_json::{Map, Value, json};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::agent::process::AgentProcess;
use crate::cli::{ClientToolMode, ClientToolPolicy, ReplayTurns};
use crate::multiplex::subscriber::{OutMsg, Subscriber};
use crate::protocol::amux::{self, AmuxTurnId, EndReason, SegmentId};
use crate::protocol::attach::{self, SegmentSummary};
use crate::protocol::jsonrpc::{
    Id, Incoming, IncomingRequest, IncomingResponse, JsonRpcError, JsonRpcVersion,
};
pub use crate::room::attach::AttachStreamBackfill;
use crate::room::replay_store::ReplayStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionListAmuxMetadata {
    pub room_id: String,
    pub subscriber_count: usize,
    pub driving_subscriber: Option<String>,
}

#[derive(Debug, Default)]
pub struct SessionListMetadataIndex {
    by_acp_session_id: RwLock<HashMap<String, SessionListAmuxMetadata>>,
}

impl SessionListMetadataIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, acp_session_id: &str) -> Option<SessionListAmuxMetadata> {
        self.by_acp_session_id
            .read()
            .expect("session list metadata index poisoned")
            .get(acp_session_id)
            .cloned()
    }

    fn upsert(&self, acp_session_id: &str, metadata: SessionListAmuxMetadata) {
        self.by_acp_session_id
            .write()
            .expect("session list metadata index poisoned")
            .insert(acp_session_id.to_string(), metadata);
    }

    fn remove_if_room(&self, acp_session_id: &str, room_id: &str) {
        let mut index = self
            .by_acp_session_id
            .write()
            .expect("session list metadata index poisoned");
        if index
            .get(acp_session_id)
            .is_some_and(|meta| meta.room_id == room_id)
        {
            index.remove(acp_session_id);
        }
    }
}

const SESSION_QUEUE_CAPACITY: usize = 256;
const MAX_MUX_QUEUE_PROMPTS: usize = 6;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

/// Mux ids start at 1; 0 is reserved as a sentinel.
const FIRST_MUX_ID: u64 = 1;

/// JSON-RPC error code returned to a subscriber that issues a second
/// `session/prompt` while another turn is already in flight. The
/// -32000..=-32099 range is reserved by the spec for implementation
/// defined errors; -32001 was chosen by the ROADMAP.
const SESSION_BUSY_ERROR_CODE: i64 = -32001;

/// JSON-RPC error code returned for strict amux active-turn controls
/// that cannot be applied to the current turn state.
const NO_ACTIVE_TURN_ERROR_CODE: i64 = -32002;

/// JSON-RPC error code returned when the mux-owned prompt queue is at
/// capacity. Kept distinct from ordinary `session/prompt` busy errors so
/// clients can render "queue full" rather than generic turn serialization.
const QUEUE_FULL_ERROR_CODE: i64 = -32003;

/// Standard JSON-RPC invalid params code used when an amux control request
/// is missing its text/session payload.
const INVALID_PARAMS_ERROR_CODE: i64 = -32602;

/// JSON-RPC error code for implementation-defined ACP client-tool policy
/// rejections. The structured `data.reason` distinguishes this from other
/// mux-owned failures.
const CLIENT_TOOL_BLOCKED_ERROR_CODE: i64 = -32000;

/// WebSocket close code used when the agent subprocess exits while
/// subscribers are still attached. 1011 = "internal error" per RFC 6455.
const WS_CLOSE_AGENT_DEAD: u16 = 1011;

/// JSON-RPC method name for the cancellation notification (request-cancellation
/// RFD; LSP-derived). Either direction may emit it.
const CANCEL_REQUEST_METHOD: &str = "$/cancel_request";
/// ACP-native session cancellation. Agents can wire this method to
/// its cooperative interrupt path, so active-turn cancellation must use
/// this session-scoped primitive rather than request-id cancellation.
const SESSION_CANCEL_METHOD: &str = "session/cancel";

#[derive(Debug, Clone)]
pub(super) struct ReplayEntry {
    pub(super) frame: Bytes,
    recorded_at: String,
    pub(super) seq: u64,
    /// Segment id at the time the frame was recorded. Frames recorded
    /// before any segment opens (e.g. `initialize` handshake replies)
    /// carry `SegmentId(0)` as a sentinel — those frames sit in no
    /// segment and are not counted against any segment's range.
    pub(super) segment_id: SegmentId,
}

impl ReplayEntry {
    fn new(seq: u64, frame: Bytes, segment_id: SegmentId) -> Self {
        Self {
            frame,
            recorded_at: utc_rfc3339_now(),
            seq,
            segment_id,
        }
    }

    /// Rebuild a `ReplayEntry` from a persisted record. Preserves the
    /// original `recorded_at` (criterion: replay metadata must use the
    /// mux-recorded time, not now).
    fn from_persisted(seq: u64, frame: Bytes, segment_id: SegmentId, recorded_at: String) -> Self {
        Self {
            frame,
            recorded_at,
            seq,
            segment_id,
        }
    }

    pub(super) fn frame_for_replay(&self) -> Bytes {
        inject_replay_metadata(&self.frame, &self.recorded_at, self.seq)
    }
}

/// Outcome of replaying the on-disk store into a fresh `RoomInner`.
struct Hydrated {
    replay_store: Option<crate::room::replay_store::RoomReplayStore>,
    segments: Vec<Segment>,
    active_segment_id: Option<SegmentId>,
    next_replay_seq: u64,
    next_segment_id: u64,
    /// Canonical ACP `sessionId` observed at the time the persisted log
    /// was written. Restored so late joiners can `session/attach` with
    /// the original `sessionId` before the fresh agent has had a chance
    /// to issue a new `session/new` / `session/load`. The next
    /// canonical-id change (fresh `session/new` returning a different
    /// id, or `session/load`) rotates the segment as normal.
    canonical_session_id: Option<String>,
}

impl Default for Hydrated {
    /// Matches the no-store-configured baseline: 1 is the first id that
    /// `RoomInner::new` historically used for both seq and segment id
    /// allocators.
    fn default() -> Self {
        Self {
            replay_store: None,
            segments: Vec::new(),
            active_segment_id: None,
            next_replay_seq: 1,
            next_segment_id: 1,
            canonical_session_id: None,
        }
    }
}

fn hydrate_from_store(
    store: &Arc<ReplayStore>,
    room_id: &str,
    log: &mut VecDeque<ReplayEntry>,
) -> Hydrated {
    let mut handle = match store.open_room(room_id) {
        Ok(h) => h,
        Err(err) => {
            tracing::error!(
                room = %room_id,
                error = %err,
                "replay store: failed to open per-room handle; continuing without persistence",
            );
            return Hydrated::default();
        }
    };

    let loaded = handle.take_loaded();
    let mut max_seq = 0u64;
    let mut max_segment_id = 0u64;
    let mut segments: Vec<Segment> = Vec::new();
    let mut active_segment_id: Option<SegmentId> = None;

    for record in &loaded {
        max_seq = max_seq.max(record.seq);
        max_segment_id = max_segment_id.max(record.segment_id);
        rebuild_segment_from_frame(
            &record.frame,
            SegmentId(record.segment_id),
            record.seq,
            &record.recorded_at,
            &mut segments,
            &mut active_segment_id,
        );
        log.push_back(ReplayEntry::from_persisted(
            record.seq,
            record.frame_bytes(),
            SegmentId(record.segment_id),
            record.recorded_at.clone(),
        ));
    }

    if !loaded.is_empty() {
        tracing::info!(
            room = %room_id,
            frames = loaded.len(),
            segments = segments.len(),
            "replay store: hydrated room from disk",
        );
    }

    // The most recently observed acpSessionId — taken from the
    // latest segment that carries one. This lets late joiners attach
    // to the prior session id before the fresh agent has spoken.
    let canonical_session_id = segments
        .iter()
        .rev()
        .find_map(|seg| seg.acp_session_id.clone());

    Hydrated {
        replay_store: Some(handle),
        segments,
        active_segment_id,
        next_replay_seq: max_seq.saturating_add(1).max(1),
        next_segment_id: max_segment_id.saturating_add(1).max(1),
        canonical_session_id,
    }
}

/// Walk a persisted broadcast frame and update the rebuilt `segments`
/// vec / `active_segment_id` based on `amux/segment_started` and
/// `amux/segment_ended` notifications. Other frames are ignored.
///
/// Restored fields per segment, sourced from the bookend frame's
/// `params`: `acp_session_id` and `end_reason`. The
/// `opened_replay_seq` / `closed_replay_seq` come from the persisted
/// record's `seq` so cross-restart segment-bracket slicing stays
/// correct.
fn rebuild_segment_from_frame(
    frame: &Value,
    frame_segment_id: SegmentId,
    record_seq: u64,
    recorded_at: &str,
    segments: &mut Vec<Segment>,
    active_segment_id: &mut Option<SegmentId>,
) {
    let Some(method) = frame.get("method").and_then(Value::as_str) else {
        return;
    };
    let params = frame.get("params").and_then(Value::as_object);
    match method {
        "amux/segment_started" => {
            let acp_session_id = params
                .and_then(|p| p.get("acpSessionId"))
                .and_then(Value::as_str)
                .map(str::to_string);
            let mut seg = Segment::open(frame_segment_id, acp_session_id, record_seq);
            seg.opened_at = recorded_at.to_string();
            // De-dup: a `segment_started` bookend lives in its own
            // segment, so a second observation of the same id would be
            // unexpected. Skip if already present.
            if !segments.iter().any(|s| s.id == frame_segment_id) {
                segments.push(seg);
            }
            *active_segment_id = Some(frame_segment_id);
        }
        "amux/segment_ended" => {
            // SegmentId serializes as the string "seg-N"; accept either
            // shape for forward/backward compatibility (older persisted
            // logs may have carried a raw u64).
            let closing_id = params
                .and_then(|p| p.get("segmentId"))
                .and_then(parse_segment_id_value)
                .unwrap_or(frame_segment_id);
            let end_reason = params
                .and_then(|p| p.get("endReason"))
                .and_then(|v| serde_json::from_value::<EndReason>(v.clone()).ok());
            if let Some(seg) = segments.iter_mut().find(|s| s.id == closing_id) {
                seg.closed_at = Some(recorded_at.to_string());
                seg.closed_replay_seq = Some(record_seq);
                if seg.end_reason.is_none() {
                    seg.end_reason = end_reason;
                }
            }
            if *active_segment_id == Some(closing_id) {
                *active_segment_id = None;
            }
        }
        _ => {}
    }
}

fn parse_segment_id_value(value: &Value) -> Option<SegmentId> {
    if let Some(n) = value.as_u64() {
        return Some(SegmentId(n));
    }
    let s = value.as_str()?;
    let trimmed = s.strip_prefix("seg-").unwrap_or(s);
    trimmed.parse::<u64>().ok().map(SegmentId)
}

/// Sentinel segment id used for frames recorded before any canonical ACP
/// session id has been captured (i.e. before the first segment opens).
pub(super) const PRE_SEGMENT_ID: SegmentId = SegmentId(0);

fn inject_replay_metadata(frame: &Bytes, recorded_at: &str, replay_seq: u64) -> Bytes {
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
    let Some(amux) = object_field(meta, "amux") else {
        return frame.clone();
    };
    amux.insert(
        "recordedAt".to_string(),
        Value::String(recorded_at.to_string()),
    );
    amux.insert(
        "replaySeq".to_string(),
        Value::Number(serde_json::Number::from(replay_seq)),
    );

    serde_json::to_vec(&value)
        .map(Bytes::from)
        .unwrap_or_else(|err| {
            tracing::warn!(error = %err, "failed to serialize replay metadata frame; replaying original");
            frame.clone()
        })
}

struct RequestTrace<'a> {
    peer_id: &'a str,
    peer_name: Option<&'a str>,
    role: Option<&'a str>,
    mux_id: u64,
    amux_turn_id: Option<AmuxTurnId>,
}

fn inject_request_trace_metadata(req: &mut IncomingRequest, trace: RequestTrace<'_>) {
    let Some(params) = object_params(req) else {
        return;
    };
    let Some(meta) = object_field(params, "_meta") else {
        return;
    };
    let Some(amux) = object_field(meta, "amux") else {
        return;
    };

    amux.insert(
        "peerId".to_string(),
        Value::String(trace.peer_id.to_string()),
    );
    if let Some(peer_name) = trace.peer_name {
        amux.insert("peerName".to_string(), Value::String(peer_name.to_string()));
    }
    if let Some(role) = trace.role {
        amux.insert("role".to_string(), Value::String(role.to_string()));
    }
    amux.insert(
        "muxId".to_string(),
        Value::Number(serde_json::Number::from(trace.mux_id)),
    );
    if let Some(turn_id) = trace.amux_turn_id {
        amux.insert("amuxTurnId".to_string(), Value::String(turn_id.formatted()));
    }
}

fn inject_session_list_amux_metadata(session: &mut Value, metadata: &SessionListAmuxMetadata) {
    let Value::Object(session) = session else {
        return;
    };
    let Some(meta) = object_field(session, "_meta") else {
        return;
    };
    let Some(amux) = object_field(meta, "amux") else {
        return;
    };

    amux.insert(
        "roomId".to_string(),
        Value::String(metadata.room_id.clone()),
    );
    amux.insert(
        "subscriberCount".to_string(),
        Value::Number(serde_json::Number::from(metadata.subscriber_count)),
    );
    if let Some(driving_subscriber) = metadata.driving_subscriber.as_ref() {
        amux.insert(
            "drivingSubscriber".to_string(),
            Value::String(driving_subscriber.clone()),
        );
    } else {
        amux.remove("drivingSubscriber");
    }
}

fn object_params(req: &mut IncomingRequest) -> Option<&mut Map<String, Value>> {
    let params = req.params.get_or_insert_with(|| Value::Object(Map::new()));
    match params {
        Value::Object(map) => Some(map),
        _ => None,
    }
}

fn object_field<'a>(
    object: &'a mut Map<String, Value>,
    key: &str,
) -> Option<&'a mut Map<String, Value>> {
    let value = object
        .entry(key.to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    match value {
        Value::Object(map) => Some(map),
        _ => None,
    }
}

fn utc_rfc3339_now() -> String {
    system_time_to_rfc3339_utc(SystemTime::now())
}

fn system_time_to_rfc3339_utc(time: SystemTime) -> String {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let total_secs = duration.as_secs() as i64;
    let days = total_secs.div_euclid(86_400);
    let secs_of_day = total_secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3_600;
    let minute = (secs_of_day % 3_600) / 60;
    let second = secs_of_day % 60;
    format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{nanos:09}Z",
        nanos = duration.subsec_nanos(),
    )
}

// Howard Hinnant's civil-from-days algorithm, with day 0 = 1970-01-01.
fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    (year, month as u32, day as u32)
}

/// Extract a non-null `requestId` from a `$/cancel_request` params object.
/// Per the RFD the field is `number | string`; we additionally treat
/// `Id::Null` as invalid (cancellation of an id-less notification is
/// meaningless).
fn parse_cancel_request_id(params: Option<&Value>) -> Option<Id> {
    let id_value = params.and_then(|v| v.get("requestId"))?.clone();
    let id: Id = serde_json::from_value(id_value).ok()?;
    match id {
        Id::Null => None,
        other => Some(other),
    }
}

/// Build a `$/cancel_request` notification frame as NDJSON bytes (no
/// trailing newline; the writer adds framing). Used both for forwarding
/// a subscriber-originated cancel with a translated id.
fn build_cancel_request(request_id: Id) -> Vec<u8> {
    #[derive(serde::Serialize)]
    struct CancelParams<'a> {
        #[serde(rename = "requestId")]
        request_id: &'a Id,
    }
    #[derive(serde::Serialize)]
    struct CancelFrame<'a> {
        jsonrpc: &'static str,
        method: &'static str,
        params: CancelParams<'a>,
    }
    serde_json::to_vec(&CancelFrame {
        jsonrpc: "2.0",
        method: CANCEL_REQUEST_METHOD,
        params: CancelParams {
            request_id: &request_id,
        },
    })
    .expect("cancel_request frame is always serializable")
}

/// Build a `session/cancel` notification frame as NDJSON bytes (no
/// trailing newline; the writer adds framing). Used for
/// `amux/cancel_active_turn`, where the intended target is the active
/// ACP session/turn rather than a JSON-RPC request id.
fn build_session_cancel(session_id: &str) -> Vec<u8> {
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

fn build_client_tool_blocked_response(id: Id, method: &str) -> Vec<u8> {
    let resp = IncomingResponse {
        jsonrpc: JsonRpcVersion,
        id,
        result: None,
        error: Some(JsonRpcError {
            code: CLIENT_TOOL_BLOCKED_ERROR_CODE,
            message: format!("client tool request blocked by acp-mux policy: {method}"),
            data: Some(json!({
                "reason": "client_tool_blocked",
                "method": method,
                "policy": "block",
            })),
        }),
    };
    serde_json::to_vec(&resp).expect("client-tool blocked response is always serializable")
}

/// Extract session/update counts from replay entries for debug snapshots.
fn replay_log_update_counts_by_acp_session_id(
    log: &VecDeque<ReplayEntry>,
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for entry in log {
        let Ok(value) = serde_json::from_slice::<Value>(&entry.frame) else {
            continue;
        };
        if value.get("method").and_then(Value::as_str) != Some("session/update") {
            continue;
        }
        let Some(session_id) = value
            .get("params")
            .and_then(|params| params.get("sessionId"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        *counts.entry(session_id.to_string()).or_insert(0) += 1;
    }
    counts
}

pub enum RoomMsg {
    Attach {
        subscriber: Subscriber,
        ack: oneshot::Sender<Result<(), AttachError>>,
    },
    Detach {
        peer_id: String,
    },
    InboundFromSubscriber {
        peer_id: String,
        bytes: Vec<u8>,
    },
    AgentStdoutLine(Vec<u8>),
    AgentStderrLine(Vec<u8>),
    AgentDied,
    AttachStreamBackfill(AttachStreamBackfill),
    /// Build a JSON snapshot of session state for `/debug/sessions`.
    Snapshot {
        ack: oneshot::Sender<RoomSnapshot>,
    },
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayResetSnapshot {
    pub loaded_session_id: String,
    pub replay_generation: u64,
    pub dropped_frame_count: usize,
    pub retained_frame_count: usize,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Segment {
    pub id: SegmentId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acp_session_id: Option<String>,
    pub opened_at: String,
    pub opened_replay_seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed_replay_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_reason: Option<EndReason>,
}

impl Segment {
    fn open(id: SegmentId, acp_session_id: Option<String>, opened_replay_seq: u64) -> Self {
        Self {
            id,
            acp_session_id,
            opened_at: utc_rfc3339_now(),
            opened_replay_seq,
            closed_at: None,
            closed_replay_seq: None,
            end_reason: None,
        }
    }
}

pub(crate) fn segment_summary(seg: &Segment) -> SegmentSummary {
    SegmentSummary {
        id: seg.id,
        acp_session_id: seg.acp_session_id.clone(),
        opened_at: seg.opened_at.clone(),
        opened_replay_seq: seg.opened_replay_seq,
        closed_at: seg.closed_at.clone(),
        closed_replay_seq: seg.closed_replay_seq,
        end_reason: seg.end_reason,
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RoomSnapshot {
    pub room_id: String,
    pub agent_cwd: String,
    pub subscribers: Vec<SubscriberSnapshot>,
    pub pending_request_count: usize,
    pub initialize_cached: bool,
    pub cached_session_id: Option<String>,
    pub active_turn_mux_id: Option<u64>,
    pub active_amux_turn_id: Option<String>,
    pub driving_subscriber: Option<String>,
    pub subprocess_dead: bool,
    pub ttl_pending: bool,
    pub replay_log_len: Option<usize>,
    pub replay_generation: u64,
    pub replay_log_update_frames_by_acp_session_id: Option<BTreeMap<String, usize>>,
    pub last_replay_reset: Option<ReplayResetSnapshot>,
    pub next_mux_id: u64,
    pub next_amux_turn_id: u64,
    /// Segments observed in this room, oldest→newest. Empty before the
    /// first canonical ACP session id arrives.
    pub segments: Vec<Segment>,
    pub active_segment_id: Option<SegmentId>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriberSnapshot {
    pub peer_id: String,
    pub peer_name: Option<String>,
    pub role: Option<String>,
    pub is_driving: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttachError {
    PeerIdInUse,
}

#[derive(Clone)]
pub struct RoomHandle {
    pub tx: mpsc::Sender<RoomMsg>,
}

impl RoomHandle {
    pub fn is_alive(&self) -> bool {
        !self.tx.is_closed()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HandshakeKind {
    Initialize,
    SessionNew,
    /// `session/load` request that asked the agent to switch to an
    /// existing session id. On success the room's canonical session id
    /// is rebound to this value so late joiners' `session/new` calls
    /// return the loaded session (not the original one). Captured at
    /// request-translation time from the client's `params.sessionId`
    /// — re-reading it off the response isn't reliable because not
    /// every agent echoes the loaded id back.
    SessionLoad {
        loaded_session_id: String,
        replay_start_len: usize,
    },
}

#[derive(Debug)]
pub(super) struct PendingRequest {
    pub(super) peer_id: String,
    original_id: Id,
    handshake: Option<HandshakeKind>,
    decorate_session_list: bool,
    deliver_response: bool,
    queue_item_id: Option<String>,
}

/// Lifecycle of an agent-initiated request id while we wait for the first
/// subscriber to reply. `InFlight` accepts the next response; `Consumed`
/// drops all further responses for the same id with a debug log so the
/// agent never receives duplicate replies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentReqState {
    InFlight,
    Consumed,
}

#[derive(Debug, Clone)]
pub(super) enum QueuedPromptKind {
    Prompt,
    Queue,
    HardSteer { supersedes_turn_id: AmuxTurnId },
}

#[derive(Debug, Clone)]
pub(super) struct QueuedPrompt {
    pub(super) queue_item_id: Option<String>,
    pub(super) peer_id: String,
    session_id: String,
    prompt_text: String,
    pub(super) kind: QueuedPromptKind,
}

#[derive(Debug)]
struct ActiveControlParams {
    session_id: String,
    text: String,
}

#[derive(Debug, Default)]
struct AgentLineAction {
    session_empty: bool,
    writes_to_agent: Vec<Vec<u8>>,
}

impl AgentLineAction {
    fn none() -> Self {
        Self::default()
    }

    fn session_empty(session_empty: bool) -> Self {
        Self {
            session_empty,
            writes_to_agent: Vec::new(),
        }
    }

    fn write_to_agent(write_to_agent: Vec<u8>) -> Self {
        Self {
            session_empty: false,
            writes_to_agent: vec![write_to_agent],
        }
    }
}

fn text_from_text_only_prompt(prompt: &Value) -> Option<String> {
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
    supersedes_turn_id: AmuxTurnId,
    original_prompt: Option<&str>,
    steering_text: &str,
) -> String {
    let original_prompt = original_prompt.unwrap_or("(unavailable/non-text)");
    // SAFETY: This prompt-injection template is only for trusted attached
    // peers in a private mux session. If acp-mux ever exposes steer text to
    // untrusted/public clients, revisit this plain format! construction and
    // add explicit quoting/sandboxing for peer-controlled text.
    format!(
        "Active turn steered by peer `{peer_id}` (supersedes {supersedes}). Use the steer below to answer the original prompt.\n\nOriginal:\n{original_prompt}\n\nSteer:\n{steering_text}",
        supersedes = supersedes_turn_id.formatted(),
    )
}

#[derive(Debug)]
pub(super) struct RoomInner {
    pub(super) room_id: String,
    agent_cwd: String,
    session_list_index: Arc<SessionListMetadataIndex>,
    canonical_session_id: Option<String>,
    pub(super) subscribers: HashMap<String, Subscriber>,
    next_mux_id: u64,
    pub(super) pending: HashMap<u64, PendingRequest>,
    initialize_cache: Option<Value>,
    session_new_cache: Option<Value>,
    /// Last subscriber to issue a substantive (non-`initialize`) request.
    /// Target for agent-initiated requests. Cleared when that subscriber
    /// detaches; falls back to an arbitrary subscriber at routing time.
    driving_subscriber_peer_id: Option<String>,
    /// `mux_id` of the in-flight `session/prompt`, if any. While set, a
    /// second `session/prompt` is rejected locally with `-32001`.
    pub(super) active_turn_mux_id: Option<u64>,
    /// `amuxTurnId` paired with the in-flight `session/prompt`. Used to
    /// bookend `amux/turn_started` and `amux/turn_complete`.
    pub(super) active_amux_turn_id: Option<AmuxTurnId>,
    /// Upstream ACP `sessionId` paired with the in-flight `session/prompt`.
    /// Used to translate `amux/cancel_active_turn` into ACP-native
    /// `session/cancel`.
    active_turn_session_id: Option<String>,
    /// Text-only view of the in-flight prompt, used when hard steer needs
    /// to inject the superseded prompt into the replacement prompt.
    active_turn_prompt_text: Option<String>,
    /// Mux-owned queue of future prompts to submit after active turns settle.
    pub(super) queued_prompts: VecDeque<QueuedPrompt>,
    /// Monotonic per-session counter for queue ids.
    next_queue_item_id: u64,
    /// Monotonic per-session counter for `amuxTurnId` allocation.
    next_amux_turn_id: u64,
    /// Replay log. `None` when policy is `Disabled` (saves memory).
    /// Otherwise, every broadcast-tier frame (amux/* + agent notifications)
    /// is appended with mux-recorded provenance; new subscribers receive a
    /// metadata-augmented snapshot at attach time.
    pub(super) replay_log: Option<VecDeque<ReplayEntry>>,
    /// Monotonic per-session counter for replay provenance metadata.
    next_replay_seq: u64,
    /// Incremented whenever a successful `session/load` establishes a new
    /// canonical upstream ACP session and the replay log is segmented.
    pub(super) replay_generation: u64,
    /// Last successful replay segmentation event, exposed through
    /// `/debug/sessions` for operator diagnostics.
    last_replay_reset: Option<ReplayResetSnapshot>,
    /// Sender back into this actor. Used to pace attach-stream backfill
    /// pages through the same serialized queue as live traffic.
    pub(super) self_tx: mpsc::Sender<RoomMsg>,
    /// Opt-in propagation of mux-owned trace metadata into outbound
    /// subscriber → agent requests under `params._meta.amux`.
    meta_propagate: bool,
    /// Policy for agent-initiated ACP client-tool request namespaces
    /// (`fs/*`, `terminal/*`).
    client_tool_policy: ClientToolPolicy,
    /// State for every agent-initiated request id we have ever broadcast
    /// in this session. `InFlight` until the first subscriber reply
    /// arrives; `Consumed` thereafter. We keep `Consumed` ids around for
    /// the session lifetime so late/duplicate responses can be recognized
    /// and dropped instead of leaking back to the agent.
    agent_pending: HashMap<Id, AgentReqState>,
    /// Original frames for unresolved agent-initiated
    /// `session/request_permission` requests. RFD #533 requires these to be
    /// re-issued to clients that attach after the first broadcast so the
    /// permission remains actionable, not just visible in history.
    pub(super) pending_permission_frames: Vec<(Id, Bytes)>,
    /// Segments observed in this room, in open-order. The last entry is
    /// the active segment when `active_segment_id` is `Some(..)`. Empty
    /// before the first canonical ACP session id arrives.
    pub(super) segments: Vec<Segment>,
    /// Currently-open segment, or `None` if no canonical session id has
    /// been captured yet.
    pub(super) active_segment_id: Option<SegmentId>,
    /// Monotonic id allocator for segments. First segment is `SegmentId(1)`.
    next_segment_id: u64,
    /// Whether to broadcast `amux/segment_started` / `amux/segment_ended`
    /// frames on rotation. Default true; gated by `--emit-segment-frames`.
    emit_segment_frames: bool,
    /// Opt-in on-disk persistence for the broadcast replay log. Open
    /// only when `--replay-store <DIR>` is configured and the replay log
    /// itself is enabled. Disk-write failures are logged and dropped so
    /// live fan-out is never blocked by storage issues.
    replay_store: Option<crate::room::replay_store::RoomReplayStore>,
}

impl RoomInner {
    #[allow(clippy::too_many_arguments)]
    fn new(
        room_id: String,
        agent_cwd: String,
        replay_policy: ReplayTurns,
        meta_propagate: bool,
        client_tool_policy: ClientToolPolicy,
        session_list_index: Arc<SessionListMetadataIndex>,
        self_tx: mpsc::Sender<RoomMsg>,
        emit_segment_frames: bool,
        replay_store: Option<Arc<ReplayStore>>,
    ) -> Self {
        let mut replay_log = match replay_policy {
            ReplayTurns::Disabled => None,
            ReplayTurns::Unbounded => Some(VecDeque::new()),
            ReplayTurns::Bounded(n) => {
                tracing::warn!(
                    bound = n,
                    "--replay-turns N (bounded eviction) accepted but not yet implemented; behaving as unbounded for v0.1",
                );
                Some(VecDeque::new())
            }
        };

        // Open the per-room persistence handle and rehydrate any prior
        // on-disk state. Only attempted when both the in-memory log is
        // enabled and a store is configured; `--replay-turns 0` disables
        // persistence transparently.
        let mut hydrated = Hydrated::default();
        if replay_log.is_some()
            && let Some(store) = replay_store.as_ref()
        {
            hydrated = hydrate_from_store(store, &room_id, replay_log.as_mut().unwrap());
        }
        let Hydrated {
            replay_store: room_store_handle,
            segments,
            active_segment_id,
            next_replay_seq,
            next_segment_id,
            canonical_session_id,
        } = hydrated;

        Self {
            room_id,
            agent_cwd,
            session_list_index,
            canonical_session_id,
            subscribers: HashMap::new(),
            next_mux_id: FIRST_MUX_ID,
            pending: HashMap::new(),
            initialize_cache: None,
            session_new_cache: None,
            driving_subscriber_peer_id: None,
            active_turn_mux_id: None,
            active_amux_turn_id: None,
            active_turn_session_id: None,
            active_turn_prompt_text: None,
            queued_prompts: VecDeque::new(),
            next_queue_item_id: 1,
            next_amux_turn_id: 1,
            replay_log,
            next_replay_seq,
            replay_generation: 0,
            last_replay_reset: None,
            self_tx,
            meta_propagate,
            client_tool_policy,
            agent_pending: HashMap::new(),
            pending_permission_frames: Vec::new(),
            segments,
            active_segment_id,
            next_segment_id,
            emit_segment_frames,
            replay_store: room_store_handle,
        }
    }

    fn set_canonical_session_id(&mut self, acp_session_id: &str) {
        self.set_canonical_session_id_with_reason(acp_session_id, EndReason::AcpSessionIdChanged);
    }

    /// Sets the canonical ACP session id and, if the id observably
    /// changed, rotates the segment. `reason` is consumed only when an
    /// active segment is being closed — the first call that opens the
    /// initial segment ignores it.
    fn set_canonical_session_id_with_reason(&mut self, acp_session_id: &str, reason: EndReason) {
        if self.canonical_session_id.as_deref() == Some(acp_session_id) {
            self.publish_session_list_metadata();
            return;
        }
        if let Some(previous) = self
            .canonical_session_id
            .replace(acp_session_id.to_string())
        {
            self.session_list_index
                .remove_if_room(&previous, &self.room_id);
        }
        self.rotate_segment(Some(acp_session_id.to_string()), reason);
        self.publish_session_list_metadata();
    }

    /// Open or rotate the active segment. When no segment is open this
    /// just opens the initial one (no `amux/segment_ended` emitted). When
    /// a segment is already open this closes it (recording `reason`),
    /// broadcasts `amux/segment_ended`, then opens a fresh segment and
    /// broadcasts `amux/segment_started`. Both lifecycle frames pass
    /// through the regular `broadcast()` path so they're appended to the
    /// transcript and tagged with the segment that owns them.
    fn rotate_segment(&mut self, new_acp_session_id: Option<String>, reason: EndReason) {
        let now = utc_rfc3339_now();

        // Initial open: no prior segment, no `amux/segment_ended`.
        let Some(current_id) = self.active_segment_id else {
            let id = SegmentId(self.next_segment_id);
            self.next_segment_id = self.next_segment_id.saturating_add(1);
            let mut seg = Segment::open(id, new_acp_session_id.clone(), self.next_replay_seq);
            seg.opened_at = now.clone();
            self.segments.push(seg);
            self.active_segment_id = Some(id);

            if self.emit_segment_frames {
                let frame =
                    amux::segment_started(&self.room_id, id, new_acp_session_id.as_deref(), &now);
                self.broadcast(frame);
            }
            return;
        };

        let previous_acp = self
            .segments
            .iter()
            .find(|s| s.id == current_id)
            .and_then(|s| s.acp_session_id.clone());

        // Mark the closing metadata in place but defer `closed_replay_seq`
        // — the `amux/segment_ended` bookend needs to land inside the
        // closing segment, and broadcasting it below advances
        // `next_replay_seq`. Setting `closed_replay_seq` afterwards keeps
        // the bookend frame's seq inside the closing range.
        if let Some(seg) = self.segments.iter_mut().find(|s| s.id == current_id) {
            seg.closed_at = Some(now.clone());
            seg.end_reason = Some(reason);
        }

        // Retarget queued prompts to the new ACP session id whenever the
        // canonical id observably moves.
        if let Some(new_acp) = new_acp_session_id.as_deref()
            && previous_acp.as_deref() != Some(new_acp)
        {
            for item in &mut self.queued_prompts {
                item.session_id = new_acp.to_string();
            }
        }

        // Allocate the new segment id so segment_ended can reference it
        // as `successorSegmentId`.
        let new_id = SegmentId(self.next_segment_id);
        self.next_segment_id = self.next_segment_id.saturating_add(1);

        // Broadcast `amux/segment_ended` while the OLD segment is still
        // active — current_segment_id() returns current_id, so the frame
        // is tagged with and lands inside the closing segment.
        if self.emit_segment_frames {
            let frame = amux::segment_ended(&self.room_id, current_id, &now, reason, Some(new_id));
            self.broadcast(frame);
        }

        // Now finalize closed_replay_seq. next_replay_seq has been
        // advanced past the bookend (or unchanged if --emit-segment-frames
        // is off), so seq - 1 is either the seq of segment_ended or of
        // the last regular frame in the segment.
        let closed_replay_seq = self.next_replay_seq.saturating_sub(1);
        if let Some(seg) = self.segments.iter_mut().find(|s| s.id == current_id) {
            seg.closed_replay_seq = Some(closed_replay_seq);
        }

        // Open the new segment.
        let mut new_segment =
            Segment::open(new_id, new_acp_session_id.clone(), self.next_replay_seq);
        new_segment.opened_at = now.clone();
        self.segments.push(new_segment);
        self.active_segment_id = Some(new_id);

        // Broadcast `amux/segment_started`. current_segment_id() now ==
        // new_id so this frame lands in the opening segment.
        if self.emit_segment_frames {
            let frame =
                amux::segment_started(&self.room_id, new_id, new_acp_session_id.as_deref(), &now);
            self.broadcast(frame);
        }
    }

    /// Segment id used to tag freshly recorded frames. Frames recorded
    /// before any segment opens carry `PRE_SEGMENT_ID` so the transcript
    /// can still slice on segment boundaries without a fallible lookup.
    fn current_segment_id(&self) -> SegmentId {
        self.active_segment_id.unwrap_or(PRE_SEGMENT_ID)
    }

    fn publish_session_list_metadata(&self) {
        let Some(acp_session_id) = self.canonical_session_id.as_deref() else {
            return;
        };
        if self.subscribers.is_empty() {
            self.session_list_index
                .remove_if_room(acp_session_id, &self.room_id);
            return;
        }
        self.session_list_index.upsert(
            acp_session_id,
            SessionListAmuxMetadata {
                room_id: self.room_id.clone(),
                subscriber_count: self.subscribers.len(),
                driving_subscriber: self.driving_subscriber_peer_id.clone(),
            },
        );
    }

    fn clear_session_list_metadata(&self) {
        if let Some(acp_session_id) = self.canonical_session_id.as_deref() {
            self.session_list_index
                .remove_if_room(acp_session_id, &self.room_id);
        }
    }

    pub(super) fn acp_session_id(&self) -> Option<&str> {
        self.canonical_session_id.as_deref().or_else(|| {
            self.session_new_cache
                .as_ref()
                .and_then(|v| v.get("sessionId"))
                .and_then(Value::as_str)
        })
    }

    fn decorate_session_list_response(&self, resp: &mut IncomingResponse) {
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
            inject_session_list_amux_metadata(session, &metadata);
        }
    }

    /// Attach a subscriber. Order:
    ///
    /// 1. Snapshot the replay log (before newcomer's own peer_joined enters).
    /// 2. Emit amux/peer_joined → broadcast to existing subs (newcomer is
    ///    not in the map yet) + append to log.
    /// 3. Insert newcomer into subscriber map.
    /// 4. Unless the subscriber opted out with transport `replay=skip`,
    ///    deliver the snapshot to the newcomer's outbound. The snapshot
    ///    contains every broadcast-tier frame that happened before this
    ///    attach, in order, so the newcomer reconstructs the session.
    ///
    /// Because the actor serializes all RoomMsg handling, no live frames
    /// can interleave during this sequence.
    fn attach(&mut self, subscriber: Subscriber) -> Result<(), AttachError> {
        if self.subscribers.contains_key(&subscriber.peer_id) {
            return Err(AttachError::PeerIdInUse);
        }
        let suppress_legacy_replay = subscriber.suppress_legacy_replay;
        let snapshot: Vec<ReplayEntry> = if suppress_legacy_replay {
            Vec::new()
        } else {
            self.replay_log
                .as_ref()
                .map(|log| log.iter().cloned().collect())
                .unwrap_or_default()
        };

        let frame = amux::peer_joined(
            &self.room_id,
            &subscriber.peer_id,
            subscriber.peer_name.as_deref(),
            subscriber.role.as_deref(),
        );
        self.broadcast(frame);

        let peer_id = subscriber.peer_id.clone();
        tracing::info!(
            session = %self.room_id,
            peer_id = %peer_id,
            replay_frames = snapshot.len(),
            suppress_legacy_replay,
            "subscriber joined session",
        );
        self.subscribers.insert(peer_id.clone(), subscriber);
        self.publish_session_list_metadata();
        self.send_session_context_to(&peer_id);

        if let Some(sub) = self.subscribers.get(&peer_id) {
            for entry in snapshot {
                let frame = entry.frame_for_replay();
                if sub.outbound.send(OutMsg::Frame(frame)).is_err() {
                    tracing::debug!(%peer_id, "newcomer dropped during replay");
                    break;
                }
            }
        }
        Ok(())
    }

    fn send_session_context_to(&self, peer_id: &str) {
        let Some(sub) = self.subscribers.get(peer_id) else {
            return;
        };
        let frame = Bytes::from(amux::session_context(&self.room_id, &self.agent_cwd));
        if sub.outbound.send(OutMsg::Frame(frame)).is_err() {
            tracing::debug!(%peer_id, "subscriber dropped before session context delivered");
        }
    }

    /// Build a serializable snapshot of session state for /debug/sessions.
    fn build_snapshot(&self, ttl_pending: bool) -> RoomSnapshot {
        let subs: Vec<SubscriberSnapshot> = self
            .subscribers
            .values()
            .map(|s| SubscriberSnapshot {
                peer_id: s.peer_id.clone(),
                peer_name: s.peer_name.clone(),
                role: s.role.clone(),
                is_driving: self.driving_subscriber_peer_id.as_ref() == Some(&s.peer_id),
            })
            .collect();
        let cached_session_id = self.session_new_cache.as_ref().and_then(|v| {
            v.get("sessionId")
                .and_then(|s| s.as_str())
                .map(|s| s.to_string())
        });
        RoomSnapshot {
            room_id: self.room_id.clone(),
            agent_cwd: self.agent_cwd.clone(),
            subscribers: subs,
            pending_request_count: self.pending.len(),
            initialize_cached: self.initialize_cache.is_some(),
            cached_session_id,
            active_turn_mux_id: self.active_turn_mux_id,
            active_amux_turn_id: self.active_amux_turn_id.map(|t| t.formatted()),
            driving_subscriber: self.driving_subscriber_peer_id.clone(),
            subprocess_dead: false,
            ttl_pending,
            replay_log_len: self.replay_log.as_ref().map(|l| l.len()),
            replay_generation: self.replay_generation,
            segments: self.segments.clone(),
            active_segment_id: self.active_segment_id,
            replay_log_update_frames_by_acp_session_id: self
                .replay_log
                .as_ref()
                .map(replay_log_update_counts_by_acp_session_id),
            last_replay_reset: self.last_replay_reset.clone(),
            next_mux_id: self.next_mux_id,
            next_amux_turn_id: self.next_amux_turn_id,
        }
    }

    fn close_all_subscribers(&self, code: u16, reason: &str) {
        for (peer_id, sub) in &self.subscribers {
            let msg = OutMsg::Close {
                code,
                reason: reason.to_string(),
            };
            if sub.outbound.send(msg).is_err() {
                tracing::debug!(%peer_id, "subscriber already gone during close");
            }
        }
    }

    /// Returns true if the session should end (no subscribers left).
    /// Emits `amux/peer_left` to every remaining subscriber. The mux does not
    /// fabricate ACP `session/update` lifecycle notifications; clients that
    /// need mux lifecycle should consume `amux/*`.
    fn detach(&mut self, peer_id: &str) -> bool {
        let removed = self.subscribers.remove(peer_id);
        if removed.is_some() {
            tracing::info!(session = %self.room_id, %peer_id, "subscriber detached");
            let frame = amux::peer_left(&self.room_id, peer_id);
            self.broadcast(frame);
            let orphaned_queue_item_ids: Vec<String> = self
                .queued_prompts
                .iter()
                .filter(|item| item.peer_id == peer_id)
                .filter_map(|item| item.queue_item_id.clone())
                .collect();
            for queue_item_id in orphaned_queue_item_ids {
                self.broadcast(amux::queue_item_orphaned(
                    &self.room_id,
                    &queue_item_id,
                    peer_id,
                ));
            }
        }
        if self.driving_subscriber_peer_id.as_deref() == Some(peer_id) {
            self.driving_subscriber_peer_id = None;
        }
        if self.subscribers.is_empty() {
            self.clear_session_list_metadata();
            tracing::info!(session = %self.room_id, "last subscriber gone; ending session");
            return true;
        }
        self.publish_session_list_metadata();
        false
    }

    /// Process one inbound frame from a subscriber. Returns Ok(Some(bytes))
    /// when a frame should be written to the agent stdin; Ok(None) means
    /// the frame was either dropped, answered locally from cache, or
    /// otherwise handled without touching the agent.
    fn handle_inbound(&mut self, peer_id: &str, bytes: Vec<u8>) -> Option<Vec<u8>> {
        let frame = match Incoming::parse(&bytes) {
            Ok(f) => f,
            Err(err) => {
                tracing::warn!(
                    session = %self.room_id,
                    %peer_id,
                    error = %err,
                    "invalid JSON-RPC frame from subscriber; dropping",
                );
                return None;
            }
        };
        match frame {
            Incoming::Notification(notif) => {
                self.handle_subscriber_notification(peer_id, notif, bytes)
            }
            Incoming::Response(resp) => self.gate_subscriber_response(peer_id, resp, bytes),
            Incoming::Request(req) => self.translate_outbound_request(peer_id, req),
        }
    }

    /// Intercept proxy-handled subscriber-emitted notifications
    /// (`$/cancel_request`, `amux/cancel_active_turn`) before forwarding
    /// the bytes verbatim. Returns Ok(Some(bytes)) when the frame should
    /// be written to the agent stdin, Ok(None) when it was handled
    /// entirely in the proxy.
    fn handle_subscriber_notification(
        &mut self,
        peer_id: &str,
        notif: crate::protocol::jsonrpc::IncomingNotification,
        bytes: Vec<u8>,
    ) -> Option<Vec<u8>> {
        match notif.method.as_str() {
            CANCEL_REQUEST_METHOD => self.handle_subscriber_cancel(peer_id, notif),
            amux::METHOD_CANCEL_ACTIVE_TURN => self.handle_amux_cancel_active_turn(peer_id, notif),
            _ => Some(bytes),
        }
    }

    /// Strict `$/cancel_request` from a subscriber: cancel the
    /// subscriber's *own* in-flight request. Find `(peer_id,
    /// original_id)` in `pending`, rewrite the notification's
    /// `requestId` to the corresponding `mux_id`, forward to the agent.
    /// Subscribers cannot cancel other subscribers' requests — the
    /// JSON-RPC id space is per-connection — they'd use
    /// `amux/cancel_active_turn` for cross-peer "stop this turn"
    /// instead.
    fn handle_subscriber_cancel(
        &mut self,
        peer_id: &str,
        notif: crate::protocol::jsonrpc::IncomingNotification,
    ) -> Option<Vec<u8>> {
        let original_id = match parse_cancel_request_id(notif.params.as_ref()) {
            Some(id) => id,
            None => {
                tracing::debug!(
                    session = %self.room_id,
                    %peer_id,
                    "subscriber $/cancel_request with invalid/null requestId; dropping",
                );
                return None;
            }
        };

        let Some(mux_id) = self.find_pending_mux_id(peer_id, &original_id) else {
            tracing::debug!(
                session = %self.room_id,
                %peer_id,
                id = ?original_id,
                "subscriber $/cancel_request for unknown id; dropping",
            );
            return None;
        };

        tracing::info!(
            session = %self.room_id,
            %peer_id,
            ?original_id,
            mux_id,
            "forwarding subscriber-initiated $/cancel_request to agent",
        );
        Some(build_cancel_request(Id::Number(mux_id as i64)))
    }

    /// `amux/cancel_active_turn`: any attached peer can cancel the
    /// in-flight turn. Resolves to ACP-native `session/cancel` toward
    /// the agent using the active turn's ACP `sessionId`. Broadcasts
    /// `amux/turn_cancelled` to all peers immediately (intent), while
    /// `amux/turn_complete` follows later when the agent settles.
    fn handle_amux_cancel_active_turn(
        &mut self,
        peer_id: &str,
        notif: crate::protocol::jsonrpc::IncomingNotification,
    ) -> Option<Vec<u8>> {
        let Some(active_mux_id) = self.active_turn_mux_id else {
            tracing::debug!(
                session = %self.room_id,
                %peer_id,
                "amux/cancel_active_turn with no active turn; dropping",
            );
            return None;
        };
        let Some(amux_turn_id) = self.active_amux_turn_id else {
            tracing::warn!(
                session = %self.room_id,
                %peer_id,
                "active_turn_mux_id set but active_amux_turn_id missing; dropping cancel",
            );
            return None;
        };
        let Some(pending) = self.pending.get(&active_mux_id) else {
            tracing::warn!(
                session = %self.room_id,
                %peer_id,
                active_mux_id,
                "active turn has no pending entry; dropping cancel",
            );
            return None;
        };
        let original_driver = pending.peer_id.clone();
        let Some(active_session_id) = self.active_turn_session_id.clone() else {
            tracing::warn!(
                session = %self.room_id,
                %peer_id,
                active_mux_id,
                "active turn has no ACP sessionId; dropping cancel",
            );
            return None;
        };
        let reason = notif
            .params
            .as_ref()
            .and_then(|v| v.get("reason"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        tracing::info!(
            session = %self.room_id,
            cancelled_by = %peer_id,
            %original_driver,
            active_mux_id,
            acp_session_id = %active_session_id,
            reason = ?reason,
            "amux/cancel_active_turn sending session/cancel to agent",
        );

        let frame = amux::turn_cancelled(
            &self.room_id,
            amux_turn_id,
            peer_id,
            &original_driver,
            reason.as_deref(),
        );
        self.broadcast(frame);

        Some(build_session_cancel(&active_session_id))
    }

    /// Agent-emitted `$/cancel_request`: cancels an agent-initiated
    /// request that's still InFlight in `agent_pending` (currently only
    /// broadcast-tier requests such as `session/request_permission`; ACP
    /// client-tool requests are policy-blocked by default before they
    /// enter this lifecycle). Forward the cancellation to every
    /// subscriber so their UIs dismiss, mark the entry Consumed to
    /// swallow late replies, and emit the
    /// `amux/agent_request_resolved` sibling for amux clients.
    fn handle_agent_cancel(
        &mut self,
        notif: crate::protocol::jsonrpc::IncomingNotification,
        line: Vec<u8>,
    ) -> bool {
        let request_id = match parse_cancel_request_id(notif.params.as_ref()) {
            Some(id) => id,
            None => {
                tracing::debug!(
                    session = %self.room_id,
                    "agent $/cancel_request with invalid/null requestId; dropping",
                );
                return false;
            }
        };
        match self.agent_pending.get_mut(&request_id) {
            Some(state @ AgentReqState::InFlight) => {
                *state = AgentReqState::Consumed;
                self.pending_permission_frames
                    .retain(|(id, _)| id != &request_id);
                tracing::info!(
                    session = %self.room_id,
                    id = ?request_id,
                    "agent cancelled in-flight agent-initiated request; broadcasting",
                );
            }
            Some(AgentReqState::Consumed) => {
                tracing::debug!(
                    session = %self.room_id,
                    id = ?request_id,
                    "agent $/cancel_request for already-consumed id; broadcasting anyway so late UIs dismiss",
                );
            }
            None => {
                tracing::debug!(
                    session = %self.room_id,
                    id = ?request_id,
                    "agent $/cancel_request for unknown id; broadcasting anyway",
                );
            }
        }

        // Forward the raw cancellation notification to every subscriber
        // so RFD-aware clients see the standard JSON-RPC form. Capture
        // the empty-after-fanout signal so the caller can wind down the
        // session if this drained the last subscriber.
        let mut session_empty = self.broadcast(line);

        // Emit the amux-namespace sibling so amux-aware clients
        // dismiss without needing to recognize $/cancel_request.
        // (The second broadcast is harmless if the map is already
        // empty — it appends to the replay log either way.)
        let request_id_value = match serde_json::to_value(&request_id) {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(
                    session = %self.room_id,
                    error = %err,
                    "failed to serialize cancelled request id; skipping amux/agent_request_resolved",
                );
                return session_empty;
            }
        };
        let frame = amux::agent_request_resolved(
            &self.room_id,
            &request_id_value,
            amux::RESOLVED_BY_AGENT_CANCELLED,
            None,
            None,
        );
        session_empty |= self.broadcast(frame);
        session_empty
    }

    /// Linear search through `pending` for the entry matching
    /// `(peer_id, original_id)`. N is bounded by concurrent in-flight
    /// requests per session — small in practice. Returns the `mux_id`
    /// key.
    fn find_pending_mux_id(&self, peer_id: &str, original_id: &Id) -> Option<u64> {
        self.pending
            .iter()
            .find(|(_, pr)| pr.peer_id == peer_id && &pr.original_id == original_id)
            .map(|(mux_id, _)| *mux_id)
    }

    /// First-reply-wins gate for subscriber-originated responses. If the
    /// id matches an agent-initiated request we broadcast, only the first
    /// reply is forwarded; later replies for the same id are dropped.
    /// Responses whose id is not tracked (stray replies, or replies
    /// against ids the agent never asked us to multiplex) are forwarded
    /// unchanged for robustness — the agent will ignore them if it has
    /// no matching outstanding request.
    fn gate_subscriber_response(
        &mut self,
        peer_id: &str,
        resp: IncomingResponse,
        bytes: Vec<u8>,
    ) -> Option<Vec<u8>> {
        enum Decision {
            Forward,
            Drop,
            Passthrough,
        }
        let decision = match self.agent_pending.get_mut(&resp.id) {
            Some(state @ AgentReqState::InFlight) => {
                *state = AgentReqState::Consumed;
                self.pending_permission_frames
                    .retain(|(id, _)| id != &resp.id);
                Decision::Forward
            }
            Some(AgentReqState::Consumed) => Decision::Drop,
            None => Decision::Passthrough,
        };
        match decision {
            Decision::Forward => {
                tracing::debug!(
                    session = %self.room_id,
                    %peer_id,
                    id = ?resp.id,
                    "first reply to agent-initiated request; forwarding to agent",
                );
                self.emit_agent_request_resolved(peer_id, &resp);
                Some(bytes)
            }
            Decision::Drop => {
                tracing::debug!(
                    session = %self.room_id,
                    %peer_id,
                    id = ?resp.id,
                    "duplicate reply to agent-initiated request; dropping",
                );
                None
            }
            Decision::Passthrough => Some(bytes),
        }
    }

    /// Transition every `InFlight` `agent_pending` entry to `Consumed`
    /// and broadcast a cleanup `amux/agent_request_resolved` for each.
    /// Called at turn-end (`route_agent_response` clearing
    /// `active_turn_mux_id`) — by that point any unresolved
    /// agent-initiated request has been abandoned by the agent,
    /// for example, internally times out at 60s and proceeds without
    /// writing a response frame), so peers need to dismiss the prompt.
    /// `result` and `error` are both `null` on the broadcast since no
    /// reply was ever forwarded to the agent.
    fn sweep_stale_agent_pending(&mut self, reason: &str) {
        let stale_ids: Vec<Id> = self
            .agent_pending
            .iter()
            .filter(|(_, state)| matches!(state, AgentReqState::InFlight))
            .map(|(id, _)| id.clone())
            .collect();
        if stale_ids.is_empty() {
            return;
        }
        for id in &stale_ids {
            if let Some(state) = self.agent_pending.get_mut(id) {
                *state = AgentReqState::Consumed;
            }
            self.pending_permission_frames
                .retain(|(pending_id, _)| pending_id != id);
        }
        tracing::info!(
            session = %self.room_id,
            stale_count = stale_ids.len(),
            reason,
            "sweeping unresolved agent-initiated requests",
        );
        for id in stale_ids {
            let request_id_value = match serde_json::to_value(&id) {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(
                        session = %self.room_id,
                        error = %err,
                        "failed to serialize stale agent-request id; skipping cleanup broadcast",
                    );
                    continue;
                }
            };
            let frame =
                amux::agent_request_resolved(&self.room_id, &request_id_value, reason, None, None);
            self.broadcast(frame);
        }
    }

    /// Broadcast `amux/agent_request_resolved` so peers can dismiss the
    /// matching pending UI. Called once per agent-initiated request id
    /// (on the InFlight → Consumed transition). The frame echoes the
    /// winning subscriber's `result` or `error` verbatim. For
    /// `session/request_permission`, the result is derived entirely from
    /// `options[]` that was already broadcast in the request, so no new
    /// information leaks. ACP client-tool requests (`fs/*`,
    /// `terminal/*`) are not broadcast in the default policy.
    fn emit_agent_request_resolved(&mut self, resolved_by: &str, resp: &IncomingResponse) {
        let request_id_value = match serde_json::to_value(&resp.id) {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(
                    session = %self.room_id,
                    error = %err,
                    "failed to serialize agent-request id; skipping resolved broadcast",
                );
                return;
            }
        };
        let error_value = resp
            .error
            .as_ref()
            .and_then(|e| serde_json::to_value(e).ok());
        let frame = amux::agent_request_resolved(
            &self.room_id,
            &request_id_value,
            resolved_by,
            resp.result.as_ref(),
            error_value.as_ref(),
        );
        self.broadcast(frame);
    }

    fn sanitize_initialize_client_capabilities(&self, req: &mut IncomingRequest) {
        let Some(Value::Object(params)) = req.params.as_mut() else {
            return;
        };
        let Some(Value::Object(client_capabilities)) = params.get_mut("clientCapabilities") else {
            return;
        };

        let mut stripped = vec![];
        if self.client_tool_policy.fs == ClientToolMode::Block
            && client_capabilities.remove("fs").is_some()
        {
            stripped.push("fs");
        }
        if self.client_tool_policy.terminal == ClientToolMode::Block
            && client_capabilities.remove("terminal").is_some()
        {
            stripped.push("terminal");
        }
        if !stripped.is_empty() {
            tracing::info!(
                session = %self.room_id,
                namespaces = ?stripped,
                "stripped blocked client-tool capabilities from initialize",
            );
        }
    }

    fn parse_amux_active_turn_control_params(
        &mut self,
        peer_id: &str,
        req: &IncomingRequest,
        require_active_turn: bool,
    ) -> Option<ActiveControlParams> {
        if require_active_turn && self.active_turn_mux_id.is_none() {
            self.send_error_response(
                peer_id,
                req.id.clone(),
                NO_ACTIVE_TURN_ERROR_CODE,
                "amux active-turn control requires an active turn",
            );
            return None;
        }

        let Some(Value::Object(params)) = req.params.as_ref() else {
            self.send_error_response(
                peer_id,
                req.id.clone(),
                INVALID_PARAMS_ERROR_CODE,
                "amux control params must be an object",
            );
            return None;
        };

        let text = match params.get("text") {
            Some(Value::String(text)) => text.clone(),
            Some(_) => {
                self.send_error_response(
                    peer_id,
                    req.id.clone(),
                    INVALID_PARAMS_ERROR_CODE,
                    "amux control params.text must be a string",
                );
                return None;
            }
            None => match params.get("prompt").and_then(text_from_text_only_prompt) {
                Some(text) => text,
                None => {
                    self.send_error_response(
                        peer_id,
                        req.id.clone(),
                        INVALID_PARAMS_ERROR_CODE,
                        "amux control params.text or text-only params.prompt is required",
                    );
                    return None;
                }
            },
        };
        let text = text.trim();
        if text.is_empty() {
            self.send_error_response(
                peer_id,
                req.id.clone(),
                INVALID_PARAMS_ERROR_CODE,
                "amux control text must be non-empty",
            );
            return None;
        }

        let requested_session_id = match params.get("sessionId") {
            Some(Value::String(session_id)) => Some(session_id.clone()),
            Some(_) => {
                self.send_error_response(
                    peer_id,
                    req.id.clone(),
                    INVALID_PARAMS_ERROR_CODE,
                    "amux control params.sessionId must be a string when present",
                );
                return None;
            }
            None => None,
        };
        let active_session_id = self
            .active_turn_session_id
            .clone()
            .or_else(|| self.canonical_session_id.clone());
        if let (Some(requested), Some(active)) = (&requested_session_id, &active_session_id)
            && requested != active
        {
            self.send_error_response(
                peer_id,
                req.id.clone(),
                INVALID_PARAMS_ERROR_CODE,
                "amux control params.sessionId must match the active or canonical sessionId",
            );
            return None;
        }
        let Some(session_id) = requested_session_id.or(active_session_id) else {
            self.send_error_response(
                peer_id,
                req.id.clone(),
                INVALID_PARAMS_ERROR_CODE,
                "amux control could not determine an ACP sessionId",
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

    fn handle_amux_queue_prompt_request(
        &mut self,
        peer_id: &str,
        req: IncomingRequest,
    ) -> Option<Vec<u8>> {
        let control = self.parse_amux_active_turn_control_params(peer_id, &req, false)?;
        if self.pending_queue_prompt_count() >= MAX_MUX_QUEUE_PROMPTS {
            self.send_error_response(peer_id, req.id, QUEUE_FULL_ERROR_CODE, "queue full");
            return None;
        }
        let submit_immediately = self.active_turn_mux_id.is_none();
        let queue_item_id = format!("q-{}", self.next_queue_item_id);
        self.next_queue_item_id += 1;
        let (peer_name, role) = self
            .subscribers
            .get(peer_id)
            .map(|s| (s.peer_name.as_deref(), s.role.as_deref()))
            .unwrap_or((None, None));
        self.queued_prompts.push_back(QueuedPrompt {
            queue_item_id: Some(queue_item_id.clone()),
            peer_id: peer_id.to_string(),
            session_id: control.session_id,
            prompt_text: control.text.clone(),
            kind: QueuedPromptKind::Queue,
        });
        self.broadcast(amux::queue_item_added(
            &self.room_id,
            &queue_item_id,
            peer_id,
            peer_name,
            role,
            &control.text,
        ));
        let write_to_agent = submit_immediately
            .then(|| self.submit_next_queued_prompt())
            .flatten();
        let status = if write_to_agent.is_some() {
            "submitted"
        } else {
            "queued"
        };
        self.send_result_response(
            peer_id,
            req.id,
            json!({ "queueItemId": queue_item_id, "status": status }),
        );
        write_to_agent
    }

    fn handle_amux_unqueue_prompt_request(
        &mut self,
        peer_id: &str,
        req: IncomingRequest,
    ) -> Option<Vec<u8>> {
        let Some(Value::Object(params)) = req.params.as_ref() else {
            self.send_error_response(
                peer_id,
                req.id,
                INVALID_PARAMS_ERROR_CODE,
                "amux/unqueue_prompt params must be an object",
            );
            return None;
        };
        let Some(queue_item_id) = params.get("queueItemId").and_then(Value::as_str) else {
            self.send_error_response(
                peer_id,
                req.id,
                INVALID_PARAMS_ERROR_CODE,
                "amux/unqueue_prompt params.queueItemId must be a string",
            );
            return None;
        };
        let queue_item_id = queue_item_id.trim().to_string();
        if queue_item_id.is_empty() {
            self.send_error_response(
                peer_id,
                req.id,
                INVALID_PARAMS_ERROR_CODE,
                "amux/unqueue_prompt params.queueItemId must be non-empty",
            );
            return None;
        }

        let Some(position) = self
            .queued_prompts
            .iter()
            .position(|item| item.queue_item_id.as_deref() == Some(queue_item_id.as_str()))
        else {
            self.send_error_response(
                peer_id,
                req.id,
                INVALID_PARAMS_ERROR_CODE,
                "queue item not found",
            );
            return None;
        };
        self.queued_prompts.remove(position);
        self.broadcast(amux::queue_item_removed(
            &self.room_id,
            queue_item_id.as_str(),
            peer_id,
        ));
        self.send_result_response(
            peer_id,
            req.id,
            json!({ "queueItemId": queue_item_id, "status": "removed" }),
        );
        None
    }

    fn handle_amux_steer_request(
        &mut self,
        peer_id: &str,
        req: IncomingRequest,
    ) -> Option<Vec<u8>> {
        let control = self.parse_amux_active_turn_control_params(peer_id, &req, false)?;
        if self.active_turn_mux_id.is_none() {
            return self.handle_amux_idle_steer_request(peer_id, req.id, control);
        }
        if self.has_pending_hard_steer() {
            self.send_error_response(
                peer_id,
                req.id,
                NO_ACTIVE_TURN_ERROR_CODE,
                "a hard steer is already pending for this turn",
            );
            return None;
        }
        let active_mux_id = self.active_turn_mux_id?;
        let supersedes_turn_id = self.active_amux_turn_id?;
        let Some(active_session_id) = self.active_turn_session_id.clone() else {
            self.send_error_response(
                peer_id,
                req.id.clone(),
                INVALID_PARAMS_ERROR_CODE,
                "amux control could not determine the active ACP sessionId",
            );
            return None;
        };
        let original_driver = self
            .pending
            .get(&active_mux_id)
            .map(|pending| pending.peer_id.clone())
            .unwrap_or_else(|| peer_id.to_string());
        let original_prompt = self.active_turn_prompt_text.as_deref();
        let replacement_prompt =
            build_hard_steer_prompt(peer_id, supersedes_turn_id, original_prompt, &control.text);
        let (peer_name, role) = self
            .subscribers
            .get(peer_id)
            .map(|s| (s.peer_name.as_deref(), s.role.as_deref()))
            .unwrap_or((None, None));

        self.broadcast(amux::control_submitted(amux::ControlSubmitted {
            room_id: &self.room_id,
            kind: "steer",
            mode: "hard",
            peer_id,
            peer_name,
            role,
            amux_turn_id: Some(supersedes_turn_id),
            text: &control.text,
        }));
        self.broadcast(amux::turn_cancelled(
            &self.room_id,
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
            peer_id,
            req.id,
            json!({
                "accepted": true,
                "mode": "hard",
                "supersedesTurnId": supersedes_turn_id.formatted(),
            }),
        );
        Some(build_session_cancel(&active_session_id))
    }

    fn handle_amux_idle_steer_request(
        &mut self,
        peer_id: &str,
        req_id: Id,
        control: ActiveControlParams,
    ) -> Option<Vec<u8>> {
        let turn_id = AmuxTurnId(self.next_amux_turn_id);
        let (peer_name, role) = self
            .subscribers
            .get(peer_id)
            .map(|s| (s.peer_name.as_deref(), s.role.as_deref()))
            .unwrap_or((None, None));

        self.broadcast(amux::control_submitted(amux::ControlSubmitted {
            room_id: &self.room_id,
            kind: "steer",
            mode: "prompt",
            peer_id,
            peer_name,
            role,
            amux_turn_id: Some(turn_id),
            text: &control.text,
        }));
        self.queued_prompts.push_front(QueuedPrompt {
            queue_item_id: None,
            peer_id: peer_id.to_string(),
            session_id: control.session_id,
            prompt_text: control.text,
            kind: QueuedPromptKind::Prompt,
        });
        let write_to_agent = self.submit_next_queued_prompt();
        self.send_result_response(
            peer_id,
            req_id,
            json!({
                "accepted": true,
                "mode": "prompt",
                "status": "submitted",
                "amuxTurnId": turn_id.formatted(),
            }),
        );
        write_to_agent
    }

    fn translate_outbound_request(
        &mut self,
        peer_id: &str,
        mut req: IncomingRequest,
    ) -> Option<Vec<u8>> {
        // RFD #533 attach/detach are proxy-local methods. The WebSocket
        // transport peer is already attached; these logical ACP handshakes
        // must not be forwarded to the wrapped agent.
        if req.method == attach::METHOD_ATTACH {
            self.handle_attach(peer_id, req);
            return None;
        }
        if req.method == attach::METHOD_DETACH {
            self.handle_detach(peer_id, req);
            return None;
        }

        // Cache short-circuits. A cached `session/new` still updates the
        // driving subscriber — the subscriber asked the session a question,
        // even if we answered it locally.
        if req.method == "initialize"
            && let Some(cached) = self.initialize_cache.clone()
        {
            self.send_cached_response(peer_id, req.id, cached);
            return None;
        }
        if req.method == "session/new"
            && let Some(cached) = self.session_new_cache.clone()
        {
            self.note_driving_subscriber(peer_id);
            self.send_cached_response(peer_id, req.id, cached);
            return None;
        }

        match req.method.as_str() {
            amux::METHOD_STEER_ACTIVE_TURN => {
                return self.handle_amux_steer_request(peer_id, req);
            }
            amux::METHOD_QUEUE_PROMPT => {
                return self.handle_amux_queue_prompt_request(peer_id, req);
            }
            amux::METHOD_UNQUEUE_PROMPT => {
                return self.handle_amux_unqueue_prompt_request(peer_id, req);
            }
            _ => {}
        };

        // Turn serialization: a second concurrent ordinary `session/prompt`
        // is rejected locally with -32001 and does NOT update the driver
        // (the in-flight turn's originator stays the driver). Active-turn
        // control must use the explicit amux/* request surface above; plain
        // ACP `session/prompt` stays generic and serialized.
        // Also broadcast an amux/session_busy notification for rejections
        // so peers see the rejection.
        if req.method == "session/prompt"
            && let Some(active) = self.active_turn_mux_id
        {
            let held_by = self.pending.get(&active).map(|pr| pr.peer_id.clone());
            tracing::warn!(
                session = %self.room_id,
                %peer_id,
                active_turn = active,
                held_by = ?held_by,
                "rejecting concurrent session/prompt with -32001",
            );
            let busy_frame = amux::session_busy(&self.room_id, true, held_by.as_deref());
            self.broadcast(busy_frame);
            self.send_error_response(
                peer_id,
                req.id,
                SESSION_BUSY_ERROR_CODE,
                "session busy: another turn is in flight",
            );
            return None;
        }

        if req.method == "initialize" {
            self.sanitize_initialize_client_capabilities(&mut req);
        }

        if req.method != "initialize" {
            self.note_driving_subscriber(peer_id);
        }

        let handshake = match req.method.as_str() {
            "initialize" => Some(HandshakeKind::Initialize),
            "session/new" => Some(HandshakeKind::SessionNew),
            "session/load" => {
                let replay_start_len = self.replay_log.as_ref().map(|log| log.len()).unwrap_or(0);
                req.params
                    .as_ref()
                    .and_then(|p| p.get("sessionId"))
                    .and_then(|s| s.as_str())
                    .map(|s| HandshakeKind::SessionLoad {
                        loaded_session_id: s.to_string(),
                        replay_start_len,
                    })
            }
            _ => None,
        };

        let mux_id = self.next_mux_id;
        self.next_mux_id += 1;
        let original_id = req.id.clone();
        let is_prompt = req.method == "session/prompt";
        let active_turn_session_id = if is_prompt {
            req.params
                .as_ref()
                .and_then(|p| p.get("sessionId"))
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| self.canonical_session_id.clone())
        } else {
            None
        };
        let active_turn_prompt_text = if is_prompt {
            req.params
                .as_ref()
                .and_then(|p| p.get("prompt"))
                .and_then(text_from_text_only_prompt)
        } else {
            None
        };
        let decorate_session_list = req.method == "session/list";
        let amux_turn_id = if is_prompt {
            let turn_id = AmuxTurnId(self.next_amux_turn_id);
            self.next_amux_turn_id += 1;
            Some(turn_id)
        } else {
            None
        };
        self.pending.insert(
            mux_id,
            PendingRequest {
                peer_id: peer_id.to_string(),
                original_id,
                handshake,
                decorate_session_list,
                deliver_response: true,
                queue_item_id: None,
            },
        );
        req.id = Id::Number(mux_id as i64);

        if self.meta_propagate {
            let (peer_name, role) = self
                .subscribers
                .get(peer_id)
                .map(|s| (s.peer_name.as_deref(), s.role.as_deref()))
                .unwrap_or((None, None));
            inject_request_trace_metadata(
                &mut req,
                RequestTrace {
                    peer_id,
                    peer_name,
                    role,
                    mux_id,
                    amux_turn_id,
                },
            );
        }

        match serde_json::to_vec(&req) {
            Ok(out) => {
                if let Some(turn_id) = amux_turn_id {
                    self.active_turn_mux_id = Some(mux_id);
                    self.active_amux_turn_id = Some(turn_id);
                    self.active_turn_session_id = active_turn_session_id;
                    self.active_turn_prompt_text = active_turn_prompt_text;
                    tracing::info!(
                        session = %self.room_id,
                        %peer_id,
                        mux_id,
                        amux_turn_id = %turn_id.formatted(),
                        acp_session_id = ?self.active_turn_session_id,
                        "session/prompt forwarded; active turn opened",
                    );
                    self.emit_turn_started(peer_id, turn_id, req.params.as_ref(), None);
                }
                Some(out)
            }
            Err(err) => {
                tracing::error!(
                    session = %self.room_id,
                    mux_id,
                    error = %err,
                    "failed to serialize translated request; dropping",
                );
                self.pending.remove(&mux_id);
                None
            }
        }
    }

    /// Build and broadcast `amux/turn_started`. The `content` field carries
    /// `params.prompt` verbatim; if missing we send `null`.
    fn emit_turn_started(
        &mut self,
        peer_id: &str,
        turn_id: AmuxTurnId,
        params: Option<&Value>,
        supersedes_turn_id: Option<AmuxTurnId>,
    ) {
        let null = Value::Null;
        let content = params.and_then(|p| p.get("prompt")).unwrap_or(&null);
        let (peer_name, role) = self
            .subscribers
            .get(peer_id)
            .map(|s| (s.peer_name.clone(), s.role.clone()))
            .unwrap_or((None, None));
        let frame = amux::turn_started(
            &self.room_id,
            turn_id,
            peer_id,
            peer_name.as_deref(),
            role.as_deref(),
            content,
            supersedes_turn_id,
        );
        self.broadcast(frame);
    }

    /// Build and broadcast `amux/turn_complete`. `stop_reason` is the
    /// `result.stopReason` value if present, else `null` (abnormal turns
    /// land here in chunk 9; for chunk 7 only the happy path is wired).
    fn emit_turn_complete(&mut self, turn_id: AmuxTurnId, result: Option<&Value>) {
        let null = Value::Null;
        let stop_reason = result.and_then(|r| r.get("stopReason")).unwrap_or(&null);
        let frame = amux::turn_complete(&self.room_id, turn_id, stop_reason);
        self.broadcast(frame);
    }

    fn submit_next_queued_prompt(&mut self) -> Option<Vec<u8>> {
        let item = self.queued_prompts.pop_front()?;
        self.note_driving_subscriber(&item.peer_id);

        let mux_id = self.next_mux_id;
        self.next_mux_id += 1;
        let turn_id = AmuxTurnId(self.next_amux_turn_id);
        self.next_amux_turn_id += 1;
        let supersedes_turn_id = match item.kind {
            QueuedPromptKind::Prompt => None,
            QueuedPromptKind::Queue => None,
            QueuedPromptKind::HardSteer { supersedes_turn_id } => Some(supersedes_turn_id),
        };
        let queue_item_id = item.queue_item_id.clone();
        let params = json!({
            "sessionId": item.session_id,
            "prompt": [{ "type": "text", "text": item.prompt_text }],
        });
        let mut req = IncomingRequest {
            jsonrpc: JsonRpcVersion,
            id: Id::Number(mux_id as i64),
            method: "session/prompt".to_string(),
            params: Some(params),
        };
        if self.meta_propagate {
            let (peer_name, role) = self
                .subscribers
                .get(&item.peer_id)
                .map(|s| (s.peer_name.as_deref(), s.role.as_deref()))
                .unwrap_or((None, None));
            inject_request_trace_metadata(
                &mut req,
                RequestTrace {
                    peer_id: &item.peer_id,
                    peer_name,
                    role,
                    mux_id,
                    amux_turn_id: Some(turn_id),
                },
            );
        }
        let out = match serde_json::to_vec(&req) {
            Ok(out) => out,
            Err(err) => {
                tracing::error!(
                    session = %self.room_id,
                    mux_id,
                    error = %err,
                    "failed to serialize mux-owned queued prompt; dropping",
                );
                return None;
            }
        };

        self.pending.insert(
            mux_id,
            PendingRequest {
                peer_id: item.peer_id.clone(),
                original_id: Id::Number(mux_id as i64),
                handshake: None,
                decorate_session_list: false,
                deliver_response: false,
                queue_item_id: queue_item_id.clone(),
            },
        );
        self.active_turn_mux_id = Some(mux_id);
        self.active_amux_turn_id = Some(turn_id);
        self.active_turn_session_id = req
            .params
            .as_ref()
            .and_then(|p| p.get("sessionId"))
            .and_then(Value::as_str)
            .map(str::to_string);
        self.active_turn_prompt_text = req
            .params
            .as_ref()
            .and_then(|p| p.get("prompt"))
            .and_then(text_from_text_only_prompt);
        tracing::info!(
            session = %self.room_id,
            peer_id = %item.peer_id,
            mux_id,
            amux_turn_id = %turn_id.formatted(),
            queue_item_id = ?queue_item_id,
            supersedes_turn_id = ?supersedes_turn_id.map(|t| t.formatted()),
            "mux-owned prompt submitted; active turn opened",
        );
        self.emit_turn_started(
            &item.peer_id,
            turn_id,
            req.params.as_ref(),
            supersedes_turn_id,
        );
        if let Some(queue_item_id) = queue_item_id.as_deref() {
            self.broadcast(amux::queue_item_submitted(
                &self.room_id,
                queue_item_id,
                turn_id,
            ));
        }
        Some(out)
    }

    /// Rebind the room's canonical session id to `loaded` after a
    /// successful `session/load`. Two cases:
    ///
    /// - **Existing `session_new_cache`**: mutate the `sessionId` field
    ///   in place. Other fields the upstream agent included in its
    ///   `session/new` response (e.g. agent-specific metadata) are
    ///   preserved so a late joiner's `session/new` call still gets
    ///   a structurally-valid response.
    /// - **No prior `session_new_cache`** (client opened with
    ///   `initialize` → `session/load` directly): synthesize a minimal
    ///   `{"sessionId": loaded}` value. A late joiner gets just the id
    ///   — enough to operate, missing any agent-specific session/new
    ///   fields the room never observed.
    ///
    /// Idempotent; safe to call multiple times for the same loaded id.
    fn rebind_canonical_session(&mut self, loaded: &str) {
        match self.session_new_cache.as_mut() {
            Some(Value::Object(obj)) => {
                let previous = obj
                    .insert("sessionId".to_string(), Value::String(loaded.to_string()))
                    .and_then(|v| v.as_str().map(|s| s.to_string()));
                tracing::info!(
                    session = %self.room_id,
                    previous = ?previous,
                    loaded,
                    "session/load: rebound canonical session id (existing cache)",
                );
            }
            _ => {
                self.session_new_cache = Some(serde_json::json!({
                    "sessionId": loaded,
                }));
                tracing::info!(
                    session = %self.room_id,
                    loaded,
                    "session/load: rebound canonical session id (synthesized cache)",
                );
            }
        }
        for item in &mut self.queued_prompts {
            item.session_id = loaded.to_string();
        }
    }

    fn reset_replay_generation_after_load(&mut self, loaded: &str, replay_start_len: usize) {
        self.replay_generation += 1;
        let presence_frames = self.current_peer_joined_replay_frames();
        let mut dropped_frame_count = 0;
        let mut retained_frame_count = 0;

        let mut retained_entries = None;
        if let Some(log) = self.replay_log.as_mut() {
            dropped_frame_count = replay_start_len.min(log.len());
            if dropped_frame_count > 0 {
                log.drain(..dropped_frame_count);
            }
            retained_entries = Some(std::mem::take(log));
        }

        if let Some(mut retained_entries) = retained_entries {
            let mut segmented =
                VecDeque::with_capacity(presence_frames.len() + retained_entries.len());
            for frame in presence_frames {
                let entry = ReplayEntry::new(
                    self.next_replay_seq,
                    Bytes::from(frame),
                    self.current_segment_id(),
                );
                self.next_replay_seq += 1;
                segmented.push_back(entry);
            }
            segmented.append(&mut retained_entries);
            retained_frame_count = segmented.len();
            self.replay_log = Some(segmented);
        }

        self.last_replay_reset = Some(ReplayResetSnapshot {
            loaded_session_id: loaded.to_string(),
            replay_generation: self.replay_generation,
            dropped_frame_count,
            retained_frame_count,
        });
        tracing::info!(
            session = %self.room_id,
            loaded,
            replay_generation = self.replay_generation,
            dropped_frame_count,
            retained_frame_count,
            "session/load: segmented replay generation",
        );
    }

    fn current_peer_joined_replay_frames(&self) -> Vec<Vec<u8>> {
        let mut peers: Vec<_> = self.subscribers.values().collect();
        peers.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));
        peers
            .into_iter()
            .map(|subscriber| {
                amux::peer_joined(
                    &self.room_id,
                    &subscriber.peer_id,
                    subscriber.peer_name.as_deref(),
                    subscriber.role.as_deref(),
                )
            })
            .collect()
    }

    fn note_driving_subscriber(&mut self, peer_id: &str) {
        if !self.subscribers.contains_key(peer_id) {
            tracing::debug!(
                session = %self.room_id,
                %peer_id,
                "skipping driving subscriber update for detached peer",
            );
            return;
        }
        if self.driving_subscriber_peer_id.as_deref() != Some(peer_id) {
            tracing::debug!(session = %self.room_id, %peer_id, "driving subscriber updated");
            self.driving_subscriber_peer_id = Some(peer_id.to_string());
            self.publish_session_list_metadata();
        }
    }

    pub(super) fn send_error_response(
        &self,
        peer_id: &str,
        original_id: Id,
        code: i64,
        message: &str,
    ) {
        let resp = IncomingResponse {
            jsonrpc: JsonRpcVersion,
            id: original_id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.to_string(),
                data: None,
            }),
        };
        let bytes = match serde_json::to_vec(&resp) {
            Ok(b) => Bytes::from(b),
            Err(err) => {
                tracing::error!(error = %err, "failed to serialize error response");
                return;
            }
        };
        if let Some(sub) = self.subscribers.get(peer_id)
            && sub.outbound.send(OutMsg::Frame(bytes)).is_err()
        {
            tracing::debug!(%peer_id, "subscriber dropped before error response delivered");
        }
    }

    pub(super) fn send_result_response(&self, peer_id: &str, original_id: Id, result: Value) {
        let resp = IncomingResponse {
            jsonrpc: JsonRpcVersion,
            id: original_id,
            result: Some(result),
            error: None,
        };
        let bytes = match serde_json::to_vec(&resp) {
            Ok(b) => Bytes::from(b),
            Err(err) => {
                tracing::error!(error = %err, "failed to serialize result response");
                return;
            }
        };
        if let Some(sub) = self.subscribers.get(peer_id)
            && sub.outbound.send(OutMsg::Frame(bytes)).is_err()
        {
            tracing::debug!(%peer_id, "subscriber dropped before result response delivered");
        }
    }

    fn send_cached_response(&self, peer_id: &str, original_id: Id, cached: Value) {
        let resp = IncomingResponse {
            jsonrpc: JsonRpcVersion,
            id: original_id,
            result: Some(cached),
            error: None,
        };
        let bytes = match serde_json::to_vec(&resp) {
            Ok(b) => Bytes::from(b),
            Err(err) => {
                tracing::error!(error = %err, "failed to serialize cached response");
                return;
            }
        };
        if let Some(sub) = self.subscribers.get(peer_id)
            && sub.outbound.send(OutMsg::Frame(bytes)).is_err()
        {
            tracing::debug!(%peer_id, "subscriber dropped before cached response delivered");
        }
    }

    /// Process one stderr line from the agent subprocess. The mux mirrors
    /// every stderr line into its own logs. Raw stderr is never broadcast to
    /// subscribers as transcript content — it stays in mux-side diagnostics.
    #[cfg(not(test))]
    fn mirror_agent_stderr_to_terminal(&self, line: &str) {
        eprintln!("[AGENT {room}] {line}", room = self.room_id);
    }

    #[cfg(test)]
    fn mirror_agent_stderr_to_terminal(&self, _line: &str) {
        // Keep the test runner's stderr quiet.
    }

    pub(super) fn handle_agent_stderr_line(&mut self, raw: Vec<u8>) {
        let line = String::from_utf8_lossy(&raw);
        let line = line.as_ref();
        self.mirror_agent_stderr_to_terminal(line);
        tracing::debug!(session = %self.room_id, agent_stderr = %line, "agent stderr");
    }

    fn handle_agent_line(&mut self, line: Vec<u8>) -> AgentLineAction {
        let frame = match Incoming::parse(&line) {
            Ok(f) => f,
            Err(err) => {
                tracing::warn!(
                    session = %self.room_id,
                    error = %err,
                    "invalid JSON-RPC frame from agent; falling back to raw broadcast",
                );
                return AgentLineAction::session_empty(self.broadcast(line));
            }
        };
        match frame {
            Incoming::Notification(notif) => {
                AgentLineAction::session_empty(self.handle_agent_notification(notif, line))
            }
            Incoming::Response(resp) => match self.route_agent_response(resp) {
                Some(write_to_agent) => AgentLineAction::write_to_agent(write_to_agent),
                None => AgentLineAction::none(),
            },
            Incoming::Request(req) => match self.route_agent_request(req, line) {
                Some(write_to_agent) => AgentLineAction::write_to_agent(write_to_agent),
                None => AgentLineAction::none(),
            },
        }
    }

    /// Agent-emitted notifications: most are broadcast-tier (forwarded to
    /// every subscriber and appended to the replay log). `$/cancel_request`
    /// is special — it cancels an *agent-initiated* request that's still
    /// InFlight in `agent_pending`. We translate by id, forward the
    /// cancellation to every subscriber, mark the entry Consumed, and emit
    /// `amux/agent_request_resolved { resolvedBy: "agent:cancelled" }` so
    /// peer UIs dismiss.
    ///
    /// The mux also peeks into notification params for observable ACP
    /// `sessionId` changes. Provider-specific metadata is passed through
    /// untouched and does not influence segment state.
    fn handle_agent_notification(
        &mut self,
        notif: crate::protocol::jsonrpc::IncomingNotification,
        line: Vec<u8>,
    ) -> bool {
        if notif.method == CANCEL_REQUEST_METHOD {
            return self.handle_agent_cancel(notif, line);
        }
        self.detect_segment_signal_from_agent_notification(&notif);
        self.broadcast(line)
    }

    /// Inspect an agent notification's params for an observable ACP
    /// `sessionId` change and rotate the segment if warranted. Pure
    /// observation: it never reads or writes outside the parsed `Value`,
    /// and emits no frames besides the ones `rotate_segment` itself produces.
    fn detect_segment_signal_from_agent_notification(
        &mut self,
        notif: &crate::protocol::jsonrpc::IncomingNotification,
    ) {
        let Some(params) = notif.params.as_ref() else {
            return;
        };
        let Some(acp_session_id) = params.get("sessionId").and_then(Value::as_str) else {
            return;
        };
        let Some(active) = self.active_segment() else {
            return;
        };
        if active.acp_session_id.as_deref() == Some(acp_session_id) {
            return;
        }
        self.rotate_segment(
            Some(acp_session_id.to_string()),
            EndReason::AcpSessionIdChanged,
        );
    }

    fn active_segment(&self) -> Option<&Segment> {
        let id = self.active_segment_id?;
        self.segments.iter().find(|s| s.id == id)
    }

    /// Fan out an agent-initiated request to every attached subscriber and
    /// record the request id in `agent_pending` so the first subscriber
    /// reply wins. The raw request is not replayed — replies are
    /// per-subscriber, and rejoining peers shouldn't be asked to confirm
    /// something already resolved. Instead, emit an inert
    /// `amux/agent_request_opened` sibling through `broadcast` first so
    /// the request context is durable for late joiners and ordered before
    /// the matching `amux/agent_request_resolved` lifecycle event.
    fn route_agent_request(&mut self, req: IncomingRequest, line: Vec<u8>) -> Option<Vec<u8>> {
        let id = req.id.clone();
        if let Some(mode) = self.client_tool_policy.mode_for_method(&req.method) {
            match mode {
                ClientToolMode::Block => {
                    tracing::warn!(
                        session = %self.room_id,
                        id = ?id,
                        method = %req.method,
                        "blocking agent-initiated client-tool request by policy",
                    );
                    return Some(build_client_tool_blocked_response(id, &req.method));
                }
                ClientToolMode::UnsafeDebug => {
                    tracing::warn!(
                        session = %self.room_id,
                        id = ?id,
                        method = %req.method,
                        subscribers = self.subscribers.len(),
                        "UNSAFE: broadcasting agent-initiated client-tool request by explicit debug policy",
                    );
                }
            }
        }
        if self.subscribers.is_empty() {
            tracing::warn!(
                session = %self.room_id,
                id = ?id,
                method = %req.method,
                "agent-initiated request with no attached subscribers; dropping",
            );
            return None;
        }
        self.agent_pending
            .insert(id.clone(), AgentReqState::InFlight);
        let request_id_value = match serde_json::to_value(&id) {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(
                    session = %self.room_id,
                    error = %err,
                    id = ?id,
                    method = %req.method,
                    "failed to serialize agent-request id; skipping opened lifecycle broadcast",
                );
                Value::Null
            }
        };
        let opened = amux::agent_request_opened(
            &self.room_id,
            &request_id_value,
            &req.method,
            req.params.as_ref(),
            self.active_amux_turn_id,
        );
        self.broadcast(opened);
        tracing::debug!(
            session = %self.room_id,
            id = ?id,
            method = %req.method,
            subscribers = self.subscribers.len(),
            amux_turn_id = ?self.active_amux_turn_id.map(|t| t.formatted()),
            "broadcasting agent-initiated request",
        );
        let frame = Bytes::from(line);
        if req.method == "session/request_permission" {
            self.pending_permission_frames
                .retain(|(pending_id, _)| pending_id != &id);
            self.pending_permission_frames
                .push((id.clone(), frame.clone()));
        }
        self.subscribers.retain(|peer_id, sub| {
            match sub.outbound.send(OutMsg::Frame(frame.clone())) {
                Ok(()) => true,
                Err(_) => {
                    tracing::debug!(%peer_id, "outbound channel closed; dropping subscriber");
                    false
                }
            }
        });
        None
    }

    fn route_agent_response(&mut self, mut resp: IncomingResponse) -> Option<Vec<u8>> {
        let mux_id = match resp.id {
            Id::Number(n) if n >= 0 => n as u64,
            ref other => {
                tracing::warn!(
                    session = %self.room_id,
                    id = ?other,
                    "agent response with non-numeric or negative id; dropping",
                );
                return None;
            }
        };
        let Some(pr) = self.pending.remove(&mux_id) else {
            tracing::warn!(session = %self.room_id, mux_id, "no pending request matches agent response; dropping");
            return None;
        };

        let mut next_write = None;
        if self.active_turn_mux_id == Some(mux_id) {
            self.active_turn_mux_id = None;
            self.active_turn_session_id = None;
            self.active_turn_prompt_text = None;
            let turn_id = self.active_amux_turn_id.take();
            tracing::info!(
                session = %self.room_id,
                mux_id,
                amux_turn_id = ?turn_id.map(|t| t.formatted()),
                "session/prompt response received; active turn cleared",
            );
            // Sweep before emitting amux/turn_complete so subscribers see
            // any abandoned-request cleanup events ahead of the turn
            // closure. Any agent-initiated request still InFlight at this
            // point was given up on by the agent (for example due to an
            // agent-side permission timeout) without writing a
            // response frame).
            self.sweep_stale_agent_pending("mux:turn-ended");
            if let Some(turn_id) = turn_id {
                self.emit_turn_complete(turn_id, resp.result.as_ref());
                if let Some(queue_item_id) = pr.queue_item_id.as_deref() {
                    let stop_reason = resp
                        .result
                        .as_ref()
                        .and_then(|r| r.get("stopReason"))
                        .cloned()
                        .unwrap_or(Value::Null);
                    self.broadcast(amux::queue_item_completed(
                        &self.room_id,
                        queue_item_id,
                        turn_id,
                        &stop_reason,
                    ));
                }
            }
            next_write = self.submit_next_queued_prompt();
        }

        // First-success handshake response caching. `session/load`
        // success rebinds the room's canonical session to the loaded
        // id — late joiners that call `session/new` get the loaded
        // session, not the previously-cached one. A failed load
        // (error response) leaves the existing cache untouched.
        if let Some(kind) = pr.handshake
            && let Some(result) = &resp.result
        {
            match kind {
                HandshakeKind::Initialize => {
                    if self.initialize_cache.is_none() {
                        tracing::info!(session = %self.room_id, "caching initialize result");
                        self.initialize_cache = Some(result.clone());
                    }
                }
                HandshakeKind::SessionNew => {
                    if self.session_new_cache.is_none() {
                        tracing::info!(session = %self.room_id, "caching session/new result");
                        self.session_new_cache = Some(result.clone());
                    }
                    if let Some(acp_session_id) = result.get("sessionId").and_then(Value::as_str) {
                        self.set_canonical_session_id(acp_session_id);
                    }
                }
                HandshakeKind::SessionLoad {
                    loaded_session_id,
                    replay_start_len,
                } => {
                    self.rebind_canonical_session(&loaded_session_id);
                    self.set_canonical_session_id_with_reason(
                        &loaded_session_id,
                        EndReason::SessionLoad,
                    );
                    self.reset_replay_generation_after_load(&loaded_session_id, replay_start_len);
                }
            }
        }

        if pr.decorate_session_list {
            self.decorate_session_list_response(&mut resp);
        }

        if pr.deliver_response {
            resp.id = pr.original_id;
            let bytes = match serde_json::to_vec(&resp) {
                Ok(b) => Bytes::from(b),
                Err(err) => {
                    tracing::error!(error = %err, "failed to serialize translated response");
                    return next_write;
                }
            };
            if let Some(sub) = self.subscribers.get(&pr.peer_id) {
                if sub.outbound.send(OutMsg::Frame(bytes)).is_err() {
                    tracing::debug!(peer_id = %pr.peer_id, "subscriber dropped before response delivered");
                }
            } else {
                tracing::debug!(peer_id = %pr.peer_id, "originator no longer attached; dropping response");
            }
        }

        next_write
    }

    /// Send `frame` to every subscriber and append to the replay log if
    /// enabled. Drops subscribers whose outbound channel has closed.
    /// Returns `true` only when fan-out *drained* subscribers — i.e.
    /// the map was non-empty going in and is empty coming out. A
    /// broadcast against an already-empty map (e.g. the initial
    /// `amux/peer_joined` emitted before the first subscriber is
    /// inserted) returns `false` and emits no "ending session" log,
    /// since no subscribers were lost.
    ///
    /// Only broadcast-tier frames flow through here: amux/* notifications
    /// (including inert agent-request lifecycle metadata) and the agent's
    /// session/update (and other notification-shaped) frames.
    /// Per-subscriber frames (responses, raw actionable agent-initiated
    /// requests) do NOT go through `broadcast` and are NOT logged.
    fn broadcast(&mut self, frame: impl Into<Bytes>) -> bool {
        let frame: Bytes = frame.into();
        let segment_id = self.current_segment_id();
        if let Some(log) = self.replay_log.as_mut() {
            let entry = ReplayEntry::new(self.next_replay_seq, frame.clone(), segment_id);
            if let Some(store) = self.replay_store.as_ref()
                && let Err(err) =
                    store.append(entry.seq, segment_id.0, &entry.recorded_at, &entry.frame)
            {
                tracing::warn!(
                    room = %self.room_id,
                    seq = entry.seq,
                    error = %err,
                    "replay store: append failed; frame not persisted",
                );
            }
            self.next_replay_seq += 1;
            log.push_back(entry);
        }
        let pre_fanout = self.subscribers.len();
        self.subscribers.retain(|peer_id, sub| {
            match sub.outbound.send(OutMsg::Frame(frame.clone())) {
                Ok(()) => true,
                Err(_) => {
                    tracing::debug!(%peer_id, "outbound channel closed; dropping subscriber");
                    false
                }
            }
        });
        if pre_fanout > 0 && self.subscribers.is_empty() {
            tracing::info!(session = %self.room_id, "no live subscribers after fan-out; ending session");
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::attach::HistoryPolicy;

    fn test_inner() -> RoomInner {
        let (tx, _rx) = mpsc::channel(1);
        RoomInner::new(
            "test-room".to_string(),
            "/tmp".to_string(),
            ReplayTurns::Disabled,
            false,
            ClientToolPolicy::default(),
            Arc::new(SessionListMetadataIndex::new()),
            tx,
            true,
            None,
        )
    }

    fn test_inner_with_store(replay_store: Arc<ReplayStore>) -> RoomInner {
        let (tx, _rx) = mpsc::channel(1);
        RoomInner::new(
            "hydrate-room".to_string(),
            "/tmp".to_string(),
            ReplayTurns::Unbounded,
            false,
            ClientToolPolicy::default(),
            Arc::new(SessionListMetadataIndex::new()),
            tx,
            true,
            Some(replay_store),
        )
    }

    #[test]
    fn first_canonical_session_id_opens_initial_segment() {
        let mut inner = test_inner();
        assert!(inner.active_segment_id.is_none(), "no segment before id");
        assert!(inner.segments.is_empty());

        inner.set_canonical_session_id("sess-mock");

        assert_eq!(inner.active_segment_id, Some(SegmentId(1)));
        assert_eq!(inner.segments.len(), 1);
        let seg = &inner.segments[0];
        assert_eq!(seg.id, SegmentId(1));
        assert_eq!(seg.acp_session_id.as_deref(), Some("sess-mock"));
        assert!(seg.closed_at.is_none());
        assert!(seg.end_reason.is_none());
    }

    #[test]
    fn repeated_canonical_session_id_does_not_reopen_segment() {
        let mut inner = test_inner();
        inner.set_canonical_session_id("sess-mock");
        inner.set_canonical_session_id("sess-mock");

        assert_eq!(inner.segments.len(), 1, "same id must not open new segment");
        assert_eq!(inner.active_segment_id, Some(SegmentId(1)));
    }

    #[test]
    fn rotate_segment_closes_current_and_opens_new() {
        let mut inner = test_inner();
        // Use the unbounded replay path so broadcasts are observable.
        inner.replay_log = Some(VecDeque::new());

        inner.set_canonical_session_id_with_reason("sess-1", EndReason::SessionLoad);
        assert_eq!(inner.active_segment_id, Some(SegmentId(1)));
        let seg1_opened_seq = inner.segments[0].opened_replay_seq;

        // Rotate explicitly: closes seg-1, opens seg-2.
        inner.set_canonical_session_id_with_reason("sess-2", EndReason::SessionLoad);

        assert_eq!(
            inner.segments.len(),
            2,
            "second canonical id must open a second segment",
        );
        assert_eq!(inner.active_segment_id, Some(SegmentId(2)));

        let closed = &inner.segments[0];
        assert_eq!(closed.id, SegmentId(1));
        assert!(closed.closed_at.is_some(), "closed_at populated");
        assert_eq!(closed.end_reason, Some(EndReason::SessionLoad));
        assert!(closed.closed_replay_seq.is_some());

        let opened = &inner.segments[1];
        assert_eq!(opened.id, SegmentId(2));
        assert_eq!(opened.acp_session_id.as_deref(), Some("sess-2"));
        assert!(opened.closed_at.is_none());

        // segment_ended for seg-1 + segment_started for seg-2 emitted at
        // initial open + at rotation. Two opens, one ended → 3 frames.
        let log = inner.replay_log.as_ref().unwrap();
        let methods: Vec<String> = log
            .iter()
            .filter_map(|e| {
                let v: serde_json::Value = serde_json::from_slice(&e.frame).ok()?;
                v.get("method")?.as_str().map(String::from)
            })
            .collect();
        assert_eq!(
            methods,
            vec![
                "amux/segment_started",
                "amux/segment_ended",
                "amux/segment_started",
            ],
            "lifecycle frames in transcript: open seg-1, end seg-1, open seg-2",
        );

        // segment_ended must be tagged with the OLD segment (seg-1) so
        // current-segment-only replay omits it cleanly.
        let ended_entry = log
            .iter()
            .find(|e| {
                let v: serde_json::Value = serde_json::from_slice(&e.frame).unwrap_or_default();
                v.get("method")
                    .and_then(|m| m.as_str())
                    .map(|s| s == "amux/segment_ended")
                    .unwrap_or(false)
            })
            .expect("segment_ended must exist");
        assert_eq!(
            ended_entry.segment_id,
            SegmentId(1),
            "segment_ended sits in the closing segment",
        );
        // closed_replay_seq must include the bookend frame so per-segment
        // slicing on (opened_replay_seq..=closed_replay_seq) covers it.
        assert_eq!(
            closed.closed_replay_seq,
            Some(ended_entry.seq),
            "closed_replay_seq must cover the segment_ended bookend",
        );

        // opened_replay_seq for seg-2 must be strictly after seg-1's open.
        assert!(
            opened.opened_replay_seq > seg1_opened_seq,
            "new segment's opened_replay_seq must monotonically advance",
        );

        // segment_started for seg-2 must be the first frame at-or-after
        // seg-2's opened_replay_seq, completing the bookend invariant.
        let started_entry = log
            .iter()
            .find(|e| e.segment_id == SegmentId(2))
            .expect("seg-2 must have at least one frame (segment_started)");
        let v: serde_json::Value = serde_json::from_slice(&started_entry.frame).unwrap_or_default();
        assert_eq!(
            v["method"].as_str(),
            Some("amux/segment_started"),
            "first frame in new segment is the segment_started bookend",
        );
        assert_eq!(started_entry.seq, opened.opened_replay_seq);
    }

    fn make_notification(
        method: &str,
        params: Value,
    ) -> crate::protocol::jsonrpc::IncomingNotification {
        crate::protocol::jsonrpc::IncomingNotification {
            jsonrpc: crate::protocol::jsonrpc::JsonRpcVersion,
            method: method.to_string(),
            params: Some(params),
        }
    }

    #[test]
    fn provider_specific_metadata_does_not_rotate_segment_under_stable_acp_id() {
        let mut inner = test_inner();
        inner.replay_log = Some(VecDeque::new());
        inner.set_canonical_session_id_with_reason("acp-stable", EndReason::SessionLoad);

        let notif = make_notification(
            "session/update",
            json!({
                "sessionId": "acp-stable",
                "_meta": {
                    "vendor": {
                        "internalSessionId": "vendor-2",
                        "lastMode": "session_split"
                    }
                }
            }),
        );
        inner.detect_segment_signal_from_agent_notification(&notif);

        assert_eq!(
            inner.segments.len(),
            1,
            "provider metadata is passthrough-only"
        );
        assert_eq!(
            inner.active_segment().unwrap().acp_session_id.as_deref(),
            Some("acp-stable"),
        );
    }

    #[test]
    fn repeated_signals_with_same_acp_session_id_do_not_rotate() {
        let mut inner = test_inner();
        inner.replay_log = Some(VecDeque::new());
        inner.set_canonical_session_id_with_reason("acp-1", EndReason::SessionLoad);
        let notif = make_notification("session/update", json!({ "sessionId": "acp-1" }));
        inner.detect_segment_signal_from_agent_notification(&notif);
        inner.detect_segment_signal_from_agent_notification(&notif);
        inner.detect_segment_signal_from_agent_notification(&notif);

        assert_eq!(inner.segments.len(), 1, "stable ACP id never rotates");
    }

    #[test]
    fn history_full_returns_current_segment_only() {
        let mut inner = test_inner();
        inner.replay_log = Some(VecDeque::new());

        inner.set_canonical_session_id_with_reason("acp-1", EndReason::SessionLoad);
        // Stuff a frame into seg-1.
        let frame_seg1 = br#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp-1","update":{"kind":"seg1-marker"}}}"#;
        inner.broadcast(Bytes::from_static(frame_seg1));

        inner.set_canonical_session_id_with_reason("acp-2", EndReason::SessionLoad);
        let frame_seg2 = br#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp-2","update":{"kind":"seg2-marker"}}}"#;
        inner.broadcast(Bytes::from_static(frame_seg2));

        let full = inner.history_full();
        let kinds: Vec<&str> = full
            .iter()
            .filter_map(|e| e.params.get("update")?.get("kind")?.as_str())
            .collect();
        assert_eq!(
            kinds,
            vec!["seg2-marker"],
            "Full must surface only the active segment's update frames",
        );

        // Lineage view sees both updates in chronological order.
        let lineage = inner.history_full_lineage();
        let lineage_kinds: Vec<&str> = lineage
            .iter()
            .filter_map(|e| e.params.get("update")?.get("kind")?.as_str())
            .collect();
        assert_eq!(lineage_kinds, vec!["seg1-marker", "seg2-marker"]);
    }

    #[test]
    fn stream_path_respects_history_policy() {
        // The streaming-delivery path must honour `historyPolicy: full` =
        // current-segment-only; without the filter it leaked all
        // segments to clients that opted into `full`.
        let mut inner = test_inner();
        inner.replay_log = Some(VecDeque::new());

        inner.set_canonical_session_id_with_reason("acp-1", EndReason::SessionLoad);
        inner.broadcast(Bytes::from_static(
            br#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp-1","update":{"kind":"seg1"}}}"#,
        ));
        inner.set_canonical_session_id_with_reason("acp-2", EndReason::SessionLoad);
        inner.broadcast(Bytes::from_static(
            br#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp-2","update":{"kind":"seg2"}}}"#,
        ));

        let full_entries = inner.replay_entries_for_policy(HistoryPolicy::Full);
        let full_kinds: Vec<String> = full_entries
            .iter()
            .filter_map(|entry| {
                let v: serde_json::Value = serde_json::from_slice(&entry.frame).ok()?;
                v.get("params")?
                    .get("update")?
                    .get("kind")?
                    .as_str()
                    .map(str::to_string)
            })
            .collect();
        assert_eq!(
            full_kinds,
            vec!["seg2"],
            "stream + full must only surface the active segment's updates",
        );

        let lineage_entries = inner.replay_entries_for_policy(HistoryPolicy::FullLineage);
        let lineage_kinds: Vec<String> = lineage_entries
            .iter()
            .filter_map(|entry| {
                let v: serde_json::Value = serde_json::from_slice(&entry.frame).ok()?;
                v.get("params")?
                    .get("update")?
                    .get("kind")?
                    .as_str()
                    .map(str::to_string)
            })
            .collect();
        assert_eq!(
            lineage_kinds,
            vec!["seg1", "seg2"],
            "stream + full_lineage must surface every segment's updates",
        );
    }

    fn turn_started_frame(room_id: &str, amux_turn_id: u64, peer_id: &str) -> Bytes {
        Bytes::from(format!(
            r#"{{"jsonrpc":"2.0","method":"amux/turn_started","params":{{"roomId":"{room_id}","amuxTurnId":"at-{amux_turn_id}","peerId":"{peer_id}","content":[]}}}}"#,
        ))
    }

    fn turn_complete_frame(room_id: &str, amux_turn_id: u64) -> Bytes {
        Bytes::from(format!(
            r#"{{"jsonrpc":"2.0","method":"amux/turn_complete","params":{{"roomId":"{room_id}","amuxTurnId":"at-{amux_turn_id}","stopReason":"end_turn"}}}}"#,
        ))
    }

    fn update_frame(session_id: &str) -> Bytes {
        Bytes::from(format!(
            r#"{{"jsonrpc":"2.0","method":"session/update","params":{{"sessionId":"{session_id}","update":{{"kind":"agent_message_chunk"}}}}}}"#,
        ))
    }

    #[test]
    fn history_full_carries_turn_started_when_turn_completed_across_segments() {
        // turn_started lands in seg-1, then a segment rotation moves
        // to seg-2 mid-turn, then turn_complete lands in seg-2.
        // historyPolicy: full must carry the seg-1 turn_started forward so
        // clients see a bracketed turn instead of an orphan turn_complete.
        let mut inner = test_inner();
        inner.replay_log = Some(VecDeque::new());

        inner.set_canonical_session_id_with_reason("acp-1", EndReason::SessionLoad);
        inner.broadcast(turn_started_frame("test-room", 7, "alice"));
        inner.broadcast(update_frame("acp-1"));

        inner.rotate_segment(Some("acp-2".to_string()), EndReason::AcpSessionIdChanged);

        inner.broadcast(update_frame("acp-1"));
        inner.broadcast(turn_complete_frame("test-room", 7));

        let history = inner.history_full();
        let methods: Vec<&str> = history.iter().map(|e| e.method.as_str()).collect();

        // turn_started should appear (carried from seg-1) before turn_complete.
        let started_idx = methods
            .iter()
            .position(|m| *m == "amux/turn_started")
            .expect("turn_started must be carried into full view");
        let complete_idx = methods
            .iter()
            .position(|m| *m == "amux/turn_complete")
            .expect("turn_complete must be present");
        assert!(
            started_idx < complete_idx,
            "carried turn_started must precede turn_complete in replay order",
        );
    }

    #[test]
    fn history_full_carries_turn_started_when_turn_is_still_active_across_segments() {
        // turn_started lands in seg-1, rotation happens mid-turn, the turn
        // has not yet completed when a late joiner attaches. The carry
        // should pull turn_started forward by way of active_amux_turn_id.
        let mut inner = test_inner();
        inner.replay_log = Some(VecDeque::new());

        inner.set_canonical_session_id_with_reason("acp-1", EndReason::SessionLoad);
        inner.broadcast(turn_started_frame("test-room", 11, "alice"));
        inner.active_amux_turn_id = Some(AmuxTurnId(11));
        inner.active_turn_mux_id = Some(99);
        inner.broadcast(update_frame("acp-1"));

        inner.rotate_segment(Some("acp-2".to_string()), EndReason::AcpSessionIdChanged);
        inner.broadcast(update_frame("acp-1"));
        // Turn still in flight: no turn_complete yet.

        let history = inner.history_full();
        let methods: Vec<&str> = history.iter().map(|e| e.method.as_str()).collect();
        assert!(
            methods.contains(&"amux/turn_started"),
            "active turn's started bookend must be carried even without turn_complete",
        );
    }

    #[test]
    fn history_full_does_not_carry_turn_that_completed_before_active_segment() {
        // A turn fully contained in seg-1 (started AND completed there)
        // followed by rotation into seg-2: the historyPolicy: full view
        // for a late joiner attached in seg-2 must NOT show that prior
        // turn — only frames in seg-2 plus any cross-segment bookend.
        let mut inner = test_inner();
        inner.replay_log = Some(VecDeque::new());

        inner.set_canonical_session_id_with_reason("acp-1", EndReason::SessionLoad);
        inner.broadcast(turn_started_frame("test-room", 1, "alice"));
        inner.broadcast(turn_complete_frame("test-room", 1));

        inner.rotate_segment(Some("acp-2".to_string()), EndReason::SessionLoad);
        inner.broadcast(turn_started_frame("test-room", 2, "alice"));
        inner.broadcast(turn_complete_frame("test-room", 2));

        let history = inner.history_full();
        let turn_ids: Vec<&str> = history
            .iter()
            .filter(|e| e.method == "amux/turn_started" || e.method == "amux/turn_complete")
            .filter_map(|e| e.params.get("amuxTurnId")?.as_str())
            .collect();
        assert!(
            turn_ids.iter().all(|id| *id == "at-2"),
            "only the active segment's turn bookends should appear (got {turn_ids:?})",
        );
    }

    #[test]
    fn history_full_lineage_returns_every_segment_unchanged() {
        // Full lineage path is unaffected by the carry rule — it already
        // returns everything. Regression guard.
        let mut inner = test_inner();
        inner.replay_log = Some(VecDeque::new());

        inner.set_canonical_session_id_with_reason("acp-1", EndReason::SessionLoad);
        inner.broadcast(turn_started_frame("test-room", 1, "alice"));
        inner.broadcast(turn_complete_frame("test-room", 1));
        inner.rotate_segment(Some("acp-2".to_string()), EndReason::SessionLoad);
        inner.broadcast(turn_started_frame("test-room", 2, "alice"));
        inner.broadcast(turn_complete_frame("test-room", 2));

        let history = inner.history_full_lineage();
        let bookends: Vec<&str> = history
            .iter()
            .filter(|e| e.method == "amux/turn_started" || e.method == "amux/turn_complete")
            .filter_map(|e| e.params.get("amuxTurnId")?.as_str())
            .collect();
        assert_eq!(bookends, vec!["at-1", "at-1", "at-2", "at-2"]);
    }

    #[test]
    fn segment_summary_populates_after_rotation() {
        let mut inner = test_inner();
        inner.replay_log = Some(VecDeque::new());
        inner.set_canonical_session_id_with_reason("acp-1", EndReason::SessionLoad);
        inner.set_canonical_session_id_with_reason("acp-2", EndReason::SessionLoad);

        let summaries: Vec<_> = inner.segments.iter().map(segment_summary).collect();
        assert_eq!(summaries.len(), 2);
        assert_eq!(summaries[0].id, SegmentId(1));
        assert_eq!(summaries[0].end_reason, Some(EndReason::SessionLoad));
        assert!(summaries[0].closed_at.is_some());
        assert_eq!(summaries[1].id, SegmentId(2));
        assert_eq!(summaries[1].acp_session_id.as_deref(), Some("acp-2"));
        assert!(summaries[1].closed_at.is_none());
    }

    #[test]
    fn rotate_segment_respects_emit_segment_frames_flag() {
        let (tx, _rx) = mpsc::channel(1);
        let mut inner = RoomInner::new(
            "test-room".to_string(),
            "/tmp".to_string(),
            ReplayTurns::Unbounded,
            false,
            ClientToolPolicy::default(),
            Arc::new(SessionListMetadataIndex::new()),
            tx,
            false, // emit_segment_frames OFF
            None,
        );

        inner.set_canonical_session_id_with_reason("sess-1", EndReason::SessionLoad);
        inner.set_canonical_session_id_with_reason("sess-2", EndReason::SessionLoad);

        // Internal state still rotates…
        assert_eq!(inner.segments.len(), 2);
        assert_eq!(inner.active_segment_id, Some(SegmentId(2)));

        // …but no segment lifecycle frames hit the transcript.
        let log = inner.replay_log.as_ref().unwrap();
        let has_segment_method = log.iter().any(|e| {
            let v: serde_json::Value = serde_json::from_slice(&e.frame).unwrap_or_default();
            v.get("method")
                .and_then(|m| m.as_str())
                .map(|s| s.starts_with("amux/segment_"))
                .unwrap_or(false)
        });
        assert!(
            !has_segment_method,
            "--emit-segment-frames=false must suppress lifecycle frames",
        );
    }

    #[test]
    fn rebind_canonical_session_rewrites_queued_prompt_session_ids() {
        let mut inner = test_inner();
        inner.queued_prompts.push_back(QueuedPrompt {
            queue_item_id: Some("q-1".to_string()),
            peer_id: "A".to_string(),
            session_id: "sess-old".to_string(),
            prompt_text: "queued".to_string(),
            kind: QueuedPromptKind::Queue,
        });
        inner.queued_prompts.push_back(QueuedPrompt {
            queue_item_id: None,
            peer_id: "B".to_string(),
            session_id: "sess-old".to_string(),
            prompt_text: "steered".to_string(),
            kind: QueuedPromptKind::HardSteer {
                supersedes_turn_id: AmuxTurnId(1),
            },
        });

        inner.rebind_canonical_session("sess-loaded");

        assert!(
            inner
                .queued_prompts
                .iter()
                .all(|item| item.session_id == "sess-loaded"),
            "session/load must retarget queued prompts to the newly loaded canonical session"
        );
    }
}

#[derive(Debug, Clone)]
pub struct RoomOptions {
    pub replay_policy: ReplayTurns,
    pub session_ttl: Duration,
    pub meta_propagate: bool,
    pub client_tool_policy: ClientToolPolicy,
    pub session_list_index: Arc<SessionListMetadataIndex>,
    pub agent_cwd: String,
    /// Whether to emit `amux/segment_started` / `amux/segment_ended`
    /// lifecycle frames on rotation. Default true. Set false to preserve
    /// byte-equivalence with v0.1.x clients during the rooms rollout.
    pub emit_segment_frames: bool,
    /// Opt-in on-disk replay persistence. When `Some`, every broadcast
    /// frame is appended to the store and the room is rehydrated from
    /// the store on construction. Has no effect when `replay_policy ==
    /// ReplayTurns::Disabled` (the in-memory log is `None` so there is
    /// nothing to persist or restore).
    pub replay_store: Option<Arc<ReplayStore>>,
}

pub fn spawn_room(
    initial_subscriber: Subscriber,
    mut agent: AgentProcess,
    room_id: String,
    options: RoomOptions,
) -> (RoomHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel::<RoomMsg>(SESSION_QUEUE_CAPACITY);
    let stdout_rx = agent
        .take_stdout_rx()
        .expect("AgentProcess::take_stdout_rx must succeed on a fresh process");
    let stderr_rx = agent
        .take_stderr_rx()
        .expect("AgentProcess::take_stderr_rx must succeed on a fresh process");

    let pump_tx = tx.clone();
    let pump_room_id = room_id.clone();
    let pump = tokio::spawn(async move {
        let mut rx = stdout_rx;
        while let Some(line) = rx.recv().await {
            if pump_tx.send(RoomMsg::AgentStdoutLine(line)).await.is_err() {
                return;
            }
        }
        let _ = pump_tx.send(RoomMsg::AgentDied).await;
        tracing::debug!(room = %pump_room_id, "stdout pump finished");
    });

    let stderr_tx = tx.clone();
    let stderr_room_id = room_id.clone();
    let stderr_pump = tokio::spawn(async move {
        let mut rx = stderr_rx;
        while let Some(line) = rx.recv().await {
            if stderr_tx
                .send(RoomMsg::AgentStderrLine(line))
                .await
                .is_err()
            {
                return;
            }
        }
        tracing::debug!(room = %stderr_room_id, "stderr pump finished");
    });

    let actor = tokio::spawn(run_room(
        rx,
        tx.clone(),
        agent,
        initial_subscriber,
        pump,
        stderr_pump,
        room_id,
        options,
    ));
    (RoomHandle { tx }, actor)
}

/// Reason the session loop exited. Drives the teardown sequence — agent
/// death gets a structured 1011 close to subscribers; TTL expiry and
/// natural shutdown drop subscriber senders without a close frame.
enum ExitReason {
    LastSubscriberLeft, // followed TTL grace; subscribers map already empty
    TtlExpired,         // subscribers map still empty after grace
    AgentDied,
    ChannelClosed, // registry dropped tx; uncommon
}

#[allow(clippy::too_many_arguments)]
async fn run_room(
    mut rx: mpsc::Receiver<RoomMsg>,
    self_tx: mpsc::Sender<RoomMsg>,
    mut agent: AgentProcess,
    initial_subscriber: Subscriber,
    pump: JoinHandle<()>,
    stderr_pump: JoinHandle<()>,
    session_id: String,
    options: RoomOptions,
) {
    let mut inner = RoomInner::new(
        session_id.clone(),
        options.agent_cwd.clone(),
        options.replay_policy,
        options.meta_propagate,
        options.client_tool_policy,
        options.session_list_index.clone(),
        self_tx,
        options.emit_segment_frames,
        options.replay_store.clone(),
    );
    inner
        .attach(initial_subscriber)
        .expect("initial subscriber cannot collide on an empty map");
    tracing::info!(session = %session_id, subscribers = inner.subscribers.len(), "session started");

    // None when at least one subscriber is attached; Some(sleep) while in
    // TTL grace. The select! arm only fires when the sleep is armed.
    let mut ttl_sleep: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;

    let reason = loop {
        let exit = tokio::select! {
            biased;
            msg = rx.recv() => {
                match msg {
                    None => Some(ExitReason::ChannelClosed),
                    Some(RoomMsg::Attach { subscriber, ack }) => {
                        let result = inner.attach(subscriber);
                        if result.is_ok() && ttl_sleep.take().is_some() {
                            tracing::info!(
                                session = %session_id,
                                "TTL grace cancelled by attach",
                            );
                        }
                        let _ = ack.send(result);
                        None
                    }
                    Some(RoomMsg::Detach { peer_id }) => {
                        let now_empty = inner.detach(&peer_id);
                        if now_empty {
                            tracing::info!(
                                session = %session_id,
                                ttl_secs = options.session_ttl.as_secs_f64(),
                                "last subscriber gone; starting TTL grace",
                            );
                            ttl_sleep = Some(Box::pin(tokio::time::sleep(options.session_ttl)));
                        }
                        None
                    }
                    Some(RoomMsg::InboundFromSubscriber { peer_id, bytes }) => {
                        if let Some(out) = inner.handle_inbound(&peer_id, bytes)
                            && let Err(err) = agent.send(&out).await
                        {
                            tracing::warn!(
                                session = %session_id,
                                %peer_id,
                                error = %err,
                                "agent stdin write failed",
                            );
                        }
                        None
                    }
                    Some(RoomMsg::AgentStdoutLine(line)) => {
                        let action = inner.handle_agent_line(line);
                        for out in action.writes_to_agent {
                            if let Err(err) = agent.send(&out).await {
                                tracing::warn!(
                                    session = %session_id,
                                    error = %err,
                                    "agent stdin write failed while handling agent response/request policy",
                                );
                                break;
                            }
                        }
                        if action.session_empty {
                            Some(ExitReason::LastSubscriberLeft)
                        } else {
                            None
                        }
                    }
                    Some(RoomMsg::AgentStderrLine(line)) => {
                        inner.handle_agent_stderr_line(line);
                        None
                    }
                    Some(RoomMsg::AgentDied) => {
                        Some(ExitReason::AgentDied)
                    }
                    Some(RoomMsg::AttachStreamBackfill(plan)) => {
                        inner.send_attach_backfill_page(plan);
                        None
                    }
                    Some(RoomMsg::Snapshot { ack }) => {
                        let snap = inner.build_snapshot(ttl_sleep.is_some());
                        let _ = ack.send(snap);
                        None
                    }
                }
            }
            _ = async {
                match ttl_sleep.as_mut() {
                    Some(s) => s.as_mut().await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                if inner.subscribers.is_empty() {
                    Some(ExitReason::TtlExpired)
                } else {
                    // Shouldn't happen — sleep was armed when no subs were
                    // present, attach disarms it. Defensive: clear and
                    // continue.
                    ttl_sleep = None;
                    None
                }
            }
        };
        if let Some(r) = exit {
            break r;
        }
    };

    match reason {
        ExitReason::AgentDied => {
            tracing::warn!(session = %session_id, "agent subprocess exited; closing subscribers with 1011");
            inner.close_all_subscribers(WS_CLOSE_AGENT_DEAD, "agent subprocess exited");
        }
        ExitReason::TtlExpired => {
            tracing::info!(session = %session_id, "TTL expired; tearing down session");
        }
        ExitReason::LastSubscriberLeft => {
            tracing::info!(session = %session_id, "session ended (no subscribers)");
        }
        ExitReason::ChannelClosed => {
            tracing::info!(session = %session_id, "session ended (channel closed)");
        }
    }

    inner.clear_session_list_metadata();
    inner.subscribers.clear();
    if let Err(err) = agent.shutdown(SHUTDOWN_TIMEOUT).await {
        tracing::warn!(session = %session_id, error = %err, "agent shutdown error");
    }
    pump.abort();
    stderr_pump.abort();
    tracing::info!(session = %session_id, "session task exiting");
}
