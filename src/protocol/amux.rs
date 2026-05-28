//! `amux/*` namespace frames — out-of-band metadata emitted by the
//! multiplexer (peer presence, turn boundaries, busy state).
//!
//! Each builder returns a complete JSON-RPC notification frame as NDJSON-
//! ready bytes (no trailing newline; the WS-out / replay-log path adds
//! framing as needed). The shapes match `docs/design/amux-namespace.md`.
//!
//! `amuxTurnId` is formatted `at-<u64>` with a monotonic per-session
//! counter; the prefix exists so a token shows its origin in logs.

use serde::Serialize;

const METHOD_PEER_JOINED: &str = "amux/peer_joined";
const METHOD_PEER_LEFT: &str = "amux/peer_left";
const METHOD_SESSION_CONTEXT: &str = "amux/session_context";
const METHOD_TURN_STARTED: &str = "amux/turn_started";
const METHOD_TURN_COMPLETE: &str = "amux/turn_complete";
const METHOD_SESSION_BUSY: &str = "amux/session_busy";
const METHOD_AGENT_REQUEST_OPENED: &str = "amux/agent_request_opened";
const METHOD_AGENT_REQUEST_RESOLVED: &str = "amux/agent_request_resolved";
const METHOD_TURN_CANCELLED: &str = "amux/turn_cancelled";
const METHOD_CONTROL_SUBMITTED: &str = "amux/control_submitted";
const METHOD_QUEUE_ITEM_ADDED: &str = "amux/queue_item_added";
const METHOD_QUEUE_ITEM_SUBMITTED: &str = "amux/queue_item_submitted";
const METHOD_QUEUE_ITEM_COMPLETED: &str = "amux/queue_item_completed";
const METHOD_QUEUE_ITEM_REMOVED: &str = "amux/queue_item_removed";
const METHOD_QUEUE_ITEM_ORPHANED: &str = "amux/queue_item_orphaned";
const METHOD_REPLAY_STARTED: &str = "amux/replay_started";
const METHOD_REPLAY_COMPLETE: &str = "amux/replay_complete";
const METHOD_SEGMENT_STARTED: &str = "amux/segment_started";
const METHOD_SEGMENT_ENDED: &str = "amux/segment_ended";
const METHOD_CONTEXT_COMPACTION_STARTED: &str = "amux/context_compaction_started";
const METHOD_CONTEXT_COMPACTION_DONE: &str = "amux/context_compaction_done";

/// Source label for compaction signals that originated from the Hermes
/// agent's stderr stream. Used in `amux/context_compaction_*` frames so
/// clients can tell whether they're looking at a stderr-derived signal
/// (best-effort, may be missing fields) or a future structured signal.
pub const COMPACTION_SOURCE_HERMES_STDERR: &str = "hermes_stderr";

/// Method name for the amux extension that lets any attached peer steer the
/// current composer state. If a turn is in flight, current mux-owned semantics
/// cancel/supersede the active turn and submit a replacement prompt after the
/// agent settles; if the mux is idle, the steer text is submitted immediately
/// as the next prompt.
pub const METHOD_STEER_ACTIVE_TURN: &str = "amux/steer_active_turn";

/// Method name for the amux extension that asks the mux to enqueue text for
/// the next turn while another turn is active.
pub const METHOD_QUEUE_PROMPT: &str = "amux/queue_prompt";

/// Method name for the amux extension that removes a queued prompt before it
/// is submitted. The queue item must still be pending; active/already-complete
/// items are not removable through this control path.
pub const METHOD_UNQUEUE_PROMPT: &str = "amux/unqueue_prompt";

/// Method name for the amux extension that lets any attached peer cancel
/// the in-flight turn (not just the driver). Internally resolves to ACP
/// `session/cancel` toward the agent; strict `$/cancel_request`
/// semantics remain reserved for request-id cancellation.
pub const METHOD_CANCEL_ACTIVE_TURN: &str = "amux/cancel_active_turn";

/// Resolved-by sentinel used in `amux/agent_request_resolved` cleanup
/// broadcasts when the agent itself cancels an agent-initiated request
/// via `$/cancel_request`. Companion to the existing `"mux:turn-ended"`
/// sentinel used by the turn-end sweep.
pub const RESOLVED_BY_AGENT_CANCELLED: &str = "agent:cancelled";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AmuxTurnId(pub u64);

impl AmuxTurnId {
    pub fn formatted(self) -> String {
        format!("at-{}", self.0)
    }
}

/// Monotonic, per-room segment counter. A segment is the interval during
/// which a single canonical ACP `sessionId` is in force. Compaction inside
/// the agent (hermes rotating its internal session id under a stable ACP
/// id) ends one segment and opens the next; a client-initiated
/// `session/load` does the same. The first segment of a room opens once
/// the canonical session id is captured from the agent's `session/new` or
/// `session/load` response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId(pub u64);

impl SegmentId {
    pub fn formatted(self) -> String {
        format!("seg-{}", self.0)
    }
}

impl serde::Serialize for SegmentId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.formatted())
    }
}

/// Why a segment closed. Captured on `amux/segment_ended` and exposed via
/// `/debug/sessions` so operators can distinguish a client-driven session
/// rotation from a hermes-driven compression rotation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndReason {
    /// Client called `session/load`, swapping the canonical ACP session id.
    SessionLoad,
    /// `_meta.hermes` reported a session-split compression — same ACP
    /// session id, different internal hermes session id.
    HermesCompression,
    /// Heuristic detection: an agent frame carried a different ACP
    /// `sessionId` than the active segment's known value. Used when
    /// `_meta.hermes` is unavailable.
    AcpSessionIdChanged,
}

/// Mirror of the (forthcoming) `_meta.hermes.sessionProvenance` +
/// `_meta.hermes.compaction` payloads. Fields are populated opportunistic-
/// ally: heuristic detection (Phase D) leaves most fields `None`; once
/// hermes ships the metadata, parser fills them in. Late metadata for an
/// already-open segment is allowed to backfill in place — see
/// `RoomInner::maybe_rotate_segment` for the policy.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct HermesProvenance {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acp_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hermes_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_hermes_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_hermes_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creator_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edge_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_depth: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lineage_hermes_session_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_reason: Option<String>,
}

impl HermesProvenance {
    pub fn is_empty(&self) -> bool {
        self == &HermesProvenance::default()
    }
}

#[derive(Serialize)]
struct Frame<'a, P: Serialize> {
    jsonrpc: &'a str,
    method: &'a str,
    params: P,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PeerJoinedParams<'a> {
    room_id: &'a str,
    peer_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PeerLeftParams<'a> {
    room_id: &'a str,
    peer_id: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionContextParams<'a> {
    room_id: &'a str,
    cwd: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TurnStartedParams<'a> {
    room_id: &'a str,
    amux_turn_id: &'a str,
    peer_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<&'a str>,
    content: &'a serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    supersedes_turn_id: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TurnCompleteParams<'a> {
    room_id: &'a str,
    amux_turn_id: &'a str,
    stop_reason: &'a serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionBusyParams<'a> {
    room_id: &'a str,
    busy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    held_by: Option<&'a str>,
}

/// Inert, mux-owned lifecycle sibling for an agent-initiated JSON-RPC
/// request. The raw request remains the only actionable ACP frame and is
/// forwarded live to attached subscribers only; this notification carries
/// the request context that is safe to retain in replay logs for late
/// joiners and audit UIs.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AgentRequestOpenedParams<'a> {
    room_id: &'a str,
    request_id: &'a serde_json::Value,
    request_method: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_params: Option<&'a serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    amux_turn_id: Option<&'a str>,
}

/// Sibling of an agent-initiated request that the multiplexer broadcast
/// to every subscriber. Emitted as soon as the first subscriber reply
/// for a given agent request id is forwarded to the agent, so peers
/// that lost the race (or never replied at all) can clear the request
/// from their UI. Exactly one of `result` / `error` is populated and
/// echoes the winning reply verbatim.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AgentRequestResolvedParams<'a> {
    room_id: &'a str,
    request_id: &'a serde_json::Value,
    resolved_by: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<&'a serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a serde_json::Value>,
}

/// Intent broadcast emitted when *any* attached peer triggers
/// `amux/cancel_active_turn`. Distinct from `amux/turn_complete`, which
/// fires later when the agent actually returns the (possibly partial)
/// response. The pair lets peers distinguish "stop was clicked" from
/// "turn finished," and the `cancelledBy` / `originalDriver` fields
/// preserve the cross-peer attribution that JSON-RPC `$/cancel_request`
/// can't carry on its own.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TurnCancelledParams<'a> {
    room_id: &'a str,
    amux_turn_id: &'a str,
    cancelled_by: &'a str,
    original_driver: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ControlSubmittedParams<'a> {
    room_id: &'a str,
    kind: &'a str,
    mode: &'a str,
    peer_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    amux_turn_id: Option<&'a str>,
    text: &'a str,
}

pub struct ControlSubmitted<'a> {
    pub room_id: &'a str,
    pub kind: &'a str,
    pub mode: &'a str,
    pub peer_id: &'a str,
    pub peer_name: Option<&'a str>,
    pub role: Option<&'a str>,
    pub amux_turn_id: Option<AmuxTurnId>,
    pub text: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueueItemAddedParams<'a> {
    room_id: &'a str,
    queue_item_id: &'a str,
    peer_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<&'a str>,
    text: &'a str,
    status: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueueItemSubmittedParams<'a> {
    room_id: &'a str,
    queue_item_id: &'a str,
    amux_turn_id: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueueItemCompletedParams<'a> {
    room_id: &'a str,
    queue_item_id: &'a str,
    amux_turn_id: &'a str,
    stop_reason: &'a serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueueItemRemovedParams<'a> {
    room_id: &'a str,
    queue_item_id: &'a str,
    removed_by: &'a str,
    reason: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueueItemOrphanedParams<'a> {
    room_id: &'a str,
    queue_item_id: &'a str,
    peer_id: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReplayMarkerParams<'a> {
    room_id: &'a str,
    phase: &'a str,
    replay_order: &'a str,
    generation: u64,
    replay_boundary_seq: u64,
    frame_count: usize,
}

fn encode<P: Serialize>(method: &'static str, params: P) -> Vec<u8> {
    serde_json::to_vec(&Frame {
        jsonrpc: "2.0",
        method,
        params,
    })
    .expect("amux frame is always serializable")
}

pub fn peer_joined(
    room_id: &str,
    peer_id: &str,
    peer_name: Option<&str>,
    role: Option<&str>,
) -> Vec<u8> {
    encode(
        METHOD_PEER_JOINED,
        PeerJoinedParams {
            room_id,
            peer_id,
            peer_name,
            role,
        },
    )
}

pub fn peer_left(room_id: &str, peer_id: &str) -> Vec<u8> {
    encode(METHOD_PEER_LEFT, PeerLeftParams { room_id, peer_id })
}

pub fn session_context(room_id: &str, cwd: &str) -> Vec<u8> {
    encode(
        METHOD_SESSION_CONTEXT,
        SessionContextParams { room_id, cwd },
    )
}

pub fn turn_started(
    room_id: &str,
    amux_turn_id: AmuxTurnId,
    peer_id: &str,
    peer_name: Option<&str>,
    role: Option<&str>,
    content: &serde_json::Value,
    supersedes_turn_id: Option<AmuxTurnId>,
) -> Vec<u8> {
    let id = amux_turn_id.formatted();
    let supersedes = supersedes_turn_id.map(AmuxTurnId::formatted);
    encode(
        METHOD_TURN_STARTED,
        TurnStartedParams {
            room_id,
            amux_turn_id: &id,
            peer_id,
            peer_name,
            role,
            content,
            supersedes_turn_id: supersedes.as_deref(),
        },
    )
}

pub fn turn_complete(
    room_id: &str,
    amux_turn_id: AmuxTurnId,
    stop_reason: &serde_json::Value,
) -> Vec<u8> {
    let id = amux_turn_id.formatted();
    encode(
        METHOD_TURN_COMPLETE,
        TurnCompleteParams {
            room_id,
            amux_turn_id: &id,
            stop_reason,
        },
    )
}

pub fn session_busy(room_id: &str, busy: bool, held_by: Option<&str>) -> Vec<u8> {
    encode(
        METHOD_SESSION_BUSY,
        SessionBusyParams {
            room_id,
            busy,
            held_by,
        },
    )
}

pub fn turn_cancelled(
    room_id: &str,
    amux_turn_id: AmuxTurnId,
    cancelled_by: &str,
    original_driver: &str,
    reason: Option<&str>,
) -> Vec<u8> {
    let id = amux_turn_id.formatted();
    encode(
        METHOD_TURN_CANCELLED,
        TurnCancelledParams {
            room_id,
            amux_turn_id: &id,
            cancelled_by,
            original_driver,
            reason,
        },
    )
}

pub fn control_submitted(event: ControlSubmitted<'_>) -> Vec<u8> {
    let id = event.amux_turn_id.map(AmuxTurnId::formatted);
    encode(
        METHOD_CONTROL_SUBMITTED,
        ControlSubmittedParams {
            room_id: event.room_id,
            kind: event.kind,
            mode: event.mode,
            peer_id: event.peer_id,
            peer_name: event.peer_name,
            role: event.role,
            amux_turn_id: id.as_deref(),
            text: event.text,
        },
    )
}

pub fn queue_item_added(
    room_id: &str,
    queue_item_id: &str,
    peer_id: &str,
    peer_name: Option<&str>,
    role: Option<&str>,
    text: &str,
) -> Vec<u8> {
    encode(
        METHOD_QUEUE_ITEM_ADDED,
        QueueItemAddedParams {
            room_id,
            queue_item_id,
            peer_id,
            peer_name,
            role,
            text,
            status: "queued",
        },
    )
}

pub fn queue_item_submitted(
    room_id: &str,
    queue_item_id: &str,
    amux_turn_id: AmuxTurnId,
) -> Vec<u8> {
    let id = amux_turn_id.formatted();
    encode(
        METHOD_QUEUE_ITEM_SUBMITTED,
        QueueItemSubmittedParams {
            room_id,
            queue_item_id,
            amux_turn_id: &id,
        },
    )
}

pub fn queue_item_completed(
    room_id: &str,
    queue_item_id: &str,
    amux_turn_id: AmuxTurnId,
    stop_reason: &serde_json::Value,
) -> Vec<u8> {
    let id = amux_turn_id.formatted();
    encode(
        METHOD_QUEUE_ITEM_COMPLETED,
        QueueItemCompletedParams {
            room_id,
            queue_item_id,
            amux_turn_id: &id,
            stop_reason,
        },
    )
}

pub fn queue_item_removed(room_id: &str, queue_item_id: &str, removed_by: &str) -> Vec<u8> {
    encode(
        METHOD_QUEUE_ITEM_REMOVED,
        QueueItemRemovedParams {
            room_id,
            queue_item_id,
            removed_by,
            reason: "unqueued",
        },
    )
}

pub fn queue_item_orphaned(room_id: &str, queue_item_id: &str, peer_id: &str) -> Vec<u8> {
    encode(
        METHOD_QUEUE_ITEM_ORPHANED,
        QueueItemOrphanedParams {
            room_id,
            queue_item_id,
            peer_id,
        },
    )
}

pub fn replay_started(
    room_id: &str,
    phase: &str,
    replay_order: &str,
    generation: u64,
    replay_boundary_seq: u64,
    frame_count: usize,
) -> Vec<u8> {
    encode(
        METHOD_REPLAY_STARTED,
        ReplayMarkerParams {
            room_id,
            phase,
            replay_order,
            generation,
            replay_boundary_seq,
            frame_count,
        },
    )
}

pub fn replay_complete(
    room_id: &str,
    phase: &str,
    replay_order: &str,
    generation: u64,
    replay_boundary_seq: u64,
    frame_count: usize,
) -> Vec<u8> {
    encode(
        METHOD_REPLAY_COMPLETE,
        ReplayMarkerParams {
            room_id,
            phase,
            replay_order,
            generation,
            replay_boundary_seq,
            frame_count,
        },
    )
}

pub fn agent_request_opened(
    room_id: &str,
    request_id: &serde_json::Value,
    request_method: &str,
    request_params: Option<&serde_json::Value>,
    amux_turn_id: Option<AmuxTurnId>,
) -> Vec<u8> {
    let id = amux_turn_id.map(AmuxTurnId::formatted);
    encode(
        METHOD_AGENT_REQUEST_OPENED,
        AgentRequestOpenedParams {
            room_id,
            request_id,
            request_method,
            request_params,
            amux_turn_id: id.as_deref(),
        },
    )
}

/// Lifecycle frame emitted when a segment opens. The first segment of a
/// room emits this alone (no `amux/segment_ended` precedes it). Subsequent
/// segments emit `amux/segment_ended` for the closing segment first, then
/// this frame for the opening one. Recorded in the transcript so late
/// joiners on `historyPolicy: full_lineage` see the boundary.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SegmentStartedParams<'a> {
    room_id: &'a str,
    segment_id: SegmentId,
    #[serde(skip_serializing_if = "Option::is_none")]
    acp_session_id: Option<&'a str>,
    opened_at: &'a str,
    #[serde(skip_serializing_if = "HermesProvenance::is_empty")]
    provenance: &'a HermesProvenance,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SegmentEndedParams<'a> {
    room_id: &'a str,
    segment_id: SegmentId,
    closed_at: &'a str,
    end_reason: EndReason,
    #[serde(skip_serializing_if = "Option::is_none")]
    successor_segment_id: Option<SegmentId>,
}

pub fn segment_started(
    room_id: &str,
    segment_id: SegmentId,
    acp_session_id: Option<&str>,
    opened_at: &str,
    provenance: &HermesProvenance,
) -> Vec<u8> {
    encode(
        METHOD_SEGMENT_STARTED,
        SegmentStartedParams {
            room_id,
            segment_id,
            acp_session_id,
            opened_at,
            provenance,
        },
    )
}

pub fn segment_ended(
    room_id: &str,
    segment_id: SegmentId,
    closed_at: &str,
    end_reason: EndReason,
    successor_segment_id: Option<SegmentId>,
) -> Vec<u8> {
    encode(
        METHOD_SEGMENT_ENDED,
        SegmentEndedParams {
            room_id,
            segment_id,
            closed_at,
            end_reason,
            successor_segment_id,
        },
    )
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ContextCompactionStartedParams<'a> {
    room_id: &'a str,
    source: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    hermes_session_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    messages_before: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tokens_approx_before: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    focus: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ContextCompactionDoneParams<'a> {
    room_id: &'a str,
    source: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    hermes_session_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    messages_before: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    messages_after: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tokens_approx_after: Option<u64>,
    compression_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    successor_segment_id: Option<SegmentId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    previous_segment_id: Option<SegmentId>,
}

pub struct ContextCompactionStarted<'a> {
    pub room_id: &'a str,
    pub source: &'a str,
    pub hermes_session_id: Option<&'a str>,
    pub messages_before: Option<u64>,
    pub tokens_approx_before: Option<u64>,
    pub model: Option<&'a str>,
    pub focus: Option<&'a str>,
}

pub struct ContextCompactionDone<'a> {
    pub room_id: &'a str,
    pub source: &'a str,
    pub hermes_session_id: Option<&'a str>,
    pub messages_before: Option<u64>,
    pub messages_after: Option<u64>,
    pub tokens_approx_after: Option<u64>,
    pub compression_count: u64,
    pub previous_segment_id: Option<SegmentId>,
    pub successor_segment_id: Option<SegmentId>,
}

pub fn context_compaction_started(event: ContextCompactionStarted<'_>) -> Vec<u8> {
    encode(
        METHOD_CONTEXT_COMPACTION_STARTED,
        ContextCompactionStartedParams {
            room_id: event.room_id,
            source: event.source,
            hermes_session_id: event.hermes_session_id,
            messages_before: event.messages_before,
            tokens_approx_before: event.tokens_approx_before,
            model: event.model,
            focus: event.focus,
        },
    )
}

pub fn context_compaction_done(event: ContextCompactionDone<'_>) -> Vec<u8> {
    encode(
        METHOD_CONTEXT_COMPACTION_DONE,
        ContextCompactionDoneParams {
            room_id: event.room_id,
            source: event.source,
            hermes_session_id: event.hermes_session_id,
            messages_before: event.messages_before,
            messages_after: event.messages_after,
            tokens_approx_after: event.tokens_approx_after,
            compression_count: event.compression_count,
            successor_segment_id: event.successor_segment_id,
            previous_segment_id: event.previous_segment_id,
        },
    )
}

pub fn agent_request_resolved(
    room_id: &str,
    request_id: &serde_json::Value,
    resolved_by: &str,
    result: Option<&serde_json::Value>,
    error: Option<&serde_json::Value>,
) -> Vec<u8> {
    encode(
        METHOD_AGENT_REQUEST_RESOLVED,
        AgentRequestResolvedParams {
            room_id,
            request_id,
            resolved_by,
            result,
            error,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn parse(bytes: &[u8]) -> Value {
        serde_json::from_slice(bytes).expect("frame is JSON")
    }

    #[test]
    fn turn_id_format() {
        assert_eq!(AmuxTurnId(42).formatted(), "at-42");
        assert_eq!(AmuxTurnId(0).formatted(), "at-0");
    }

    #[test]
    fn peer_joined_includes_optional_fields() {
        let bytes = peer_joined("work", "phone-1", Some("phone"), Some("default"));
        let v = parse(&bytes);
        assert_eq!(v["jsonrpc"], json!("2.0"));
        assert_eq!(v["method"], json!("amux/peer_joined"));
        assert_eq!(v["params"]["roomId"], json!("work"));
        assert_eq!(v["params"]["peerId"], json!("phone-1"));
        assert_eq!(v["params"]["peerName"], json!("phone"));
        assert_eq!(v["params"]["role"], json!("default"));
    }

    #[test]
    fn peer_joined_omits_missing_optionals() {
        let bytes = peer_joined("work", "p1", None, None);
        let v = parse(&bytes);
        assert!(v["params"].get("peerName").is_none());
        assert!(v["params"].get("role").is_none());
    }

    #[test]
    fn peer_left_shape() {
        let v = parse(&peer_left("work", "p1"));
        assert_eq!(v["method"], json!("amux/peer_left"));
        assert_eq!(v["params"]["roomId"], json!("work"));
        assert_eq!(v["params"]["peerId"], json!("p1"));
        assert!(v.get("id").is_none());
    }

    #[test]
    fn turn_started_carries_content() {
        let content = json!([{"type": "text", "text": "hi"}]);
        let v = parse(&turn_started(
            "work",
            AmuxTurnId(7),
            "phone-1",
            Some("phone"),
            None,
            &content,
            None,
        ));
        assert_eq!(v["method"], json!("amux/turn_started"));
        assert_eq!(v["params"]["amuxTurnId"], json!("at-7"));
        assert_eq!(v["params"]["peerId"], json!("phone-1"));
        assert_eq!(v["params"]["peerName"], json!("phone"));
        assert!(v["params"].get("role").is_none());
        assert_eq!(v["params"]["content"], content);
    }

    #[test]
    fn turn_complete_shape() {
        let reason = json!("end_turn");
        let v = parse(&turn_complete("work", AmuxTurnId(7), &reason));
        assert_eq!(v["method"], json!("amux/turn_complete"));
        assert_eq!(v["params"]["amuxTurnId"], json!("at-7"));
        assert_eq!(v["params"]["stopReason"], json!("end_turn"));
    }

    #[test]
    fn session_busy_shape() {
        let v = parse(&session_busy("work", true, Some("desktop-1")));
        assert_eq!(v["method"], json!("amux/session_busy"));
        assert_eq!(v["params"]["roomId"], json!("work"));
        assert_eq!(v["params"]["busy"], json!(true));
        assert_eq!(v["params"]["heldBy"], json!("desktop-1"));

        let v = parse(&session_busy("work", false, None));
        assert!(v["params"].get("heldBy").is_none());
    }

    #[test]
    fn agent_request_opened_shape() {
        let req_id = json!(10001);
        let params = json!({
            "sessionId": "sess-mock",
            "options": [{"optionId": "allow_once"}],
        });
        let v = parse(&agent_request_opened(
            "work",
            &req_id,
            "session/request_permission",
            Some(&params),
            Some(AmuxTurnId(7)),
        ));
        assert_eq!(v["method"], json!("amux/agent_request_opened"));
        assert!(v.get("id").is_none());
        assert_eq!(v["params"]["roomId"], json!("work"));
        assert_eq!(v["params"]["requestId"], req_id);
        assert_eq!(
            v["params"]["requestMethod"],
            json!("session/request_permission")
        );
        assert_eq!(v["params"]["requestParams"], params);
        assert_eq!(v["params"]["amuxTurnId"], json!("at-7"));
    }

    #[test]
    fn agent_request_opened_omits_missing_optionals() {
        let req_id = json!("perm-7");
        let v = parse(&agent_request_opened(
            "work",
            &req_id,
            "session/request_permission",
            None,
            None,
        ));
        assert_eq!(v["params"]["requestId"], req_id);
        assert!(v["params"].get("requestParams").is_none());
        assert!(v["params"].get("amuxTurnId").is_none());
    }

    #[test]
    fn agent_request_resolved_with_result() {
        let req_id = json!(10001);
        let result = json!({"outcome": {"outcome": "selected", "optionId": "allow_once"}});
        let v = parse(&agent_request_resolved(
            "work",
            &req_id,
            "alice",
            Some(&result),
            None,
        ));
        assert_eq!(v["method"], json!("amux/agent_request_resolved"));
        assert_eq!(v["params"]["roomId"], json!("work"));
        assert_eq!(v["params"]["requestId"], req_id);
        assert_eq!(v["params"]["resolvedBy"], json!("alice"));
        assert_eq!(v["params"]["result"], result);
        assert!(v["params"].get("error").is_none());
    }

    #[test]
    fn agent_request_resolved_with_error() {
        let req_id = json!("perm-7");
        let err = json!({"code": -32603, "message": "rejected"});
        let v = parse(&agent_request_resolved(
            "work",
            &req_id,
            "bob",
            None,
            Some(&err),
        ));
        assert_eq!(v["params"]["requestId"], req_id);
        assert_eq!(v["params"]["error"], err);
        assert!(v["params"].get("result").is_none());
    }

    #[test]
    fn agent_request_resolved_omits_missing_result_and_error() {
        let req_id = json!("stale-perm");
        let v = parse(&agent_request_resolved(
            "work",
            &req_id,
            "mux:turn-ended",
            None,
            None,
        ));
        assert_eq!(v["params"]["requestId"], req_id);
        assert_eq!(v["params"]["resolvedBy"], json!("mux:turn-ended"));
        assert!(v["params"].get("result").is_none());
        assert!(v["params"].get("error").is_none());
    }
}
