# Rooms

This document specifies the v0.2 **rooms** abstraction. It complements
`docs/design/amux-namespace.md`, which spells out the `amux/*` namespace
itself, and assumes familiarity with the per-session actor model.

## Why

Today `acp-mux` exposes one mux session = one ACP session id = one
subprocess. When hermes performs context compaction it internally
rotates its `hermesSessionId` while keeping the wire-level ACP
`sessionId` stable. From every client's perspective that rotation is
invisible: there's no concept of "this conversation spans multiple agent
session ids" and no replay primitive that carries history across the
boundary.

Rooms make the multiplexer the place that tracks the lineage and serves
a continuous transcript across compaction.

## Vocabulary

- **Room** — a persistent multiplexer-level container. Stable id, one
  subprocess, one actor task, one outbound channel pair. URL-addressable
  via `?room=<id>`. `?session=` is accepted as a deprecated alias during
  the v0.2 transition.
- **Segment** — an interval during which exactly one canonical ACP
  `sessionId` is in force. The first segment opens when the agent's
  response to `session/new` or `session/load` captures the canonical id.
  Subsequent segments open on `session/load` (client-initiated) or on
  hermes compression (`_meta.hermes` rotation, or heuristic-detected
  ACP `sessionId` change).
- **Transcript** — the room's append-only sequence of broadcast-tier
  frames plus segment lifecycle markers. Same in-memory storage as the
  v0.1 `replay_log` `VecDeque<ReplayEntry>`, with `segment_id` added to
  each entry.

## Invariants

1. Exactly one segment is *active* at any moment for a live room; closed
   segments are immutable.
2. `replaySeq` is one global monotonic counter spanning all segments.
   Per-segment slicing is recoverable via `Segment.opened_replay_seq` /
   `Segment.closed_replay_seq`.
3. Segment rotation emits exactly two frames in order: `amux/segment_ended`
   for the closing segment, then `amux/segment_started` for the opening
   one. The first segment of a room emits only `amux/segment_started`.
4. Heuristic-detected and `_meta.hermes`-confirmed segments share one
   data type. Provenance arriving late backfills `Segment.provenance` in
   place — no extra frame, no rotation.
5. Mid-turn rotation does not tear down the active turn. The rotation
   frames interleave between agent chunks. `amux/turn_complete` fires
   normally when the agent finishes. Clients render this as one turn
   that crossed compaction.
6. Frames recorded before any canonical session id arrives carry
   `SegmentId(0)` (a sentinel). They surface in current-segment
   replay alongside the active segment so the bootstrap context
   (`amux/peer_joined` for early subscribers) is preserved.

## Wire shapes

### `amux/segment_started`

Emitted once on initial open and once per rotation. The first segment
of a room emits this alone (no `amux/segment_ended` precedes it).

```jsonc
{
  "jsonrpc": "2.0",
  "method": "amux/segment_started",
  "params": {
    "roomId": "<room>",
    "segmentId": "seg-3",
    "acpSessionId": "<acp id or null>",
    "openedAt": "2026-05-27T19:00:00.000000000Z",
    "provenance": {
      "hermesSessionId": "...",
      "compressionDepth": 2,
      "lastMode": "session_split",
      "lastReason": "automatic_threshold"
      // any subset of HermesProvenance; omitted when empty
    }
  }
}
```

### `amux/segment_ended`

Emitted as the closing bookend on rotation. Carries the closing
segment id; `successorSegmentId` points at the opening one so clients
can pair the bookends without state-tracking.

```jsonc
{
  "jsonrpc": "2.0",
  "method": "amux/segment_ended",
  "params": {
    "roomId": "<room>",
    "segmentId": "seg-2",
    "closedAt": "2026-05-27T19:00:00.000000000Z",
    "endReason": "hermes_compression",
    "successorSegmentId": "seg-3"
  }
}
```

`endReason` values:

- `session_load` — client called `session/load`, swapping the
  canonical ACP session id.
- `hermes_compression` — `_meta.hermes` reported a session-split
  compression: same ACP id, different hermes id.
- `acp_session_id_changed` — heuristic: the agent emitted a frame with
  a different ACP `sessionId` than the active segment carries, and no
  `_meta.hermes` was attached.

Both bookend frames flow through the existing `broadcast()` path so they
pick up the same `_meta.amux { recordedAt, replaySeq }` envelope every
other frame uses. The transcript records `amux/segment_ended` while the
closing segment is still active (it lands in the closing segment); the
mux then rotates `active_segment_id` and broadcasts `amux/segment_started`
(which lands in the opening segment). Late joiners replaying frames by
`segment_id` can slice the transcript cleanly.

## Detection: `RoomInner::detect_segment_signal_from_agent_notification`

Single bottleneck. Every agent notification is peeked for an observable
`sessionId` and `_meta.hermes`. Rotation is warranted iff *any* of:

1. Observed `acpSessionId != active.acp_session_id`.
2. Observed `_meta.hermes.sessionProvenance.hermesSessionId !=
   active.provenance.hermes_session_id` (both must be `Some`).
3. Observed `_meta.hermes.compaction.lastMode == "session_split"` after
   the active segment's provenance has not yet recorded a hermes id
   (first-time compaction observation).

Otherwise the metadata is merged into the active segment in place (no
rotation, no frame). Rotation reason precedence: hermes signals →
`HermesCompression`; bare ACP id change → `AcpSessionIdChanged`;
explicit `session/load` handler → `SessionLoad`.

## `historyPolicy`

`session/attach` accepts an extended `historyPolicy` enum:

- `full` (default) — frames from the active segment only, plus any
  pre-segment bootstrap frames tagged with `SegmentId(0)`, plus any
  `amux/turn_started` / `amux/turn_complete` / `amux/turn_cancelled`
  bookend from a prior segment whose `amuxTurnId` brackets at least
  one frame in the active segment or matches the currently active
  turn. The lifecycle-frame carry exists so a hermes-compaction
  rotation that splits a turn across segments doesn't leave clients
  staring at an unmatched `turn_complete`. Agent chunks from prior
  segments are still excluded — those belong to `full_lineage`.
- `full_lineage` — every frame across every segment, in `replaySeq`
  order. The view a TUI wants when rendering history that strings
  along compaction.
- `pending_only`, `none`, `after_message` — unchanged from v0.1.

`replayOrder: newest_turn_first` interacts with `full_lineage` by
reversing turn order across all segments while preserving chronology
*within* each turn. Segment order is not reversed independently —
that would interleave pre- and post-compaction content confusingly.

## Snapshot

`session/attach` result includes a lineage summary so even
`full`-mode clients can see that earlier segments exist:

```jsonc
"result": {
  // …
  "_meta": {
    "amux": {
      "snapshot": {
        // …
        "activeSegmentId": "seg-3",
        "segments": [
          { "id": "seg-1", "acpSessionId": "...", "openedAt": "...",
            "closedAt": "...", "endReason": "hermes_compression",
            "provenance": { "compressionDepth": 0, ... } },
          { "id": "seg-2", ... },
          { "id": "seg-3", ... }
        ]
      }
    }
  }
}
```

## Mid-turn rotation

The active turn is keyed by `active_turn_mux_id` and `amuxTurnId`,
neither of which is tied to a segment. When `detect_segment_signal_*`
fires mid-turn the rotation frames interleave between agent chunks and
`amux/turn_complete` fires normally on the agent's natural settlement.
Transcript ordering inside that turn is:

```
amux/turn_started (old segment)
agent chunks (old segment)
amux/segment_ended (old segment)
amux/segment_started (new segment)
agent chunks (new segment)
amux/turn_complete (new segment)
```

Clients render this as one turn that crossed compaction. The `Turn`
correlator on the client side does not need to special-case it.

## Queued prompts across segments

On any rotation where the canonical ACP id observably moves, queued
prompts are retargeted to the new ACP id. When only `hermesSessionId`
rotates (ACP id stable), no rewrite is needed — the queued prompts
already point at the right ACP head.

## Persistence (deferred)

Out of scope for v0.2. The natural seam is
`RoomInner::{ replay_log, segments }`: a SQLite layer keyed on
`(room_id, segment_id, replay_seq)` would persist transcripts across
mux restarts without changing the in-memory shape. Tracked for v0.3+.

## Feature flag

`--emit-segment-frames` (default `true`) gates emission of
`amux/segment_started` and `amux/segment_ended`. The internal state
machine rotates regardless; the flag exists only to preserve
byte-equivalence with v0.1.x for clients that haven't picked up the
new frame methods yet.
