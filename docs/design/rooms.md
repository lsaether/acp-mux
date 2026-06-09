# Rooms

This document specifies the v0.2 **rooms** abstraction. It complements
`docs/design/rooms-namespace.md`, which spells out the `rooms/*` namespace
itself, and assumes familiarity with the per-session actor model.

## Why

`acp-mux` exposes one mux room as one mirrored upstream ACP agent process.
Multiple clients can attach to that room, see one continuous transcript,
and take turns driving the same upstream conversation. ACP agents can also
change the canonical `sessionId` over time — most explicitly through
`session/load`, and in some implementations by emitting notifications with
a different `params.sessionId` than the mux currently considers active.

Rooms make the multiplexer the place that tracks those provider-neutral
session-id segments and serves a continuous transcript across the
boundaries. Provider-specific metadata is not part of the room state
machine; it remains opaque payload data for clients that understand it.

## Vocabulary

- **Room** — a persistent multiplexer-level container. Stable id, one
  subprocess, one actor task, one outbound channel pair. URL-addressable
  via `?room=<id>`. `?session=` is accepted as a deprecated alias during
  the v0.2 transition.
- **Segment** — an interval during which exactly one canonical ACP
  `sessionId` is in force. The first segment opens when the agent's
  response to `session/new` or `session/load` captures the canonical id.
  Subsequent segments open on `session/load` or when the mux observes a
  different ACP `sessionId` in an agent notification.
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
3. Segment rotation emits exactly two frames in order: `rooms/segment_ended`
   for the closing segment, then `rooms/segment_started` for the opening
   one. The first segment of a room emits only `rooms/segment_started`.
4. Mid-turn rotation does not tear down the active turn. The rotation
   frames interleave between agent chunks. `rooms/turn_complete` fires
   normally when the agent finishes. Clients render this as one turn that
   crossed a segment boundary.
5. Frames recorded before any canonical session id arrives carry
   `SegmentId(0)` (a sentinel). They surface in current-segment replay
   alongside the active segment so the bootstrap context (`rooms/peer_joined`
   for early subscribers) is preserved.

## Wire shapes

### `rooms/segment_started`

Emitted once on initial open and once per rotation. The first segment of a
room emits this alone (no `rooms/segment_ended` precedes it).

```jsonc
{
  "jsonrpc": "2.0",
  "method": "rooms/segment_started",
  "params": {
    "roomId": "<room>",
    "segmentId": "seg-3",
    "acpSessionId": "<acp id or null>",
    "openedAt": "2026-05-27T19:00:00.000000000Z"
  }
}
```

### `rooms/segment_ended`

Emitted as the closing bookend on rotation. Carries the closing segment id;
`successorSegmentId` points at the opening one so clients can pair the
bookends without state-tracking.

```jsonc
{
  "jsonrpc": "2.0",
  "method": "rooms/segment_ended",
  "params": {
    "roomId": "<room>",
    "segmentId": "seg-2",
    "closedAt": "2026-05-27T19:00:00.000000000Z",
    "endReason": "acp_session_id_changed",
    "successorSegmentId": "seg-3"
  }
}
```

`endReason` values:

- `session_load` — client called `session/load`, swapping the canonical ACP
  session id.
- `acp_session_id_changed` — the agent emitted a notification with a
  different ACP `sessionId` than the active segment carries.

Both bookend frames flow through the existing `broadcast()` path so they
pick up the same `_meta.rooms { recordedAt, replaySeq }` envelope every
other frame uses. The transcript records `rooms/segment_ended` while the
closing segment is still active (it lands in the closing segment); the mux
then rotates `active_segment_id` and broadcasts `rooms/segment_started`
(which lands in the opening segment). Late joiners replaying frames by
`segment_id` can slice the transcript cleanly.

## Detection: `RoomInner::detect_segment_signal_from_agent_notification`

Single bottleneck. Every agent notification is peeked for an observable
`params.sessionId`. Rotation is warranted iff the observed `sessionId`
differs from the active segment's canonical ACP id. Provider-specific
metadata is not interpreted by the mux; it remains payload data for
clients that understand it.

## `historyPolicy`

`session/attach` accepts an extended `historyPolicy` enum:

- `full` (default) — frames from the active segment only, plus any
  pre-segment bootstrap frames tagged with `SegmentId(0)`, plus any
  `rooms/turn_started` / `rooms/turn_complete` / `rooms/turn_cancelled`
  bookend from a prior segment whose `roomsTurnId` brackets at least one
  frame in the active segment or matches the currently active turn. The
  lifecycle-frame carry exists so a mid-turn segment rotation doesn't
  leave clients staring at an unmatched `turn_complete`. Agent chunks
  from prior segments are still excluded — those belong to
  `full_lineage`.
- `full_lineage` — every frame across every segment, in `replaySeq`
  order. The view a TUI wants when rendering history across session-id
  rotations.
- `pending_only`, `none`, `after_message` — unchanged from v0.1.

`replayOrder: newest_turn_first` interacts with `full_lineage` by
reversing turn order across all segments while preserving chronology
*within* each turn. Segment order is not reversed independently — that
would interleave old and new session content confusingly.

## Snapshot

`session/attach` result includes a lineage summary so even `full`-mode
clients can see that earlier segments exist:

```jsonc
"result": {
  // …
  "_meta": {
    "rooms": {
      "snapshot": {
        // …
        "activeSegmentId": "seg-3",
        "segments": [
          { "id": "seg-1", "acpSessionId": "...", "openedAt": "...",
            "closedAt": "...", "endReason": "session_load" },
          { "id": "seg-2", ... },
          { "id": "seg-3", ... }
        ]
      }
    }
  }
}
```

## Mid-turn rotation

The active turn is keyed by `active_turn_mux_id` and `roomsTurnId`, neither
of which is tied to a segment. When `detect_segment_signal_*` fires
mid-turn the rotation frames interleave between agent chunks and
`rooms/turn_complete` fires normally on the agent's natural settlement.
Transcript ordering inside that turn is:

```
rooms/turn_started (old segment)
agent chunks (old segment)
rooms/segment_ended (old segment)
rooms/segment_started (new segment)
agent chunks (new segment)
rooms/turn_complete (new segment)
```

Clients render this as one turn that crossed a segment. The `Turn`
correlator on the client side does not need to special-case it.

## Queued prompts across segments

On any rotation where the canonical ACP id observably moves, queued
prompts are retargeted to the new ACP id.

## Persistence

The room transcript is in-memory by default. With `--replay-store <DIR>`,
broadcast-tier frames are also appended to one JSONL file per room. On
restart, the mux rehydrates those persisted broadcast frames so late joiners
can recover the transcript via `historyPolicy: full_lineage`.

Persistence has a narrow scope:

- It persists mux broadcast history, not the upstream agent's internal
  conversation state.
- It does not persist in-flight agent requests or unresolved permissions.
- The current store is append-only and unbounded. Bounded eviction remains a
  follow-up.

### Known limitation: cross-restart segment fidelity

Restart rehydrates the broadcast *frames* but not the *segment lineage* layered
on top of them. The rooms layer comes back with no `segments`, no
`activeSegmentId`, and a reset `replayGeneration`, and the core canonical
session id is not restored. So after a restart only `historyPolicy:
full_lineage` (the whole transcript) is correct; the segment-aware views —
current-segment `full`, the attach `snapshot` lineage, `replayGeneration`, and
the resolved `sessionId` — are wrong until new agent activity re-establishes
them.

This bites long-lived rooms that span multiple segments across a restart.
Example: a team keeps a shared coding room open for days, during which the
agent compacts / `session/load`s several times (many segments), and the mux
host is redeployed nightly. When a teammate's client reconnects the next
morning and asks for `historyPolicy: full` — "just the current segment, not the
whole multi-day lineage" — it gets a near-empty `full` view and a stale session
id, forcing every client to fall back to replaying the entire `full_lineage`
history to see the current conversation. Until reconstruction lands, use
`full_lineage` for cross-restart recovery (and `--emit-segment-frames=true`, the
default, so the segment bookends are at least persisted for a future rebuild).

Reconstructing segment state and the canonical session id from the persisted
`rooms/segment_*` frames on restart is a tracked follow-up.

The natural future seam is a SQLite layer keyed on
`(room_id, segment_id, replay_seq)` if JSONL stops being enough.

## Feature flag

`--emit-segment-frames` (default `true`) gates emission of
`rooms/segment_started` and `rooms/segment_ended`. The internal state
machine rotates regardless; the flag exists only to preserve
byte-equivalence with v0.1.x for clients that haven't picked up the new
frame methods yet.
