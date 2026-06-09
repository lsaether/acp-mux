# `rooms/*` namespace

**Status:** v0.2 design surface.

This document describes the optional Rooms collaboration layer. It is not the generic ACP mux contract: raw ACP passthrough, id translation, subprocess routing, safe client-tool defaults, and basic replay belong to the core mux even when a client ignores every `rooms/*` frame.

`acp-mux` mirrors one upstream ACP agent into a multi-client room. The upstream agent owns ACP frames. The Rooms layer owns collaboration facts. Those Rooms-owned facts live under the `rooms/*` namespace so clients can tell the two channels apart.

```text
session/*, fs/*, terminal/*, ...  agent-owned ACP frames
rooms/*                           mux-owned room / replay / control frames
```

## Boundary

The generic mux core is intentionally provider-neutral:

- parse JSON-RPC envelopes: `id`, `method`, `params`, `result`, `error`;
- key mux policy from method names and mux-owned control payloads;
- pass agent payloads and provider `_meta` through opaquely;
- do not parse provider stderr/logs;
- do not interpret provider-private metadata to drive room lifecycle;
- rotate room segments only on provider-neutral signals: `session/load` or an observable ACP `params.sessionId` change.

The mux MUST NOT fabricate agent-owned `session/*` notifications. If a frame says `method: "session/update"`, it came from the agent. If the Rooms layer needs to say something about peers, turns, replay, queueing, or segment lineage, it emits a `rooms/*` frame.

Clients that only need the generic mux can treat `rooms/*` as extension metadata or request no history/replay where appropriate. Clients that need multiplayer UX should consume this namespace deliberately instead of inferring room state from provider-private payloads.

Proxy-local methods such as `session/attach` and `session/detach` are the exception: clients address those requests to the mux, and the mux answers them. They are not forwarded to the wrapped agent and are not pretending to be agent notifications.

## Names and IDs

- **roomId** — stable mux-level collaboration id from `?room=`.
- **peerId** — caller-supplied subscriber id, unique within a room.
- **roomsTurnId** — mux turn id formatted as `at-<n>`.
- **queueItemId** — mux queue item id, currently formatted as `aq-<n>`.
- **segmentId** — mux segment id formatted as `seg-<n>`.
- **acpSessionId** — upstream ACP `sessionId`, when known.

All `rooms/*` payload fields are camelCase.

## Why a separate namespace

ACP is a 1:1 client/agent protocol. A mux room needs extra facts that ACP itself does not model:

- which peer opened a turn;
- which peers are attached;
- whether a turn is busy;
- which queued item was submitted or removed;
- whether an old agent request is replay context or still actionable;
- when an upstream ACP session id changes inside the same mirrored room.

Keeping these as `rooms/*` frames gives clients a clean rule:

- render ACP frames as agent conversation;
- use `rooms/*` frames for room UI, replay bookkeeping, and controls.

## Transport replay vs `session/attach`

There are two bootstrap paths:

1. **Legacy WebSocket replay** — on connect, the mux sends the broadcast replay log before live frames.
2. **Attach-aware replay** — connect with `replay=skip`, then call `session/attach` and use the returned `history` or streamed replay markers as the bootstrap source.

Attach-aware clients SHOULD prefer the second path so all bootstrap state comes from one request/response.

## Proxy-local `session/attach`

`session/attach` is answered by the mux. It returns the effective room view and optional history.

Request shape:

```jsonc
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "session/attach",
  "params": {
    "sessionId": "<current ACP session id, if the client knows it>",
    "historyPolicy": "full_lineage",
    "_meta": {
      "rooms": {
        "replayOrder": "chronological",
        "historyDelivery": "response"
      }
    }
  }
}
```

Response shape:

```jsonc
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "sessionId": "<effective ACP session id or null>",
    "clientId": "<peerId>",
    "historyPolicy": "full_lineage",
    "history": [ /* optional frames, depending on policy/delivery */ ],
    "_meta": {
      "rooms": {
        "connectedClients": [ /* roster */ ],
        "appliedReplayOrder": "chronological",
        "appliedHistoryDelivery": "response",
        "snapshot": {
          "roomId": "<room>",
          "activeSegmentId": "seg-2",
          "segments": [ /* lineage summary */ ]
        }
      }
    }
  }
}
```

Supported `historyPolicy` values:

| Policy | Behavior |
|---|---|
| `full` | Current segment history plus pre-segment bootstrap frames and cross-segment turn bookends needed to bracket an active/current-segment turn. |
| `full_lineage` | Every segment's broadcast frames in global `replaySeq` order. Use this for normal full transcript restore across `session/load` or ACP session-id rotations. |
| `none` | No history. Useful for status-only attaches. |
| `pending_only` | Only unresolved permission/request state; not a transcript restore path. |
| `after_message` | Accepted as provisional syntax, currently falls back to `full` until stable ACP message ids are available end-to-end. |

Supported `params._meta.rooms.replayOrder` values:

| Order | Behavior |
|---|---|
| `chronological` | Replay frames in durable `replaySeq` order. |
| `newest_turn_first` | Keep setup/context frames first, then return completed turn groups newest-first while preserving frame order inside each turn. |

Supported `params._meta.rooms.historyDelivery` values:

| Delivery | Behavior |
|---|---|
| `response` | Include `history` directly in the attach result. |
| `stream` | Return snapshot metadata immediately, then stream history through `rooms/replay_started` and `rooms/replay_complete` markers. |

## Proxy-local `session/detach`

`session/detach` is answered by the mux and then the WebSocket closes normally. Remaining peers receive `rooms/peer_left`. The mux does not fabricate ACP `session/update` disconnect siblings.

## Broadcast notifications

### `rooms/session_context`

Sent to an attaching peer with the local process context inherited by the upstream agent.

```json
{"jsonrpc":"2.0","method":"rooms/session_context","params":{"roomId":"work","cwd":"/repo"}}
```

### `rooms/peer_joined` / `rooms/peer_left`

Presence notifications.

```json
{"jsonrpc":"2.0","method":"rooms/peer_joined","params":{"roomId":"work","peerId":"phone","peerName":"Phone","role":"mobile"}}
```

```json
{"jsonrpc":"2.0","method":"rooms/peer_left","params":{"roomId":"work","peerId":"phone"}}
```

### `rooms/turn_started`

Broadcast before the mux forwards a `session/prompt` to the agent.

```jsonc
{
  "jsonrpc": "2.0",
  "method": "rooms/turn_started",
  "params": {
    "roomId": "work",
    "roomsTurnId": "at-42",
    "peerId": "desktop",
    "peerName": "Desktop",
    "role": "primary",
    "content": [{"type":"text","text":"..."}],
    "supersedesTurnId": "at-41"
  }
}
```

`content` is the originator's `session/prompt.params.prompt` value, mirrored verbatim. `supersedesTurnId` is present for replacement turns created by hard steer.

### `rooms/turn_complete`

Broadcast when the active `session/prompt` response lands.

```json
{"jsonrpc":"2.0","method":"rooms/turn_complete","params":{"roomId":"work","roomsTurnId":"at-42","stopReason":"end_turn"}}
```

### `rooms/turn_cancelled`

Intent broadcast emitted immediately when a peer uses `rooms/cancel_active_turn` or when hard steer cancels/supersedes an active turn. The later `rooms/turn_complete` still marks actual settlement.

```json
{"jsonrpc":"2.0","method":"rooms/turn_cancelled","params":{"roomId":"work","roomsTurnId":"at-42","cancelledBy":"phone","originalDriver":"desktop","reason":"user clicked stop"}}
```

### `rooms/session_busy`

Broadcast when an ordinary `session/prompt` arrives while another turn is active and is rejected with JSON-RPC `-32001`.

```json
{"jsonrpc":"2.0","method":"rooms/session_busy","params":{"roomId":"work","busy":true,"heldBy":"desktop"}}
```

### `rooms/control_submitted`

Replay-safe intent event for accepted mux controls such as hard steer or immediate idle steer.

```json
{"jsonrpc":"2.0","method":"rooms/control_submitted","params":{"roomId":"work","kind":"steer","mode":"replace_active","peerId":"phone","text":"try a shorter answer","roomsTurnId":"at-42"}}
```

### Queue lifecycle

`rooms/queue_prompt` creates queue state owned by the mux. Queue state is visible through lifecycle notifications:

| Method | Meaning |
|---|---|
| `rooms/queue_item_added` | A pending item was accepted. |
| `rooms/queue_item_submitted` | A pending item became an actual `session/prompt`. |
| `rooms/queue_item_completed` | The submitted queued turn settled. |
| `rooms/queue_item_removed` | A still-pending item was removed via `rooms/unqueue_prompt`. |
| `rooms/queue_item_orphaned` | The submitting peer detached before the item was submitted; the item remains in queue but no longer has a live owner. |

Representative shape:

```json
{"jsonrpc":"2.0","method":"rooms/queue_item_added","params":{"roomId":"work","queueItemId":"aq-3","peerId":"phone","text":"next, write tests"}}
```

### Agent-request lifecycle

Agent-initiated requests such as `session/request_permission` are live actionable frames. The mux records inert lifecycle siblings so replay clients can understand what happened without re-answering stale requests.

| Method | Meaning |
|---|---|
| `rooms/agent_request_opened` | Replay-safe context for an agent-initiated request. |
| `rooms/agent_request_resolved` | The request was consumed by a peer reply, agent cancellation, or mux turn-end cleanup. |

```jsonc
{
  "jsonrpc": "2.0",
  "method": "rooms/agent_request_opened",
  "params": {
    "roomId": "work",
    "requestId": 99,
    "requestMethod": "session/request_permission",
    "requestParams": { /* original params */ },
    "roomsTurnId": "at-42"
  }
}
```

```jsonc
{
  "jsonrpc": "2.0",
  "method": "rooms/agent_request_resolved",
  "params": {
    "roomId": "work",
    "requestId": 99,
    "resolvedBy": "phone",
    "result": { /* winning reply result, if any */ }
  }
}
```

Resolved-by sentinels include:

- a peer id — first subscriber reply won;
- `agent:cancelled` — upstream cancelled the request;
- `mux:turn-ended` — mux cleaned up stale pending state after turn settlement.

Unresolved permission requests are stored separately and re-issued after `session/attach` so late joiners can answer a fresh actionable request. If another peer wins first, the late reply is dropped.

### Replay markers

When attach history uses streamed delivery, the stream is bracketed with replay markers.

```json
{"jsonrpc":"2.0","method":"rooms/replay_started","params":{"roomId":"work","phase":"attach_history","replayOrder":"chronological","generation":3,"replayBoundarySeq":120,"frameCount":42}}
```

```json
{"jsonrpc":"2.0","method":"rooms/replay_complete","params":{"roomId":"work","phase":"attach_history","replayOrder":"chronological","generation":3,"replayBoundarySeq":120,"frameCount":42}}
```

### Segment lifecycle

Segments describe ACP session-id lineage inside one mux room.

`rooms/segment_started` opens a segment:

```json
{"jsonrpc":"2.0","method":"rooms/segment_started","params":{"roomId":"work","segmentId":"seg-2","acpSessionId":"sess-child","openedAt":"2026-05-27T19:00:00Z"}}
```

`rooms/segment_ended` closes one:

```json
{"jsonrpc":"2.0","method":"rooms/segment_ended","params":{"roomId":"work","segmentId":"seg-1","closedAt":"2026-05-27T19:00:00Z","endReason":"session_load","successorSegmentId":"seg-2"}}
```

Supported `endReason` values:

- `session_load` — client called `session/load` and the canonical ACP session id changed.
- `acp_session_id_changed` — the agent emitted a notification with a different `params.sessionId` than the active segment.

`endReason` is best-effort diagnostic metadata, not a strict priority contract: when both signals coincide (e.g. an agent emits loaded-session `session/update`s before its `session/load` response), the segment is labeled by whichever signal the mux observes first, which can be `acp_session_id_changed` even for a load-initiated rotation.

Provider-specific metadata is never a segment-rotation signal. If an agent emits provider metadata, it remains opaque payload data for clients that care.

## Subscriber control requests

These requests are addressed to the mux, not the agent.

### `rooms/cancel_active_turn`

Any peer can cancel the active turn. The mux broadcasts `rooms/turn_cancelled`, sends ACP-native `session/cancel { sessionId }` to the agent, and waits for normal turn settlement.

```json
{"jsonrpc":"2.0","id":10,"method":"rooms/cancel_active_turn","params":{"reason":"stop"}}
```

### `rooms/steer_active_turn`

When a turn is active, hard steer cancels/supersedes it and starts a replacement prompt after settlement. When idle, the steer text is submitted immediately as the next prompt.

```json
{"jsonrpc":"2.0","id":11,"method":"rooms/steer_active_turn","params":{"text":"make it concise"}}
```

A second hard steer while one is pending is rejected with JSON-RPC `-32002`.

### `rooms/queue_prompt`

Queues text behind the active turn, or submits it immediately if idle. The queue is capped at six pending items; full queue returns JSON-RPC `-32003`.

```json
{"jsonrpc":"2.0","id":12,"method":"rooms/queue_prompt","params":{"text":"after that, add tests"}}
```

### `rooms/unqueue_prompt`

Removes a still-pending queued item.

```json
{"jsonrpc":"2.0","id":13,"method":"rooms/unqueue_prompt","params":{"queueItemId":"aq-3"}}
```

## Replay metadata

Broadcast-tier frames may gain mux metadata under `params._meta.rooms` when replayed or persisted:

```jsonc
{
  "_meta": {
    "rooms": {
      "recordedAt": "2026-05-27T19:00:00.000Z",
      "replaySeq": 42,
      "segmentId": "seg-2"
    }
  }
}
```

This metadata describes mux recording/replay, not agent semantics. Live agent payload metadata is preserved; mux metadata is additive and namespaced.

## Client rules

Clients SHOULD:

1. Treat ACP frames as agent conversation and `rooms/*` frames as room/control metadata.
2. Use `roomId` for mux state and `sessionId` only for upstream ACP payloads.
3. Use `roomsTurnId` to bracket turns across streamed agent chunks.
4. Prefer `session/attach` with `replay=skip` for reconnect/bootstrap.
5. Request `historyPolicy: "full_lineage"` when rendering a full room transcript across segment rotations.
6. Treat replayed `rooms/agent_request_opened` as inert context; only live or re-issued raw `session/request_permission` frames are actionable.
7. Tolerate unknown `rooms/*` methods and unknown fields.
