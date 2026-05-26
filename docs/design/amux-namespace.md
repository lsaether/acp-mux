# amux-namespace multiplex metadata

**Status:** v0.1 spec.

## Principle

The multiplexer owns **transport mechanics** — id translation, fan-out,
subscriber lifecycle, subprocess management, and the minimum
turn-of-conversation serialization required to not corrupt a 1:1 agent under
N subscribers.

Clients own **ACP semantics** — assembling chunks into turns, rendering tool
calls, tracking plan state, and everything else that depends on what ACP
*means*.

Concretely, the multiplexer MUST NOT fabricate `session/*` notifications in
the ACP namespace. Its mux-native control plane remains `amux/*`: peer
presence, turn boundaries, busy state, queue lifecycle, and replay-safe
lifecycle metadata all go under `amux/*` with explicit payload.

Method responses to proxy-implemented methods (`session/attach`,
`session/detach`) are not fabrication: they answer requests addressed to the
proxy, not notifications pretending to come from the wrapped agent. Any future
proxy-fabricated ACP notification needs an explicit design note and a concrete
client/RFD trigger.

Agent-owned ACP frames flow byte-for-byte from agent to live clients; multiplex
facts flow through `amux/*`. Late-join replay is mux-owned state
reconstruction, not a second chance to answer old ACP requests: replayed frames
may gain `params._meta.amux` fields that describe when the mux originally
recorded the frame, and resolved agent-initiated requests replay through inert
`amux/*` lifecycle events rather than re-emitting stale actionable `session/*`
requests. Unresolved permission requests are the exception: `session/attach`
re-issues them after attach so the late client has a fresh actionable request
to answer. Clients receive distinguishable signals and demultiplex them.

Implementation rule: **the multiplexer parses JSON-RPC envelopes only, except
for mux-owned `amux/*` control methods and proxy-implemented attach/detach.**
Everything past `{id, method, params, result, error}` is `serde_json::Value`
unless the method is proxy-owned. Policy hooks (response caching, request
routing) key off the `method` string; agent-owned ACP payload contents remain
opaque passthrough. Active-turn steering/queueing is not inferred from ACP
`session/prompt` text. Clients use explicit `amux/steer_active_turn` or
`amux/queue_prompt` requests, whose small `params` payloads are parsed by the
mux control plane.

## Why a separate namespace

The multiplexer needs to publish facts that ACP itself doesn't model: which
subscriber initiated a turn, which subscribers are currently attached, when
a turn opened and closed, when a session is busy. Two ways to do this:

- **In-band:** synthesize ACP-namespace frames that encode the metadata.
- **Out-of-band:** publish under a distinct method namespace (`amux/*`).

Out-of-band wins for these reasons:

- **Frames are unambiguous.** A frame under `session/*` is something the
  agent emitted. A frame under `amux/*` is something the multiplexer
  emitted. Clients can't confuse them.
- **Attribution survives.** ACP frame shapes don't carry a "from which
  subscriber" field — it isn't in the spec. A custom namespace can
  include it explicitly.
- **No spec drift.** ACP is designed for 1:1 client/agent; real ACP
  servers don't emit (e.g.) `user_message_chunk` because the local
  client renders its own input. Synthesizing such frames in a multiplex
  context pretends the agent did something it didn't.
- **Forward compatible.** New multiplex facts add new `amux/*` methods
  without touching the ACP surface.

The cost: mux-native clients must demultiplex two channels. `amux/*` is a small
namespace and clients already need to handle unknown methods gracefully, so this
is cheap. Generic ACP clients that only understand the attach/detach foundation
can connect and receive re-issued pending permissions, but they will not receive
standard lifecycle notifications until a concrete RFD/client target exists.

## RFD #533 attach/detach foundation

amux implements the proxy-local attach/detach foundation inspired by RFD #533
over the existing mux state:

- `session/attach` is answered by the mux, never forwarded to the wrapped
  agent. It returns the effective ACP `sessionId`, caller `clientId`, effective
  `historyPolicy`, optional `history`, and amux-specific roster metadata under
  `result._meta.amux.connectedClients`. Mux-owned replay ordering is requested
  under `params._meta.amux.replayOrder` and echoed as
  `result._meta.amux.appliedReplayOrder`.
- `session/detach` is answered by the mux and then closes that WebSocket
  normally. Remaining peers receive `amux/peer_left`.

This is deliberately **not** full RFD #533 lifecycle interop yet. amux does not
emit proxy-owned `session/update` siblings for `prompt_received`,
`turn_complete`, `permission_resolved`, or `client_disconnected`; lifecycle
remains in `amux/*`. Add those siblings only when the accepted RFD/schema shape
and a real generic client both require them.

This support is intentionally conservative: amux does **not** inject
`agentCapabilities.sessionCapabilities.attach` into the upstream `initialize`
response. The wrapped agent's capabilities are passed through honestly; callers
that opt into the proxy facade discover it out of band or by knowing they are
connected to amux.

`historyPolicy` handling:

| Policy | Behavior |
|---|---|
| `full` | Returns the current broadcast replay log as `{ method, params }` history entries. |
| `none` | Omits history from the attach result. |
| `pending_only` | Returns unresolved actionable permission requests tracked by the mux. |
| `after_message` | Accepted as provisional RFD #533 syntax, but currently falls back to `full` when `afterMessageId` cannot be resolved. Durable ACP message IDs are not yet available consistently enough for precise slicing. |

`params._meta.amux.replayOrder` is optional and mux-owned:

| Order | Behavior |
|---|---|
| `chronological` (default) | Return `history` in the same order as the durable replay log. |
| `newest_turn_first` | Keep non-turn setup frames first, then return completed turn groups newest first. Frames inside each turn group remain chronological (`amux/turn_started`, agent updates, `amux/turn_complete`). |

The applied value is reported as `result._meta.amux.appliedReplayOrder`. This is
an attach-response history shape, not a URL/query replay mode and not a streamed
`amux/replay_started` / snapshot / `amux/replay_complete` marker protocol.

Unresolved `session/request_permission` requests are stored separately from the
inert replay log. After a successful `session/attach`, the mux re-issues those
raw request frames to the attaching subscriber so the UI can show an actionable
permission prompt. The stored request is cleared on first winning reply, agent
cancel, or stale-pending sweep. If a newly attached generic client replies after
another peer already won the first-writer-wins race, the late reply is dropped;
amux-aware clients learn the outcome via `amux/agent_request_resolved`.

## The `amux/*` namespace

### `amux/turn_started`

Broadcast to every subscriber (including the originator) when the
multiplexer forwards a `session/prompt` to the subprocess. Pairs with
`amux/turn_complete` as a bookend, and carries the prompt content so peer
clients can render it without a separate notification.

```json
{
  "jsonrpc": "2.0",
  "method": "amux/turn_started",
  "params": {
    "sessionId": "work",
    "amuxTurnId": "at-42",
    "peerId": "phone-1",
    "peerName": "phone",
    "role": "default",
    "content": [{"type": "text", "text": "..."}],
    "supersedesTurnId": "at-41"
  }
}
```

- `content` is the originator's `session/prompt` `prompt` array, mirrored
  verbatim — opaque to the multiplexer, byte-passthrough.
- `supersedesTurnId` is optional and present when a mux-owned replacement
  turn supersedes an earlier turn (currently hard steer).
- Originator branches on `peerId == self.peerId` to skip rendering. It
  already rendered locally.
- Emitted *before* forwarding the request to the subprocess, so peers see
  the prompt ahead of the agent's streamed response.

This event single-handedly carries everything needed to open a turn:
attribution, content, and a stable id. Clients route all ACP output between
`turn_started` and `turn_complete` into the named turn.

### `amux/turn_complete`

Broadcast to every subscriber when the active turn's `session/prompt`
response lands, or when the subprocess aborts the turn.

```json
{
  "jsonrpc": "2.0",
  "method": "amux/turn_complete",
  "params": {
    "sessionId": "work",
    "amuxTurnId": "at-42",
    "stopReason": "end_turn"
  }
}
```

`amuxTurnId` matches a prior `amux/turn_started`. Abnormal terminations
(subprocess crash mid-turn, etc.) surface as a distinguished `stopReason`
value rather than a separate event type.

### `amux/turn_cancelled`

Intent broadcast emitted when any attached peer triggers
`amux/cancel_active_turn` (see "Cancellation" below). Distinct from
`amux/turn_complete` — `turn_cancelled` fires immediately on the cancel
request (announce intent), `turn_complete` fires later when the agent
actually settles (turn finished, possibly with a partial response).

```json
{
  "jsonrpc": "2.0",
  "method": "amux/turn_cancelled",
  "params": {
    "sessionId": "work",
    "amuxTurnId": "at-42",
    "cancelledBy": "phone-1",
    "originalDriver": "desktop-1",
    "reason": "user clicked stop"
  }
}
```

- `cancelledBy` is the peer that issued `amux/cancel_active_turn`.
- `originalDriver` is the peer whose `session/prompt` started the turn.
- `reason` mirrors the `reason` field on the inbound notification when
  present; omitted otherwise.

The pair (`cancelled_by`, `original_driver`) preserves cross-peer
attribution that the JSON-RPC `$/cancel_request` method can't carry on
its own — `$/cancel_request` has only a `requestId`, no information
about who issued the cancel.

### `amux/peer_joined` / `amux/peer_left`

```json
{
  "jsonrpc": "2.0",
  "method": "amux/peer_joined",
  "params": {
    "sessionId": "work",
    "peerId": "phone-1",
    "peerName": "phone",
    "role": "default"
  }
}
```

```json
{
  "jsonrpc": "2.0",
  "method": "amux/peer_left",
  "params": {
    "sessionId": "work",
    "peerId": "phone-1"
  }
}
```

`peer_joined` is broadcast to every existing subscriber when a new
subscriber attaches. The newcomer's view of the existing roster comes from
the replay log (see below), which already contains `peer_joined` events for
every peer still in the session — no per-peer presence replay needed at
attach time.

### `amux/session_context`

Sent directly to each subscriber on attach with the mux-owned execution
context for the room. This is not an ACP session metadata claim: it identifies
the cwd inherited by the agent subprocess, which is the context used for
tools/terminal work even if a client connected from a different local cwd.

```json
{
  "jsonrpc": "2.0",
  "method": "amux/session_context",
  "params": {
    "sessionId": "work",
    "cwd": "/home/volt/Code/acp-mux"
  }
}
```

- Emitted once per attach to the attaching subscriber.
- Clients can use it for chrome/status UI that should reflect the agent's
  actual working directory rather than the local client's launch cwd.

### `amux/session_busy`

Broadcast when a plain ACP `session/prompt` is rejected because another
turn is already in flight. The rejected subscriber also gets a JSON-RPC
error response with code `-32001`. Active-turn control does not rely on
slash-command text inside `session/prompt`: clients use explicit
`amux/steer_active_turn`, `amux/queue_prompt`, or `amux/unqueue_prompt`
requests instead. Accepted amux control requests are mux-owned and
replay-visible: active steer cancels and replaces the active turn, idle steer
submits immediately as the next prompt, queue stores a queue item and submits
it after the active turn settles, and unqueue removes a pending queue item.

```json
{
  "jsonrpc": "2.0",
  "method": "amux/session_busy",
  "params": {
    "sessionId": "work",
    "busy": true,
    "heldBy": "desktop-1"
  }
}
```

### `amux/steer_active_turn`

Subscriber → proxy JSON-RPC request. Mux-owned steer/send primitive: if a turn
is active, any attached peer can interrupt it, then start a replacement turn
that carries prompt-injected steering context. If no turn is active, the steer
text is submitted immediately as the next normal prompt with `mode: "prompt"`.
This is intentionally distinct from future native/soft steer support, where a
compatible agent may inject guidance into the existing active turn without
cancelling it. Only one hard steer can be pending for an active turn; a second
`amux/steer_active_turn` before the replacement pops is rejected with
`-32002` and message `"a hard steer is already pending for this turn"`.

```json
{
  "jsonrpc": "2.0",
  "id": 17,
  "method": "amux/steer_active_turn",
  "params": {
    "sessionId": "sess-mock",
    "text": "focus on the migration path"
  }
}
```

### `amux/queue_prompt`

Subscriber → proxy JSON-RPC request. Mux-owned queue/send primitive: stores text
as the next turn when a turn is active, or submits it immediately when idle.
The queue item is visible to every peer and replayed to late joiners. The
pending queue is capped at six `amux/queue_prompt` items; the seventh pending
item receives JSON-RPC `-32003` with message `"queue full"`.

```json
{
  "jsonrpc": "2.0",
  "id": 18,
  "method": "amux/queue_prompt",
  "params": {
    "sessionId": "sess-mock",
    "text": "after that, update the docs"
  }
}
```

### `amux/unqueue_prompt`

Subscriber → proxy JSON-RPC request. Removes a still-pending mux queue item by
its `queueItemId`. The item is not submitted after removal, and the removal is
visible to every peer and replayed to late joiners.

```json
{
  "jsonrpc": "2.0",
  "id": 19,
  "method": "amux/unqueue_prompt",
  "params": {
    "queueItemId": "q-1"
  }
}
```

Control validation semantics:

- `amux/steer_active_turn` accepts both states. If a prompt is active, it
  performs hard cancel-and-replace. If the mux is idle, it submits the steer
  text immediately as a normal next prompt, returns `mode: "prompt"` plus
  `status: "submitted"`, and does not emit cancellation or queue lifecycle
  events.
- `amux/queue_prompt` accepts both states. If a prompt is active, the item is
  held until that turn settles; if the mux is idle, the item is submitted
  immediately and the acknowledgement reports `status: "submitted"`. At most
  six mux queue items may be pending.
- `amux/unqueue_prompt` removes only pending queue items. Items already popped
  into an active turn are no longer removable through this control path.
- `params.text` is the preferred payload. A text-only ACP-style
  `params.prompt` array is also accepted for clients that already model
  composer content as blocks. Empty text, non-text blocks, or non-string
  `sessionId` values receive JSON-RPC `-32602`.
- `params.sessionId` is optional when the mux already knows the active or
  canonical ACP session id. When present, it must match that known session id.
- Accepted controls never reuse the generic busy prompt path. The JSON-RPC
  response returns only to the requester; mux-owned lifecycle notifications
  broadcast and replay to every peer.

Hard-steer acceptance flow:

1. Broadcast `amux/control_submitted { kind: "steer", mode: "hard", ... }`.
2. Broadcast `amux/turn_cancelled { reason: "hard_steer" }` for immediate
   peer-visible intent.
3. Send ACP-native `session/cancel { sessionId }` southbound and wait for the
   active prompt response / `amux/turn_complete` settlement.
4. Submit a replacement `session/prompt` with a new `amuxTurnId` and
   `supersedesTurnId` on `amux/turn_started`. Because Hermes does not yet
   consume mux `_meta` for this, the replacement prompt includes a small
   plaintext context block naming the superseded turn, original prompt text
   when available, and the new steering instruction.

Idle steer acceptance flow:

1. Broadcast `amux/control_submitted { kind: "steer", mode: "prompt", ... }`
   with the new `amuxTurnId`.
2. Submit the steer text directly as a downstream `session/prompt`; do not send
   `session/cancel`, do not emit `amux/turn_cancelled`, and do not create
   public queue-item lifecycle events.
3. Broadcast `amux/turn_started` / `amux/turn_complete` for the submitted
   prompt like any other mux-owned turn. The control response is an ack only;
   the agent result is represented by normal turn lifecycle/update traffic.

Queue acceptance flow:

1. Broadcast `amux/queue_item_added { queueItemId, peerId, text, status:
   "queued" }`.
2. If the mux is idle, immediately submit the item as a downstream
   `session/prompt`, allocate a new `amuxTurnId`, broadcast
   `amux/turn_started` and `amux/queue_item_submitted`, and return an ack with
   `status: "submitted"`.
3. If another turn is active, leave the item pending and return an ack with
   `status: "queued"`; when the active turn settles, pop the next queue item,
   submit it as a normal downstream `session/prompt`, allocate a new
   `amuxTurnId`, and broadcast `amux/turn_started` plus
   `amux/queue_item_submitted`.
4. When that queued prompt completes, broadcast `amux/turn_complete` and
   `amux/queue_item_completed`.

Queue removal/disconnect flow:

1. `amux/unqueue_prompt { queueItemId }` removes the matching still-pending
   queue item, returns `{ queueItemId, status: "removed" }`, and broadcasts
   `amux/queue_item_removed`.
2. If a peer disconnects while it still owns pending queue items, the items
   persist. The mux broadcasts `amux/queue_item_orphaned` for each affected
   public queue item so clients can render the owner as detached.

### `amux/control_submitted`

Replay-safe accepted-control intent. Currently emitted for steer controls:
`mode: "hard"` when replacing an active turn and `mode: "prompt"` when idle
steer submits as the next prompt.

```json
{
  "jsonrpc": "2.0",
  "method": "amux/control_submitted",
  "params": {
    "sessionId": "work",
    "kind": "steer",
    "mode": "hard",
    "peerId": "phone-1",
    "amuxTurnId": "at-42",
    "text": "focus on the migration path"
  }
}
```

### `amux/queue_item_added` / `amux/queue_item_submitted` / `amux/queue_item_completed` / `amux/queue_item_removed` / `amux/queue_item_orphaned`

Replay-safe mux-owned queue lifecycle. `queue_item_added` records accepted
pending work, `queue_item_submitted` ties it to the real turn id, and
`queue_item_completed` records terminal settlement. `queue_item_removed`
records explicit unqueue, and `queue_item_orphaned` records that the owning
peer detached while the item stayed queued.

```json
{
  "jsonrpc": "2.0",
  "method": "amux/queue_item_added",
  "params": {
    "sessionId": "work",
    "queueItemId": "q-1",
    "peerId": "phone-1",
    "text": "after that, update the docs",
    "status": "queued"
  }
}
```

```json
{
  "jsonrpc": "2.0",
  "method": "amux/queue_item_submitted",
  "params": {
    "sessionId": "work",
    "queueItemId": "q-1",
    "amuxTurnId": "at-43"
  }
}
```

```json
{
  "jsonrpc": "2.0",
  "method": "amux/queue_item_completed",
  "params": {
    "sessionId": "work",
    "queueItemId": "q-1",
    "amuxTurnId": "at-43",
    "stopReason": "end_turn"
  }
}
```

```json
{
  "jsonrpc": "2.0",
  "method": "amux/queue_item_removed",
  "params": {
    "sessionId": "work",
    "queueItemId": "q-1",
    "removedBy": "desktop-1",
    "reason": "unqueued"
  }
}
```

```json
{
  "jsonrpc": "2.0",
  "method": "amux/queue_item_orphaned",
  "params": {
    "sessionId": "work",
    "queueItemId": "q-2",
    "peerId": "phone-1"
  }
}
```

### `amux/agent_request_opened`

Broadcast when the agent emits an agent-initiated JSON-RPC request (for
example `session/request_permission`). This frame is **not** actionable:
it has no JSON-RPC `id` at the top level, clients must not answer it, and
the raw ACP request remains the only frame that can be replied to. The
purpose is durable replay/audit context for late joiners, which must not
receive stale actionable requests after the agent has already moved on.

```json
{
  "jsonrpc": "2.0",
  "method": "amux/agent_request_opened",
  "params": {
    "sessionId": "work",
    "requestId": 10001,
    "requestMethod": "session/request_permission",
    "requestParams": {
      "sessionId": "sess-mock",
      "toolCall": { "toolCallId": "tool-1", "title": "demo_tool" },
      "options": [{ "optionId": "allow_once", "name": "Allow once" }]
    },
    "amuxTurnId": "at-42"
  }
}
```

Fields:

- `requestId` is the original agent-owned JSON-RPC request id. It is data
  here, not a top-level reply target.
- `requestMethod` / `requestParams` mirror the original request envelope
  enough for a replaying client to explain what was asked.
- `amuxTurnId` is present when the request happened during an active mux
  turn and matches the surrounding `amux/turn_started` /
  `amux/turn_complete` pair.

Live subscribers receive `amux/agent_request_opened` plus the raw ACP
request. Late joiners replay only the `amux/*` lifecycle (`opened` then
`resolved`) and never receive the raw, already-stale request.

### `amux/agent_request_resolved`

Broadcast when an agent-initiated request (e.g. `session/request_permission`)
that the mux fanned out live to every subscriber receives its first reply.
The first reply is forwarded to the agent; later replies for the same
id are dropped. This notification lets peers that lost the race (or
never replied) dismiss the request from their UI. Together with the prior
`amux/agent_request_opened`, it is also the replay-safe lifecycle for late
joiners.

```json
{
  "jsonrpc": "2.0",
  "method": "amux/agent_request_resolved",
  "params": {
    "sessionId": "work",
    "requestId": 10001,
    "resolvedBy": "phone-1",
    "result": {
      "outcome": { "outcome": "selected", "optionId": "allow_once" }
    }
  }
}
```

For peer replies, exactly one of `result` or `error` is populated and
echoes the winning reply verbatim. For `session/request_permission` the
body is derived entirely from `params.options[]` of the original request
(which every peer already saw), so no new information leaks. If/when
other agent → client request types start flowing through this path with
sensitive response bodies, the design should be revisited.

#### Turn-end cleanup variant

When `session/prompt` completes with an agent-initiated request still
unresolved (no subscriber ever replied — the agent likely fired its own
deadline and proceeded without writing a response), the mux emits one
`amux/agent_request_resolved` per stale id immediately before
`amux/turn_complete`:

```json
{
  "jsonrpc": "2.0",
  "method": "amux/agent_request_resolved",
  "params": {
    "sessionId": "work",
    "requestId": 10001,
    "resolvedBy": "mux:turn-ended"
  }
}
```

Clients can distinguish a peer-resolved request (`resolvedBy` is a peer
id) from a turn-end cleanup (`resolvedBy` is the literal string
`"mux:turn-ended"` and `result` and `error` are omitted). After the
sweep the mux drops any late subscriber reply for the same id at the
first-reply-wins gate, so the agent never sees a duplicate or stale
answer.

## `_meta.amux` request trace metadata

amux can optionally use ACP `_meta` passthrough to carry mux-owned trace
fields on subscriber → agent requests. This is disabled by default to
preserve byte-passthrough behavior for request payloads; start amux with
`--meta-propagate` to enable it.

When enabled, amux merges fields into `params._meta.amux` after request id
translation and before forwarding the request to the agent:

```json
{
  "jsonrpc": "2.0",
  "id": 17,
  "method": "session/prompt",
  "params": {
    "sessionId": "sess-mock",
    "prompt": [{"type": "text", "text": "hi"}],
    "_meta": {
      "amux": {
        "peerId": "phone-1",
        "peerName": "phone",
        "role": "driver",
        "muxId": 17,
        "amuxTurnId": "at-42"
      }
    }
  }
}
```

Fields:

- `peerId` — subscriber identity from the WebSocket query or mux fallback.
- `peerName` — optional display name when known.
- `role` — optional subscriber role when known.
- `muxId` — the translated JSON-RPC request id visible to the agent.
- `amuxTurnId` — only present on forwarded `session/prompt` requests, and
  matches the accompanying `amux/turn_started` bookend.

Existing `params._meta` keys and unrelated namespaces are preserved. If
`_meta.amux` already exists as an object, amux merges into it and leaves
unknown keys intact. The `--meta-propagate` request-trace feature does not
rewrite arbitrary agent → subscriber frames.

## `_meta.amux` `session/list` response decoration

Independently of `--meta-propagate`, amux decorates `session/list` responses
when an upstream entry corresponds to a live mux room. The mux learns that
mapping from successful `session/new` and `session/load` responses: the
upstream ACP `sessionId` becomes the lookup key, while the WebSocket
`?session=` value remains the mux/proxy room id.

For each `result.sessions[]` entry whose `sessionId` matches live mux state,
amux merges fields under `sessions[i]._meta.amux`:

```json
{
  "sessionId": "sess-mock",
  "_meta": {
    "amux": {
      "proxySessionId": "work",
      "subscriberCount": 2,
      "drivingSubscriber": "desktop-1"
    }
  }
}
```

Fields:

- `proxySessionId` — mux room/session id from the attach URL.
- `subscriberCount` — number of currently attached subscribers.
- `drivingSubscriber` — optional peer id that last sent a substantive
  non-`initialize` request.

Entries with no live mux match are left unchanged. Existing `sessions[i]._meta`
keys and unknown `sessions[i]._meta.amux` keys are preserved; amux-owned fields
above are refreshed with current live state.

## HTTP control-plane session discovery

`GET /acp/sessions?cwd=<optional>` is the cold-start complement to the
WebSocket `session/list` path. It is intended for dashboards that need to show
persisted upstream ACP sessions before the user chooses a mux room to attach or
resume.

Handling:

1. amux spawns a transient `--agent-cmd` subprocess.
2. amux sends `initialize` to put the agent in a normal ACP-ready state.
3. amux sends `session/list`, forwarding the optional `cwd` query parameter as
   JSON-RPC params `{ "cwd": "..." }`.
4. amux returns the agent's `result` JSON as the HTTP response body, e.g.
   `{ "sessions": [...], "nextCursor": "..." }` if the agent includes a cursor.
5. amux tears down the transient subprocess and does **not** register a live mux
   session or attach any peer.

The endpoint returns `503` when `--agent-cmd` is not configured. Agent spawn,
protocol, timeout, or JSON-RPC errors return `502` with a small JSON error body.

## Late-join / replay log

The multiplexer maintains a per-session event log: every broadcast-tier
frame it has sent — its own `amux/*` notifications, including inert
`amux/agent_request_opened` request context, and the agent's `session/update`
notifications.
Per-subscriber frames (responses to a specific subscriber's requests, raw
actionable agent-initiated requests such as `session/request_permission`) are
NOT logged; they're directed by definition and may already be resolved by the
time a late joiner arrives. Unresolved permission requests are tracked in a
separate pending-permission store for `session/attach` reissue, not in the
general replay log.

When a new subscriber attaches:

1. The multiplexer replays the entire log to the newcomer in original
   order. Live frame payloads are stored unchanged, then replay delivery
   augments JSON-RPC frames with mux-owned provenance under
   `params._meta.amux`:

   ```json
   {
     "jsonrpc": "2.0",
     "method": "session/update",
     "params": {
       "sessionId": "work",
       "update": {"sessionUpdate": "agent_message_chunk"},
       "_meta": {
         "amux": {
           "recordedAt": "2026-05-23T20:15:42.123456789Z",
           "replaySeq": 17
         }
       }
     }
   }
   ```

   Existing `params._meta` keys are preserved; `amux` is the mux-owned
   subnamespace. Non-JSON/raw frames replay unchanged.
2. Live events that arrive during replay are queued for the newcomer and
   flushed after replay completes, preserving global ordering.
3. Only then does the newcomer enter the live broadcast set.

If the newcomer then sends RFD #533-inspired `session/attach`, the response's
`historyPolicy` shapes the returned `history` field over the same durable
broadcast-tier data. `params._meta.amux.replayOrder` can additionally shape
that response history; `"newest_turn_first"` reverses completed turn groups
while preserving chronological order within each turn. `historyPolicy:
"after_message"` is accepted for forward compatibility but falls back to
`"full"` until stable ACP message IDs let amux resolve `afterMessageId`
precisely. When unresolved permission requests exist, attach also re-issues
those raw request frames after the attach response so the late client can answer
them.

The legacy WebSocket attach path still auto-replays chronological history before
the client can send `session/attach`. Attach-aware clients should pick one
history source: consume the legacy pre-attach stream and request
`historyPolicy: "none"`, or ignore pre-attach replay frames and use the
`session/attach` response history (for example when they need
`newest_turn_first`). This avoids rendering duplicate history while preserving
backward compatibility for clients that do not call `session/attach`.

This gives newcomers a complete reconstruction of session state — every
peer that joined or left, every completed turn (with its prompt content via
the `turn_started` bookend), any agent-initiated request context via inert
`amux/agent_request_opened` / `amux/agent_request_resolved` pairs, and any
in-flight turn's already-streamed chunks.

**v0.1 ships unbounded replay.** The log grows for the life of the session.
Storage pressure on long-running sessions is acceptable for early use and
deferred — see *Future work*.

`--replay-turns` exists from day one as the future-bounding hook + disable
switch:

| Value | Behavior |
|---|---|
| `0` | Disable replay; new subscribers see only live events. |
| `unbounded` (default) | Full session log, no eviction. |
| positive `N` | *(future)* Keep only the last N completed turns. Eviction is bookend-driven — the multiplexer uses its own `turn_started` / `turn_complete` markers to decide boundaries, never introspecting ACP payloads. |

The bounded variant is wire-compatible with unbounded — clients see fewer
historical events but the protocol shape is identical.

## Cancellation

`$/cancel_request` is implemented from the ACP request-cancellation RFD and
the upstream unstable schema; it is not part of the stable ACP v1 schema as of
ACP `v0.13.3`. `amux/cancel_active_turn` is an acp-mux extension layered on
top of stable ACP `session/cancel`.

amux keeps two cancellation paths distinct:

- `$/cancel_request` is strict request-id cancellation in the sender's
  JSON-RPC id space.
- `amux/cancel_active_turn` is a mux extension for cross-peer "stop the
  current turn" and resolves to ACP-native `session/cancel` for the
  active upstream ACP `sessionId`.

### `$/cancel_request` — strict

**Subscriber → agent.** When a subscriber sends `$/cancel_request`,
amux treats the `requestId` as belonging to that subscriber's local JSON-RPC
id space. It looks up `(peer_id, original_id)`, rewrites `requestId` to the
corresponding `mux_id`, and forwards the notification to the agent.
If no matching entry exists (the id was already resolved, or the
subscriber is trying to cancel another subscriber's request), the
cancel is dropped silently. Cross-peer "stop the active turn" uses
`amux/cancel_active_turn` instead.

**Agent → subscribers.** When the agent emits `$/cancel_request` for
an in-flight broadcast-tier agent-initiated request (for example
`session/request_permission`; ACP client-tool requests are blocked by default
before entering this lifecycle), amux marks the `agent_pending` entry
`Consumed` so late subscriber replies are swallowed by the existing
first-writer-wins gate. The cancellation is then broadcast to every subscriber,
and an
`amux/agent_request_resolved { resolvedBy: "agent:cancelled" }` is
emitted alongside for amux-aware clients.

### `amux/cancel_active_turn` — extension

```json
{
  "jsonrpc": "2.0",
  "method": "amux/cancel_active_turn",
  "params": { "reason": "user clicked stop" }
}
```

Notification, no response. `reason` is optional. `sessionId` is
implicit (the WS is session-scoped).

When amux receives this from any attached peer (including the
driver):

1. If no active turn, drop silently.
2. Look up the active turn's driver and ACP `sessionId`.
3. Broadcast `amux/turn_cancelled { cancelledBy, originalDriver, reason? }`.
4. Forward `session/cancel { sessionId }` to the agent.
5. The existing path takes over: agent eventually responds (cancelled
   or partial), `route_agent_response` clears the active turn,
   `amux/turn_complete` fires with whatever `stopReason` the agent
   returned.

`amux/turn_cancelled` is the *intent* event ("stop was clicked").
`amux/turn_complete` is the *settlement* event ("turn finished").
They are separate events because the agent may take some time
between receiving cancellation and producing a final response.

### Agent compliance

amux forwards cancellation primitives honestly. If the agent does not
honor `session/cancel` for active turns and finishes normally,
subscribers see the regular response. This is documented behavior, not
a bug in amux — the proxy stays plumbing.

## Peer identity

**Resolution: client-supplied `peer_id` with multiplexer fallback.**

- `?peer_id=<stable-id>` on the WS query → client claims this identity.
  Stable across reconnects: the multiplexer treats two connections with
  the same `peer_id` as the same logical subscriber for the purpose of
  `peer_joined` / `peer_left` accounting.
- `?peer_id` omitted → multiplexer mints `sub-<n>` for this connection
  only. Reconnect produces a new id.
- Collision: a new connection arrives with a `peer_id` already in the
  live subscriber set → reject with WS close code **4409** (`peer_id
  conflict`).

`?peer_name=<display>` is independently optional and defaults to `peer_id`.
Display-only; carries no routing semantics.

Clients are expected to default `peer_id` to something stable per host+user
(e.g. `${hostname}-${user}`) or per-device (e.g. a stored UUID) so
reconnects feel continuous without the human picking an identity by hand.

## Client integration model

A client consuming this protocol needs to:

1. On `amux/turn_started`, open a Turn record attributed to `peerId`,
   with `content` as the prompt. Render `content` unless
   `peerId == self.peerId` — that subscriber already rendered the prompt
   locally before sending it.
2. Route subsequent ACP `session/update` content into the open Turn:
   `agent_message_chunk` → response text, `agent_thought_chunk` →
   thinking buffer, `tool_call` / `tool_call_update` → tool call records,
   `plan` → plan, `usage_update` → context-window indicator.
3. On live raw `session/request_permission`, show a reply affordance. Late
   joiners that use RFD #533 `session/attach` may receive unresolved
   permissions re-issued after the attach response; those re-issued raw
   requests are also actionable. On replayed `amux/agent_request_opened`, render
   only inert context: no top-level JSON-RPC `id`, no response should be sent.
   Use `amux/agent_request_resolved` to dismiss/annotate the request outcome.
4. On `amux/turn_complete`, close the Turn with `stopReason`.

The `amux/*` bookends remove the need for client-side heuristics like
"close the previous turn when a new prompt arrives" or "dedup my own
prompt's echo from the amux fan-out" — those become protocol-side
problems the `amux/*` namespace solves explicitly via `peerId` and
explicit boundaries.

## Tradeoffs

- **Vanilla ACP clients pointed at the multiplexer still lose amux-native
  detail.** The attach/detach foundation lets generic clients connect and may
  re-issue pending permissions, but prompt/turn/permission/disconnect lifecycle
  remains in `amux/*`. Pretending every multiplex fact is an agent fact would
  still produce subtle confusion downstream; explicit partial support beats
  silent corruption.
- **Two-channel mental model on clients.** Clients demultiplex ACP from
  `amux/*` rather than treating everything as `session/update`.
- **Multiplexer models turn-of-conversation boundaries explicitly.** It
  already needs to (to serialize `session/prompt`); the `amux/*`
  namespace just makes the boundary visible to clients. Not new state,
  just published state.
- **Unbounded replay log in v0.1.** Long-running sessions accumulate
  memory. Acceptable for early use; bounded mode is the planned fix.

## Out of scope for this design

- Per-subscriber auth (transport-level trust only).
- Cross-host federation.
- Subprocess crash recovery as a distinct event type — fold abnormal
  terminations into `stopReason` on `turn_complete`.

## Future work

- **Bounded replay log.** Ship the `--replay-turns N` (positive integer)
  eviction logic once storage growth becomes a real concern on
  long-running sessions. The flag is already in v0.1; only the eviction
  code is deferred. Eviction is bookend-driven (uses the multiplexer's
  own `turn_started` / `turn_complete` markers); never introspects ACP
  payloads.
- **Replay buffer compaction.** Once bounding lands, consider compacting
  retained turns (e.g., drop intermediate `tool_call_update` frames when
  a later frame supersedes them). Always a byte-level operation on raw
  frames — no ACP semantic introspection.
- **`session/list` response decoration.** Request-side `_meta.amux` trace
  propagation is opt-in and local to the request path. A separate change
  can decorate returned `sessions[]` entries with live mux state (for
  example subscriber count and mux/proxy session id) once the response path
  has registry-wide session lookup.
- **Persistent log on disk.** v0.1 logs live in process memory and die
  with the session. A future revision could persist the log to disk for
  crash recovery or for resume after multiplexer restart.
- **Replay-on-resume protocol negotiation.** If the multiplexer ever
  needs to support resuming an older session that has been torn down,
  the resume protocol (likely leveraging `session/load`) lives there,
  not in the broadcast log.
