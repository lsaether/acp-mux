# Bridge-namespace multiplex metadata

**Status:** v0.1 spec.

## Principle

The multiplexer owns **transport mechanics** — id translation, fan-out,
subscriber lifecycle, subprocess management, and the minimum
turn-of-conversation serialization required to not corrupt a 1:1 agent under
N subscribers.

Clients own **ACP semantics** — assembling chunks into turns, rendering tool
calls, tracking plan state, and everything else that depends on what ACP
*means*.

Concretely, the multiplexer MUST NOT fabricate frames in the ACP namespace.
Any out-of-band signal it needs to publish — peer presence, turn boundaries,
busy state, late-join history — goes under `bridge/*` with explicit payload.

ACP frames flow byte-for-byte from agent to clients; multiplex facts flow
through their own namespace. Clients receive two distinguishable channels and
demultiplex them.

Implementation rule: **the multiplexer parses JSON-RPC envelopes only.**
Everything past `{id, method, params, result, error}` is `serde_json::Value`.
Policy hooks (turn serialization, response caching) key off the `method`
string; payload contents are opaque passthrough.

## Why a separate namespace

The multiplexer needs to publish facts that ACP itself doesn't model: which
subscriber initiated a turn, which subscribers are currently attached, when
a turn opened and closed, when a session is busy. Two ways to do this:

- **In-band:** synthesize ACP-namespace frames that encode the metadata.
- **Out-of-band:** publish under a distinct method namespace (`bridge/*`).

Out-of-band wins for these reasons:

- **Frames are unambiguous.** A frame under `session/*` is something the
  agent emitted. A frame under `bridge/*` is something the multiplexer
  emitted. Clients can't confuse them.
- **Attribution survives.** ACP frame shapes don't carry a "from which
  subscriber" field — it isn't in the spec. A custom namespace can
  include it explicitly.
- **No spec drift.** ACP is designed for 1:1 client/agent; real ACP
  servers don't emit (e.g.) `user_message_chunk` because the local
  client renders its own input. Synthesizing such frames in a multiplex
  context pretends the agent did something it didn't.
- **Forward compatible.** New multiplex facts add new `bridge/*` methods
  without touching the ACP surface.

The cost: clients must demultiplex two channels. `bridge/*` is a small
namespace and clients already need to handle unknown methods gracefully, so
this is cheap.

## The `bridge/*` namespace

### `bridge/turn_started`

Broadcast to every subscriber (including the originator) when the
multiplexer forwards a `session/prompt` to the subprocess. Pairs with
`bridge/turn_complete` as a bookend, and carries the prompt content so peer
clients can render it without a separate notification.

```json
{
  "jsonrpc": "2.0",
  "method": "bridge/turn_started",
  "params": {
    "sessionId": "work",
    "bridgeTurnId": "bt-42",
    "peerId": "phone-1",
    "peerName": "phone",
    "role": "default",
    "content": [{"type": "text", "text": "..."}]
  }
}
```

- `content` is the originator's `session/prompt` `prompt` array, mirrored
  verbatim — opaque to the multiplexer, byte-passthrough.
- Originator branches on `peerId == self.peerId` to skip rendering. It
  already rendered locally.
- Emitted *before* forwarding the request to the subprocess, so peers see
  the prompt ahead of the agent's streamed response.

This event single-handedly carries everything needed to open a turn:
attribution, content, and a stable id. Clients route all ACP output between
`turn_started` and `turn_complete` into the named turn.

### `bridge/turn_complete`

Broadcast to every subscriber when the active turn's `session/prompt`
response lands, or when the subprocess aborts the turn.

```json
{
  "jsonrpc": "2.0",
  "method": "bridge/turn_complete",
  "params": {
    "sessionId": "work",
    "bridgeTurnId": "bt-42",
    "stopReason": "end_turn"
  }
}
```

`bridgeTurnId` matches a prior `bridge/turn_started`. Abnormal terminations
(subprocess crash mid-turn, etc.) surface as a distinguished `stopReason`
value rather than a separate event type.

### `bridge/peer_joined` / `bridge/peer_left`

```json
{
  "jsonrpc": "2.0",
  "method": "bridge/peer_joined",
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
  "method": "bridge/peer_left",
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

### `bridge/session_busy`

Broadcast when a `session/prompt` is rejected because another turn is
already in flight. The rejected subscriber also gets a JSON-RPC error
response with code `-32001`.

```json
{
  "jsonrpc": "2.0",
  "method": "bridge/session_busy",
  "params": {
    "sessionId": "work",
    "busy": true,
    "heldBy": "desktop-1"
  }
}
```

## Late-join / replay log

The multiplexer maintains a per-session event log: every broadcast-tier
frame it has sent — its own `bridge/*` notifications and the agent's
`session/update` notifications. Per-subscriber frames (responses to a
specific subscriber's requests, agent-initiated `permission/request`) are
NOT logged; they're directed by definition.

When a new subscriber attaches:

1. The multiplexer replays the entire log to the newcomer in original
   order, verbatim. Frame contents are unchanged from when first sent.
2. Live events that arrive during replay are queued for the newcomer and
   flushed after replay completes, preserving global ordering.
3. Only then does the newcomer enter the live broadcast set.

This gives newcomers a complete reconstruction of session state — every
peer that joined or left, every completed turn (with its prompt content via
the `turn_started` bookend), and any in-flight turn's already-streamed
chunks.

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

1. On `bridge/turn_started`, open a Turn record attributed to `peerId`,
   with `content` as the prompt. Render `content` unless
   `peerId == self.peerId` — that subscriber already rendered the prompt
   locally before sending it.
2. Route subsequent ACP `session/update` content into the open Turn:
   `agent_message_chunk` → response text, `agent_thought_chunk` →
   thinking buffer, `tool_call` / `tool_call_update` → tool call records,
   `plan` → plan, `usage_update` → context-window indicator.
3. On `bridge/turn_complete`, close the Turn with `stopReason`.

The `bridge/*` bookends remove the need for client-side heuristics like
"close the previous turn when a new prompt arrives" or "dedup my own
prompt's echo from the bridge fan-out" — those become protocol-side
problems the `bridge/*` namespace solves explicitly via `peerId` and
explicit boundaries.

## Tradeoffs

- **Vanilla ACP clients pointed at the multiplexer lose peer visibility.**
  They won't understand `bridge/turn_started`, so peer prompts become
  invisible to them. This is the correct cost: pretending multiplex facts
  were agent facts produces subtle confusion downstream. Explicit
  ignorance beats silent corruption.
- **Two-channel mental model on clients.** Clients demultiplex ACP from
  `bridge/*` rather than treating everything as `session/update`.
- **Multiplexer models turn-of-conversation boundaries explicitly.** It
  already needs to (to serialize `session/prompt`); the `bridge/*`
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
- **Persistent log on disk.** v0.1 logs live in process memory and die
  with the session. A future revision could persist the log to disk for
  crash recovery or for resume after multiplexer restart.
- **Replay-on-resume protocol negotiation.** If the multiplexer ever
  needs to support resuming an older session that has been torn down,
  the resume protocol (likely leveraging `session/load`) lives there,
  not in the broadcast log.
