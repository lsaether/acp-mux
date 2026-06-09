# acp-mux

`acp-mux` is the provider-neutral core ACP multiplexer.

It runs one stdio ACP agent process per mux id and exposes that process to one
or more WebSocket subscribers. It only understands JSON-RPC envelopes, ACP
method names needed for mux correctness, and proxy-local attach/detach methods.
It does not implement Rooms collaboration UX.

## What It Does

For each `?mux=<id>`, `acp-mux`:

1. Spawns one child process from `--agent-cmd`.
2. Treats the child process as newline-delimited JSON-RPC over stdio.
3. Accepts WebSocket subscribers at `/acp`.
4. Forwards subscriber JSON-RPC requests to the agent after rewriting the
   request id to a mux-local numeric id.
5. Restores the original request id on the agent response.
6. Sends each agent response only to the subscriber that originated the request.
7. Broadcasts agent notifications to all subscribers.
8. Records broadcast frames in an in-memory replay log unless replay is
   disabled.
9. Replays that broadcast log to later subscribers unless the subscriber uses
   `replay=skip`.
10. Keeps one upstream session alive until the last subscriber leaves and the
    mux TTL expires.

The result is a plain 1-to-N mirror for ACP traffic:

```text
ACP WebSocket clients  ->  acp-mux  ->  stdio ACP agent
                         one mux id     one child process
```

## What It Owns

`acp-mux` owns these behaviors:

- WebSocket accept/close handling.
- Subscriber identity inside one mux, keyed by `peer_id`.
- Peer-id collision rejection with WebSocket close code `4409`.
- Bad query rejection with WebSocket close code `4400`.
- Agent spawn/configuration failures with WebSocket close code `1011`.
- One actor task per live mux.
- One agent subprocess per live mux.
- Subscriber detach when the WebSocket closes.
- TTL shutdown after the last subscriber leaves.
- JSON-RPC parsing at the envelope level.
- Subscriber request-id translation.
- Agent response id restoration.
- Response routing to the originating subscriber.
- Agent notification fanout.
- First-writer-wins fan-in for agent-initiated requests.
- Pending `session/request_permission` tracking.
- Subscriber `$/cancel_request` id translation.
- Agent `$/cancel_request` broadcast and pending-request cleanup.
- First successful `initialize` response caching.
- First successful `session/new` response caching.
- Successful `session/load` canonical session-id rebinding.
- In-memory broadcast replay.
- Replay metadata: sequence number, recorded timestamp, and opaque extension tag.
- Optional replay-store plumbing for library users.
- Safe default blocking for `fs/*` and `terminal/*` client-tool requests.
- Baseline proxy-local `session/attach`.
- Baseline proxy-local `session/detach`.
- Baseline `/debug/sessions`.
- Transient control-plane `session/list` through `/acp/sessions`.

## What It Does Not Do

`acp-mux` intentionally does not:

- emit `rooms/*` frames;
- track Rooms rooms, turns, queues, controls, or segments;
- parse provider-specific `_meta`;
- parse provider stderr or logs;
- execute filesystem or terminal client tools;
- persist the upstream agent's conversation state;
- interpret model/tool/provider semantics;
- fabricate agent-owned `session/*` notifications;
- perform authentication or authorization;
- run a shell for `--agent-cmd`.

The Rooms crate builds those collaboration features as an extension on top of
this core.

## HTTP And WebSocket Surface

### `GET /healthz`

Returns:

```text
ok
```

### `GET /acp`

Upgrades to a WebSocket subscriber.

Required query parameters:

| Query | Meaning |
|---|---|
| `mux` | Mux id. Must match `[A-Za-z0-9_-]{1,128}`. Subscribers with the same mux id share one agent subprocess. |
| `peer_id` | Subscriber id. Must be unique within the mux. |

Optional query parameters:

| Query | Meaning |
|---|---|
| `peer_name` | Human-readable subscriber name stored in snapshots and attach roster. |
| `role` | Caller-provided role string stored in snapshots. |
| `replay=skip` | Suppresses legacy transport replay on WebSocket attach. |

Example:

```text
ws://127.0.0.1:8765/acp?mux=work&peer_id=desktop&peer_name=Desktop
```

Frames are text JSON-RPC. Binary frames are accepted if they contain UTF-8
JSON-RPC bytes.

### `GET /acp/sessions?cwd=<optional>`

Runs a transient control-plane query:

1. Spawns a fresh `--agent-cmd`.
2. Sends `initialize`.
3. Sends `session/list`, forwarding `cwd` when present.
4. Returns the agent's `result` JSON.
5. Shuts the transient process down.

This does not create or attach to a live mux.

### `GET /debug/sessions`

Returns core snapshots:

```json
{
  "muxes": [
    {
      "muxId": "work",
      "subscribers": [],
      "pendingRequestCount": 0,
      "initializeCached": true,
      "cachedSessionId": "sess-123",
      "canonicalSessionId": "sess-123",
      "promptInFlight": null,
      "replayLogLen": 4
    }
  ],
  "muxCount": 1
}
```

Extensions can add fields to each mux snapshot through `MuxExtension`.

## Proxy-Local `session/attach`

`session/attach` is handled by the mux and is not forwarded to the agent.

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "session/attach",
  "params": {
    "sessionId": "sess-123",
    "clientId": "desktop",
    "historyPolicy": "full"
  }
}
```

Core response:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "sessionId": "sess-123",
    "clientId": "desktop",
    "connectedClients": [
      { "clientId": "desktop", "name": "Desktop" }
    ],
    "historyPolicy": "full",
    "history": [
      {
        "method": "session/update",
        "params": {
          "sessionId": "sess-123",
          "update": { "kind": "agent_message_chunk" }
        }
      }
    ]
  }
}
```

Supported `historyPolicy` values:

| Policy | Core behavior |
|---|---|
| `full` | All broadcast replay frames as `HistoryEntry` values. |
| `full_lineage` | Same as `full` in core. Rooms gives this a segment-aware meaning. |
| `pending_only` | Unresolved permission requests only. |
| `none` | No history. |
| `after_message` | Accepted, currently falls back to `full`. |

If `params.sessionId` is set and does not match the mux's current canonical
session id or mux id, the mux returns JSON-RPC error `-32001`.

## Proxy-Local `session/detach`

`session/detach` is handled by the mux and is not forwarded to the agent.

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "session/detach",
  "params": { "sessionId": "sess-123" }
}
```

Response:

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "result": {
    "sessionId": "sess-123",
    "status": "detached"
  }
}
```

After sending the response, the mux removes that subscriber from the live mux.

## Replay

The core replay log contains only broadcast-tier frames:

- agent notifications;
- agent `$/cancel_request` notifications;
- mux/extension broadcasts sent through the core broadcast path.

It does not contain per-subscriber agent responses. A late subscriber does not
receive another subscriber's old request response.

Each replay entry carries:

- raw frame bytes;
- `recorded_at`;
- monotonic `seq`;
- opaque `ext_tag`.

The core does not interpret `ext_tag`. Rooms uses it as a segment id.

`--replay-turns` controls the in-memory replay log:

| Value | Behavior |
|---|---|
| `unbounded` | Keep all broadcast frames in memory. |
| `0` | Disable replay. |
| `N > 0` | Accepted and currently treated as unbounded with a warning. |

The core library has replay-store types and registry plumbing, but the
standalone `acp-mux` binary does not currently expose a `--replay-store` flag.
The `rooms` binary does expose replay persistence.

## Request Routing

Subscriber requests are forwarded like this:

1. Parse the JSON-RPC request.
2. Allocate the next mux-local numeric id.
3. Store `{ mux_id -> peer_id, original_id, handshake }`.
4. Replace the request id with the mux id.
5. Forward the request to the agent subprocess.
6. When the agent response arrives, restore the original id.
7. Send the response only to `peer_id`.

Notifications have no id and are broadcast to all subscribers.

## Handshake Caching

The mux caches successful handshakes so late subscribers do not accidentally
create duplicate upstream sessions.

Cached:

- first successful `initialize` result;
- first successful `session/new` result;
- successful `session/load` result, which also updates the canonical session id.

When a later subscriber sends `initialize` or `session/new`, the mux can answer
from cache without forwarding to the agent.

## Agent-Initiated Requests

When the agent sends a JSON-RPC request to clients:

1. The mux records the request id as in flight.
2. The mux broadcasts the raw request to all subscribers.
3. The first subscriber response for that id is forwarded to the agent.
4. Later duplicate responses for that id are dropped.

`session/request_permission` requests are also tracked in `pending_permissions`.
They can be returned from `session/attach` with `historyPolicy: "pending_only"`.

## Safety Defaults

ACP agents may ask clients to perform local side effects with methods such as
`fs/*` and `terminal/*`. Broadcasting those requests to multiple clients can
duplicate side effects or run an action on the wrong machine.

So the core defaults are fail-closed:

- `initialize.params.clientCapabilities.fs` is stripped before forwarding.
- `initialize.params.clientCapabilities.terminal` is stripped before forwarding.
- Runtime `fs/*` and `terminal/*` agent requests are blocked with JSON-RPC
  error `-32000`.
- Blocked client-tool requests are not broadcast and are not replayed.

`--unsafe-debug-client-tool-broadcast` disables that protection and raw-broadcasts
those requests. Use it only for diagnostics.

## CLI

```sh
acp-mux \
  --agent-cmd 'cat' \
  --host 127.0.0.1 \
  --port 8765
```

Flags:

| Flag | Default | Meaning |
|---|---:|---|
| `--host` | `127.0.0.1` | HTTP/WebSocket bind address. |
| `--port` | `8765` | HTTP/WebSocket port. |
| `--agent-cmd` | none | Command and whitespace-split args used to spawn each mux's ACP agent. |
| `--mux-ttl-seconds` | `60` | Seconds to retain an empty mux before shutting down its agent. |
| `--replay-turns` | `unbounded` | In-memory replay policy. |
| `--unsafe-debug-client-tool-broadcast` | `false` | Raw-broadcast delegated `fs/*` and `terminal/*` requests. |
| `--log-level` | `info` | Logging level. `RUST_LOG` takes precedence. |

`--agent-cmd` is split on whitespace and is not run through a shell. Put
environment variables on the `acp-mux` process itself.

## Library Extension Seam

The core exposes `MuxExtension` for higher-level protocols.

The extension receives hooks around:

- subscriber requests;
- outbound request translation;
- successful request forwarding;
- subscriber notifications;
- agent notifications;
- agent requests;
- agent responses;
- prompt settlement;
- agent-request resolution;
- canonical session-id changes;
- subscriber attach/detach;
- proxy-local `session/attach`;
- scheduled extension wakes;
- debug snapshots.

The extension can:

- inspect core state through `MuxCtx`;
- broadcast frames;
- send frames to one subscriber;
- write requests/notifications to the agent;
- submit a prompt through core id routing;
- set the opaque replay tag;
- schedule a wake-up message.

The default `NoopExtension` preserves pure core behavior.

## Run A Smoke Test

Using `cat` as an echo agent:

```sh
cargo run -p acp-mux -- --agent-cmd 'cat'
```

Connect:

```text
ws://127.0.0.1:8765/acp?mux=echo&peer_id=a
```

Send:

```json
{"jsonrpc":"2.0","id":1,"method":"initialize"}
```

Because `cat` echoes stdin, the mux receives the same frame back as an agent
request/notification/response depending on the JSON-RPC shape. This is useful
for transport smoke tests, not for ACP semantic tests.

## Relationship To Rooms

Rooms is a separate crate that composes this core via `MuxExtension`.

Use `rooms` when you want:

- `?room=` URLs;
- `rooms/*` collaboration frames;
- turn lifecycle;
- queue/steer/cancel controls;
- segment lineage;
- Rooms-enriched attach snapshots;
- streamed backfill;
- `--replay-store` from the binary.

Use `acp-mux` when you want the smallest provider-neutral ACP multiplexer and
no Rooms wire extension.
