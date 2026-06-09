# rooms

`rooms` is the Rooms collaboration layer for `acp-mux`.

It depends on the core `acp-mux` crate and supplies a `RoomsExtension`. The core
still owns subprocess management, JSON-RPC id routing, fanout, replay storage,
permission fan-in, and baseline `session/attach`. Rooms owns the multiplayer
protocol that lives under `rooms/*`.

## What It Does

For each `?room=<id>`, `rooms` runs one core mux with Rooms hooks installed.
Subscribers in the same room share one upstream ACP agent subprocess and also
receive Rooms collaboration events.

Rooms adds:

- `?room=` WebSocket naming, with deprecated `?session=` alias;
- peer presence frames;
- initial `rooms/session_context`;
- turn lifecycle frames;
- active-turn busy events;
- active-turn steering;
- queued prompts;
- active-turn cancellation;
- permission request opened/resolved projection frames;
- pending permission reissue after attach;
- room segment tracking around `session/load` and observed ACP `sessionId`
  changes;
- Rooms metadata in `session/attach`;
- segment-scoped `full` history;
- `full_lineage` history across room segments;
- newest-turn-first attach ordering;
- streamed attach history with `rooms/replay_started` and
  `rooms/replay_complete`;
- optional append-only JSONL replay persistence through `--replay-store`;
- enriched `/debug/sessions` room snapshots.

Rooms does not replace ACP. Agent-owned frames stay agent-owned. If a frame uses
`method: "session/update"`, it came from the upstream agent. Rooms-owned room
facts use `rooms/*`.

## Run

```sh
rooms \
  --agent-cmd 'cat' \
  --host 127.0.0.1 \
  --port 8765
```

Connect:

```text
ws://127.0.0.1:8765/acp?room=work&peer_id=desktop&peer_name=Desktop
```

Attach-aware clients usually suppress legacy WebSocket replay and request
history explicitly:

```text
ws://127.0.0.1:8765/acp?room=work&peer_id=desktop&replay=skip
```

Then send:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "session/attach",
  "params": {
    "historyPolicy": "full",
    "_meta": {
      "rooms": {
        "replayOrder": "chronological",
        "historyDelivery": "response"
      }
    }
  }
}
```

## CLI

| Flag | Default | Meaning |
|---|---:|---|
| `--host` | `127.0.0.1` | HTTP/WebSocket bind address. |
| `--port` | `8765` | HTTP/WebSocket port. |
| `--agent-cmd` | none | Command and whitespace-split args used to spawn each room's ACP agent. |
| `--session-ttl-seconds` | `60` | Seconds to retain an empty room before shutting down its agent. |
| `--replay-turns` | `unbounded` | In-memory replay policy. |
| `--replay-store` | none | Optional append-only JSONL replay directory. |
| `--meta-propagate` | `false` | Add mux trace fields under `params._meta.rooms` on subscriber-to-agent requests. |
| `--unsafe-debug-client-tool-broadcast` | `false` | Raw-broadcast delegated `fs/*` and `terminal/*` requests. |
| `--emit-segment-frames` | `true` | Emit segment lifecycle frames. |
| `--log-level` | `info` | Logging level. `RUST_LOG` takes precedence. |

`--agent-cmd` is split on whitespace and is not run through a shell.

## Main `rooms/*` Frames

Presence and context:

- `rooms/session_context`
- `rooms/peer_joined`
- `rooms/peer_left`

Turns:

- `rooms/turn_started`
- `rooms/turn_complete`
- `rooms/turn_cancelled`
- `rooms/session_busy`
- `rooms/control_submitted`

Queue:

- `rooms/queue_item_added`
- `rooms/queue_item_submitted`
- `rooms/queue_item_completed`
- `rooms/queue_item_removed`
- `rooms/queue_item_orphaned`

Agent request projection:

- `rooms/agent_request_opened`
- `rooms/agent_request_resolved`

Replay and segments:

- `rooms/replay_started`
- `rooms/replay_complete`
- `rooms/segment_started`
- `rooms/segment_ended`

Control requests from subscribers:

- `rooms/steer_active_turn`
- `rooms/queue_prompt`
- `rooms/unqueue_prompt`
- `rooms/cancel_active_turn`

See [../../docs/design/rooms-namespace.md](../../docs/design/rooms-namespace.md)
for wire shapes.

## Replay Store

With `--replay-store <DIR>`, Rooms persists broadcast-tier replay frames in one
JSONL file per room:

```text
<DIR>/<room_id>.jsonl
```

This persists visible mux replay history. It does not persist the upstream
agent's private conversation store or in-flight permissions.

On restart the broadcast log is rehydrated for `historyPolicy: full_lineage`
recovery, but segment lineage and current-segment (`full`) scoping are not
reconstructed yet — see "cross-restart segment fidelity" in
[../../docs/design/rooms.md](../../docs/design/rooms.md).

`--replay-turns 0` disables replay and prevents replay-store writes.

## Debug Endpoint

`GET /debug/sessions` returns `rooms` rather than the core `muxes` field and
adds Rooms fields such as:

- `roomId`;
- `activeTurnMuxId`;
- `drivingSubscriber`;
- `activeRoomsTurnId`;
- `replayGeneration`;
- `lastReplayReset`;
- `segments`;
- `activeSegmentId`;
- `replayLogUpdateFramesByAcpSessionId`.

## More Docs

- [Core mux README](../acp-mux/README.md)
- [`rooms/*` namespace](../../docs/design/rooms-namespace.md)
- [Rooms and segments](../../docs/design/rooms.md)
- [Client contract examples](../../docs/examples/client-contract)
