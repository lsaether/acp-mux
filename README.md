# acp-mux

**Generic ACP multiplexer / agent mirror.** `acp-mux` runs one stdio ACP agent process behind a WebSocket room and mirrors that live agent session to many clients — desktop, phone, web, TUI, or anything else that can speak JSON-RPC over WebSocket.

The mux is provider-neutral. It does not parse provider logs, interpret provider-private metadata, or make one ACP implementation canonical. Provider `_meta` passes through as payload data; mux state is driven by JSON-RPC envelopes, ACP method names, and ACP-visible `sessionId` changes.

```text
ACP client(s) ── WebSocket JSON-RPC ──► amux ── stdio ACP JSON-RPC ──► ACP agent
   phone             same room              │             any stdio ACP agent
   desktop           same transcript         └─ replay, turn control, presence
   web UI            same permissions
```

## What it does

`acp-mux` has one job: **mirror one upstream ACP agent session into a collaborative, reconnectable room.**

The project keeps a hard boundary between the generic ACP mux core and the optional AMUX collaboration layer.

The **core mux** owns:

- one agent subprocess per room;
- WebSocket attach/detach for multiple subscribers;
- JSON-RPC request-id translation and response routing;
- broadcast fanout for agent notifications;
- initialize / `session/new` response caching for late joiners;
- replay history and optional persistent replay storage;
- provider-neutral room/session-id tracking needed for reconnect and replay;
- safe defaults for delegated client tools such as `fs/*` and `terminal/*`.

The **AMUX layer** owns multiplayer conveniences layered on top of the mux:

- turn bookends and busy-state visibility;
- queue, steer, and active-turn cancellation controls;
- first-writer-wins coordination for agent-initiated permission requests;
- replay-safe room, queue, request, and segment projection frames under `amux/*`.

It does **not** own:

- the agent's model, tools, memory, auth, or persisted conversation store;
- provider-specific lifecycle semantics;
- provider-specific stderr/log parsing;
- terminal or filesystem client-tool execution by default;
- changes to upstream ACP agents or the ACP protocol.

## Install

```sh
git clone https://github.com/lsaether/acp-mux
cd acp-mux
cargo build --release
# binary: ./target/release/amux
```

## Run with Claude Agent ACP

The most useful smoke path is a real ACP coding agent. Zed's Claude Agent adapter is published as `@agentclientprotocol/claude-agent-acp` (`@zed-industries/claude-agent-acp` was the earlier package name and is still what older Zed docs mention). `acp-mux` can run it like any other stdio ACP agent.

Use `npx` directly:

```sh
# Provide auth however the adapter expects it; this is just one common path.
export ANTHROPIC_API_KEY='<your-api-key>'

target/release/amux \
  --agent-cmd 'npx -y @agentclientprotocol/claude-agent-acp' \
  --port 8765
```

Or install the adapter globally and use its binary:

```sh
npm install -g @agentclientprotocol/claude-agent-acp

target/release/amux \
  --agent-cmd 'claude-agent-acp' \
  --host 127.0.0.1 \
  --port 8765
```

Do **not** put shell-only syntax such as `ANTHROPIC_API_KEY=... claude-agent-acp` inside `--agent-cmd`; `amux` splits the command into argv and does not run it through a shell. Put environment variables on the `amux` process itself.

Then connect clients to:

```text
ws://127.0.0.1:8765/acp?room=<room-id>&peer_id=<unique-peer>&peer_name=<display-name>&role=<optional>
```

`?room=` is the mux-level collaboration id. Multiple clients using the same `room` share the same upstream Claude Agent subprocess and transcript. `?session=` is accepted as a deprecated alias during the v0.2 transition.

Attach-aware clients can add `&replay=skip` and then call proxy-local `session/attach` so attach history becomes their single bootstrap source. See [`docs/examples/client-contract`](docs/examples/client-contract) for copyable client frames and expected `amux/*` shapes.

## HTTP endpoints

- `GET /healthz` — returns `200 ok`.
- `GET /acp/sessions?cwd=<optional>` — cold-start session discovery. Spawns a transient `--agent-cmd`, initializes it, sends `session/list`, returns the agent's `result` JSON, then tears the subprocess down without creating a live room.
- `GET /debug/sessions` — JSON snapshot of live rooms: subscribers, cache state, active turn, queue state, replay length, and segment lineage.

## CLI flags

| Flag | Default | Notes |
|---|---:|---|
| `--host` | `127.0.0.1` | Bind address. |
| `--port` | `8765` | TCP port. |
| `--agent-cmd` | _(none)_ | Command + args used to spawn a stdio ACP agent for each new room. Without this, attaches close with WS code `1011`. |
| `--session-ttl-seconds` | `60` | Grace window after the last subscriber leaves. A reconnect within the window keeps the same subprocess alive. |
| `--replay-turns` | `unbounded` | `unbounded` keeps the broadcast log; `0` disables it; `N > 0` is accepted and currently behaves as unbounded with a warning. |
| `--replay-store` | _(none)_ | Optional directory for append-only JSONL replay persistence, one file per room. |
| `--meta-propagate` | `false` | Opt into adding mux trace fields under `params._meta.amux` on subscriber → agent requests. |
| `--unsafe-debug-client-tool-broadcast` | `false` | **Unsafe/debug only.** Raw-broadcasts agent-initiated `fs/*` and `terminal/*` requests; may duplicate side effects. |
| `--emit-segment-frames` | `true` | Emit `amux/segment_started` and `amux/segment_ended` when `session/load` or observed ACP `sessionId` changes rotate the room segment. |
| `--log-level` | `info` | `trace`, `debug`, `info`, `warn`, or `error`. `RUST_LOG` wins when set. |

## Agent compatibility

`acp-mux` expects a child process that speaks ACP-style newline-delimited JSON-RPC over stdio.

| Agent | Status | Notes |
|---|---|---|
| `@agentclientprotocol/claude-agent-acp` / `claude-agent-acp` | ✅ Preferred real-agent example | Zed's Claude Agent adapter, runnable through `npx -y @agentclientprotocol/claude-agent-acp` or a global `claude-agent-acp` install. |
| ACP agents that execute tools inside their own process | ✅ Generic path | Conversation, permission, cancellation, replay, attach/detach, and segment lineage are mux-owned and provider-neutral. |
| ACP agents that delegate `fs/*` or `terminal/*` to the client | ⚠️ Blocked by default | `acp-mux` strips advertised filesystem/terminal client capabilities and returns a structured blocked error if the agent sends these requests anyway. Use `--unsafe-debug-client-tool-broadcast` only for diagnostics. |
| Agents with provider-specific `_meta` | ✅ Opaque passthrough | Metadata remains in payloads for clients that understand it; the mux does not use it to drive lifecycle state. |

## Room model

A **room** is the stable mux container named by `?room=`. It owns one upstream subprocess, one subscriber set, one replay log, and one continuous transcript.

A room can contain multiple **segments**. A segment is the interval where one canonical ACP `sessionId` is active. Segments rotate on provider-neutral signals only:

- a successful `session/load`; or
- an agent notification whose `params.sessionId` differs from the active segment's ACP session id.

The transcript continues across segments. Clients that want only the current head use `historyPolicy: "full"`; clients that want the whole mirrored room history use `historyPolicy: "full_lineage"` on `session/attach`.

## Routing and replay

- Subscriber request IDs are rewritten to mux-local IDs before forwarding to the agent.
- Agent responses are rewritten back and sent only to the originating subscriber.
- Agent notifications are broadcast to every subscriber and appended to replay.
- First `initialize` and `session/new` responses are cached so late joiners do not accidentally create a second upstream session.
- Unresolved `session/request_permission` requests are re-issued to attaching clients after `session/attach`; resolved permission history replays as inert `amux/*` lifecycle context, not stale actionable requests.

## `amux/*` extension namespace

`amux/*` is the optional AMUX collaboration layer, not the generic ACP mux contract. `acp-mux` keeps ACP frames and mux facts separate: agent-owned ACP frames stay in the ACP namespace; mux-owned collaboration/control events use `amux/*`.

Common notifications:

- `amux/session_context`
- `amux/peer_joined`, `amux/peer_left`
- `amux/turn_started`, `amux/turn_complete`, `amux/turn_cancelled`
- `amux/session_busy`
- `amux/control_submitted`
- `amux/queue_item_added`, `amux/queue_item_submitted`, `amux/queue_item_completed`, `amux/queue_item_removed`, `amux/queue_item_orphaned`
- `amux/agent_request_opened`, `amux/agent_request_resolved`
- `amux/replay_started`, `amux/replay_complete`
- `amux/segment_started`, `amux/segment_ended`

Subscriber control requests:

- `amux/steer_active_turn`
- `amux/queue_prompt`
- `amux/unqueue_prompt`
- `amux/cancel_active_turn`

See [`docs/design/amux-namespace.md`](docs/design/amux-namespace.md) for wire shapes.

## Safety defaults

ACP includes client-tool methods where an agent can ask the client to read/write files or run terminal commands. In a multi-subscriber room, naïve fanout can duplicate side effects or send a local action to the wrong machine.

So `acp-mux` is fail-closed by default:

- strips `initialize.params.clientCapabilities.fs` and `.terminal` before forwarding initialize to the agent;
- blocks runtime `fs/*` and `terminal/*` requests with JSON-RPC `-32000`;
- does not broadcast or replay blocked client-tool requests;
- preserves collaborative `session/request_permission` fanout.

Use `--unsafe-debug-client-tool-broadcast` only when deliberately debugging delegated-client behavior.

## Persistent replay store

Pass `--replay-store <DIR>` to persist broadcast-tier replay frames to disk. The store is append-only JSONL, one file per room:

```text
<DIR>/<room_id>.jsonl
```

Persisted frames include mux replay metadata (`replaySeq`, `segmentId`, `recordedAt`) and are rehydrated on restart so late joiners can recover history. The upstream agent's actual conversation state remains the agent's responsibility; use ACP `session/load` or the agent's own persistence for that.

Operational notes:

- `--replay-turns 0` disables both in-memory replay and replay persistence.
- The store is unbounded in the current release.
- Delete the room JSONL file to clear persisted history for that room.
- Do not run multiple `amux` processes writing to the same replay-store directory.

## Docs

- Protocol extension spec: [`docs/design/amux-namespace.md`](docs/design/amux-namespace.md)
- Rooms, segments, and transcript lineage: [`docs/design/rooms.md`](docs/design/rooms.md)
- Client contract fixtures: [`docs/examples/client-contract`](docs/examples/client-contract)
- Roadmap: [`ROADMAP.md`](ROADMAP.md)
- Release notes: [`CHANGELOG.md`](CHANGELOG.md)

## License

MIT — see [LICENSE](LICENSE).
