# acp-mux

Multi-subscriber session-sharing layer for ACP (Agent Client Protocol). Lets multiple clients — desktop, phone, web — attach to one ACP agent session in real time. Each client sees the same conversation, can take turns prompting, and receives streaming updates from the agent.

**Status:** v0.1.2.

## Install

```sh
git clone https://github.com/lsaether/acp-mux
cd acp-mux
cargo build --release
# binary: ./target/release/amux
```

## Run

```sh
amux --agent-cmd 'hermes acp' --port 8765
```

Then connect WebSocket clients to `ws://127.0.0.1:8765/acp?session=<id>&peer_id=<unique>&peer_name=<display>&role=<optional>`.

Health and debug endpoints:

- `GET /healthz` — `200 ok`
- `GET /acp/sessions?cwd=<optional>` — cold-start session discovery. Spawns a transient `--agent-cmd`, initializes it, sends `session/list`, returns the agent's `result` JSON, then tears the subprocess down without creating a live mux session.
- `GET /debug/sessions` — JSON snapshot of every live session (subscribers, cache state, active turn, replay log length)

### CLI flags

| Flag                       | Default       | Notes |
|----------------------------|---------------|-------|
| `--host`                   | `127.0.0.1`   | Bind address. |
| `--port`                   | `8765`        | TCP port. |
| `--agent-cmd`              | _(none)_      | Command + args (whitespace-split). Without this, subscriber attaches close with WS code 1011. |
| `--session-ttl-seconds`    | `60`          | Grace window after last subscriber leaves — a reconnect within this window keeps the same subprocess. |
| `--replay-turns`           | `unbounded`   | `unbounded` keeps the full broadcast log; `0` disables it; `N > 0` is accepted and warned (bounded eviction lands in v0.2). |
| `--meta-propagate`         | `false`       | Opt into injecting mux trace fields into subscriber → agent requests at `params._meta.amux`. |
| `--unsafe-debug-client-tool-broadcast` | `false` | **Unsafe/debug only.** Restores raw fanout for agent-initiated `fs/*` and `terminal/*` requests; side effects may duplicate across subscribers. |
| `--log-level`              | `info`        | `trace`/`debug`/`info`/`warn`/`error`. `RUST_LOG` wins when set. |

## Agent compatibility

`acp-mux` is currently developed and directly tested against the Hermes ACP harness. Other ACP agents may work for the generic conversation, permission, cancellation, and replay paths, but Hermes is the only directly supported harness at the moment.

| Agent / harness | Status | Notes |
|---|---|---|
| **[hermes-agent](https://github.com/hermes-agent/hermes)** | ✅ Directly supported | Hermes self-handles fs/terminal/tool execution inside its own process and does not delegate those calls over ACP. |
| **Codex (Zed-bundled)**, **claude-code-acp**, **copilot-acp** | ⚠️ Best-effort / partial | Basic ACP envelope routing should work, but these are not directly harnessed right now. Agents that delegate `fs/*` or `terminal/*` to the client are safe-blocked by default; full delegated-client compatibility is tracked in [#37](https://github.com/lsaether/acp-mux/issues/37) and follow-ups. |

## How it works

- **One subprocess per session.** Each `?session=` value spawns a fresh `--agent-cmd` subprocess. Multiple subscribers on the same session share that subprocess.
- **JSON-RPC envelope routing.** The mux parses only the envelope (`id`, `method`, `params`, `result`, `error`) unless a mux-owned policy needs a narrow payload check. Payloads are otherwise forwarded byte-for-byte. Policy primarily keys off the `method` string.
- **Per-session id translation.** Each subscriber's request `id` is rewritten to a per-session `mux_id` before forwarding; the response is rewritten back and sent only to the originator.
- **`initialize` / `session/new` caching.** First response is cached; later joiners are answered locally without re-sending to the agent.
- **Collaborative agent-initiated requests.** `session/request_permission` is fanned out live to every attached subscriber; any peer can reply. The mux also emits inert `amux/agent_request_opened` lifecycle metadata for replay/audit context, and re-issues unresolved permission requests to clients that later call `session/attach` with actionable history. The first reply for a given id is forwarded to the agent and later replies for the same id are dropped, so the agent always sees exactly one response.
- **RFD #533 ACP facade.** In addition to the durable `amux/*` namespace, the mux handles proxy-local `session/attach` / `session/detach` and emits ACP `session/update` siblings for `prompt_received`, `turn_complete`, `permission_resolved`, and `client_disconnected`. It does **not** inject `agentCapabilities.sessionCapabilities.attach` into upstream `initialize` responses; callers discover/use this proxy feature out of band.
- **Client-tool policy.** By default, agent-initiated `fs/*` and `terminal/*` client-tool requests are blocked in the mux, answered to the agent with JSON-RPC `-32000`, and not broadcast/replayed. `initialize.params.clientCapabilities.fs` and `.terminal` are stripped before the first initialize reaches the agent. `--unsafe-debug-client-tool-broadcast` explicitly restores the old raw fanout for diagnostics only.
- **Turn serialization.** Concurrent ordinary `session/prompt` while a turn is in flight is rejected with JSON-RPC `-32001`; active-turn controls go through explicit `amux/*` requests. `amux/steer_active_turn` is mux-owned steer/send state: when a turn is active it broadcasts intent, sends ACP `session/cancel`, waits for settlement, then starts a replacement `session/prompt` with prompt-injected context and `supersedesTurnId`; when idle it submits the steer text immediately as the next prompt with `mode: "prompt"`. A second hard steer while one is pending is rejected with `-32002`. `amux/queue_prompt` is mux-owned queue/send state capped at six pending items (`-32003` when full): it broadcasts/replays queue lifecycle, starts immediately when no turn is active, or starts the queued prompt as the next turn after active-turn settlement. `amux/unqueue_prompt` removes a still-pending queue item. The last attached subscriber to issue a substantive request is surfaced as the "driving subscriber" in `/debug/sessions` and `amux/turn_started` for UI attribution.
- **Opt-in request trace metadata.** With `--meta-propagate`, outbound subscriber → agent requests get mux-owned `params._meta.amux` fields (`peerId`, `peerName`, `role`, `muxId`, and `amuxTurnId` for prompts) for cross-client debugging. Default mode leaves request payload metadata unchanged.
- **Cold-start session discovery.** `GET /acp/sessions` runs a transient agent-side `session/list` query before any WebSocket attach, useful for dashboards that need to browse persisted sessions before choosing one to resume.
- **Live `session/list` decoration.** Returned `sessions[]` entries that match a live muxed upstream session get `sessions[i]._meta.amux` fields (`proxySessionId`, `subscriberCount`, optional `drivingSubscriber`), preserving existing `_meta` keys and leaving non-live entries unchanged.
- **`amux/*` extension namespace.** The mux publishes its own metadata/control plane out-of-band: `amux/session_context`, `amux/peer_joined`, `amux/peer_left`, `amux/turn_started`, `amux/turn_complete`, `amux/turn_cancelled`, `amux/session_busy`, `amux/control_submitted`, `amux/queue_item_added`, `amux/queue_item_submitted`, `amux/queue_item_completed`, `amux/queue_item_removed`, `amux/queue_item_orphaned`, `amux/agent_request_opened`, `amux/agent_request_resolved`, plus subscriber-request controls such as `amux/steer_active_turn`, `amux/queue_prompt`, `amux/unqueue_prompt`, and `amux/cancel_active_turn`. ACP frames stay clean; clients see two distinguishable channels and demultiplex by method prefix.
- **Cancellation.** `$/cancel_request` (request-cancellation RFD / unstable schema, not stable ACP v1) works both directions: subscribers can cancel their own in-flight requests; agents can cancel agent-initiated requests (broadcast to peers + `amux/agent_request_resolved { resolvedBy: "agent:cancelled" }`). The amux extension `amux/cancel_active_turn` lets *any* attached peer cancel the in-flight turn (not just the driver) — internally it sends ACP-native `session/cancel { sessionId }` toward the agent and emits `amux/turn_cancelled` to peers.
- **Replay log.** Every broadcast-tier frame (`amux/*` + agent notifications + proxy-owned `session/update` siblings) is appended; a late joiner receives the full history before any live event. Raw collaborative agent-initiated requests are live-only in WebSocket replay, but unresolved `session/request_permission` requests are stored separately and re-issued after `session/attach` so late joiners can answer them. `historyPolicy: "after_message"` is accepted but currently falls back to `"full"` until upstream ACP message IDs are available consistently end-to-end. Blocked client-tool requests never enter this lifecycle.
- **TTL grace.** Last subscriber leaving starts a countdown; a reconnect within `--session-ttl-seconds` reuses the same subprocess with all of its caches intact.

## Client contract

Clients SHOULD:

- Treat `amux/peer_joined` (with `peerId == self.peer_id`) as the empty-roster signal — used only by replay log late joiners.
- Treat `amux/turn_started` / `amux/turn_complete` as turn bookends; the `peerId` field attributes the turn.
- Treat `amux/agent_request_opened` / `amux/agent_request_resolved` as the non-actionable lifecycle for agent-initiated requests. Only raw live `session/request_permission` requests should create a reply affordance; replayed `amux/agent_request_opened` is context, not a request to answer.
- Filter `amux/*` frames out of the conversation render and use them for presence / turn UI.
- Allow the mux to rewrite request `id` fields freely (preserve client-side correlation by tracking your own original ids).

Detailed protocol spec: [`docs/design/amux-namespace.md`](docs/design/amux-namespace.md).

## ACP coverage

amux parses only JSON-RPC envelopes (`id`, `method`, `params`, `result`, `error`) and forwards payloads byte-for-byte unless a method has mux-specific semantics. Any ACP method amux does not specifically intercept passes through transparently.

This table was audited against the stable ACP v1 schema release [`v0.13.3`](https://github.com/agentclientprotocol/agent-client-protocol/releases/tag/v0.13.3) (published 2026-05-22) and the upstream docs at `agentclientprotocol/agent-client-protocol@a3b012c`. **Stable v1** means present in `schema/schema.json` and the current `/protocol/*` docs. **Unstable/RFD** means it appears only in `schema.unstable.json`, `docs/rfds/*`, or an unmerged proposal; amux support there is intentionally called out as extension behavior, not stable ACP compliance.

### Client-initiated (subscriber → agent)

| Method | amux | Spec status | Notes |
|---|---|---|---|
| `initialize` | ✅ | Stable v1 | Forwarded after stripping blocked client-tool capabilities; first response cached; upstream `agentCapabilities` passed through. |
| `authenticate` | ✅ (envelope passthrough) | Stable v1 | Auth state belongs to the shared upstream agent subprocess. |
| `logout` | ✅ (envelope passthrough) | Stable v1 | Stable as of the logout-method RFD completion; amux does not currently clear cached initialize/session state after logout. |
| `session/new` | ✅ | Stable v1 | Forwarded; first response cached for late joiners. |
| `session/load` | ✅ | Stable v1 | Forwarded to the agent. On success, amux rebinds the room's canonical session id and replay-generation boundary to the loaded session; failed loads leave the cache untouched. |
| `session/resume` | ⚠️ envelope passthrough | Stable v1 | Forwarded, but not yet given `session/load`-style canonical-session rebinding for late joiners. |
| `session/close` | ⚠️ envelope passthrough | Stable v1 | Forwarded, but amux does not yet tear down the mux room or clear local caches after a successful close. |
| `session/attach` | ✅ proxy-local | RFD #533 | Answered by the mux, never forwarded to the agent. Returns `sessionId`, `clientId`, `connectedClients`, effective `historyPolicy`, and optional `history` for `full` / `pending_only`; `none` omits history; `after_message` falls back to `full` when `afterMessageId` cannot be resolved. |
| `session/detach` | ✅ proxy-local | RFD #533 | Answered by the mux, then the WebSocket is closed normally; remaining peers receive `amux/peer_left` and `session/update { update: { type: "client_disconnected" } }`. |
| `session/list` | ✅ | Stable v1 | Over WS, forwarded with id translation and optional `params._meta.amux` trace fields. Returned `sessions[]` entries matching live mux state are decorated under `sessions[i]._meta.amux`; non-live entries and agent-owned metadata are preserved. `GET /acp/sessions?cwd=...` performs a transient agent-side `session/list` before any WS attach. |
| `session/prompt` | ✅ | Stable v1 | Forwarded with id translation; turn serialization; ordinary concurrent prompts rejected with `-32001`. Plain ACP prompts stay serialized/generic; active-turn steering/queueing uses explicit `amux/*` controls. |
| `session/cancel` | ✅ | Stable v1 | Forwarded unchanged from vanilla clients; also emitted southbound by `amux/cancel_active_turn` for active-turn interruption. |
| `session/set_mode` | ✅ (envelope passthrough) | Stable v1 | Not specifically handled. Session modes remain in stable v1 but are expected to change in ACP v2. |
| `session/set_config_option` | ✅ (envelope passthrough) | Stable v1 | Not specifically handled; upstream agent owns config state and any resulting `session/update` notifications. |
| `$/cancel_request` | ✅ | Unstable/RFD | Optional request-cancellation RFD; not in stable `schema.json`. Strict per-peer semantics; subscribers can cancel their own in-flight requests only. |

### Agent-initiated (agent → subscriber)

| Method | amux | Spec status | Notes |
|---|---|---|---|
| `session/update` | ✅ | Stable v1 + RFD #533 | Agent-emitted updates are broadcast to every attached subscriber and appended to replay log. The mux also emits proxy-owned RFD #533 update variants using `update.type`: `prompt_received`, `turn_complete`, `permission_resolved`, and `client_disconnected`. |
| `session/request_permission` | ✅ | Stable v1 | Broadcast live with first-writer-wins reply; `amux/agent_request_opened` records inert replay context; `amux/agent_request_resolved` and `session/update { update: { type: "permission_resolved" } }` fire when consumed; unresolved permissions are re-issued after `session/attach`; turn-end sweep cleans up abandoned requests. |
| `fs/read_text_file`, `fs/write_text_file` | ✅ safe default / 🚧 not provided | Stable v1 client-tool methods | amux does not advertise filesystem client capabilities by default. If an agent sends `fs/*` anyway, amux returns structured `-32000 { reason: "client_tool_blocked" }` to the agent and does not broadcast/replay. Full delegated-client modes are tracked in [#37](https://github.com/lsaether/acp-mux/issues/37). |
| `terminal/create`, `terminal/output`, `terminal/wait_for_exit`, `terminal/kill`, `terminal/release` | ✅ safe default / 🚧 not provided | Stable v1 client-tool methods | Same policy as `fs/*`: `terminal` is stripped from advertised client capabilities and runtime requests are blocked unless `--unsafe-debug-client-tool-broadcast` is explicitly enabled. |
| `$/cancel_request` | ✅ | Unstable/RFD | Optional request-cancellation RFD; not in stable `schema.json`. Marks `agent_pending` Consumed; broadcasts to all peers; emits `amux/agent_request_resolved { resolvedBy: "agent:cancelled" }`. |

### Unstable ACP / RFD surfaces

| Surface | amux | Status | Notes |
|---|---|---|---|
| `params._meta.amux` trace propagation | ✅ opt-in | `_meta` stable, propagation convention from RFD | `--meta-propagate` writes mux-owned metadata under the reserved `_meta` extension field without replacing existing agent/client metadata. |
| `session/delete`, `session/fork`, provider methods, NES methods, MCP-over-ACP, elicitation | ➡️ generic passthrough only | Unstable schema / RFDs | Not intentionally implemented by amux. If experimental peers send them, amux envelope-routes them unless they later need mux-specific state handling. |
| `session/attach`, `session/detach`, proxy-owned `session/update` variants | ✅ | RFD #533 | Implemented as a proxy facade while preserving `amux/*` as the authoritative mux namespace. No upstream capability injection; `after_message` is provisional and falls back to `full` without stable message-id coverage. |

### amux extensions (not part of ACP)

| Method | Direction | Purpose |
|---|---|---|
| `amux/session_context` | proxy → subscriber | Per-attach mux/agent process context, including the cwd inherited by the agent subprocess. |
| `amux/peer_joined`, `amux/peer_left` | proxy → subscribers | Presence. |
| `amux/turn_started`, `amux/turn_complete` | proxy → subscribers | Turn bookends with `amuxTurnId`; replacement turns may include `supersedesTurnId`. |
| `amux/turn_cancelled` | proxy → subscribers | Intent broadcast when any peer triggers cancellation. |
| `amux/session_busy` | proxy → subscribers | Companion to `-32001` rejection on concurrent prompts. |
| `amux/control_submitted` | proxy → subscribers | Replay-safe accepted-control intent for steer controls. |
| `amux/queue_item_added`, `amux/queue_item_submitted`, `amux/queue_item_completed`, `amux/queue_item_removed`, `amux/queue_item_orphaned` | proxy → subscribers | Replay-safe mux-owned queue lifecycle. |
| `amux/steer_active_turn` | subscriber → proxy | Steer the active turn if busy; submit the text immediately as the next prompt if idle. |
| `amux/queue_prompt` | subscriber → proxy | Queue text after the active turn if busy, or submit it immediately if idle; capped at six pending items. |
| `amux/unqueue_prompt` | subscriber → proxy | Remove a pending `amux/queue_prompt` item by `queueItemId`. |
| `amux/agent_request_opened` | proxy → subscribers | Non-actionable context for agent-initiated requests; replay-safe companion to the live raw request. |
| `amux/agent_request_resolved` | proxy → subscribers | Dismissal signal for agent-initiated requests (`request_permission`, etc.). |
| `amux/cancel_active_turn` | subscriber → proxy | Any peer can cancel the active turn; resolves to ACP-native `session/cancel` toward the agent. |

Detailed shape and semantics: [`docs/design/amux-namespace.md`](docs/design/amux-namespace.md).

## Docs

- Protocol spec: [`docs/design/amux-namespace.md`](docs/design/amux-namespace.md)
- Build plan: [`ROADMAP.md`](ROADMAP.md)
- Release notes: [`CHANGELOG.md`](CHANGELOG.md)

## License

MIT — see [LICENSE](LICENSE).
