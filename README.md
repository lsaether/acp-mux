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
| `--log-level`              | `info`        | `trace`/`debug`/`info`/`warn`/`error`. `RUST_LOG` wins when set. |

## How it works

- **One subprocess per session.** Each `?session=` value spawns a fresh `--agent-cmd` subprocess. Multiple subscribers on the same session share that subprocess.
- **JSON-RPC envelope routing.** The mux parses only the envelope (`id`, `method`, `params`, `result`, `error`). Payloads are forwarded byte-for-byte. Policy keys off the `method` string.
- **Per-session id translation.** Each subscriber's request `id` is rewritten to a per-session `mux_id` before forwarding; the response is rewritten back and sent only to the originator.
- **`initialize` / `session/new` caching.** First response is cached; later joiners are answered locally without re-sending to the agent.
- **Broadcast agent-initiated requests.** Agent-initiated requests (e.g. `session/request_permission`) are fanned out to every attached subscriber; any peer can reply. The first reply for a given id is forwarded to the agent and later replies for the same id are dropped, so the agent always sees exactly one response.
- **Turn serialization.** Concurrent `session/prompt` while a turn is in flight is rejected with JSON-RPC `-32001`. The last subscriber to issue a substantive request is still surfaced as the "driving subscriber" in `/debug/sessions` and `amux/turn_started` for UI attribution.
- **Opt-in request trace metadata.** With `--meta-propagate`, outbound subscriber → agent requests get mux-owned `params._meta.amux` fields (`peerId`, `peerName`, `role`, `muxId`, and `amuxTurnId` for prompts) for cross-client debugging. Default mode leaves request payload metadata unchanged.
- **Cold-start session discovery.** `GET /acp/sessions` runs a transient agent-side `session/list` query before any WebSocket attach, useful for dashboards that need to browse persisted sessions before choosing one to resume.
- **Live `session/list` decoration.** Returned `sessions[]` entries that match a live muxed upstream session get `sessions[i]._meta.amux` fields (`proxySessionId`, `subscriberCount`, optional `drivingSubscriber`), preserving existing `_meta` keys and leaving non-live entries unchanged.
- **`amux/*` notification namespace.** The mux publishes its own metadata out-of-band: `amux/peer_joined`, `amux/peer_left`, `amux/turn_started`, `amux/turn_complete`, `amux/turn_cancelled`, `amux/session_busy`, `amux/agent_request_resolved`. ACP frames stay clean; clients see two distinguishable channels and demultiplex by method prefix.
- **Cancellation.** `$/cancel_request` (request-cancellation RFD) works both directions: subscribers can cancel their own in-flight requests; agents can cancel agent-initiated requests (broadcast to peers + `amux/agent_request_resolved { resolvedBy: "agent:cancelled" }`). The amux extension `amux/cancel_active_turn` lets *any* attached peer cancel the in-flight turn (not just the driver) — internally it synthesizes a `$/cancel_request` toward the agent and emits `amux/turn_cancelled` to peers.
- **Replay log.** Every broadcast-tier frame (`amux/*` + agent notifications) is appended; a late joiner receives the full history before any live event.
- **TTL grace.** Last subscriber leaving starts a countdown; a reconnect within `--session-ttl-seconds` reuses the same subprocess with all of its caches intact.

## Client contract

Clients SHOULD:

- Treat `amux/peer_joined` (with `peerId == self.peer_id`) as the empty-roster signal — used only by replay log late joiners.
- Treat `amux/turn_started` / `amux/turn_complete` as turn bookends; the `peerId` field attributes the turn.
- Filter `amux/*` frames out of the conversation render and use them for presence / turn UI.
- Allow the mux to rewrite request `id` fields freely (preserve client-side correlation by tracking your own original ids).

Detailed protocol spec: [`docs/design/amux-namespace.md`](docs/design/amux-namespace.md).

## ACP coverage

amux parses only JSON-RPC envelopes (`id`, `method`, `params`, `result`, `error`) and forwards payloads byte-for-byte. Any ACP method amux doesn't specifically intercept passes through transparently. The table below lists methods that need special handling and where amux stands.

"Spec status" reflects the upstream ACP lifecycle ([RFD process](https://github.com/agentclientprotocol/agent-client-protocol/blob/main/docs/rfds/about.mdx)): **Core** = part of the stable spec; **Draft RFD** = merged into `docs/rfds/` on main but not yet promoted to Preview/Completed (implementations may begin, not a stability commitment); **Open RFD** = still an unmerged PR.

### Client-initiated (subscriber → agent)

| Method | amux | Spec status | Notes |
|---|---|---|---|
| `initialize` | ✅ | Core | Forwarded; first response cached; `agentCapabilities` from the upstream agent passed through. |
| `session/new` | ✅ | Core | Forwarded; first response cached for late joiners. |
| `session/load` | ✅ | Core | Forwarded to the agent like any other request. On success, amux rebinds the room's canonical session id (used by late joiners' `session/new` calls) to the loaded session; failed loads leave the cache untouched. |
| `session/prompt` | ✅ | Core | Forwarded with id translation; turn serialization; concurrent prompts rejected with `-32001`. |
| `session/cancel` | ✅ (envelope passthrough) | Core | Per-turn notification; flows through unchanged. |
| `session/set_mode` | ✅ (envelope passthrough) | Core | Not specifically handled. |
| `$/cancel_request` | ✅ | Draft RFD (optional per spec) | Strict per-peer semantics; cancels own in-flight requests only. |
| `session/attach`, `session/detach` | ⏳ | Open RFD ([#533](https://github.com/agentclientprotocol/agent-client-protocol/pull/533)) | Implemented on branch [`rfd-533-alignment`](https://github.com/lsaether/acp-mux/pull/3), shelved pending RFD ratification. |
| `session/list` | ✅ | Draft RFD | Over WS, forwarded to the agent like any other request; capability advertisement (`sessionCapabilities.list`) propagates from the agent. The outbound request can carry `params._meta.amux` trace fields when `--meta-propagate` is enabled. Returned `sessions[]` entries that match live mux state are decorated under `sessions[i]._meta.amux`; non-live entries and existing agent-owned metadata are preserved. For cold-start UIs, `GET /acp/sessions?cwd=...` performs a transient agent-side `session/list` before any WS attach. |

### Agent-initiated (agent → subscriber)

| Method | amux | Spec status | Notes |
|---|---|---|---|
| `session/update` | ✅ | Core | Broadcast to every attached subscriber; appended to replay log. |
| `session/request_permission` | ✅ | Core | Broadcast with first-writer-wins reply; `amux/agent_request_resolved` fires when consumed; turn-end sweep cleans up abandoned requests. |
| `$/cancel_request` | ✅ | Draft RFD (optional per spec) | Marks `agent_pending` Consumed; broadcasts to all peers; emits `amux/agent_request_resolved { resolvedBy: "agent:cancelled" }`. |
| `fs/read_text_file`, `fs/write_text_file` | ❌ | Core | Tracked in [#2](https://github.com/lsaether/acp-mux/issues/2). amux currently broadcasts these to subscribers, which is broken for any agent that delegates fs to the client (Codex, claude-code-acp, copilot-acp). Self-handling design agreed; implementation deferred. |
| `terminal/create`, `terminal/output`, `terminal/wait_for_exit`, `terminal/kill`, `terminal/release` | ❌ | Core | Same as `fs/*` — tracked in [#2](https://github.com/lsaether/acp-mux/issues/2). |

### Agent compatibility

- **[hermes-agent](https://github.com/hermes-agent/hermes)** — fully supported. hermes self-handles fs/terminal in its own process and never delegates over ACP, so issue #2 doesn't apply.
- **Codex (Zed-bundled)**, **claude-code-acp**, **copilot-acp** — partially supported. Conversation, permissions, and cancellation work; fs/terminal delegation will misbehave until [#2](https://github.com/lsaether/acp-mux/issues/2) lands.

### amux extensions (not part of ACP)

| Method | Direction | Purpose |
|---|---|---|
| `amux/peer_joined`, `amux/peer_left` | proxy → subscribers | Presence. |
| `amux/turn_started`, `amux/turn_complete` | proxy → subscribers | Turn bookends with `amuxTurnId`. |
| `amux/turn_cancelled` | proxy → subscribers | Intent broadcast when any peer triggers cancellation. |
| `amux/session_busy` | proxy → subscribers | Companion to `-32001` rejection on concurrent prompts. |
| `amux/agent_request_resolved` | proxy → subscribers | Dismissal signal for agent-initiated requests (`request_permission`, etc.). |
| `amux/cancel_active_turn` | subscriber → proxy | Any peer can cancel the active turn; resolves to a synthesized `$/cancel_request` toward the agent. |

Detailed shape and semantics: [`docs/design/amux-namespace.md`](docs/design/amux-namespace.md).

## Docs

- Protocol spec: [`docs/design/amux-namespace.md`](docs/design/amux-namespace.md)
- Build plan: [`ROADMAP.md`](ROADMAP.md)
- Release notes: [`CHANGELOG.md`](CHANGELOG.md)

## License

MIT — see [LICENSE](LICENSE).
