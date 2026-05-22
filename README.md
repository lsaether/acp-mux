# acp-mux

Multi-subscriber session-sharing layer for ACP (Agent Client Protocol). Lets multiple clients — desktop, phone, web — attach to one ACP agent session in real time. Each client sees the same conversation, can take turns prompting, and receives streaming updates from the agent.

**Status:** v0.1.0.

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
- `GET /debug/sessions` — JSON snapshot of every live session (subscribers, cache state, active turn, replay log length)

### CLI flags

| Flag                       | Default       | Notes |
|----------------------------|---------------|-------|
| `--host`                   | `127.0.0.1`   | Bind address. |
| `--port`                   | `8765`        | TCP port. |
| `--agent-cmd`              | _(none)_      | Command + args (whitespace-split). Without this, subscriber attaches close with WS code 1011. |
| `--session-ttl-seconds`    | `60`          | Grace window after last subscriber leaves — a reconnect within this window keeps the same subprocess. |
| `--replay-turns`           | `unbounded`   | `unbounded` keeps the full broadcast log; `0` disables it; `N > 0` is accepted and warned (bounded eviction lands in v0.2). |
| `--log-level`              | `info`        | `trace`/`debug`/`info`/`warn`/`error`. `RUST_LOG` wins when set. |

## RFD #533 alignment

amux implements the [Multi-Client Session Attach](https://github.com/agentclientprotocol/agent-client-protocol/pull/533) RFD. RFD-aware clients can use the standard methods and frames directly; existing amux clients keep working against the `amux/*` namespace via dual-emit.

- `session/attach` and `session/detach` are intercepted by the proxy. Attach returns `{ sessionId, clientId, historyPolicy, connectedClients[], history? }` and re-issues any unresolved `session/request_permission` to the attaching client so it's actionable.
- `historyPolicy`: `full` (default), `pending_only`, `none`, `after_message` (falls back to `full` pending the Message ID RFD).
- `session/update` siblings of `amux/*` are emitted whenever an ACP session id is known: `prompt_received`, `turn_complete`, `permission_resolved`, `client_disconnected`. Distinguished from agent-emitted updates by `update.type` (proxy) vs `update.kind` (agent).
- `initialize` advertises `agentCapabilities.sessionCapabilities.attach: true`, synthesized by the proxy on top of the upstream agent's reply.

## How it works

- **One subprocess per session.** Each `?session=` value spawns a fresh `--agent-cmd` subprocess. Multiple subscribers on the same session share that subprocess.
- **JSON-RPC envelope routing.** The mux parses only the envelope (`id`, `method`, `params`, `result`, `error`). Payloads are forwarded byte-for-byte. Policy keys off the `method` string.
- **Per-session id translation.** Each subscriber's request `id` is rewritten to a per-session `mux_id` before forwarding; the response is rewritten back and sent only to the originator.
- **`initialize` / `session/new` caching.** First response is cached; later joiners are answered locally without re-sending to the agent.
- **Broadcast agent-initiated requests.** Agent-initiated requests (e.g. `session/request_permission`) are fanned out to every attached subscriber; any peer can reply. The first reply for a given id is forwarded to the agent and later replies for the same id are dropped, so the agent always sees exactly one response.
- **Turn serialization.** Concurrent `session/prompt` while a turn is in flight is rejected with JSON-RPC `-32001`. The last subscriber to issue a substantive request is still surfaced as the "driving subscriber" in `/debug/sessions` and `amux/turn_started` for UI attribution.
- **`amux/*` notification namespace.** The mux publishes its own metadata out-of-band: `amux/peer_joined`, `amux/peer_left`, `amux/turn_started`, `amux/turn_complete`, `amux/session_busy`, `amux/agent_request_resolved`. ACP frames stay clean; clients see two distinguishable channels and demultiplex by method prefix.
- **Replay log.** Every broadcast-tier frame (`amux/*` + agent notifications) is appended; a late joiner receives the full history before any live event.
- **TTL grace.** Last subscriber leaving starts a countdown; a reconnect within `--session-ttl-seconds` reuses the same subprocess with all of its caches intact.

## Client contract

Clients SHOULD:

- Treat `amux/peer_joined` (with `peerId == self.peer_id`) as the empty-roster signal — used only by replay log late joiners.
- Treat `amux/turn_started` / `amux/turn_complete` as turn bookends; the `peerId` field attributes the turn.
- Filter `amux/*` frames out of the conversation render and use them for presence / turn UI.
- Allow the mux to rewrite request `id` fields freely (preserve client-side correlation by tracking your own original ids).

Detailed protocol spec: [`docs/design/amux-namespace.md`](docs/design/amux-namespace.md).

## Docs

- Protocol spec: [`docs/design/amux-namespace.md`](docs/design/amux-namespace.md)
- Build plan: [`ROADMAP.md`](ROADMAP.md)
- Release notes: [`CHANGELOG.md`](CHANGELOG.md)

## License

MIT — see [LICENSE](LICENSE).
