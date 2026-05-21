# Changelog

## v0.1.0

Initial release. Ten chunks per the roadmap, ~50 tests.

### Architecture

- Per-session-per-subprocess model: each `?session=<id>` spawns a fresh `--agent-cmd` subprocess.
- JSON-RPC envelope parsing only; payloads flow byte-for-byte.
- Single-threaded actor task per session serializes all state mutation through a `SessionMsg` queue (`Attach`, `Detach`, `InboundFromSubscriber`, `AgentStdoutLine`, `AgentDied`, `Snapshot`).
- Subscriber outbound channel uses `bytes::Bytes` so broadcast fan-out is a cheap atomic ref-count clone.

### Multiplex behavior

- **Per-session id translation.** Subscriber request ids are rewritten to per-session bridge ids; responses are rewritten back and routed only to the originator.
- **Handshake caching.** First `initialize` and `session/new` responses are cached; later joiners are answered locally.
- **Driving subscriber.** Whoever last sent a substantive request becomes the target for agent-initiated requests; falls back to an arbitrary attached subscriber on detach.
- **Turn serialization.** Concurrent `session/prompt` while a turn is in flight is rejected with JSON-RPC `-32001` and broadcasts `amux/session_busy`.
- **Replay log.** Every broadcast-tier frame is appended; late joiners receive the full history before any live event. `--replay-turns unbounded|0|N` (N accepted, behaves as unbounded; bounded eviction deferred to v0.2).
- **TTL reconnect grace.** `--session-ttl-seconds` (default 60) keeps the subprocess alive after the last subscriber leaves; a reconnect within the window reuses the same session.
- **Agent death.** Subprocess stdout EOF closes attached subscribers with structured WS code 1011.

### `amux/*` namespace

Out-of-band metadata frames the mux emits:

- `amux/peer_joined`, `amux/peer_left` — presence
- `amux/turn_started`, `amux/turn_complete` — turn bookends with `amuxTurnId: at-<n>`
- `amux/session_busy` — concurrent-prompt rejection broadcast

Spec: [`docs/design/amux-namespace.md`](docs/design/amux-namespace.md).

### Operational surface

- `GET /healthz` → `200 ok`
- `GET /acp` → WebSocket upgrade with query validation:
  - `session` matches `^[A-Za-z0-9_-]{1,128}$`
  - Missing required fields → close 4400
  - `peer_id` collision → close 4409
  - No `--agent-cmd` configured or agent spawn failure → close 1011
- `GET /debug/sessions` → JSON snapshot of every live session (subscribers, pending request count, cache state, active turn, driving sub, TTL pending, replay log length, next bridge id, next amux turn id)

### Testing

- 53 tests, including end-to-end integration against `mock_acp` (a small Rust binary speaking NDJSON ACP over stdin/stdout) and against `cat` for byte-relay tests.
- Manual verification against real `hermes acp` (hermes-agent 0.14.0): `initialize` cache proven against three sequential requests.
- `cargo fmt --check` + `cargo clippy --all-targets -- -D warnings` clean in CI.

### Deferred to v0.2 / future

- Bounded replay-log eviction (`--replay-turns N` for N > 0).
- Per-subscriber auth (token-based).
- Subprocess crash auto-restart with `amux/session_restored`.
- Session discovery API (`GET /sessions`).
- Concurrent turn queue mode (`--turn-policy=queue`).
- `/metrics` Prometheus endpoint.
- Session sharing URLs (one-time attach links).
