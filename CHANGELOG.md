# Changelog

## Unreleased — RFD #533 alignment

### Added

- **`session/attach` and `session/detach` methods** ([RFD #533](https://github.com/agentclientprotocol/agent-client-protocol/pull/533)). Both are intercepted by the proxy and never forwarded to the agent. `session/attach` returns `{ sessionId, clientId, historyPolicy, connectedClients[], history? }` — the connected-peers roster plus an optional history snapshot. `session/detach` returns `{ sessionId, status: "detached" }` and gracefully closes the WebSocket (code 1000).
- **`historyPolicy` parameter on attach.** Accepted values: `full` (default), `pending_only`, `none`, `after_message`. `after_message` falls back to `full` until the Message ID RFD is adopted. History entries carry the broadcast frame's `method` and `params` verbatim (amux stays envelope-only — no translation into the RFD's typed entry shape).
- **Pending-permission re-issue on attach.** When a client calls `session/attach` while one or more `session/request_permission` requests are still InFlight, the proxy re-delivers each frame to that client only — same id, so the existing first-writer-wins gate handles the eventual reply normally. Without this, late joiners saw the permission in `history` but had no actionable request to answer.
- **RFD-shape `session/update` siblings** emitted alongside the existing `amux/*` metadata frames whenever an ACP session id (from the cached `session/new` response) is known:
  - `prompt_received` (sibling of `amux/turn_started`) — carries `prompt` and `sentBy: { clientId, name }`.
  - `turn_complete` (sibling of `amux/turn_complete`) — carries `stopReason`.
  - `permission_resolved` (sibling of `amux/agent_request_resolved`) — carries `requestId`, `resolvedBy`, `chosenOptionId` (lifted from `result.outcome.optionId` when present), and the verbatim `result`/`error`.
  - `client_disconnected` (sibling of `amux/peer_left`) — carries `client: { clientId, name }`.
  - Distinguished from agent-emitted `session/update` frames by `update.type` (proxy) vs `update.kind` (agent).
- **`sessionCapabilities.attach` advertised in `initialize`.** The proxy mutates the upstream agent's initialize result to inject `agentCapabilities.sessionCapabilities.attach: true` before caching and before sending to the originator, so RFD-aware clients can detect multi-client support without knowing about amux.

### Preserved beyond the RFD

- All existing `amux/*` frames continue to be emitted (dual-emit). amux clients keep working unchanged; RFD-aware clients route off `session/update`.
- Per-session id translation, handshake caching, turn serialization with `-32001`, driving-subscriber attribution, replay log, TTL reconnect grace, and the WS query attach (`?session=&peer_id=&peer_name=&role=`) are unchanged.

## v0.1.1 — 2026-05-21

### Changed

- **Agent-initiated requests now broadcast.** `session/request_permission` (and any other agent → subscriber request) fans out to every attached subscriber instead of being delivered only to the driving subscriber. Any peer can reply; the first reply for a given id is forwarded to the agent and later replies are dropped so the agent still sees exactly one response per id. The "driving subscriber" concept remains for UI attribution (`amux/turn_started`, `/debug/sessions`) but no longer gates who can answer the agent.
- **New `amux/agent_request_resolved` notification.** When the first-reply-wins gate flips a tracked agent-initiated request to consumed, the mux broadcasts `{ sessionId, requestId, resolvedBy, result | error }` to every attached subscriber. Peers that lost the race (or never replied) use this to dismiss the request from their UI; the responder's own UI ignores it (the entry is already gone locally). For `session/request_permission` the `result` body is derived entirely from option metadata already present in the broadcast request, so no new information leaks.
- **Turn-end cleanup for abandoned agent-initiated requests.** When `session/prompt` completes with `agent_pending` entries still `InFlight` (e.g. hermes' internal 60s permission deadline fired without a response frame), the mux now sweeps those entries to `Consumed` and broadcasts one `amux/agent_request_resolved { resolvedBy: "mux:turn-ended", result: null, error: null }` per stale id immediately before `amux/turn_complete`. This unblocks TUI clients that would otherwise show a permission prompt the agent has already given up on. No competing wire-level timeout is added on the mux side — it only emits cleanup after the agent has signaled the turn is done.
- **`mock_acp` permission emission updated to ACP spec shape.** `MOCK_ACP_EMIT_PERMISSION=1` now emits the canonical `session/request_permission` (was: an ad-hoc `permission/request`) with the proper `params.toolCall` and `params.options[{optionId, kind, name}]`. Reply shape: `result.outcome = {outcome: "selected", optionId} | {outcome: "cancelled"}`.

## v0.1.0

Initial release. Ten chunks per the roadmap, ~50 tests.

### Architecture

- Per-session-per-subprocess model: each `?session=<id>` spawns a fresh `--agent-cmd` subprocess.
- JSON-RPC envelope parsing only; payloads flow byte-for-byte.
- Single-threaded actor task per session serializes all state mutation through a `SessionMsg` queue (`Attach`, `Detach`, `InboundFromSubscriber`, `AgentStdoutLine`, `AgentDied`, `Snapshot`).
- Subscriber outbound channel uses `bytes::Bytes` so broadcast fan-out is a cheap atomic ref-count clone.

### Multiplex behavior

- **Per-session id translation.** Subscriber request ids are rewritten to per-session mux ids; responses are rewritten back and routed only to the originator.
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
- `GET /debug/sessions` → JSON snapshot of every live session (subscribers, pending request count, cache state, active turn, driving sub, TTL pending, replay log length, next mux id, next amux turn id)

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
