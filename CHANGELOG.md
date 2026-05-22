# Changelog

## Unreleased — session/load canonical rebinding

### Fixed

- **Multi-client + `session/load` desync.** After a peer issued a successful `session/load` to switch the room to a different session, amux's cached canonical session id was still the original one from `session/new`. Late joiners that called `session/new` got the stale id back and silently desynced from the agent's actual current session. Now amux rebinds the room's canonical session id on every successful `session/load`:
  - When `session_new_cache` already exists, its `sessionId` field is replaced in place; other fields the agent returned are preserved.
  - When no prior `session/new` happened (client called `initialize` → `session/load` directly), amux synthesizes a minimal `{sessionId: "..."}` cache value.
  - Failed `session/load` (error response from the agent) leaves the existing cache untouched.
- `/debug/sessions` `cachedSessionId` reflects the loaded session id after a successful load, so operators can verify the rebinding without inspecting the wire.

### Notes

- Hermes 0.14.0 advertises `agentCapabilities.loadSession = true`, so this scenario is reachable on the canonical agent today.
- `mock_acp` knob: `MOCK_ACP_FAIL_LOAD=1` returns an error to `session/load` (for testing that failures don't rebind).

## Unreleased — session/list

### Added

- **`session/list` support via envelope passthrough.** When the upstream agent advertises `agentCapabilities.sessionCapabilities.list` ([Draft RFD](https://github.com/agentclientprotocol/agent-client-protocol/blob/main/docs/rfds/session-list.mdx)), amux propagates the capability to clients verbatim and forwards `session/list` requests through the standard id-translation path. The agent's response — the `sessions[]` array, optional `nextCursor`, `cwd` filter handling — flows back unmodified.
- **`mock_acp` knob**: `MOCK_ACP_SESSION_LIST=1` advertises the capability and serves a canned three-entry session list with `cwd` filtering. Used to test end-to-end passthrough.

### Notes

- amux doesn't intercept or rewrite `session/list`. The agent is the source of truth; amux is plumbing. Decorating the response with amux-known per-session info (subscriber counts, proxy session ids) is tracked in [#6](https://github.com/lsaether/acp-mux/issues/6).
- Cold-start discovery — listing the agent's persisted sessions *before* WS-attaching to one — is not supported (amux's WS contract requires a session id on every connection). Tracked as [#10](https://github.com/lsaether/acp-mux/issues/10).

## Unreleased — cancellation

### Added

- **`$/cancel_request` support, both directions.** Implements the [request-cancellation RFD](https://github.com/agentclientprotocol/agent-client-protocol/blob/main/docs/rfds/request-cancellation.mdx).
  - **Subscriber → agent**: strict per-peer semantics. A subscriber can cancel only its own in-flight requests. amux looks up `(peer_id, original_id)` in the pending table, rewrites `requestId` to the corresponding `mux_id`, and forwards to the agent. Cross-peer cancels (B cancelling A's id) are dropped silently — that's what `amux/cancel_active_turn` is for.
  - **Agent → subscribers**: when the agent emits `$/cancel_request` for an agent-initiated request still InFlight (in practice `session/request_permission`), amux marks the `agent_pending` entry Consumed (so late subscriber replies are dropped), forwards the cancellation to every subscriber, and emits `amux/agent_request_resolved { resolvedBy: "agent:cancelled" }`.
- **`amux/cancel_active_turn` extension.** Any attached peer can cancel the in-flight turn (not just the driver). amux looks up `active_turn_mux_id`, broadcasts `amux/turn_cancelled { sessionId, amuxTurnId, cancelledBy, originalDriver, reason? }` to every peer (intent), and synthesizes a `$/cancel_request` to the agent using the active-turn `mux_id`. Strict `$/cancel_request` semantics still apply on the southbound side — the agent never sees the amux extension.
- **`amux/turn_cancelled` notification.** Intent broadcast emitted on `amux/cancel_active_turn`. Distinct from `amux/turn_complete` (which fires later when the agent actually settles).

### Notes

- Cancellation is optional per the RFD. amux forwards cancellations honestly; if the agent doesn't honor them and finishes normally, subscribers see the regular response. Documented limitation, not amux's job.
- The synthesized `$/cancel_request` from `amux/cancel_active_turn` is indistinguishable to the agent from a cancel sent by the original driver — same wire shape, same `mux_id`.

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
