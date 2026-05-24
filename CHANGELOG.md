# Changelog

## Unreleased

### Fixed

- **Active-turn cancellation for Hermes-backed sessions.** `amux/cancel_active_turn` now forwards ACP-native `session/cancel { sessionId }` for the active prompt while preserving the immediate `amux/turn_cancelled` intent broadcast and later `amux/turn_complete` settlement event. Fixes [#29](https://github.com/lsaether/acp-mux/issues/29).

## v0.1.2 — 2026-05-23

### Added

- **HTTP control-plane session discovery.** `GET /acp/sessions?cwd=<optional>` runs a transient agent-side `session/list` before any WebSocket attach, returns the agent's result JSON directly, and tears the subprocess down without creating live mux session state. Covers the cold-start dashboard workflow tracked in [#10](https://github.com/lsaether/acp-mux/issues/10) / [#24](https://github.com/lsaether/acp-mux/pull/24).
- **Opt-in mux trace metadata propagation.** `--meta-propagate` injects mux-owned trace fields into subscriber → agent requests at `params._meta.amux` after id translation, preserving existing `_meta` and unknown `amux` keys. This gives debugging clients `peerId`, `peerName`, `role`, `muxId`, and prompt `amuxTurnId` without changing the default payload contract. Closes [#6](https://github.com/lsaether/acp-mux/issues/6) via [#21](https://github.com/lsaether/acp-mux/pull/21).
- **Live `session/list` response decoration.** `session/list` responses now annotate entries that match live muxed upstream sessions under `sessions[i]._meta.amux` with `proxySessionId`, `subscriberCount`, and optional `drivingSubscriber`, while preserving agent-owned metadata and leaving non-live entries unchanged. Closes [#22](https://github.com/lsaether/acp-mux/issues/22) via [#23](https://github.com/lsaether/acp-mux/pull/23).
- **Replay provenance metadata.** Late-join replay frames now carry mux provenance under `params._meta.amux`, including the original `recordedAt` timestamp and monotonic `replaySeq`, without altering live fan-out frames. Closes [#18](https://github.com/lsaether/acp-mux/issues/18) via [#20](https://github.com/lsaether/acp-mux/pull/20).

### Fixed

- **Mixed-session replay after `session/load`.** Successful `session/load` now establishes a replay-generation boundary so late joiners do not receive stale replay frames from the previous upstream ACP session. amux preserves load-time history for the loaded session, rebuilds current peer presence for the new generation, and exposes replay-generation observability through `/debug/sessions`. Fixes [#17](https://github.com/lsaether/acp-mux/issues/17) via [#19](https://github.com/lsaether/acp-mux/pull/19).

### Notes

- `--meta-propagate` is still opt-in; default subscriber → agent request payloads remain unchanged.
- `session/list` decoration is additive and scoped to `_meta.amux`; existing agent-owned `_meta` keys are preserved.
- Open follow-ups after this release: [#2](https://github.com/lsaether/acp-mux/issues/2) remains the high-priority non-Hermes fs/terminal delegation bug, [#7](https://github.com/lsaether/acp-mux/issues/7) remains upstream RFD tracking, and [PR #3](https://github.com/lsaether/acp-mux/pull/3) remains a conflicting/shelved RFD #533 alignment branch.

## v0.1.1 — 2026-05-22

### Added

- **`$/cancel_request` support, both directions.** Implements the [request-cancellation RFD](https://github.com/agentclientprotocol/agent-client-protocol/blob/main/docs/rfds/request-cancellation.mdx).
  - **Subscriber → agent**: strict per-peer semantics. A subscriber can cancel only its own in-flight requests. amux looks up `(peer_id, original_id)` in the pending table, rewrites `requestId` to the corresponding `mux_id`, and forwards to the agent. Cross-peer cancels (B cancelling A's id) are dropped silently — that's what `amux/cancel_active_turn` is for.
  - **Agent → subscribers**: when the agent emits `$/cancel_request` for an agent-initiated request still InFlight (in practice `session/request_permission`), amux marks the `agent_pending` entry Consumed (so late subscriber replies are dropped), forwards the cancellation to every subscriber, and emits `amux/agent_request_resolved { resolvedBy: "agent:cancelled" }`.
- **`amux/cancel_active_turn` extension.** Any attached peer can cancel the in-flight turn (not just the driver). amux looks up `active_turn_mux_id`, broadcasts `amux/turn_cancelled { sessionId, amuxTurnId, cancelledBy, originalDriver, reason? }` to every peer (intent), and synthesizes a `$/cancel_request` to the agent using the active-turn `mux_id`. Strict `$/cancel_request` semantics still apply on the southbound side — the agent never sees the amux extension.
- **`amux/turn_cancelled` notification.** Intent broadcast emitted on `amux/cancel_active_turn`. Distinct from `amux/turn_complete` (which fires later when the agent actually settles).
- **`session/list` support via envelope passthrough.** When the upstream agent advertises `agentCapabilities.sessionCapabilities.list` ([Draft RFD](https://github.com/agentclientprotocol/agent-client-protocol/blob/main/docs/rfds/session-list.mdx)), amux propagates the capability to clients verbatim and forwards `session/list` requests through the standard id-translation path. The agent's response — the `sessions[]` array, optional `nextCursor`, `cwd` filter handling — flows back unmodified. amux is plumbing; decorating with amux-known per-session info is tracked in [#6](https://github.com/lsaether/acp-mux/issues/6).
- **`mock_acp` knobs** for the new code paths: `MOCK_ACP_ECHO_CANCELS=1`, `MOCK_ACP_CANCEL_PERMISSION=1`, `MOCK_ACP_SESSION_LIST=1`, `MOCK_ACP_FAIL_LOAD=1`.

### Changed

- **Agent-initiated requests broadcast.** `session/request_permission` (and any other agent → subscriber request) fans out to every attached subscriber instead of being delivered only to the driving subscriber. Any peer can reply; the first reply for a given id is forwarded to the agent and later replies are dropped so the agent still sees exactly one response per id. The "driving subscriber" concept remains for UI attribution (`amux/turn_started`, `/debug/sessions`) but no longer gates who can answer the agent.
- **New `amux/agent_request_resolved` notification.** When the first-reply-wins gate flips a tracked agent-initiated request to consumed, the mux broadcasts `{ sessionId, requestId, resolvedBy, result | error }` to every attached subscriber. Peers that lost the race (or never replied) use this to dismiss the request from their UI; the responder's own UI ignores it (the entry is already gone locally). For `session/request_permission` the `result` body is derived entirely from option metadata already present in the broadcast request, so no new information leaks.
- **Turn-end cleanup for abandoned agent-initiated requests.** When `session/prompt` completes with `agent_pending` entries still `InFlight` (e.g. hermes' internal 60s permission deadline fired without a response frame), the mux now sweeps those entries to `Consumed` and broadcasts one `amux/agent_request_resolved { resolvedBy: "mux:turn-ended", result: null, error: null }` per stale id immediately before `amux/turn_complete`. This unblocks TUI clients that would otherwise show a permission prompt the agent has already given up on. No competing wire-level timeout is added on the mux side — it only emits cleanup after the agent has signaled the turn is done.
- **`mock_acp` permission emission updated to ACP spec shape.** `MOCK_ACP_EMIT_PERMISSION=1` now emits the canonical `session/request_permission` (was: an ad-hoc `permission/request`) with the proper `params.toolCall` and `params.options[{optionId, kind, name}]`. Reply shape: `result.outcome = {outcome: "selected", optionId} | {outcome: "cancelled"}`.
- **Crate split into lib + bin.** The crate is now `amux` (lib) consumed by the `amux` binary. Integration tests live under `tests/server.rs` and link the lib directly; pure-unit tests for private helpers (`strip_trailing_newline`, `validate`, `is_valid_session_id`) stay inline in `src/server.rs`. Fixes CI (`cargo test` now builds `mock_acp` automatically as a test dep via `CARGO_BIN_EXE_mock_acp`, no special workflow step). Lib name matches the bin name so `RUST_LOG=amux=trace` covers everything from a single filter.

### Fixed

- **Multi-client + `session/load` desync.** After a peer issued a successful `session/load` to switch the room to a different session, amux's cached canonical session id was still the original one from `session/new`. Late joiners that called `session/new` got the stale id back and silently desynced from the agent's actual current session. Now amux rebinds the room's canonical session id on every successful `session/load`:
  - When `session_new_cache` already exists, its `sessionId` field is replaced in place; other fields the agent returned are preserved.
  - When no prior `session/new` happened (client called `initialize` → `session/load` directly), amux synthesizes a minimal `{sessionId: "..."}` cache value.
  - Failed `session/load` (error response from the agent) leaves the existing cache untouched.
  - `/debug/sessions` `cachedSessionId` reflects the loaded session id after a successful load.
- **Spurious "no live subscribers after fan-out; ending session" log.** The first `amux/peer_joined` broadcast on session creation fired against an empty subscriber map (the new subscriber isn't inserted until after the broadcast), making `broadcast()` log "ending session" — but nothing actually ended. The log now only fires when fan-out *drained* a previously non-empty subscriber map.

### Notes

- Cancellation is optional per the RFD. amux forwards cancellations honestly; if the agent doesn't honor them and finishes normally, subscribers see the regular response.
- Hermes 0.14.0 advertises `agentCapabilities.loadSession = true` and `sessionCapabilities.list = {}`, so both the load-rebinding fix and `session/list` passthrough are exercised end-to-end against the canonical agent.
- Cold-start session discovery (listing the agent's persisted sessions *before* WS-attaching) is not supported in this release — amux's WS contract requires a session id on every connection. Tracked as [#10](https://github.com/lsaether/acp-mux/issues/10).

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
