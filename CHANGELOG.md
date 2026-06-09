# Changelog

## Unreleased

### Breaking

- **Collaboration layer renamed `amux` → `rooms`.** The crate, library, and
  binary are now `rooms`, and the wire namespace changed accordingly:
  - `amux/*` notification/control methods → `rooms/*` (e.g. `amux/turn_started`
    → `rooms/turn_started`, `amux/queue_prompt` → `rooms/queue_prompt`).
  - `_meta.amux` → `_meta.rooms` (on `session/attach` results and propagated
    request metadata); the `session/list` decoration key moved likewise.
  - The `amuxTurnId` field is now `roomsTurnId`.
  Clients written against the previous `amux/*` releases must update method
  names and `_meta` keys. The lower-level core crate/binary stays `acp-mux`.

### Added

- **Two-crate workspace: core mux vs Rooms layer.** The repo is now a Cargo
  workspace with a hard, compiler-enforced boundary:
  - `acp-mux` (lib `acp_mux`, binary `acp-mux`) — the standalone generic 1→N
    ACP multiplexer. Id translation, response routing, first-writer-wins
    agent-request fan-in, `initialize`/`session/new` caching, `fs/*`/`terminal/*`
    safety, plain replay/late-join, and an RFD-#533-baseline
    `session/attach`/`session/detach`. It contains zero `rooms/*` knowledge and
    does not depend on the `rooms` crate. The standalone binary attaches on
    `?mux=<id>`.
  - `rooms` (lib `rooms`, binary `rooms`) — the Rooms collaboration protocol,
    implemented as a `MuxExtension` plugged into the core mux actor. Owns turns,
    queue/steer/cancel, presence, segments, `_meta.rooms` attach
    enrichment, and all `rooms/*` frames. Depends on `acp-mux`. The `rooms`
    binary attaches on `?room=<id>`.
  - The boundary is realized through a `MuxExtension` trait + `MuxCtx`
    capability surface in core (core ships a no-op extension; `rooms` provides
    the real one). There is now a single multiplexer implementation.
- **Standalone `acp-mux` binary.** A pure one-agent-to-many-clients mux with no
  collaboration layer, for clients that only need raw ACP mirroring.
- **Optional persistent replay store.** `--replay-store <DIR>` persists
  broadcast-tier room history as append-only JSONL and rehydrates replay
  frames/segment bookends on restart. The upstream agent still owns actual
  conversation state.
- **Client contract fixtures.** `docs/examples/client-contract/` contains
  copyable request/response/notification JSON fixtures for `session/attach`,
  turn lifecycle, queue lifecycle, agent-request lifecycle, replay markers,
  and segment lineage.

### Changed

- **Library reorganized into two crates.** Core multiplexing moved from
  `src/room/state.rs` (`RoomInner`) into `crates/acp-mux/src/mux/` (`MuxCore` +
  actor) with no `rooms/*` concerns; the collaboration behavior moved into
  `crates/rooms/src/extension/` (`RoomsExtension: MuxExtension`). The `rooms`
  crate's `RoomRegistry`/`server` are now thin wrappers over the core
  `MuxRegistry::with_extension(...)`. Aside from the `amux/*` → `rooms/*`
  namespace rename above, the `rooms` binary's behavior is otherwise unchanged;
  the integration suite and `docs/examples/client-contract/` fixtures were
  updated to the new namespace and still pass.
- **Provider-neutral core contract.** The mainline mux is now documented and
  implemented as a generic ACP multiplexer / agent mirror rather than a
  provider-specific adapter. Provider metadata is passed through opaquely;
  mux-owned lifecycle state is driven only by JSON-RPC envelopes, ACP method
  names, `session/load`, and observable ACP `params.sessionId` changes.
- **Docs reframed around rooms, mirrors, and generic ACP clients.** README,
  roadmap, and design docs now describe `acp-mux` as a reusable ACP room
  server with provider-neutral safety defaults and an explicit `rooms/*`
  side channel.

### Removed

- Removed provider-specific stderr parsing, provider-specific CLI toggles,
  provider-named types/end reasons, and provider-only lifecycle frames from
  the core path.

## v0.1.3 — 2026-05-27

### Breaking

- **`?room=` replaces `?session=`.** WebSocket attaches now key on
  `?room=<id>`. `?session=<id>` is accepted as a deprecated alias and
  logs a one-shot WARN per attach; remove the alias next release.
  Specifying both query params returns close `4400`.
- **`amux/*` frames carry `roomId` instead of `sessionId`** where they
  previously referred to the mux-level id (peer_joined, peer_left,
  session_context, turn_started, turn_complete, session_busy, queue
  lifecycle, agent_request_opened/resolved, control_submitted,
  turn_cancelled, replay_started/complete). Frames that pass through
  upstream ACP `sessionId` payloads are unchanged.
- **`session/list` decoration field renamed.** Live mux annotations now
  appear under `sessions[i]._meta.amux.roomId` (was `proxySessionId`).
- **`/debug/sessions` JSON keys renamed.** Top-level `sessions`/`sessionCount`
  → `rooms`/`roomCount`; per-room `sessionId` field is now `roomId`.
- **Library type rename.** `src/session/*` moved to `src/room/*`;
  `SessionInner` → `RoomInner`, `SessionMsg` → `RoomMsg`,
  `SessionRegistry` → `RoomRegistry`, `SessionHandle` → `RoomHandle`,
  `SessionSnapshot` → `RoomSnapshot`, `SessionOptions` → `RoomOptions`,
  `spawn_session` → `spawn_room`. `SessionListMetadataIndex` keeps its
  name (it indexes ACP session ids, not rooms).

### Added

- **Rooms-as-transcripts abstraction.** A room now owns one or more
  segments, each pinned to a single canonical ACP `sessionId`. Rotation
  is detected from `session/load` responses and observable ACP
  `sessionId` changes in agent notifications. See `docs/design/rooms.md`
  for the full model and invariants. Closes [#56](https://github.com/lsaether/acp-mux/issues/56) via [#58](https://github.com/lsaether/acp-mux/pull/58).
- **Segment lifecycle frames.** `amux/segment_started` and
  `amux/segment_ended` mark rotation boundaries in both live broadcast
  and the replay log. Both flow through the standard `broadcast()` path
  and carry the same `_meta.amux { recordedAt, replaySeq }` envelope as
  every other broadcast-tier frame.
- **`historyPolicy: full_lineage`.** `session/attach` accepts a new
  policy that returns every segment's frames in `replaySeq` order.
  `historyPolicy: full` continues to return current-segment-only
  history (with pre-segment bootstrap frames included), plus any
  `amux/turn_*` lifecycle bookend from a prior segment whose
  `amuxTurnId` brackets the active segment.
- **`snapshot.segments`.** `session/attach` results expose lineage at
  `result._meta.amux.snapshot.segments` (with `activeSegmentId`) so
  even `historyPolicy: full` clients can see that earlier segments
  exist.
- **`--emit-segment-frames` flag.** Default `true`. Set `false` to
  suppress `amux/segment_*` emission for clients that haven't picked up
  the new frame methods yet. Internal segment state still tracks
  rotation; only the wire is gated.
- **RFD #533-inspired `session/attach` / `session/detach`.** Proxy-local
  methods answered by the mux instead of forwarded to the agent. Attach
  callers receive `connectedClients`, an effective `historyPolicy`, and
  shape-able replay history under `result._meta.amux`; detach returns a
  clean acknowledgment before the WS closes. Lifecycle resolution stays
  authoritative under `amux/*` — the mux still does not fabricate
  proxy-owned `session/update` siblings. Closes [#5](https://github.com/lsaether/acp-mux/issues/5) via [#46](https://github.com/lsaether/acp-mux/pull/46).
- **Attach-response replay ordering.** `params._meta.amux.replayOrder`
  accepts `chronological` (default) or `newest_turn_first` for
  `session/attach` history; the effective value is echoed back as
  `result._meta.amux.appliedReplayOrder`. Closes [#44](https://github.com/lsaether/acp-mux/issues/44) via [#47](https://github.com/lsaether/acp-mux/pull/47).
- **Streamed `session/attach` history.** Opt-in via
  `params._meta.amux.historyDelivery: "stream"`: the attach response
  carries snapshot metadata, then replay frames stream through
  `amux/replay_started` / `amux/replay_complete` markers. Backfill is
  paced through the session actor so live traffic interleaves cleanly.
  Closes [#52](https://github.com/lsaether/acp-mux/issues/52) via [#53](https://github.com/lsaether/acp-mux/pull/53).
- **`/acp?...&replay=skip`** suppresses the legacy WebSocket auto-replay
  for attach-aware clients that want `session/attach.result.history` as
  their single bootstrap source. Default WS replay behavior is
  unchanged. Closes [#48](https://github.com/lsaether/acp-mux/issues/48) via [#50](https://github.com/lsaether/acp-mux/pull/50).
- **`amux/session_context` attach notification.** Carries the mux/agent
  cwd to newly attached subscribers and is exposed under
  `/debug/sessions` for operator visibility. Closes [#13](https://github.com/lsaether/acp-mux/issues/13) via [#35](https://github.com/lsaether/acp-mux/pull/35).
- **Replay-safe agent request lifecycle openings.** Agent-initiated
  requests now emit inert `amux/agent_request_opened` notifications
  before the live raw request, preserving request context for
  late-join replay without replaying actionable ACP requests. Late
  joiners see `amux/agent_request_opened` followed by
  `amux/agent_request_resolved`; live subscribers still answer only
  the original `session/request_permission`. Closes [#31](https://github.com/lsaether/acp-mux/issues/31) via [#33](https://github.com/lsaether/acp-mux/pull/33).

### Fixed

- **Cross-segment turn bookends in `historyPolicy: full`.** When a turn
  straddled a segment boundary (an observable ACP session id change
  mid-turn, or `session/load` with an in-flight turn that completes after the load
  response), the default `historyPolicy: full` left late joiners with
  an unmatched `amux/turn_complete` because the matching
  `amux/turn_started` lived in the prior segment and was filtered out
  by the current-segment-only rule. `full` now carries
  `amux/turn_started` / `amux/turn_complete` / `amux/turn_cancelled`
  bookends from prior segments when their `amuxTurnId` brackets the
  active segment or matches the currently active turn; non-lifecycle
  frames (agent chunks) from prior segments stay excluded — those
  belong to `full_lineage`. Scoped subset of [#57](https://github.com/lsaether/acp-mux/issues/57) via [#59](https://github.com/lsaether/acp-mux/pull/59).
- **Block unsafe agent client-tool fanout.** Agent-initiated `fs/*` and
  `terminal/*` requests are now blocked at the mux by default and
  answered to the agent with structured JSON-RPC `-32000` instead of
  being raw-broadcast to subscribers. Collaborative
  `session/request_permission` fanout is preserved; the old raw fanout
  remains available behind `--unsafe-debug-client-tool-broadcast` for
  diagnostics only. `clientCapabilities.fs` / `.terminal` are stripped
  from forwarded `initialize` requests. Fixes [#2](https://github.com/lsaether/acp-mux/issues/2) via [#36](https://github.com/lsaether/acp-mux/pull/36).
- **AMUX active-turn steer/queue controls.** `amux/steer_active_turn` and `amux/queue_prompt` are now the canonical control surface. Ordinary concurrent `session/prompt` requests—including text that starts with `/steer ...` or `/queue ...`—remain serialized and receive `-32001`. `amux/steer_active_turn` now performs mux-owned hard steer while a turn is active: it broadcasts `amux/control_submitted` and `amux/turn_cancelled`, sends ACP-native `session/cancel`, then starts a replacement turn with prompt-injected superseded-turn context. When idle, the same steer request submits immediately as the next prompt with `mode: "prompt"` and no cancellation or queue lifecycle. A second active hard steer is rejected while the first replacement is still pending. `amux/queue_prompt` now stores mux-owned queue/send state, caps pending public queue items at six (`-32003 "queue full"`), broadcasts/replays `amux/queue_item_*` lifecycle, submits immediately when idle, and submits queued prompts as real follow-up turns after active-turn settlement. Pending queue items survive owner disconnects without creating ghost drivers, emit orphan notifications, and can be removed with `amux/unqueue_prompt`. Fixes [#39](https://github.com/lsaether/acp-mux/issues/39) via [#41](https://github.com/lsaether/acp-mux/pull/41); future native/soft steer is tracked separately in [#42](https://github.com/lsaether/acp-mux/issues/42).
- **Active-turn cancellation for active prompts.** `amux/cancel_active_turn` now forwards ACP-native `session/cancel { sessionId }` for the active prompt while preserving the immediate `amux/turn_cancelled` intent broadcast and later `amux/turn_complete` settlement event. Fixes [#29](https://github.com/lsaether/acp-mux/issues/29) via [#30](https://github.com/lsaether/acp-mux/pull/30).
- **Control-plane agent timeout.** Raised the transient `session/list`
  agent-spawn timeout from 2s to 8s to accommodate slower agent startup
  budgets. [#34](https://github.com/lsaether/acp-mux/pull/34).

### Notes

- Persistence (rooms surviving mux restart) is tracked in [#26](https://github.com/lsaether/acp-mux/issues/26); the natural seam is `RoomInner::{ replay_log, segments }` and is left explicit in the source for that follow-up.
- The deprecated `?session=` alias is one-release-only; remove in v0.1.4.

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
- Open follow-ups after this release: [#2](https://github.com/lsaether/acp-mux/issues/2) remains the high-priority delegated fs/terminal client-tool support bug, [#7](https://github.com/lsaether/acp-mux/issues/7) remains upstream RFD tracking, and [PR #3](https://github.com/lsaether/acp-mux/pull/3) remains a conflicting/shelved RFD #533 alignment branch.

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
- **Turn-end cleanup for abandoned agent-initiated requests.** When `session/prompt` completes with `agent_pending` entries still `InFlight` (for example, an agent-side permission deadline fired without a response frame), the mux now sweeps those entries to `Consumed` and broadcasts one `amux/agent_request_resolved { resolvedBy: "mux:turn-ended" }` per stale id immediately before `amux/turn_complete`; cleanup frames omit `result` and `error` because no peer reply exists. This unblocks TUI clients that would otherwise show a permission prompt the agent has already given up on. No competing wire-level timeout is added on the mux side — it only emits cleanup after the agent has signaled the turn is done.
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
- The mock ACP harness exercises load-session rebinding and `session/list` passthrough in CI.
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
- Manual verification against a stdio ACP agent: `initialize` cache proven against three sequential requests.
- `cargo fmt --check` + `cargo clippy --all-targets -- -D warnings` clean in CI.

### Deferred to v0.2 / future

- Bounded replay-log eviction (`--replay-turns N` for N > 0).
- Per-subscriber auth (token-based).
- Subprocess crash auto-restart with `amux/session_restored`.
- Session discovery API (`GET /sessions`).
- Concurrent turn queue mode (`--turn-policy=queue`).
- `/metrics` Prometheus endpoint.
- Session sharing URLs (one-time attach links).
