# acp-mux roadmap

Build plan for `acp-mux`. Protocol contract is locked in
`docs/design/amux-namespace.md`; this doc covers *when* and *how* the
implementation lands.

**Status legend:** `[ ]` not started · `[~]` in progress · `[x]` done

---

## Principles

- Parse JSON-RPC envelopes only; payloads are `serde_json::Value`
- Method-name string matching is the only ACP-aware policy hook
- `amux/*` namespace carries every multiplex fact
- No synthesized in-band ACP frames, ever
- Single static binary, no runtime dependencies beyond the agent subprocess

## Required v0.1 behaviors

- Per-subprocess-per-session model (one agent child per `?session=`)
- Per-subscriber JSON-RPC id translation table
- `initialize` response caching + replay for late subscribers
- `session/new` response caching + replay (so all subscribers share one ACP session)
- Driving-subscriber tracking for agent-initiated request routing
- Turn serialization (one in-flight `session/prompt` per session, reject concurrents with `-32001`)
- TTL reconnect grace on last-subscriber-leave
- Full `amux/*` namespace (turn_started, turn_complete, peer_joined, peer_left, session_busy)
- Unbounded broadcast-tier replay log with attach-time playback
- `/debug/sessions` introspection endpoint

---

## v0.1 — initial multiplex implementation

**Goal:** ship a non-opinionated ACP multiplexer with the full `amux/*`
namespace, multi-subscriber session sharing via id translation and
handshake caching, turn serialization, and an unbounded replay log. Single
static binary.

**Sizing:** ~7–10 focused days, 10 chunks. Each chunk is a self-contained
commit/PR.

### Phase A — core multiplex routing

#### Chunk 1 — Scaffold + JSON-RPC envelope `½ day` — **done** (`f53adcb`)

- [x] `cargo new --bin acp-mux` (run from `~/Code/acp-mux`)
- [x] add deps: tokio, axum, tokio-tungstenite, serde, serde_json, tracing, tracing-subscriber, clap, anyhow, thiserror, url, futures
- [x] module skeleton: `src/{main, cli, server, session/{mod,registry,state}, agent/{mod,process}, protocol/{mod,jsonrpc,amux}, multiplex/{mod,subscriber}}`
- [x] `protocol/jsonrpc.rs`: `Incoming` enum, `IncomingRequest`/`IncomingNotification`/`IncomingResponse`, `JsonRpcError`, parser (aliases dropped — same shape suffices for outgoing)
- [x] update `README.md` (one-line, install placeholder, CLI placeholder)
- [x] CI: rustfmt + clippy + cargo test on push (GitHub Actions)
- [x] `.gitignore`, `LICENSE` (MIT)
- [x] **DoD:** `cargo build`/`cargo test` pass; JSON-RPC envelope fixtures round-trip request/response/notification shapes (12 tests)

#### Chunk 2 — Subprocess driver + WS server skeleton `½–1 day` — **done** (`c1f5b26`)

- [x] `agent/process.rs`: `AgentProcess` — spawn via `tokio::process::Command`, NDJSON over stdin/stdout, graceful stop (close stdin, wait, kill on timeout)
- [x] `cli.rs`: clap config — `--host`, `--port`, `--agent-cmd`, `--session-ttl-seconds`, `--replay-turns`, `--log-level`
- [x] `server.rs`: axum app with `/healthz` (GET) + `/acp` (WS upgrade)
- [x] WS query parsing: `session`, `peer_id`, `peer_name`, `role`; `session` validated against `^[A-Za-z0-9_-]{1,128}$`
- [x] invalid query → WS close code 4400; `peer_id` collision → close 4409 (per-session `HashSet<peer_id>` placeholder; replace with `SessionRegistry` in chunk 3)
- [x] **DoD:** `acp-mux` launches; `curl /healthz` returns ok; WS connect + close round-trips cleanly; invalid `?session=` rejected with 4400 (15 chunk-2 tests; 27 total)

#### Chunk 3 — Session registry + single-subscriber byte relay `½ day` — **done**

- [x] `session/registry.rs`: `SessionRegistry` — `attach`/`shutdown`, lock-guarded `HashMap<session_id, SessionHandle>` (detach is signalled to the actor and processed there)
- [x] `session/state.rs`: per-session actor task owning `AgentProcess` + `HashMap<peer_id, Subscriber>`, driven by a `SessionMsg` enum (Attach/Detach/InboundFromSubscriber/AgentStdoutLine/AgentDied)
- [x] `multiplex/subscriber.rs`: `Subscriber` — peer_id, peer_name, role, mpsc::UnboundedSender for outbound frames
- [x] dispatcher: read agent stdout line-by-line, fan out to every attached subscriber (naive — chunk 4 layers id translation + handshake caching)
- [x] outbound: read subscriber inbound, strip trailing `\n`, write to agent stdin (no JSON parsing)
- [x] last-subscriber-leave → immediate session teardown; chunk 9 adds the TTL grace
- [x] agent stdout EOF → drop subscribers (causes WS close); chunk 9 will switch to explicit code 1011
- [x] missing `--agent-cmd` / agent spawn failure → close 1011
- [x] **DoD:** raw byte relay both directions; `initialize` round-trips through `acp-mux` against `hermes acp` (hermes-agent 0.14.0); 30 tests green including cat-loopback round-trip, two-subscriber naive fan-out, peer_id collision, no-agent-cmd 1011

#### Chunk 4 — Multi-subscriber fan-out + id translation + handshake caching `1–2 days` — **done**

- [x] subscriber set: multiple subscribers per `?session=` permitted (already in chunk 3)
- [x] notification fan-out: parse JSON-RPC envelope on inbound; notifications broadcast to all subscribers
- [x] id translation table: per-session `next_bridge_id: u64`, `pending: HashMap<u64, PendingRequest{peer_id, original_id, handshake}>`
- [x] outbound request rewriting (client `id` → `bridge_id` before forwarding, original preserved in pending mapping)
- [x] inbound response routing (rewrite `bridge_id` → `original_id`, send only to originator)
- [x] `initialize` cache: first response cached; later `initialize` requests answered locally without touching the agent
- [x] `session/new` cache: same pattern
- [x] agent-initiated requests: chunk-4 interim routes to one arbitrary subscriber; chunk 5 replaces with explicit driving-subscriber routing
- [x] frames with non-JSON content: agent → subscribers falls back to raw broadcast; subscriber → agent is dropped with a warn
- [x] **DoD:** integration tests (mock_acp) prove initialize/session-new caching, id translation across numeric and string ids, prompt notifications broadcast to all subscribers, prompt responses route only to originator. Manual verify against `hermes acp` (hermes-agent 0.14.0): 3 sequential `initialize` requests → hermes logs "Initialize from unknown" exactly **once**; client receives 3 distinct responses with original ids 1/2/3.

#### Chunk 5 — Driving subscriber + agent-initiated request routing `½ day` — **done**

- [x] `SessionInner.driving_subscriber_peer_id: Option<String>` updated on every substantive (non-`initialize`) request from a subscriber, including cached `session/new` short-circuits
- [x] inbound frame with both `id` and `method` (agent-initiated request) routes to driving subscriber only
- [x] driving subscriber gone → fall back to one arbitrary attached subscriber; no subscribers → drop with warn. Driver field also cleared at detach time
- [x] **DoD:** integration test `agent_request_routes_to_driving_subscriber` (mock_acp with `MOCK_ACP_EMIT_PERMISSION=1`) proves `permission/request` reaches A only when A drives; `agent_request_falls_through_when_driver_left` proves the detach-fallback path

#### Chunk 6 — Turn serialization `½ day` — **done**

- [x] `SessionInner.active_turn_bridge_id: Option<u64>` set when `session/prompt` is forwarded
- [x] concurrent `session/prompt` while turn active → reject with JSON-RPC error code `-32001` ("session busy") to the requester; does not consume a bridge_id and does not update the driver
- [x] `active_turn_bridge_id` cleared when the response matching that bridge_id arrives
- [x] **DoD:** `concurrent_prompt_rejected_with_32001` (mock_acp with `MOCK_ACP_PROMPT_DELAY_MS=600`) proves second prompt is rejected with `-32001`, A's prompt completes, B can issue a fresh prompt after A's turn clears

### Phase B — amux namespace + replay

#### Chunk 7 — `amux/*` namespace notifications `1 day` — **done**

- [x] `protocol/amux.rs`: frame builders + `AmuxTurnId(u64)` newtype with `at-<u64>` formatting. Typed inner-params structs serialize camelCase with `skip_serializing_if = Option::is_none` for optional fields
- [x] emit `amux/turn_started` on `session/prompt` forward (before agent receives) — broadcast to all subscribers, `content` lifted verbatim from `params.prompt`
- [x] emit `amux/turn_complete` when the response with matching `bridge_id` arrives — `stopReason` lifted from `result.stopReason` (or `null` if absent)
- [x] emit `amux/peer_joined` on attach — broadcast BEFORE inserting newcomer (so newcomer doesn't see their own join; replay log will surface it to later joiners)
- [x] emit `amux/peer_left` on detach — broadcast to remaining subscribers
- [x] emit `amux/session_busy` on `-32001` rejection — `heldBy` = current turn's originator peer_id
- [x] `amuxTurnId` allocation: monotonic per session, `at-<u64>`
- [x] **DoD:** integration tests prove the full event lifecycle:
  - `amux_peer_joined_and_peer_left` — A alone sees nothing; B joins, A sees `peer_joined{peerId:B,peerName:Bob}`; B doesn't see their own join; B detaches, A sees `peer_left`
  - `amux_turn_started_and_complete` — A's prompt with `content=[{type:text,text:hi}]` produces `turn_started{amuxTurnId:at-1, content:[…]}` to both A and B before mock_acp responds, and `turn_complete{amuxTurnId:at-1, stopReason:end_turn}` after
  - `amux_session_busy_on_concurrent_prompt` — B's concurrent prompt produces `session_busy{busy:true, heldBy:A}` to B (broadcast)

#### Chunk 8 — Replay log + `--replay-turns` flag `½–1 day` — **done**

- [x] `SessionInner.replay_log: Option<VecDeque<Bytes>>` — `None` when policy is `Disabled` so no memory is reserved
- [x] `Subscriber.outbound` channel switched to `mpsc::UnboundedSender<Bytes>`; fan-out is now an atomic ref-count bump per subscriber rather than a full memcpy. `ws_out_task` consumes `Bytes` and converts to `Utf8Bytes` for the text frame
- [x] `broadcast()` is the single broadcast-tier gate: appends to log (if enabled) AND fans out to subscribers in one place. Per-subscriber frames (responses, agent-initiated requests) bypass `broadcast()` and therefore the log
- [x] on attach: snapshot the log BEFORE emitting the newcomer's own `peer_joined`, then emit + broadcast + insert + send snapshot to newcomer. Because the actor serializes all SessionMsg handling, no live frames interleave
- [x] `--replay-turns 0` → `ReplayTurns::Disabled` → `replay_log = None`
- [x] `--replay-turns unbounded` (default) → `ReplayTurns::Unbounded` → empty `VecDeque`, append-only, no eviction
- [x] `--replay-turns N` (N > 0) → stub: warn at session creation, behave as unbounded for v0.1
- [x] **DoD:** `replay_log_delivers_history_to_late_joiner` proves A completes a full turn, B attaches afterward and receives `peer_joined(A)` → `turn_started` → two `session/update` → `turn_complete` in order; A's prompt response (per-subscriber) is NOT in B's replay. `replay_turns_disabled_emits_no_history` proves the disabled policy keeps B's queue empty until the next live event

### Phase C — lifecycle + polish

#### Chunk 9 — TTL reconnect grace `½ day` — **done**

- [x] TTL grace driven by `tokio::select!` in the actor loop with an optional `Pin<Box<tokio::time::Sleep>>` (no separate JoinHandle to track or cancel — drop the Sleep to "cancel"). Simpler than the roadmap's `ttl_task` sketch and avoids self-channel ownership issues
- [x] new subscriber attaching takes the Sleep out of the option (cancellation)
- [x] TTL expiry → break out of actor loop → `agent.shutdown()` + pump abort + session_handle becomes dead
- [x] subprocess crash (stdout EOF → AgentDied) → `close_all_subscribers(1011, "agent subprocess exited")` then teardown — TTL grace is skipped entirely
- [x] structured WS close plumbed via `OutMsg::Close { code, reason }` in `Subscriber.outbound`; chunk-3's "just drop the sender" path is replaced for the agent-death case (defaults still drop for clean disconnects)
- [x] CLI `--session-ttl-seconds` flows through `SessionRegistry::new` → `spawn_session` → `run_session`
- [x] **DoD:** integration tests prove all three behaviors:
  - `ttl_grace_cancelled_by_reconnect` — A connects, A initializes, A disconnects; live_session_count stays 1 during grace; B reconnects within grace; B's `initialize` is answered from A's cache (`_invocation: 1`), proving same subprocess
  - `ttl_grace_expires_when_idle` — A disconnects with no reconnect; live_session_count stays 1 during grace, drops to 0 after expiry
  - `agent_death_closes_subscribers_with_1011` — agent is `sleep 0.4`; subscriber attaches, agent exits; WS close frame arrives with application code 1011

#### Chunk 10 — Tests + `/debug/sessions` + README + v0.1.0 cut `1–1½ days` — **done**

- [x] `src/bin/mock_acp.rs` serves the canonical NDJSON ACP fixture role (the roadmap's `tests/fake_acp.rs` lives at `src/bin/` instead, so it's a `cargo` bin alongside the test binaries and tests reach it via `CARGO_MANIFEST_DIR + target/<profile>/mock_acp`). Knobs: `MOCK_ACP_SESSION_ID`, `MOCK_ACP_EMIT_PERMISSION`, `MOCK_ACP_PROMPT_DELAY_MS`
- [x] integration tests covering every chunk's DoD (see chunks 1–9). 53 tests total
- [x] `GET /debug/sessions` returns `{sessions: [...], sessionCount: N}`. Each session: `sessionId`, `subscribers[{peerId, peerName, role, isDriving}]`, `pendingRequestCount`, `initializeCached`, `cachedSessionId`, `activeTurnBridgeId`, `activeAmuxTurnId`, `drivingSubscriber`, `subprocessDead`, `ttlPending`, `replayLogLen`, `nextBridgeId`, `nextAmuxTurnId`
- [x] README: install, run snippet, CLI flags table, architecture summary, client contract, link to design doc
- [x] CHANGELOG.md noting v0.1.0
- [x] tag `v0.1.0`
- [x] **DoD:** `cargo test` green; `acp-mux` runs end-to-end against real `hermes acp` (manual verification in chunks 3 + 4); `debug_sessions_reflects_live_state` proves the snapshot endpoint reflects post-initialize/post-session-new state correctly

---

## v0.2 — bounded replay + persistence

Not committed; ideas only.

- Bounded eviction for `--replay-turns N` (N > 0) using `amux/turn_complete` bookends to mark eviction points
- Persistent on-disk log (survives `acp-mux` restart)
- `/debug/replay-log` introspection endpoint
- Replay log compaction (drop superseded `tool_call_update` frames)

## v1.0 — future scope

- Per-subscriber auth (token-based, separate from transport trust)
- Subprocess crash recovery + auto-restart + `amux/session_restored` event
- Session discovery API (`GET /sessions`)
- Concurrent turn handling — queue mode (`--turn-policy=queue`)
- Multi-session per subprocess (if upstream agents support it)
- Metrics endpoint (`/metrics`, Prometheus)
- Session sharing URLs (one-time attach links)
- Recording / playback for eval datasets

## Explicitly out of scope (forever)

- Changes to upstream ACP servers. Stay a pure consumer of public ACP.
- Embedding a full terminal.
- Cross-host federation (one `acp-mux` per host).
- ACP protocol modeling beyond JSON-RPC envelopes + method-name routing.

---

## Open implementation questions (resolve before / during chunks)

- [ ] Axum vs raw hyper for the WS surface — lean axum for ergonomics. Confirm in chunk 2.
- [ ] WS frame size limits and backpressure policy — defer to chunk 10 polish, document the v0.1 default.
- [ ] `amuxTurnId` format — `at-<u64>` is fine; confirm in chunk 7.
- [ ] Replay log storage type — lean `VecDeque<bytes::Bytes>` for cheap clone on fan-out. Confirm in chunk 8.
- [ ] `/debug/sessions` schema — fields to surface: subscribers (address + peerId + isDriving), pending requests, cached initialize/sessionId, active turn, driving sub, subprocess_dead, ttl_pending, replay log length, next bridge id. Confirm in chunk 10.
