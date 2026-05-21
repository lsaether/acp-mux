# acp-mux roadmap

Build plan for `acp-mux`. Protocol contract is locked in
`docs/design/bridge-namespace.md`; this doc covers *when* and *how* the
implementation lands.

**Status legend:** `[ ]` not started · `[~]` in progress · `[x]` done

---

## Principles

- Parse JSON-RPC envelopes only; payloads are `serde_json::Value`
- Method-name string matching is the only ACP-aware policy hook
- `bridge/*` namespace carries every multiplex fact
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
- Full `bridge/*` namespace (turn_started, turn_complete, peer_joined, peer_left, session_busy)
- Unbounded broadcast-tier replay log with attach-time playback
- `/debug/sessions` introspection endpoint

---

## v0.1 — initial multiplex implementation

**Goal:** ship a non-opinionated ACP multiplexer with the full `bridge/*`
namespace, multi-subscriber session sharing via id translation and
handshake caching, turn serialization, and an unbounded replay log. Single
static binary.

**Sizing:** ~7–10 focused days, 10 chunks. Each chunk is a self-contained
commit/PR.

### Phase A — core multiplex routing

#### Chunk 1 — Scaffold + JSON-RPC envelope `½ day` — **done** (`f53adcb`)

- [x] `cargo new --bin acp-mux` (run from `~/Code/acp-mux`)
- [x] add deps: tokio, axum, tokio-tungstenite, serde, serde_json, tracing, tracing-subscriber, clap, anyhow, thiserror, url, futures
- [x] module skeleton: `src/{main, cli, server, session/{mod,registry,state}, agent/{mod,process}, protocol/{mod,jsonrpc,bridge}, multiplex/{mod,subscriber}}`
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

#### Chunk 3 — Session registry + single-subscriber byte relay `½ day`

- [ ] `session/registry.rs`: `SessionRegistry` — `attach`/`detach`/`shutdown`, lock-guarded `HashMap<session_id, SessionState>`
- [ ] `session/state.rs`: `SessionState` — owns `AgentProcess`, `Vec<Subscriber>`, dispatcher task
- [ ] `multiplex/subscriber.rs`: `Subscriber` — peer_id, peer_name, role, mpsc::UnboundedSender for outbound frames
- [ ] dispatcher: read agent stdout line-by-line, write to all subscribers (no parsing yet)
- [ ] outbound: read subscriber inbound, write to agent stdin (no parsing yet)
- [ ] **DoD:** one subscriber per session, raw byte relay both directions, `initialize` round-trips through `acp-mux` against a real ACP server

#### Chunk 4 — Multi-subscriber fan-out + id translation + handshake caching `1–2 days`

- [ ] subscriber set: multiple subscribers per `?session=` permitted
- [ ] notification fan-out: parse JSON-RPC envelope on inbound; if no `id` → broadcast to all
- [ ] id translation table: per-session `next_bridge_id`, `pending_requests: HashMap<bridge_id, (Subscriber, original_id)>`
- [ ] outbound request rewriting (client `id` → `bridge_id` before forwarding)
- [ ] inbound response routing (rewrite `bridge_id` → `original_id`, send only to originator)
- [ ] `initialize` cache: first one forwarded; later ones answered locally from cached `result`
- [ ] `session/new` cache: same pattern, cached `sessionId` replayed for late subscribers
- [ ] **DoD:** two subscribers on same session: A sends `initialize` first, B's `initialize` answered from cache; A sends a prompt, both see streaming notifications; B's prompt response goes only to B

#### Chunk 5 — Driving subscriber + agent-initiated request routing `½ day`

- [ ] `SessionState.driving_subscriber: Option<Subscriber>` updated on every substantive (non-`initialize`) request
- [ ] inbound frame with both `id` and `method` (agent-initiated request) routes to driving subscriber only
- [ ] driving subscriber gone → fall back to first subscriber; no subscribers → drop with warning
- [ ] **DoD:** `permission/request` from agent reaches the subscriber that initiated the turn, not other subscribers; driving-sub disconnect mid-request falls through correctly

#### Chunk 6 — Turn serialization `½ day`

- [ ] `SessionState.active_turn_bridge_id: Option<u64>` set when `session/prompt` is forwarded
- [ ] concurrent `session/prompt` while turn active → reject with JSON-RPC error code `-32001` to the requester
- [ ] `active_turn_bridge_id` cleared when matching response arrives
- [ ] **DoD:** two simultaneous `session/prompt` requests: first forwarded, second gets `-32001`; after first completes, a new prompt succeeds

### Phase B — bridge namespace + replay

#### Chunk 7 — `bridge/*` namespace notifications `1 day`

- [ ] `protocol/bridge.rs`: serde types for `BridgeTurnStarted`, `BridgeTurnComplete`, `BridgePeerJoined`, `BridgePeerLeft`, `BridgeSessionBusy`
- [ ] emit `bridge/turn_started` on `session/prompt` forward (before sending to subprocess) — broadcast to all subscribers, `content` mirrored verbatim from request `params.prompt`
- [ ] emit `bridge/turn_complete` when `session/prompt` response lands (or on abnormal turn termination) — broadcast with `stopReason`
- [ ] emit `bridge/peer_joined` on attach (broadcast to existing subscribers)
- [ ] emit `bridge/peer_left` on detach (broadcast to remaining subscribers)
- [ ] emit `bridge/session_busy` on turn-rejection (finalize payload shape from chunk 6 stub)
- [ ] `bridgeTurnId` allocation: monotonic per session (e.g. `bt-<n>`)
- [ ] **DoD:** two-subscriber session: A prompts, both see `bridge/turn_started { peerId: A }` followed by ACP chunks followed by `bridge/turn_complete`; B's join triggers `peer_joined` to A

#### Chunk 8 — Replay log + `--replay-turns` flag `½–1 day`

- [ ] `SessionState.broadcast_log: VecDeque<Bytes>` — append every broadcast-tier frame (raw bytes, no introspection)
- [ ] log excludes: responses to specific subscribers, agent-initiated requests
- [ ] on subscriber attach: flush full log to newcomer before adding them to live broadcast set; queue live events during replay and drain after
- [ ] `--replay-turns 0` → skip the replay entirely (do not maintain log either, to avoid wasted memory)
- [ ] `--replay-turns unbounded` (default) → append-only, no eviction
- [ ] `--replay-turns N` (N > 0) → **stub**: accept the value, log warning that bounded eviction is not yet implemented, fall through to unbounded behavior. Bounded eviction logic deferred (see v0.2)
- [ ] **DoD:** subscriber A prompts and completes a turn; subscriber B attaches and receives a full replay (peer_joined for A, turn_started + ACP chunks + turn_complete) before any live events; ordering preserved across replay/live boundary

### Phase C — lifecycle + polish

#### Chunk 9 — TTL reconnect grace `½ day`

- [ ] `SessionState.ttl_task: Option<JoinHandle<()>>` scheduled on last-subscriber-leave
- [ ] new subscriber attaching cancels pending TTL task
- [ ] TTL expiry → tear down subprocess + remove session from registry
- [ ] subprocess crash → set `subprocess_dead = true`, skip TTL grace, close subscribers with WS code 1011
- [ ] **DoD:** disconnect → reconnect within TTL → same subprocess (verify via debug snapshot); disconnect → wait past TTL → subprocess gone

#### Chunk 10 — Tests + `/debug/sessions` + README + v0.1.0 cut `1–1½ days`

- [ ] `tests/fake_acp.rs` — a minimal NDJSON ACP server fixture for deterministic integration tests
- [ ] integration tests covering each chunk's DoD scenarios
- [ ] `/debug/sessions` GET endpoint returning JSON snapshot (subscribers, pending requests, cached initialize, cached session id, active turn, driving sub, subprocess_dead, ttl_pending, replay log length, next bridge id)
- [ ] README: install, run, CLI flags table, architecture diagram, client contract, link to design doc
- [ ] CHANGELOG.md noting v0.1.0
- [ ] tag `v0.1.0`
- [ ] **DoD:** `cargo test` green; `acp-mux` runs end-to-end with two `websocat` subscribers against the fixture; `/debug/sessions` reflects state correctly

---

## v0.2 — bounded replay + persistence

Not committed; ideas only.

- Bounded eviction for `--replay-turns N` (N > 0) using `bridge/turn_complete` bookends to mark eviction points
- Persistent on-disk log (survives `acp-mux` restart)
- `/debug/replay-log` introspection endpoint
- Replay log compaction (drop superseded `tool_call_update` frames)

## v1.0 — future scope

- Per-subscriber auth (token-based, separate from transport trust)
- Subprocess crash recovery + auto-restart + `bridge/session_restored` event
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
- [ ] `bridgeTurnId` format — `bt-<u64>` is fine; confirm in chunk 7.
- [ ] Replay log storage type — lean `VecDeque<bytes::Bytes>` for cheap clone on fan-out. Confirm in chunk 8.
- [ ] `/debug/sessions` schema — fields to surface: subscribers (address + peerId + isDriving), pending requests, cached initialize/sessionId, active turn, driving sub, subprocess_dead, ttl_pending, replay log length, next bridge id. Confirm in chunk 10.
