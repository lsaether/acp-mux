# Refactor plan: split `acp-mux` core from the `rooms` protocol layer

**Status:** first pass landed but INCOMPLETE — corrective pass required. See §0.
**Audience:** the engineer/agent executing the corrective pass.
**Owner decisions captured:** workspace + two crates; `Mux*` naming in core;
core gets its own binary; #533 conformance is in scope; no behavior change to
the existing `rooms` binary's wire output.

---

## 0. STATUS UPDATE (post-verification) — READ THIS FIRST

A first implementation pass was completed and verified. It **builds and all
tests pass (153)**, but it **did not deliver the agreed architecture**. Do not
restart from scratch; the goal of this corrective pass is to finish the part
that was skipped.

### What is actually on disk now

- ✅ **Workspace + two crates** exist (`crates/acp-mux`, `crates/rooms`); `rooms`
  depends on `acp-mux`. Builds clean.
- ✅ **The core crate (`acp-mux`) is good and real.** `crates/acp-mux/src/mux/actor.rs`
  (~1100 lines) is a genuine, standalone, standards-shaped 1→N multiplexer:
  id translation, first-writer-wins agent-request fan-in, `initialize`/
  `session/new` caching, `fs/*`/`terminal/*` safety, a #533-baseline
  `session/attach`/`session/detach` (top-level `connectedClients` roster +
  history policies), `?mux=` param, its own working binary, 21 unit tests, and
  **zero `rooms` references** (grep-clean). Keep this crate as the single source
  of truth for core multiplexing.
- ❌ **The `rooms` crate is a VERBATIM FORK of the original codebase.** These
  files are byte-for-byte identical to pre-refactor `HEAD` (0-line diffs):
  `room/state.rs` (3838 lines), `room/registry.rs`, `room/attach.rs`,
  `protocol/rooms.rs`, `server.rs`, `tests/server.rs`. The `rooms` crate only
  re-exports **5 leaf modules** from core (`agent::process`, `jsonrpc`,
  `subscriber`, `replay_store`, `cli` types) and **reimplements the entire
  multiplexer itself**.
- ❌ **There is no `MuxExtension` trait / hook seam anywhere** (grep for
  `MuxExtension`/`MuxCtx`/`Extension` finds nothing). The central mechanism of
  this plan was skipped.
- ❌ **The `rooms` binary runs its own forked multiplexer**, not core's. Its
  `main.rs`/`server.rs` import `rooms::room::registry::RoomRegistry` and
  `rooms::room::state::{RoomMsg,RoomSnapshot}` — the duplicate — and never touch
  `acp_mux::mux::MuxRegistry`/`spawn_mux`.

### Why this fails the goal

Core multiplexing logic now lives in **two divergent implementations** (core
`mux/actor.rs` and rooms `room/state.rs`). Every future routing/caching/fan-in
bug fix or ACP change must be made twice and will drift. The rooms monolith we
set out to decompose is **completely untouched** — inside `crates/rooms` there
is no layer boundary at all. "rooms lives on top of core" is false: rooms
duplicates core wholesale.

The tests passing is not evidence of success — the rooms suite passes *because
it is literally the original code*. That same suite is the byte-compat oracle
for the corrective pass.

### Corrective objective

Make the `rooms` binary run **core's one multiplexer**, with all `rooms/*`
behavior provided through a **`MuxExtension`** plugged into core. After this
pass there must be exactly ONE actor loop and ONE set of routing/caching/
fan-in logic (in core); `crates/rooms/src/room/state.rs` and the forked
`registry.rs`/`attach.rs`/`server.rs` routing must be **deleted**. Concrete
spec in §6; corrective stages in §9; acceptance gates in §11.

Sections §1–§5, §7, §8, §10 describe the intended design and remain valid (§10
lightly updated). §6 (the seam), §9 (stages), and §11 (acceptance gates) have
been rewritten for the corrective pass.

---

## 1. Goal

Today `acp-mux` is one crate where a single actor (`src/room/state.rs`,
~3.8k lines) interleaves two concerns line-by-line:

1. **Core multiplexing** — run one stdio ACP agent, present N WebSocket
   clients to it as if they were one well-behaved ACP client, route
   responses/notifications/agent-initiated requests correctly.
2. **The `rooms` protocol** — turns, queue/steer/cancel, presence, segments
   ("rooms"), replay ordering/streaming, `session/list` decoration,
   `_meta.rooms` enrichment.

We are splitting these into **two crates in a Cargo workspace** so that the
core has **no compile-time dependency on `rooms`** (the compiler enforces the
boundary — core literally cannot `use` rooms because it isn't a dependency).

End state:

```
acp-mux  (crate, lib `acp_mux`, bin `acp-mux`)   ← core: pure 1→N ACP multiplexer
rooms     (crate, lib `rooms`,    bin `rooms`)       ← depends on acp-mux; adds the rooms protocol
```

The `rooms` binary's external behavior (every `rooms/*` frame, every
`session/attach` byte, every test in the current suite, every
`docs/examples/client-contract` fixture) must be **unchanged**. The new
`acp-mux` binary is greenfield: a standards-conformant, no-frills 1→N mux.

---

## 2. The guiding principle that resolves every boundary question

**#533 is the standard for "multiplex one agent to many clients."** It
standardizes `session/attach`/`session/detach`, a `connectedClients` roster,
first-writer-wins permission resolution, history replay
(`full`/`pending_only`/`none`/`after_message`), and turn/presence broadcasts.
That is exactly core's job, so:

- **Core = the standards-compliant multiplexer for one ACP session with N
  clients**, including the #533 multi-client baseline. This is how "tiny
  core" and "track the protocol exactly" stop fighting each other.
- **`rooms` = everything #533 leaves to extension, plus the "room"
  abstraction** (segments / lineage across `session/load`). Rooms are not in
  #533 → rooms are purely `rooms`.

Two invariants fall out of this and must hold structurally:

- **I1 — Agent channel is pure ACP.** Core owns the agent subprocess. Only
  standard ACP frames ever reach the agent. The `rooms` layer can never write
  raw bytes to the agent; it can only ask core to perform *sanctioned* ACP
  actions (submit a prompt, send `session/cancel`). This is what guarantees
  compatibility with any standards-compliant agent. (`rooms/cancel_active_turn`
  already works this way — it is translated to ACP `session/cancel`.)
- **I2 — Core never emits or parses `rooms/*`.** Core has no knowledge of the
  `rooms/*` namespace, turns, queues, or segments. Anything `rooms/*` is
  produced by the extension.

---

## 3. Naming (locked)

| Concept | Core (`acp-mux`) | `rooms` |
|---|---|---|
| Crate / lib | `acp-mux` / `acp_mux` | `rooms` / `rooms` |
| Binary | `acp-mux` | `rooms` |
| The 1-agent/N-client container | **`Mux`** (was `Room`) — `MuxRegistry`, `MuxHandle`, `MuxMsg`, `MuxOptions`, `MuxSnapshot`, `MuxInner`/actor | n/a (uses core) |
| The higher-level container | n/a | **`Room`** (segments/lineage live here) |
| Connect query param | `?mux=<id>` | `?room=<id>` (unchanged, `?session=` alias kept) |
| Connected client | `Subscriber` (kept; neutral) | `Subscriber` via core |

"Session" stays reserved for the ACP-layer `sessionId`; the core container is
a **Mux**.

---

## 4. Target workspace layout

```
acp-mux/                              (workspace root)
├── Cargo.toml                        # [workspace] members = ["crates/acp-mux", "crates/rooms"]
├── crates/
│   ├── acp-mux/                      # CORE
│   │   ├── Cargo.toml                # name="acp-mux", [lib] name="acp_mux", [[bin]] name="acp-mux"
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── main.rs               # pure 1→N mux binary
│   │   │   ├── cli.rs                # core flags only
│   │   │   ├── server.rs            # core WS/HTTP surface (?mux=, /healthz, /acp, /acp/sessions, /debug/sessions baseline)
│   │   │   ├── jsonrpc.rs           # was protocol/jsonrpc.rs (pure ACP envelopes)
│   │   │   ├── subscriber.rs        # was multiplex/subscriber.rs
│   │   │   ├── agent/process.rs     # unchanged subprocess driver
│   │   │   ├── attach.rs            # #533 session/attach + session/detach standard shapes + baseline handler
│   │   │   ├── extension.rs         # MuxExtension trait, MuxCtx, NoopExtension
│   │   │   └── mux/
│   │   │       ├── mod.rs
│   │   │       ├── registry.rs      # was room/registry.rs (MuxRegistry, control-plane session/list)
│   │   │       ├── core.rs          # MuxCore state + core methods (the carved-down state.rs)
│   │   │       ├── actor.rs         # MuxMsg, spawn_mux, run_mux loop
│   │   │       └── replay_store.rs  # plain log persistence (segment_id field kept, treated as opaque tag)
│   │   └── tests/                    # core/#533 tests (use `cat` / in-process fixtures)
│   └── rooms/                         # Rooms PROTOCOL
│       ├── Cargo.toml                # name="rooms", [lib] name="rooms", [[bin]] name="rooms", deps: acp-mux
│       ├── src/
│       │   ├── lib.rs
│       │   ├── main.rs               # core + RoomsExtension binary
│       │   ├── cli.rs                # rooms-only flags, composes core cli
│       │   ├── server.rs             # ?room= + enriched /debug/sessions (wraps/composes core server)
│       │   ├── registry.rs           # composes MuxRegistry, injects RoomsExtension factory + rooms options
│       │   ├── extension/
│       │   │   ├── mod.rs            # RoomsExtension: MuxExtension (state + hook impls)
│       │   │   ├── turns.rs          # turn bookends, busy, prompt serialization UX
│       │   │   ├── queue.rs          # queue/steer/unqueue + submit-next
│       │   │   ├── segments.rs       # rooms: segments, lineage, replay-generation, session/load rotation
│       │   │   ├── presence.rs       # peer_joined/left, session_context, driving subscriber, session/list index
│       │   │   ├── permissions.rs    # agent_request_opened/resolved projection, pending re-issue, turn-end sweep
│       │   │   └── attach_views.rs   # _meta.rooms enrichment, history shaping (full=segment, full_lineage), streaming backfill
│       │   └── protocol/
│       │       ├── rooms.rs           # rooms/* frame builders, RoomsTurnId, SegmentId, EndReason
│       │       └── attach.rs         # _meta.rooms attach types (AttachRoomsMeta, AttachSnapshot, ...)
│       ├── src/bin/mock_acp.rs       # test fixture agent (stays with the integration tests)
│       └── tests/server.rs           # the existing integration suite, moved here ~verbatim
└── docs/ …                           # updated (see §10)
```

Notes:
- `mock_acp` stays in the `rooms` crate next to the migrated integration tests
  (Cargo only auto-sets `CARGO_BIN_EXE_mock_acp` for bins in the test's own
  package). Core tests use `cat` and in-process fixtures, matching the
  existing `agent/process.rs` unit tests.
- The `rooms` crate may `pub use acp_mux::…` for types its tests reference, to
  minimize churn in moved tests.

---

## 5. Responsibility partition (authoritative)

### 5.1 CORE owns

State (`MuxCore`):
- `mux_id: String` (connect/instance key; was `room_id`)
- `agent_cwd: String` (connection metadata; exposed to ext via ctx)
- `subscribers: HashMap<String, Subscriber>`
- `next_mux_id: u64`, `pending: HashMap<u64, PendingRequest>` (id translation)
- `initialize_cache`, `session_new_cache` (late-joiner dedup; #533)
- `canonical_session_id: Option<String>` (set on `session/new` capture and
  `session/load`; exposed to ext; **core does not segment on it**)
- `replay_log: Option<VecDeque<ReplayEntry>>` — plain chronological log
- `next_replay_seq: u64`
- `replay_store: Option<RoomReplayStore>` — plain persistence
- `client_tool_policy: ClientToolPolicy` (fs/terminal safety)
- `agent_pending: HashMap<Id, AgentReqState>` (first-writer-wins fan-in; #533)
- `pending_permissions: Vec<(Id, Bytes)>` (unresolved permissions, for #533
  `pending_only` baseline + fan-in cleanup)
- `prompt_in_flight: Option<u64>` (the **≤1-in-flight-prompt backstop**)
- `self_tx`, `pending_agent_writes: Vec<Vec<u8>>` (buffer for ext-requested
  agent writes — see I1)
- `extension: Box<dyn MuxExtension>`

`ReplayEntry { frame, recorded_at, seq, ext_tag: u64 }` — `ext_tag` is opaque
to core (rooms stores its `SegmentId` there). Persisted as the existing
`segment_id` JSONL field (format preserved byte-for-byte).

`PendingRequest { peer_id, original_id, handshake: Option<HandshakeKind>, deliver_response: bool }`.
`HandshakeKind` in core: `Initialize | SessionNew | SessionLoad { loaded_session_id }`.
(The rooms-only `decorate_session_list`, `queue_item_id`, `replay_start_len`,
turn association move to extension-side per-`mux_id` tracking.)

Core logic:
- envelope parse + dispatch (`handle_inbound`)
- request translation: id rewrite, `initialize`/`session/new` cache
  short-circuit, `sanitize_initialize_client_capabilities` (strip
  `clientCapabilities.fs`/`.terminal`), handshake detection, **prompt-in-flight
  backstop**, forward to agent
- agent response routing: id restore, handshake caching, `session/load`
  canonical rebind, deliver to originator, clear `prompt_in_flight`
- agent-initiated request fan-in: `client_tool_policy` block, broadcast raw to
  all subscribers, record `agent_pending=InFlight`, track `pending_permissions`
- `gate_subscriber_response` (first-writer-wins dedup → forward first to agent)
- `$/cancel_request` from subscriber (map to mux_id, forward) and from agent
  (dedup + raw broadcast)
- subscriber attach: insert + deliver plain replay snapshot (unless `replay=skip`)
- subscriber detach: remove, report emptiness
- `session/attach` / `session/detach`: **#533 baseline** (see §7)
- `broadcast` (append+persist+fanout), response send helpers
- `set_canonical_session_id` (set field + notify ext)
- actor loop, `MuxMsg`, `spawn_mux`, `MuxHandle`, `MuxOptions`, `MuxRegistry`
- baseline `/debug/sessions` snapshot (core fields)

### 5.2 Rooms owns (extension state + hooks)

State (`RoomsExtension`):
- turns: `active_rooms_turn_id`, `active_turn_session_id`,
  `active_turn_prompt_text`, `next_rooms_turn_id`, `per_request: HashMap<u64, RoomsRequestData>`
  (turn id, `queue_item_id`, `decorate_session_list`)
- queue: `queued_prompts`, `next_queue_item_id`
- segments/rooms: `segments`, `active_segment_id`, `next_segment_id`,
  `emit_segment_frames`, `replay_generation`, `last_replay_reset`
- presence/list: `driving_subscriber_peer_id`, `session_list_index: Arc<…>`
- `meta_propagate`

Rooms logic (all current `rooms/*` behavior), implemented as hook impls:
- **turns/serialization**: open turn on `session/prompt` forward
  (`turn_started`), reject concurrent prompt with `-32001` + `rooms/session_busy`,
  `turn_complete`/`turn_cancelled`
- **queue/steer**: `rooms/steer_active_turn`, `rooms/queue_prompt`,
  `rooms/unqueue_prompt`, hard-steer prompt construction, `submit_next_queued_prompt`,
  `queue_item_*` frames
- **cancel**: `rooms/cancel_active_turn` → `session/cancel` via `ctx.send_to_agent`
- **segments/rooms**: rotate on `session/load` and observed `sessionId` change,
  `segment_started`/`segment_ended`, replay-generation reset, queued-prompt
  session-id retarget
- **presence**: `peer_joined`/`peer_left`, `session_context`, driving
  subscriber, `session/list` decoration + the metadata index
- **permissions UX**: `agent_request_opened`/`agent_request_resolved`
  (first-reply / `agent:cancelled` / `mux:turn-ended` sweep), pending-permission
  re-issue on attach
- **attach enrichment**: `_meta.rooms` (connectedClients, applied replay
  order/delivery, snapshot), history shaping (`full`=current-segment,
  `full_lineage`, ordering, streamed backfill via `replay_started`/`replay_complete`)
- **meta-propagate**: inject `params._meta.rooms` trace on outbound requests
- **snapshot**: contribute active turn, segments, replay generation, driving,
  `next_rooms_turn_id` to `/debug/sessions`

### 5.3 Gray-zone rulings (decided)

| Item | Ruling |
|---|---|
| Prompt serialization | **Mechanism = core** (≤1 in-flight prompt to the agent; plain JSON-RPC busy reject as backstop). **UX = rooms** (turn bookends, `rooms/session_busy`, `-32001`, queue/steer). Rooms's `on_subscriber_request` runs first and reproduces today's exact reject; core's backstop only fires when no extension handled it. |
| `session/attach` / `detach` | **#533 baseline = core** (standard shapes, roster, history over plain log). **Enrichment = rooms** (`_meta.rooms`, ordering, streaming, `full_lineage`, segment-scoped `full`, rich snapshot). |
| Replay log + store | **Core** (plain chronological log; `ext_tag` opaque). **Segment/turn-aware views = rooms**. |
| First-writer-wins permission fan-in | **Core** (correctness; #533). Lifecycle projection frames + re-issue = rooms. |
| Segments / lineage / `session/load` rotation | **Rooms** (rooms). Core only tracks the raw `canonical_session_id`. |
| `fs/*`/`terminal/*` blocking | **Core** (safety default). |
| Driving subscriber, `session/list` decoration, `--meta-propagate` | **Rooms**. |

---

## 6. The `MuxExtension` seam (PRESCRIPTIVE — build this)

This section is normative. The first pass skipped it entirely; build it exactly
as described unless you find a concrete reason it cannot compile, in which case
note the reason and the smallest deviation.

Today core's actor type is `MuxInner` in `crates/acp-mux/src/mux/actor.rs`.
Rename it to **`MuxCore`** and split the actor into two owned pieces.

### 6.0 Wiring pattern (how the borrow works)

```rust
// crates/acp-mux/src/mux/actor.rs
struct Mux {
    core: MuxCore,                  // all core state (was MuxInner) — NO ext field inside
    ext: Box<dyn MuxExtension>,     // NoopExtension for the acp-mux binary
}
```

Core actor methods that have a seam take the extension as a **separate
parameter** (never store it on `MuxCore` — that would self-alias). Inside, wrap
`&mut self` in a `MuxCtx` and hand it to the hook:

```rust
impl MuxCore {
    fn translate_outbound_request(
        &mut self,
        ext: &mut dyn MuxExtension,      // separate param — no aliasing with &mut self
        peer_id: &str,
        mut req: IncomingRequest,
    ) {
        // ... core proxy-local + cache short-circuits ...
        let mut ctx = MuxCtx::new(self);             // borrows self
        match ext.on_subscriber_request(&mut ctx, peer_id, &mut req) {
            Disposition::Handled => return,
            Disposition::Reject { code, message } => { self.send_error_response(peer_id, req.id, code, &message); return; }
            Disposition::Forward => {}
        }
        // ... core allocates mux_id, etc ...
    }
}
```

`run_mux` owns `Mux` and threads `&mut *mux.ext` into `mux.core.*` calls. The
`acp-mux` binary constructs `Mux { core, ext: Box::new(NoopExtension) }`; the
`rooms` binary constructs `Mux { core, ext: Box::new(RoomsExtension::new(...)) }`
via `MuxRegistry::with_extension(factory)` (add this constructor — see §9).

`NoopExtension` lives in core (`crates/acp-mux/src/extension.rs`) and returns
the trivial default for every hook (`Forward` / `Passthrough` / no-op). All
core unit tests run against `NoopExtension`.

### 6.1 Two non-callback mechanisms (do these first)

Refactor these into `MuxCore` before adding hooks; they remove the awkward
return-`Vec<Vec<u8>>` plumbing and let the extension inject work:

1. **`agent_outbox: Vec<Vec<u8>>`** on `MuxCore`. Every frame bound for the
   agent (core's own forwards, `ctx.send_to_agent`, `ctx.submit_prompt`) is
   pushed here. `run_mux` drains and writes it to the `AgentProcess` after each
   `MuxMsg` is handled. This replaces `handle_inbound -> Vec<Vec<u8>>` and
   `AgentLineAction.writes_to_agent`.
2. **`replay_tag: u64`** on `MuxCore` (default `0`). `MuxCore::broadcast` stamps
   every `ReplayEntry` (and the persisted `segment_id` JSONL field) with it.
   The extension *sets* it via `ctx.set_replay_tag(id)` on segment rotation —
   it is a pushed value, **not** a pulled callback (a callback can't run while
   the extension is mid-hook because `ext` is already borrowed). This is how
   rooms's `SegmentId` rides on core's plain replay log without core knowing
   what a segment is.

### 6.2 Supporting types (in `crates/acp-mux/src/extension.rs`)

```rust
pub enum Disposition {
    Forward,                                   // core proceeds (req may have been mutated in place)
    Handled,                                   // extension fully handled it; core does nothing more
    Reject { code: i64, message: String },     // core sends a JSON-RPC error to the originator
}

pub enum NotifyDisposition {
    Passthrough,                               // core forwards the raw notification bytes to the agent
    Handled,                                   // extension handled it (may have queued agent writes via ctx)
}

pub enum ResolvedBy {
    Peer(String),       // first reply won — the replying peer_id (wire: that peer_id)
    AgentCancelled,     // agent $/cancel_request    (wire: "agent:cancelled")
    TurnEnded,          // turn-end sweep            (wire: "mux:turn-ended")
}
```

### 6.3 `MuxCtx` — capability surface core grants the extension

`MuxCtx<'a>` wraps `&'a mut MuxCore`. Its methods are the **only** way the
extension touches core. `MuxCore` fields stay private; the extension is in
another crate so it physically cannot reach them. All methods are `pub`.

```rust
impl<'a> MuxCtx<'a> {
    // ---- reads ----
    pub fn mux_id(&self) -> &str;
    pub fn agent_cwd(&self) -> &str;
    pub fn canonical_session_id(&self) -> Option<&str>;
    pub fn subscribers(&self) -> impl Iterator<Item = &Subscriber>;
    pub fn subscriber(&self, peer_id: &str) -> Option<&Subscriber>;
    pub fn replay_entries(&self) -> impl Iterator<Item = ReplayView<'_>>; // {seq, ext_tag, recorded_at, frame}
    pub fn pending_permissions(&self) -> &[(Id, Bytes)];
    pub fn prompt_in_flight(&self) -> Option<u64>;        // mux_id of in-flight session/prompt
    pub fn pending_peer(&self, mux_id: u64) -> Option<&str>; // originator of a pending request

    // ---- client-facing writes ----
    pub fn broadcast(&mut self, frame: impl Into<Bytes>) -> bool; // append(+persist w/ replay_tag)+fanout; true if drained last sub
    pub fn send_to(&mut self, peer_id: &str, frame: Bytes);

    // ---- agent-facing (I1: only sanctioned standard ACP) ----
    pub fn send_to_agent(&mut self, acp_frame: Vec<u8>);  // MUST be a valid JSON-RPC request/notification; pushes agent_outbox
    pub fn submit_prompt(&mut self, peer_id: &str, params: Value, deliver_response: bool) -> u64;
        // core: alloc mux_id, insert PendingRequest{deliver_response}, set prompt_in_flight,
        // serialize session/prompt, push agent_outbox; returns mux_id so the ext can open its turn.

    // ---- replay tagging + deferred work ----
    pub fn set_replay_tag(&mut self, tag: u64);
    pub fn schedule_wake(&mut self, delay: Duration, payload: Vec<u8>); // → MuxMsg::ExtensionWake(payload) after delay
}
```

`send_to_agent` MUST reject (debug-log + drop) anything that isn't a
well-formed JSON-RPC request/notification — this is the structural guarantee of
**I1**. `schedule_wake` replaces the old `self_tx` + `RoomMsg::AttachStreamBackfill`
plumbing: add a `MuxMsg::ExtensionWake(Vec<u8>)` variant; `run_mux` delivers it
to `ext.on_wake(ctx, payload)`. Rooms uses it for paged attach backfill.

### 6.4 The trait

```rust
pub trait MuxExtension: Send {
    // --- subscriber → agent ---
    fn on_subscriber_request(&mut self, ctx: &mut MuxCtx, peer_id: &str, req: &mut IncomingRequest) -> Disposition { Disposition::Forward }
    fn on_request_translating(&mut self, ctx: &mut MuxCtx, peer_id: &str, mux_id: u64, req: &mut IncomingRequest) {} // after id alloc, BEFORE serialize (meta-propagate inject; allocate turn id)
    fn on_request_forwarded(&mut self, ctx: &mut MuxCtx, peer_id: &str, mux_id: u64, req: &IncomingRequest) {}       // after push to agent_outbox (open turn / emit turn_started)
    fn on_subscriber_notification(&mut self, ctx: &mut MuxCtx, peer_id: &str, notif: &IncomingNotification) -> NotifyDisposition { NotifyDisposition::Passthrough }

    // --- agent → subscribers ---
    fn on_agent_notification(&mut self, ctx: &mut MuxCtx, notif: &IncomingNotification) {} // BEFORE core broadcasts it (segment detect/rotate)
    fn on_agent_request(&mut self, ctx: &mut MuxCtx, id: &Id, req: &IncomingRequest) {}     // BEFORE core's raw fan-out (emit agent_request_opened; track for re-issue)
    fn on_agent_response(&mut self, ctx: &mut MuxCtx, mux_id: u64, resp: &mut IncomingResponse) {} // after id-restore + handshake cache, BEFORE deliver (decorate session/list)
    fn on_prompt_settled(&mut self, ctx: &mut MuxCtx, mux_id: u64, resp: &IncomingResponse) {}     // when prompt_in_flight clears, AFTER the resolved-sweep
    fn on_agent_request_resolved(&mut self, ctx: &mut MuxCtx, id: &Id, by: ResolvedBy, resp: Option<&IncomingResponse>) {}

    // --- lifecycle ---
    fn on_canonical_session_id(&mut self, ctx: &mut MuxCtx, old: Option<&str>, new: &str, via_load: bool) {}
    fn on_subscriber_attaching(&mut self, ctx: &mut MuxCtx, newcomer: &Subscriber) {}  // PRE-insert: newcomer NOT yet in ctx.subscribers() (emit peer_joined to existing)
    fn on_subscriber_attached(&mut self, ctx: &mut MuxCtx, peer_id: &str) {}            // POST-insert, BEFORE core delivers the replay snapshot (send session_context; publish session-list)
    fn on_subscriber_detached(&mut self, ctx: &mut MuxCtx, peer_id: &str) {}            // POST-removal (emit peer_left; orphan queue; clear driving; session-list)

    // --- #533 attach enrichment & debug ---
    fn on_attach(&mut self, ctx: &mut MuxCtx, peer_id: &str, params: &AttachParams, result: &mut AttachResult) {}
        // mutate the core-built #533 result: add _meta.rooms, reshape `history`
        // (full→segment-scoped, full_lineage, replay order), and/or kick streaming via ctx.
    fn on_wake(&mut self, ctx: &mut MuxCtx, payload: Vec<u8>) {}                         // schedule_wake callback (attach backfill paging)
    fn debug_snapshot(&self, ctx: &MuxCtx) -> serde_json::Value { serde_json::Value::Null } // merged into /debug by the rooms server
}
```

### 6.5 Hook → exact call site → original code → ordering

Call sites name the real functions in `crates/acp-mux/src/mux/actor.rs`.

| Hook | Call site (in `MuxCore`) | Original `rooms` code it replaces |
|---|---|---|
| `on_subscriber_request` | `translate_outbound_request`, after proxy-local (`session/attach\|detach`) + `initialize`/`session/new` cache short-circuits, before the prompt backstop | `rooms/steer_active_turn\|queue_prompt\|unqueue_prompt` arms; concurrent-prompt `-32001` + `rooms/session_busy`; `note_driving_subscriber` |
| `on_request_translating` | `translate_outbound_request`, after `mux_id` alloc + `req.id` rewrite, **before** `serde_json::to_vec` | `meta_propagate` `params._meta.rooms` injection; rooms `roomsTurnId` allocation |
| `on_request_forwarded` | `translate_outbound_request`, after pushing bytes to `agent_outbox` | open active turn + `emit_turn_started`; record per-`mux_id` rooms data (turn id, `queue_item_id`, `decorate_session_list`) |
| `on_subscriber_notification` | `handle_subscriber_notification` (core keeps `$/cancel_request`) | `rooms/cancel_active_turn` → `ctx.send_to_agent(session/cancel)` + broadcast `rooms/turn_cancelled` |
| `on_agent_notification` | `handle_agent_line` notification arm, **before** `broadcast(line)` | `detect_segment_signal_from_agent_notification` → `rotate_segment` |
| `on_agent_request` | `handle_agent_request`, after policy-allow + `agent_pending=InFlight`, **before** core's per-subscriber raw fan-out | `rooms/agent_request_opened` broadcast; pending-permission re-issue tracking |
| `on_agent_response` | `route_agent_response`, after id-restore + `apply_successful_handshake`, before deliver-to-originator | `decorate_session_list_response` |
| `on_prompt_settled` | `route_agent_response`, in the `prompt_in_flight == Some(mux_id)` block, **after** the sweep | `emit_turn_complete`; `rooms/queue_item_completed`; `submit_next_queued_prompt` (via `ctx.submit_prompt`) |
| `on_agent_request_resolved` | `gate_subscriber_response` (first reply → `Peer`), `handle_agent_cancel` (→ `AgentCancelled`), `sweep_stale_agent_pending` (→ `TurnEnded`) | `emit_agent_request_resolved`; `rooms/agent_request_resolved` |
| `on_canonical_session_id` | `apply_successful_handshake` after `canonical_session_id` is set (`SessionNew` capture / `SessionLoad` rebind) | `rotate_segment`; `reset_replay_generation_after_load`; `publish_session_list_metadata` |
| `on_subscriber_attaching` / `on_subscriber_attached` | `attach` (see ordering below) | `rooms/peer_joined`; `rooms/session_context`; session-list publish |
| `on_subscriber_detached` | `detach`, after `subscribers.remove` | `rooms/peer_left`; `rooms/queue_item_orphaned`; clear driving subscriber; session-list |
| `on_attach` | `handle_attach_request`, after core builds the baseline `AttachResult`, before serialize/send | `_meta.rooms`; `history_full`(segment-scoped)/`full_lineage`; replay ordering; streamed `replay_started`/`replay_complete` backfill |
| `on_wake` | `run_mux` on `MuxMsg::ExtensionWake` | `send_attach_backfill_page` |
| `debug_snapshot` | rooms `server.rs` `/debug/sessions` (merge over core snapshot) | rooms fields of `RoomSnapshot` |

### 6.6 Ordering invariants (preserve byte-for-byte; the suite is the oracle)

- **Attach** (`MuxCore::attach`), exact sequence to reproduce the original:
  1. peer-id collision check;
  2. capture the legacy replay snapshot (clone of replay log unless `replay=skip`);
  3. `ext.on_subscriber_attaching(ctx, &newcomer)` — newcomer **not yet
     inserted**, so `ctx.broadcast(peer_joined)` reaches only existing subs and
     is appended to replay *after* the snapshot was captured;
  4. `subscribers.insert(newcomer)`;
  5. `ext.on_subscriber_attached(ctx, peer_id)` — sends `rooms/session_context`
     to the newcomer + publishes session-list;
  6. core delivers the captured snapshot frames to the newcomer.
  (So the newcomer receives `session_context` then the snapshot, and never its
  own `peer_joined`. Matches original `attach()`.)
- **Prompt settle** (`route_agent_response`): clear `prompt_in_flight` → core
  sweeps `agent_pending` calling `on_agent_request_resolved(TurnEnded)` per id
  → `on_prompt_settled` (turn_complete → queue_item_completed → submit-next) →
  handshake cache/decorate/deliver. Sweep is **before** turn_complete.
- **Agent-initiated request**: `on_agent_request` (which broadcasts
  `rooms/agent_request_opened` and appends it to replay) fires **before** core's
  raw per-subscriber fan-out (raw request is *not* replayed).
- **Agent notification**: `on_agent_notification` (segment rotate, which may
  emit `segment_started`/`segment_ended` via `ctx.broadcast`) fires **before**
  core broadcasts the agent's own notification line.
- **`meta_propagate`** must produce byte-identical `params._meta.rooms`.

---

## 7. #533 conformance (core) — in scope

Core's standalone `session/attach`/`session/detach` must conform to RFD #533.
Pin to the PR revision at implementation time (it is a *draft*; field names may
change): https://github.com/agentclientprotocol/agent-client-protocol/pull/533

Core baseline deliverables:
- `session/attach` request params: `sessionId`, `historyPolicy`, `clientInfo`,
  `clientId` (optional).
- `session/attach` result: standard `connectedClients` roster (top-level),
  `history` shaped by `historyPolicy`, conformant session-state fields.
- History policies over the **plain** log: `full`, `pending_only`, `none`,
  `after_message` (+ `afterMessageId`; may fall back to `full` if message ids
  aren't available end-to-end, as today).
- `session/detach` standard request/result.
- First-writer-wins permission resolution — already core (the `agent_pending`
  fan-in).
- These methods are answered locally and **never forwarded** to the agent.

**Divergence handling (preserves the `rooms` binary):** where the current
`rooms` wire differs from #533 (e.g. `connectedClients` currently lives under
`_meta.rooms`; `full` currently means current-segment-only via segments;
streamed delivery; rich snapshot), the **core** path emits the #533-conformant
form, and **`RoomsExtension::on_attach` overrides/reshapes** the result back to
today's exact bytes. Result: `acp-mux` binary = #533-conformant; `rooms` binary
= unchanged.

The standardized #533 session-update broadcast frames (`prompt_received`,
`turn_complete`, `client_disconnected`, `permission_resolved`) are the natural
**core** expression of presence/turn facts. To avoid duplicating rooms's
turn/presence semantics on the `rooms` binary, core emits them **only in
standalone (Noop-extension) mode**; on the `rooms` binary the existing `rooms/*`
frames represent the same facts and are preserved. Confirm the exact #533
field names against the PR when implementing this sub-item; if #533 is too far
from final, ship the attach/detach + roster + history + first-writer pieces
(which are stable) and leave the standardized update-frame names behind a small
core toggle.

Add core conformance tests under `crates/acp-mux/tests/` (drive `MuxRegistry`
with `cat` / an inline mock; assert attach/detach shapes, history policies,
first-writer permission, and that no `rooms/*` ever appears on the core path).

---

## 8. Behavior-preservation contract

For the `rooms` binary, the following must be **byte/behavior identical** after
the refactor:
- every `rooms/*` notification (names, params, ordering)
- `session/attach`/`session/detach` responses including `_meta.rooms`
- `-32001`/`-32002`/`-32003`/`-32004`/`-32000` error codes and WS close codes
- replay snapshot/stream behavior and `--replay-store` JSONL format (keep the
  `segment_id` field name)
- `/debug/sessions` JSON shape
- `?room=` + `?session=` alias, all current CLI flags
- the entire `tests/` suite (moved to `crates/rooms/tests/`) passes unchanged
  (imports may be repathed; assertions must not change)
- `docs/examples/client-contract/**` fixtures still match

The internal replay-store JSON keeps `segment_id` (core treats it as the
opaque `ext_tag`); this is the only place core "knows" a u64 tag exists, and
it never interprets it.

---

## 9. Corrective execution (start from what's on disk; each stage green)

The first pass already did the workspace, the leaf-module moves, and a clean
core crate. **Do not redo those.** The corrective work is: build the seam, move
rooms onto it, delete the fork. Commit per stage; keep `cargo test --workspace`
green throughout.

**C0 — Baseline (already done; just verify).** `cargo build --workspace` +
`cargo test --workspace` pass. Core crate is clean (`grep -ri rooms
crates/acp-mux/src` → empty). The forked `crates/rooms/src/room/*` still exists —
that's expected; C4 removes it.

**C1 — Build the seam in core (§6), behavior-neutral.**
- Rename `MuxInner` → `MuxCore`; introduce `struct Mux { core, ext }`.
- Add `MuxCore.agent_outbox` + drain in `run_mux`; convert `handle_inbound` /
  `AgentLineAction` to push there (§6.1.1).
- Add `MuxCore.replay_tag` stamped in `broadcast` + persisted as `segment_id`
  (§6.1.2).
- Add `crates/acp-mux/src/extension.rs`: `MuxExtension` (default-impl trait),
  `MuxCtx`, `NoopExtension`, `Disposition`/`NotifyDisposition`/`ResolvedBy`.
- Add `MuxMsg::ExtensionWake(Vec<u8>)` + `on_wake` dispatch.
- Add `MuxRegistry::with_extension(factory: Fn() -> Box<dyn MuxExtension>)`;
  existing `MuxRegistry::new` keeps wiring `NoopExtension`.
- Thread `&mut dyn MuxExtension` through the call sites and insert the hook
  calls at the exact points in §6.5.
- The `acp-mux` binary and all 21 core tests are unchanged (Noop = today's
  behavior). Green.

**C2 — Empty `RoomsExtension` scaffold in the rooms crate.**
`crates/rooms/src/extension/{mod,turns,queue,segments,presence,permissions,attach_views}.rs`.
`RoomsExtension: MuxExtension` holding the rooms state from §5.2; all hooks no-op
to start. Not yet used by the binary. Green.

**C3 — Port rooms behavior onto the extension, concern by concern.**
Move logic out of the forked `room/state.rs` into `RoomsExtension` hooks. Make
the **composed path** (`core` + `RoomsExtension`) reachable by the existing test
entrypoints: provide `rooms::server::router` / `rooms::AppState` / a `rooms`
registry that build on `acp_mux::mux::MuxRegistry::with_extension(...)`. Point
the existing `crates/rooms/tests/server.rs` at this composed path — those 83
integration tests + `docs/examples/client-contract` are the **byte-compat
oracle**; they must stay green at every sub-step. Keep the fork compiling under
a temporary module name (e.g. `room_legacy`) only if needed for reference; it
is deleted in C4.
Sub-order: 3a presence → 3b turns + serialization → 3c queue/steer/cancel →
3d agent-request lifecycle + permissions → 3e segments/rooms + `session/load`
→ 3f attach enrichment (`_meta.rooms`, history shaping, streaming) → 3g
meta-propagate + `/debug` snapshot fields.

**C4 — Delete the fork (the gate that proves the goal).**
Remove `crates/rooms/src/room/state.rs` and the forked `room/registry.rs`,
`room/attach.rs`, and the duplicated routing in the forked `server.rs`. After
this, `crates/rooms/src` contains only: the extension, `protocol/rooms.rs`,
`protocol/attach.rs` (`_meta` types), rooms cli additions, thin composed
`server`/registry wrappers, `main.rs`, `bin/mock_acp.rs`, `tests/`. Verify the
anti-fork gates in §11. Green.

**C5 — cli/server finalize, #533 conformance, docs.**
- `cli.rs`/`server.rs`: core owns core flags + `?mux=` + baseline `/debug`;
  rooms adds `--meta-propagate`, `--emit-segment-frames`,
  `--unsafe-debug-client-tool-broadcast` and `?room=` + enriched `/debug`
  (merging `debug_snapshot`). `--replay-store`, `ClientToolPolicy`,
  `ReplayTurns` stay core, re-exposed by rooms cli.
- #533 conformance pass on core per §7; add core conformance tests.
- Update `README.md`, `ROADMAP.md`, `docs/design/rooms-namespace.md`,
  `docs/design/rooms.md`, `CHANGELOG.md`. Run `cargo clippy --workspace` clean.

---

## 10. Risks / watch-items

- **Ordering** (§6.6) is the top risk — peer_joined-before-insert,
  opened-before-raw, sweep-before-turn_complete. The fixtures encode these;
  diff WS transcripts in tests, don't just check counts.
- **Don't re-fork** (the failure mode of pass 1). The rooms behavior must be
  *moved onto core's actor via the extension*, not reimplemented beside it.
  There must remain exactly ONE actor loop, ONE `translate_outbound_request`,
  ONE `route_agent_response`, ONE fan-in. If you find yourself copying routing
  logic into the rooms crate, stop — that logic belongs in `MuxCore`.
- **`PendingRequest` split** — core keeps `{peer_id, original_id, handshake,
  deliver_response}`; rooms tracks `{queue_item_id, decorate_session_list,
  turn}` keyed by `mux_id` (populated via `on_request_forwarded` /
  `submit_prompt`). Ensure both stay in sync on every code path that inserts a
  pending (normal forward AND queued submit).
- **`session/load`** touches both layers: core rebinds `canonical_session_id`
  + cache; rooms rotates segment + resets replay generation. Drive this through
  `on_canonical_session_id(via_load=true)` and verify replay-generation +
  segment frames are unchanged.
- **`replay_store` ownership** — log is core, but `ext_tag`/`segment_id` round
  trip must be preserved; hydration must hand the persisted tag back to rooms
  so it can rebuild `segments`. Add a hydration hook or pass loaded
  `(seq, ext_tag, recorded_at, frame)` to the extension at construction.
- **`#533` is a draft** — pin to the PR; isolate conformance to core so rooms
  can track #533 independently later.
- **Cross-crate test fixtures** — keep `mock_acp` in `rooms`; core tests use
  `cat`/inline.
- **`session/attach` `full` semantics** — core `full` = whole plain log; rooms
  `full` = current segment only. The override in `on_attach` must restore the
  segment-scoped view (incl. cross-segment turn-bookend carry) for the `rooms`
  binary.

---

## 11. Definition of done (acceptance gates)

Functional:
- `cargo build --workspace` produces two binaries: `acp-mux` (pure 1→N) and `rooms`.
- `cargo test --workspace` green; `cargo clippy --workspace` clean.
- `rooms` binary: the (verbatim) integration suite + `docs/examples/client-contract`
  fixtures pass unchanged; wire output byte-identical.
- `acp-mux` binary: #533-conformant `session/attach`/`detach`, first-writer
  permission, plain replay/late-join, fs/terminal safety, no `rooms/*` emitted;
  covered by core tests.

Anti-fork gates (these are what pass 1 failed — check them explicitly):
- **One multiplexer.** `crates/rooms/src/room/state.rs` is **deleted**. There is
  no `RoomInner`/`RoomRegistry`/`spawn_room`/forked `RoomMsg` in the rooms crate.
  `grep -rn "spawn_room\|RoomInner\|fn route_agent_response\|fn translate_outbound_request" crates/rooms/src`
  → empty. That logic exists **only** in `crates/acp-mux/src/mux/actor.rs`.
- **rooms builds on core.** The `rooms` binary/server construct
  `acp_mux::mux::MuxRegistry::with_extension(...)`; the rooms crate contains a
  real `impl MuxExtension for RoomsExtension` and does **not** re-implement
  id-translation / response-routing / agent-request fan-in / handshake caching.
- **The seam exists.** `MuxExtension`, `MuxCtx`, `NoopExtension` are present in
  `crates/acp-mux/src/extension.rs`; the `acp-mux` binary uses `NoopExtension`.
- **Core stays clean.** `grep -rni rooms crates/acp-mux/src` (excluding the
  crate name `acp_mux`/`acp-mux`) → empty; no turn/queue/segment concepts in core.

Hygiene:
- Docs updated to describe the two-crate boundary.
- Future option preserved: the two crates can be lifted into separate repos
  (core depends on nothing internal; rooms depends only on the published core).
