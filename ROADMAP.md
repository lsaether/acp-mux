# acp-mux roadmap

`acp-mux` is a generic ACP multiplexer / agent mirror: one upstream stdio ACP agent process, many WebSocket clients, one shared room transcript. The generic mux core stays small and provider-neutral; the AMUX layer adds optional multiplayer room/control events on top.

This file tracks where the project is going. Protocol details live in [`docs/design/amux-namespace.md`](docs/design/amux-namespace.md) and room/segment semantics live in [`docs/design/rooms.md`](docs/design/rooms.md).

Status legend: `[ ]` not started · `[~]` in progress · `[x]` done

## Design principles

- **Provider-neutral core.** Any stdio ACP agent can sit behind the mux. Provider metadata passes through; provider-private logs/metadata do not drive mux state.
- **One job.** Mirror an ACP agent session into a collaborative, reconnectable room.
- **Envelope-first routing.** Parse JSON-RPC envelopes and method names; keep ACP payloads as opaque `serde_json::Value` unless the method is mux-owned.
- **Layer boundary.** Core mux behavior is routing, replay, lifecycle, and safe defaults. AMUX behavior is presence, turn bookends, queue/steer/cancel controls, permission UX, and projection events.
- **Separate channels.** Agent-owned frames stay in ACP namespaces. AMUX collaboration/control facts stay in `amux/*`.
- **Fail closed on side effects.** Delegated `fs/*` and `terminal/*` client tools are blocked unless an unsafe debug flag explicitly restores raw fanout.
- **No upstream protocol changes.** `acp-mux` is a consumer/proxy of ACP, not a fork of ACP or a patched agent runtime.
- **Single static binary.** Runtime dependencies should remain limited to the configured agent subprocess.

## Current shipped shape

### Generic ACP mux core

- [x] One agent subprocess per `?room=`.
- [x] Multiple subscribers per room.
- [x] Per-subscriber JSON-RPC id translation.
- [x] Broadcast fanout for agent notifications.
- [x] `initialize` and `session/new` caching for late joiners.
- [x] Turn serialization for ordinary `session/prompt`.
- [x] Proxy-local `session/attach` / `session/detach` foundation.
- [x] Attach history policies: `full`, `full_lineage`, `none`, `pending_only`, provisional `after_message`.
- [x] Attach replay ordering: `chronological`, `newest_turn_first`.
- [x] Safe default blocking for delegated `fs/*` and `terminal/*` client-tool requests.
- [x] Cold-start `GET /acp/sessions` control-plane query.
- [x] `GET /debug/sessions` live room snapshot.
- [x] Optional append-only JSONL replay store with `--replay-store`.
- [x] Provider-neutral room/session-id tracking: room id is stable, ACP `sessionId` can rotate inside it.
- [x] Provider-neutral extraction: no provider-specific stderr parser, metadata interpreter, or lifecycle reason in the core path.

### AMUX collaboration layer

- [x] `amux/*` room/control namespace.
- [x] Active-turn cancellation via `amux/cancel_active_turn` → ACP `session/cancel`.
- [x] Hard steer via `amux/steer_active_turn`.
- [x] Prompt queue via `amux/queue_prompt` / `amux/unqueue_prompt`.
- [x] First-writer-wins fanout for `session/request_permission`.
- [x] Replay-safe agent request lifecycle via `amux/agent_request_opened` / `amux/agent_request_resolved`.
- [x] Optional streamed attach history via `amux/replay_started` / `amux/replay_complete`.
- [x] Segment projection frames: `amux/segment_started`, `amux/segment_ended`.
- [x] Client contract fixtures under `docs/examples/client-contract/` distinguish raw ACP passthrough from AMUX extension frames.

## Near-term polish

- [ ] Remove deprecated `?session=` alias; require `?room=` only.
- [ ] Expand examples around real ACP agents while keeping the core contract provider-neutral.
- [ ] Add small conformance/smoke harnesses for multiple ACP agents, kept outside the always-on suite when they require local binaries.
- [ ] Expose clearer `/debug/sessions` fields for queue state, replay generation, and segment lineage.
- [ ] Clarify error codes in one table (`-32000`, `-32001`, `-32002`, `-32003`, WS close codes).

## Replay and persistence follow-ups

- [ ] Bounded replay eviction for `--replay-turns N` using turn bookends as eviction points.
- [ ] Bounded persistent replay store compaction.
- [ ] Replay-store integrity check / repair command.
- [ ] `/debug/replay-log` endpoint for local inspection.
- [ ] Optional SQLite replay backend if JSONL becomes too limited.
- [ ] Better partial replay once stable ACP message ids are available end-to-end; replace `after_message` fallback with real slicing.

## Delegated client-tool support

Default remains fail-closed. Future support should be explicit, scoped, and non-duplicating.

- [ ] Design delegated `fs/*` / `terminal/*` routing with exactly-one executor semantics.
- [ ] Capability negotiation per subscriber / per room.
- [ ] UI-visible executor selection and audit trail.
- [ ] Tests proving no duplicate side effects across multi-subscriber fanout.

## ACP / RFD alignment

- [ ] Track accepted shape of attach/detach lifecycle RFDs.
- [ ] Only add proxy-owned ACP `session/update` siblings if an accepted schema and a real generic client both need them.
- [ ] Track `session/resume`, `session/close`, `session/delete`, `session/fork`, and other experimental surfaces as passthrough until mux state must understand them.
- [ ] Keep `_meta.amux` additive and namespaced.

## Possible v1.0 scope

- [ ] Per-subscriber auth/token model.
- [ ] Optional TLS/reverse-proxy deployment notes.
- [ ] Subprocess crash recovery / restart strategy.
- [ ] Metrics endpoint (`/metrics`, Prometheus format).
- [ ] Share links / one-time attach tokens.
- [ ] Recording/playback fixtures for eval and client regression tests.
- [ ] Multi-room dashboards using `GET /acp/sessions` plus `/debug/sessions`.

## Explicit non-goals

- Embedding or becoming an ACP agent.
- Patching upstream ACP agents.
- Interpreting provider-private metadata in the generic core.
- Parsing provider logs/stderr for lifecycle state.
- Providing terminal/filesystem client tools by default.
- Cross-host federation; run one mux per host/process boundary.
- Replacing an agent's own conversation persistence.
