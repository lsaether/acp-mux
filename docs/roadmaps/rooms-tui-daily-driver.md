# rooms-tui Daily Driver Roadmap

Goal: make `rooms-tui` useful as a daily-driver Rooms client while keeping all UI-neutral behavior reusable through `rooms-client` for future Tauri or other clients.

## 1. `feat: add rooms-client websocket transport`

Add the first live transport layer inside `crates/rooms-client`, not `rooms-tui`. It should connect to `build_attach_url(config)`, perform the `initialize` → `session/attach` bootstrap, read JSON-RPC frames from the websocket, and expose typed inbound/outbound channels. This turns the current protocol builders into an actual live client foundation.

## 2. `feat: add rooms-client room state reducer`

Add a UI-neutral `RoomState` plus reducer that folds replayed and live frames into one canonical state model. It should track connection status, session id, peers, transcript items, active turn, queue, pending permissions, and debug/errors. This is the key reuse point: the TUI and a future Tauri app should render the same reducer output instead of implementing separate room semantics.

## 3. `feat: wire rooms-tui to live rooms-client events`

Replace the scaffold screen with a real event-driven TUI loop backed by `rooms-client`. The first version only needs to show connection status, attach/bootstrap progress, incoming frame/event summaries, and the current reducer snapshot. This milestone proves `rooms-tui` is no longer a static shell and can observe a real Rooms session.

## 4. `feat: add prompt queue steer cancel controls`

Add the core operator controls needed during daily use: submit a prompt when idle, queue a prompt when busy, steer the active turn, cancel the active turn, and unqueue selected pending prompts. Keyboard behavior should be simple and predictable, with commands routed through `rooms-client` so Tauri can reuse the same command API later.

## 5. `feat: add permission request handling`

Make `session/request_permission` a first-class TUI surface. Show pending permission requests with enough detail to make a decision, support keyboard selection for allow/deny/custom option ids, send exactly one reply, and remove or mark the request once resolved. This is required before the client can safely replace ad-hoc debugging clients for real agent work.

## 6. `feat: add reconnect and replay recovery`

Handle practical failure modes: server restart, laptop sleep, broken websocket, peer-id collision, wrong port, and wrong endpoint. Reconnect should re-run attach/replay, fold history without duplicating visible state, and produce actionable errors. This is the last implementation feature before a manual daily-driver pass.

## Manual gate after feat 6

After the first six feats land, pause automated roadmap work and manually test the TUI in real use: attach to a room, submit prompts, queue/steer/cancel while busy, answer permissions, restart the server, and confirm reconnect/replay behavior feels correct. Use the manual findings to tighten the final testing harness instead of guessing the test shape too early.

## 7. `test: add daily-driver smoke and regression harness`

After manual validation, add the durable automated test layer. The harness should start a local `rooms` server with a mock agent, attach through `rooms-client`, drive prompt/control/permission/reconnect flows, observe reducer state changes, and exit cleanly. Keep this as the regression suite for the behaviors proven manually in the first six feats.

## Acceptance for daily-driver readiness

`rooms-tui` is daily-driver ready when one terminal can attach to an existing room, show transcript/queue/permissions, send all core controls, survive reconnect/replay, pass the manual gate after feat 6, and pass both unit tests and the final daily-driver smoke harness.
