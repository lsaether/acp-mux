# rooms-tui

Rust terminal client for the `acp-mux` Rooms collaboration layer.

This crate is the Rust home for the earlier `vibe-textual` room-client experiment. It speaks the current `rooms/*` namespace and `_meta.rooms` attach metadata through the reusable `rooms-client` crate; do not port old `amux/*` frames literally.

## Current status

Live TUI foundation plus reusable client transport/state:

- CLI parser for a room-native terminal peer
- reusable client core imported from `rooms-client`
- `rooms-client` WebSocket transport for `initialize` → `session/attach` bootstrap and typed inbound/outbound channels
- `rooms-client` UI-neutral `RoomState` reducer for roster, transcript, active turn, queue, permissions, replay/debug/errors
- event-driven ratatui loop that connects through `rooms-client`, folds inbound frames into `RoomState`, and renders connection/bootstrap progress, reducer snapshot, and event summaries
- keyboard controls for draft entry, idle prompt submit, busy queue, active-turn steer/cancel, and selected prompt unqueue
- first-class permission request surface with request/option selection and one-shot JSON-RPC replies
- `Ctrl-Q` / `Esc` exit

Shared non-UI logic lives in `../rooms-client` so a future Tauri app can reuse the same attach URL, protocol builders, event parser, websocket transport, and reducer.

## Run

Start a Rooms server separately:

```sh
cargo run -p rooms -- \
  --agent-cmd 'cat' \
  --port 8765
```

Print the websocket URL the TUI will attach to:

```sh
cargo run -p rooms-tui -- \
  --room demo \
  --peer-id desktop \
  --peer-name Desktop \
  --print-url
```

Open the live TUI:

```sh
cargo run -p rooms-tui -- \
  --room demo \
  --peer-id desktop \
  --peer-name Desktop
```

## Next slice

Add reconnect and replay recovery for daily-driver failure modes:

1. recover cleanly from server restarts, laptop sleep, and broken websockets;
2. surface actionable peer-id collision, wrong port, and wrong endpoint errors;
3. re-run attach/replay without duplicating visible reducer state;
4. preserve enough context for manual daily-driver validation after feat 6.
