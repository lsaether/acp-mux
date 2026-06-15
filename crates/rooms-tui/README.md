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

Add first-class permission handling on top of the live reducer snapshot:

1. highlight actionable `session/request_permission` requests;
2. support allow/deny/custom option selection;
3. send exactly one JSON-RPC response for the selected request;
4. mark or remove the request once answered/resolved.
