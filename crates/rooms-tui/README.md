# rooms-tui

Rust terminal client for the `acp-mux` Rooms collaboration layer.

This crate is the Rust home for the earlier `vibe-textual` room-client experiment. It speaks the current `rooms/*` namespace and `_meta.rooms` attach metadata through the reusable `rooms-client` crate; do not port old `amux/*` frames literally.

## Current status

Kickoff scaffold plus reusable client transport/state:

- CLI parser for a room-native terminal peer
- reusable client core imported from `rooms-client`
- `rooms-client` WebSocket transport for `initialize` → `session/attach` bootstrap and typed inbound/outbound channels
- `rooms-client` UI-neutral `RoomState` reducer for roster, transcript, active turn, queue, permissions, replay/debug/errors
- minimal ratatui shell with `q` / `Esc` exit

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

Open the scaffold TUI:

```sh
cargo run -p rooms-tui -- \
  --room demo \
  --peer-id desktop \
  --peer-name Desktop
```

## Next slice

Wire the shared `rooms-client` transport and reducer into this TUI:

1. connect to the attach URL from the TUI loop;
2. feed `InboundMessage` values into `RoomState`;
3. replace the scaffold status pane with connection/bootstrap progress and reducer snapshots;
4. render transcript, roster, queue, and permission summaries.
