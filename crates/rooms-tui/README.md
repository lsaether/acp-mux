# rooms-tui

Rust terminal client for the `acp-mux` Rooms collaboration layer.

This crate is the Rust home for the earlier `vibe-textual` room-client experiment. It speaks the current `rooms/*` namespace and `_meta.rooms` attach metadata through the reusable `rooms-client` crate; do not port old `amux/*` frames literally.

## Current status

Kickoff scaffold:

- CLI parser for a room-native terminal peer
- reusable client core imported from `rooms-client`
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

Wire websocket transport in `rooms-client`, then render it in this TUI:

1. connect to the attach URL;
2. send `initialize`;
3. send proxy-local `session/attach` with `historyPolicy: "full_lineage"` and `_meta.rooms.historyDelivery: "stream"`;
4. fold `rooms/replay_started` → replay frames → `rooms/replay_complete` plus live frames into the reducer;
5. render transcript, roster, queue, and permission affordances.
