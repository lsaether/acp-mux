# rooms-client

Reusable Rust client core for the `acp-mux` Rooms collaboration layer.

This crate intentionally contains no TUI, Tauri, webview, or terminal concerns. It is the shared protocol/client foundation that `rooms-tui` uses now and a Tauri app can reuse later.

## Current surface

- `AttachConfig` and `build_attach_url` for room-native websocket URLs:
  - `room=`
  - `peer_id=`
  - optional `peer_name=`
  - `replay=skip`
  - no legacy `session=` alias
- Protocol builders for:
  - `initialize`
  - `session/attach` with `historyPolicy: "full_lineage"` and `_meta.rooms`
  - `session/prompt`
  - `rooms/queue_prompt`
  - `rooms/steer_active_turn`
  - `rooms/cancel_active_turn`
  - `rooms/unqueue_prompt`
- WebSocket transport that:
  - connects to `build_attach_url(config)`;
  - sends `initialize` then `session/attach` bootstrap frames;
  - streams JSON-RPC frames through typed inbound messages with parsed `Event` values where available;
  - exposes a typed outbound command channel for JSON frame sends and shutdown.
- Event parser for key `rooms/*` lifecycle frames and actionable `session/request_permission`.

## Intended consumers

```text
rooms-client
  ├─ rooms-tui        ratatui/crossterm terminal UI
  └─ future Tauri app Rust backend or command bridge
```

A Tauri app can either use browser-native WebSocket from the webview or call into this Rust crate from the Tauri backend. If it needs identical reducer/permission/queue behavior to the TUI, prefer the Rust backend path so both UIs share this crate.

## Next slice

Add a UI-neutral room state reducer behind this crate, not inside `rooms-tui`:

1. fold attach snapshots, streamed replay, and live frames into one `RoomState`;
2. track connection status, session id, peers, transcript items, active turn, queue, pending permissions, and debug/errors;
3. keep reducer output reusable by both `rooms-tui` and a future Tauri client;
4. expose higher-level command helpers for prompt, queue, steer, cancel, unqueue, and permission replies on top of the outbound channel.
