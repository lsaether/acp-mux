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
- Event parser for key `rooms/*` lifecycle frames and actionable `session/request_permission`.

## Intended consumers

```text
rooms-client
  ├─ rooms-tui        ratatui/crossterm terminal UI
  └─ future Tauri app Rust backend or command bridge
```

A Tauri app can either use browser-native WebSocket from the webview or call into this Rust crate from the Tauri backend. If it needs identical reducer/permission/queue behavior to the TUI, prefer the Rust backend path so both UIs share this crate.

## Next slice

Add websocket transport behind this crate, not inside `rooms-tui`:

1. connect to `build_attach_url(config)`;
2. send `protocol::build_initialize`;
3. send `protocol::build_attach`;
4. stream frames through `events::event_from_value`;
5. expose a UI-neutral command API for prompt, queue, steer, cancel, and permission replies.
