# acp-mux

`acp-mux` is a Rust workspace for running one stdio ACP agent behind a
WebSocket multiplexer.

It contains three binaries:

- `acp-mux`: the provider-neutral core mux.
- `rooms`: the Rooms collaboration layer built on top of the core mux.
- `rooms-tui`: the Rust terminal client for Rooms.

The split is intentional. The core knows how to multiplex JSON-RPC ACP traffic.
Rooms adds room UX such as peers, turn lifecycle, queueing, segment lineage, and
streamed replay markers. `rooms-tui` consumes that Rooms surface as a room-native
client instead of trying to make generic ACP clients infer multiplayer state.

## Workspace Layout

```text
crates/acp-mux/   core ACP mux library and `acp-mux` binary
crates/rooms/      Rooms collaboration extension and `rooms` binary
crates/rooms-tui/  Rust room-native terminal client
docs/             protocol and design notes
```

Use the crate READMEs for exact behavior:

- [crates/acp-mux/README.md](crates/acp-mux/README.md) describes the core mux.
- [crates/rooms/README.md](crates/rooms/README.md) describes the Rooms layer.
- [crates/rooms-tui/README.md](crates/rooms-tui/README.md) describes the room-native terminal client.

## What The Core Does

The core `acp-mux` binary exposes:

- `GET /healthz`
- `GET /acp` WebSocket attach using `?mux=<id>&peer_id=<id>`
- `GET /acp/sessions?cwd=<optional>` transient control-plane `session/list`
- `GET /debug/sessions` core mux snapshots

For each mux id, it starts one ACP agent subprocess and lets multiple WebSocket
subscribers share that subprocess.

The core owns:

- stdio ACP subprocess management;
- subscriber attach/detach and peer-id collision handling;
- JSON-RPC request-id translation;
- response routing back to the originating subscriber;
- notification broadcast fanout;
- first `initialize` and `session/new` response caching;
- `session/load` canonical session-id rebinding;
- in-memory replay of broadcast frames;
- optional replay persistence for library users;
- first-writer-wins handling for agent-initiated requests;
- pending permission tracking for `session/attach` `pending_only`;
- safe blocking of delegated `fs/*` and `terminal/*` client-tool requests;
- baseline proxy-local `session/attach` and `session/detach`.

The core does not emit `rooms/*` frames and does not know about turns, queues,
rooms, segments, or Rooms metadata.

## What Rooms Adds

The `rooms` binary wraps the same core mux with `RoomsExtension`.

Rooms adds:

- `?room=` naming, with deprecated `?session=` alias;
- `rooms/session_context`, `rooms/peer_joined`, and `rooms/peer_left`;
- turn lifecycle notifications;
- active-turn busy UX;
- queue, steer, unqueue, and active-turn cancel controls;
- agent-request opened/resolved projection frames;
- pending permission reissue after `session/attach`;
- segment tracking across `session/load` and observed ACP `sessionId` changes;
- Rooms-enriched `session/attach` metadata;
- current-segment vs full-lineage history shaping;
- streamed replay markers;
- optional JSONL replay persistence exposed by `--replay-store`.

## Build

```sh
cargo build --workspace
```

Binaries:

```text
target/debug/acp-mux
target/debug/rooms
target/debug/rooms-tui
```

Release build:

```sh
cargo build --workspace --release
```

## Run The Core Mux

```sh
cargo run -p acp-mux -- \
  --agent-cmd 'cat' \
  --host 127.0.0.1 \
  --port 8765
```

Connect a client:

```text
ws://127.0.0.1:8765/acp?mux=demo&peer_id=desktop
```

Every subscriber with the same `mux=demo` shares the same upstream agent
subprocess until the last subscriber leaves and the TTL expires.

## Run Rooms

```sh
cargo run -p rooms -- \
  --agent-cmd 'cat' \
  --host 127.0.0.1 \
  --port 8765
```

Connect a client:

```text
ws://127.0.0.1:8765/acp?room=demo&peer_id=desktop
```

Attach-aware Rooms clients usually connect with `replay=skip` and then send
proxy-local `session/attach` to receive a shaped snapshot/history:

```text
ws://127.0.0.1:8765/acp?room=demo&peer_id=desktop&replay=skip
```

## Named Agents

Instead of passing a raw `--agent-cmd`, register agents in a TOML config (shape
mirrors Zed's `agent_servers`) and launch one by name:

```toml
# ~/.config/acp-mux/agents.toml   (override with --config <path>)
[agents.claude]
command = "npx"
args = ["-y", "@agentclientprotocol/claude-agent-acp"]
# env = { ANTHROPIC_API_KEY = "x" }        # optional; use a real value only in your private config

[agents.gemini]
command = "gemini"
args = ["acp"]
```

```sh
acp-mux --agent claude          # or: rooms --agent claude
acp-mux --list-agents           # show configured agents
```

Default config path is `$XDG_CONFIG_HOME/acp-mux/agents.toml` (falling back to
`~/.config/acp-mux/agents.toml`). `--agent` and `--agent-cmd` are mutually
exclusive; `--agent-cmd` remains the raw escape hatch. A copyable example lives
at [`docs/examples/agents.toml`](docs/examples/agents.toml).

## Tests

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

## Documentation

- [Core mux README](crates/acp-mux/README.md)
- [Rooms README](crates/rooms/README.md)
- [`rooms/*` namespace](docs/design/rooms-namespace.md)
- [Rooms and segments](docs/design/rooms.md)
- [Client contract examples](docs/examples/client-contract)

## License

MIT. See [LICENSE](LICENSE).
