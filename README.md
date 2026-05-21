# acp-mux

Multi-subscriber session-sharing layer for ACP. Lets multiple clients (desktop, phone, etc.) attach to one ACP agent session in real time.

**Status:** pre-v0.1; scaffold in place, multiplex behavior not yet wired.

## Install

```sh
git clone https://github.com/lsaether/acp-mux
cd acp-mux
cargo build --release
# binary: ./target/release/acp-mux
```

## Usage

```sh
acp-mux --help
```

CLI surface lands in chunk 2; the current binary parses `--help`/`--version` only.

## Docs

- Build plan: [ROADMAP.md](ROADMAP.md)
- Protocol spec: [docs/design/amux-namespace.md](docs/design/amux-namespace.md)

## License

MIT — see [LICENSE](LICENSE).
