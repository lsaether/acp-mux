# Client contract fixtures

Copyable JSON fixtures for clients that talk to `acp-mux`.

These are **contract examples**, not a mock-agent transcript. They use a real-agent-oriented naming convention (`sess-claude-1`, `claude-desktop`) so examples line up with the README's Claude Agent ACP quickstart, but the `amux/*` shapes are provider-neutral.

## Layout

- `requests/` — frames a client can send to `amux`.
- `responses/` — representative mux-owned responses.
- `notifications/` — mux-owned `amux/*` notifications clients should handle.
- `sequences/` — JSONL frame sequences for UI/rendering tests.

## Notes

- `roomId` is the mux collaboration container.
- `sessionId` / `acpSessionId` is the upstream ACP agent's id.
- Provider metadata, if present, is payload data and is not represented in these mux-owned fixtures.
- Treat `amux/agent_request_opened` as replay-safe context. Only raw live or re-issued ACP `session/request_permission` requests are actionable.
