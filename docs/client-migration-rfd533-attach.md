# Client migration: RFD #533 attach response history

This guide is for amux clients migrating after the RFD #533-inspired attach work in
PR #46, the attach response replay-order work in PR #47, and the legacy replay
suppression work in PR #50.

The short version:

- Existing legacy WebSocket clients can keep using the chronological replay stream
  they already receive on connect.
- Attach-aware clients should call proxy-local `session/attach` after the
  WebSocket opens and choose a `historyPolicy`.
- If a client wants newest-turn-first bootstrap rendering, request it on
  `session/attach` with `params._meta.amux.replayOrder`, then render
  `result.history` from the attach response.
- Attach-history clients should connect with `/acp?...&replay=skip` so the
  legacy WebSocket auto-replay is suppressed for that connection.
- Do not render both the legacy WebSocket replay and `session/attach.result.history`
  as transcript history.

## What changed

Before these PRs, a late WebSocket subscriber rebuilt state from the legacy
chronological replay stream sent immediately after transport attach.

After these PRs, amux also supports a logical, proxy-local attach handshake:

```json
{
  "jsonrpc": "2.0",
  "id": 5,
  "method": "session/attach",
  "params": {
    "sessionId": "sess_abc123",
    "clientId": "phone-1",
    "historyPolicy": "full",
    "_meta": {
      "amux": {
        "replayOrder": "newest_turn_first"
      }
    }
  }
}
```

The mux answers this request itself. It does not forward it to the wrapped ACP
agent.

A response looks like:

```json
{
  "jsonrpc": "2.0",
  "id": 5,
  "result": {
    "sessionId": "sess_abc123",
    "clientId": "phone-1",
    "historyPolicy": "full",
    "history": [
      {
        "method": "amux/peer_joined",
        "params": { "sessionId": "work", "peerId": "desktop" }
      },
      {
        "method": "amux/turn_started",
        "params": { "amuxTurnId": "at-2" }
      },
      {
        "method": "session/update",
        "params": { "sessionId": "sess_abc123", "update": {} }
      },
      {
        "method": "amux/turn_complete",
        "params": { "amuxTurnId": "at-2" }
      }
    ],
    "_meta": {
      "amux": {
        "connectedClients": [
          { "clientId": "desktop", "name": "Desktop" },
          { "clientId": "phone-1", "name": "Phone" }
        ],
        "appliedReplayOrder": "newest_turn_first"
      }
    }
  }
}
```

`history[]` entries are replay entries, not complete JSON-RPC frames. They carry
only `{ method, params }`; route them through the same event reducer you use for
live notifications/requests, but do not expect `jsonrpc` or `id` fields.

Per-connection frames such as `amux/session_context` are still delivered on the
WebSocket outside `result.history`; apply them to local connection/session state
if you need them, but do not treat them as transcript replay.

## Which history source should a client use?

Choose exactly one bootstrap source per connection.

### Option A: legacy-compatible clients

Use the existing WebSocket replay stream and do not request attach history:

```json
{
  "jsonrpc": "2.0",
  "id": 5,
  "method": "session/attach",
  "params": {
    "historyPolicy": "none"
  }
}
```

Use this if the client is already chronological-stream oriented and only wants
attach metadata such as the current peer roster.

Rules:

- Render the legacy WebSocket replay as before.
- Treat `result._meta.amux.connectedClients` as the current roster snapshot.
- Ignore `result.history` because it is omitted for `historyPolicy: "none"`.

### Option B: attach-response bootstrap clients

Use `session/attach.result.history` as the single bootstrap transcript source.

Recommended request for newest-first transcript UIs:

```json
{
  "jsonrpc": "2.0",
  "id": 5,
  "method": "session/attach",
  "params": {
    "historyPolicy": "full",
    "_meta": {
      "amux": {
        "replayOrder": "newest_turn_first"
      }
    }
  }
}
```

Rules:

1. Open the WebSocket with `replay=skip` in the `/acp` query string.
2. Immediately send `session/attach` before sending prompts or other session
   mutations.
3. Apply direct non-transcript context such as `amux/session_context` to
   connection state; it is still delivered even when replay is skipped.
4. When the attach response arrives, initialize the transcript from
   `result.history`.
5. Continue with live WebSocket frames after the attach handshake.

The `replay=skip` flag is transport-level only. It suppresses the legacy
chronological replay snapshot that would otherwise arrive before the attach
request, but it does not control attach response ordering. Keep using
`session/attach` metadata for `historyPolicy` and
`params._meta.amux.replayOrder`.

## History policies

`historyPolicy` is requested in `params.historyPolicy` and the effective policy
is returned in `result.historyPolicy`.

| Request policy | Client expectation |
| --- | --- |
| `full` | `result.history` contains the mux replay log as `{ method, params }` entries. |
| `none` | `result.history` is omitted. Use the legacy WS stream or live-only behavior. |
| `pending_only` | `result.history` contains unresolved pending permission context. Raw actionable permission requests are also re-issued after attach. |
| `after_message` | Accepted for forward compatibility, but currently falls back to `full`; always check `result.historyPolicy`. |

For `pending_only`, do not answer permission prompts from `result.history`
itself. Treat `history` as passive context. The actionable request is the raw
`session/request_permission` JSON-RPC request re-issued on the WebSocket after the
attach response. Dedupe display by request id if you render both passive history
and actionable prompts.

## Replay order

Replay order is amux-owned extension metadata under `_meta.amux`; it is not an
ACP-standard field.

Request:

```json
{
  "params": {
    "historyPolicy": "full",
    "_meta": {
      "amux": {
        "replayOrder": "newest_turn_first"
      }
    }
  }
}
```

Response:

```json
{
  "result": {
    "_meta": {
      "amux": {
        "appliedReplayOrder": "newest_turn_first"
      }
    }
  }
}
```

Supported values:

| Value | Behavior |
| --- | --- |
| `chronological` | Default. Return history in durable replay-log order. |
| `newest_turn_first` | Keep setup/non-turn frames first, then reverse completed turn groups. Frames inside each turn stay chronological. |

Clients should read `result._meta.amux.appliedReplayOrder` rather than assuming
the request was applied. Today it will usually match the request or default to
`chronological`; the response field exists so future fallbacks can be explicit.

If you need a perfect chronological event timeline, request `chronological`.
`newest_turn_first` is optimized for transcript bootstrap UX: latest turns first,
while each turn remains internally readable from start to completion.

## Event handling model

Use one reducer for both attach response history and live frames:

```ts
type ReplayEntry = { method: string; params?: unknown };

function applyReplayEntry(entry: ReplayEntry) {
  switch (entry.method) {
    case "amux/session_context":
      // update mux/session context
      break;
    case "amux/peer_joined":
    case "amux/peer_left":
      // update presence
      break;
    case "amux/turn_started":
    case "amux/turn_complete":
      // maintain turn bookends and attribution
      break;
    case "amux/agent_request_opened":
    case "amux/agent_request_resolved":
      // passive lifecycle/audit state, not an actionable request
      break;
    case "session/update":
      // apply wrapped-agent transcript/tool/plan update
      break;
    default:
      // preserve/ignore unknown events; do not crash on extension methods
      break;
  }
}
```

For live WebSocket messages, parse full JSON-RPC frames first:

- Notifications with `method` + `params` can be adapted into the same
  `{ method, params }` reducer path.
- Requests with `id` are actionable only when the method is a real live request
  such as `session/request_permission`.
- Attach `history[]` entries are never actionable JSON-RPC requests because they
  do not include a request `id`.

## Roster and lifecycle

`session/attach` returns the current roster at:

```text
result._meta.amux.connectedClients
```

After attach, keep roster current from live `amux/peer_joined` and
`amux/peer_left` frames.

amux still does not fabricate proxy-owned ACP `session/update` lifecycle siblings
for RFD #533 concepts such as `client_connected`, `client_disconnected`,
`prompt_received`, or `permission_resolved`. amux-aware clients should continue
using the `amux/*` namespace for mux lifecycle.

## Capability discovery

Do not expect the wrapped agent's `initialize` response to advertise attach
support. amux passes the agent's capabilities through honestly and does not
inject `agentCapabilities.sessionCapabilities.attach`.

Clients should opt into this facade because they know they are connected to
amux, or through out-of-band product configuration.

## Do / do not checklist

Do:

- Send `session/attach` immediately after WebSocket open for attach-aware modes.
- Choose one bootstrap source: legacy WS replay **or** `result.history`.
- Use `historyPolicy: "none"` if consuming legacy replay.
- Use `/acp?...&replay=skip` if consuming attach response history.
- Use `historyPolicy: "full"` plus `_meta.amux.replayOrder` if consuming attach
  response history.
- Check effective `result.historyPolicy` and
  `result._meta.amux.appliedReplayOrder`.
- Treat replay entries as passive `{ method, params }` events.
- Keep direct connection context such as `amux/session_context` separate from
  transcript replay suppression.
- Continue using `amux/*` for mux lifecycle.

Do not:

- Render both legacy WS replay and attach `result.history` into the transcript.
- Put replay ordering in the WebSocket URL.
- Wait for streamed `amux/replay_started`, `amux/session_snapshot`, or
  `amux/replay_complete` markers; this path does not use them.
- Expect `session/update` lifecycle siblings fabricated by the proxy.
- Answer permission requests from `result.history`; only answer live raw
  JSON-RPC requests with an `id`.
- Assume `after_message` is precise yet; check for the current fallback to
  `full`.

## Suggested migration plan

1. Add a small attach client wrapper that sends `session/attach` and normalizes
   `result.history` into your existing event reducer.
2. Add a feature flag per client:
   - `legacy_ws_replay`: current behavior, optionally call attach with
     `historyPolicy: "none"` for roster metadata.
   - `attach_history_chronological`: connect with `replay=skip`, then use
     `historyPolicy: "full"` and default replay order.
   - `attach_history_newest_first`: connect with `replay=skip`, then use
     `historyPolicy: "full"` and `_meta.amux.replayOrder:
     "newest_turn_first"`.
3. Keep direct connection context such as `amux/session_context` separate from
   transcript history suppression.
4. Add fixtures for:
   - attach `historyPolicy: "none"` returns no `history`;
   - attach `historyPolicy: "full"` returns `{ method, params }` entries;
   - `newest_turn_first` reverses turn groups but preserves order inside each
     turn;
   - `replay=skip` suppresses legacy WS replay but preserves live frames;
   - pending permission history is passive, and the later raw request is
     actionable;
   - `after_message` currently returns effective `historyPolicy: "full"`.

## Related references

- PR #46: RFD #533-inspired attach/detach foundation.
- PR #47: attach response replay ordering.
- PR #50 / Issue #48: server-side suppression for legacy WebSocket replay
  during attach-aware migrations.
- Design spec: [`docs/design/amux-namespace.md`](design/amux-namespace.md).
