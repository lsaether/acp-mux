# Client migration guide: v0.2 rooms and segment-aware history

This guide is for clients moving from the old single-`session` mental model to the v0.2 room model.

`acp-mux` is now best treated as a generic ACP agent mirror:

```text
roomId                stable mux-level collaboration container
currentAcpSessionId   current upstream ACP session id inside that room
segments              intervals where one canonical ACP session id is active
```

A room is the thing your WebSocket attaches to. An ACP `sessionId` is the thing the upstream agent understands. Keep them separate.

## Required client changes

1. Connect with `?room=<room-id>` instead of `?session=<id>`.
2. Keep `roomId` and `currentAcpSessionId` as separate state fields.
3. Treat `rooms/*` frames as mux control/lifecycle metadata, not agent conversation.
4. Use `session/attach` as the bootstrap source when possible. Prefer `replay=skip` on the WebSocket URL so you do not process both legacy WS replay and attach history.
5. Request `historyPolicy: "full_lineage"` when the UI should restore the whole room transcript across ACP session-id rotations.
6. Parse and ignore-or-render `rooms/segment_started` / `rooms/segment_ended` lifecycle frames.
7. For streamed attach history, branch on `result._meta.rooms.appliedHistoryDelivery`; do not assume the server accepted stream mode just because you requested it.

## Connect

Old shape:

```text
/acp?session=work&peer_id=desktop&peer_name=Desktop
```

New shape:

```text
/acp?room=work&peer_id=desktop&peer_name=Desktop&replay=skip
```

`?session=` is a deprecated alias during the transition. Do not build new clients on it.

## Bootstrap with `session/attach`

Recommended flow:

1. Open WebSocket with `replay=skip`.
2. Wait for direct attach context such as `rooms/session_context`.
3. Send `session/attach`.
4. Apply the returned snapshot/roster/history.
5. Continue with normal live frame handling.

Example:

```jsonc
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "session/attach",
  "params": {
    "historyPolicy": "full_lineage",
    "_meta": {
      "rooms": {
        "replayOrder": "chronological",
        "historyDelivery": "response"
      }
    }
  }
}
```

Expected response fields:

```jsonc
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "sessionId": "<current ACP session id or null>",
    "clientId": "<your peerId>",
    "historyPolicy": "full_lineage",
    "history": [],
    "_meta": {
      "rooms": {
        "connectedClients": [],
        "appliedReplayOrder": "chronological",
        "appliedHistoryDelivery": "response",
        "snapshot": {
          "roomId": "work",
          "activeSegmentId": "seg-1",
          "segments": []
        }
      }
    }
  }
}
```

## History policies

| Policy | Meaning | Recommended use |
|---|---|---|
| `full` | Current segment history plus bootstrap frames and turn bookends needed to bracket current-segment turns. | Lightweight reconnect when current head is enough. |
| `full_lineage` | All segment history in global `replaySeq` order. | Normal chat/TUI transcript restore. |
| `none` | No history. | Status-only attaches. |
| `pending_only` | Pending permission/request state only. | Approval/notification resume surfaces. |
| `after_message` | Accepted but currently falls back to `full`. | Do not rely on precise incremental replay yet. |

Main gotcha: **`full` is not "everything ever in the room".** It is current-head history. Use `full_lineage` for full room restore.

## Segment frames

A segment is the interval where one ACP `sessionId` is canonical for the room. The mux rotates segments on provider-neutral signals:

- successful `session/load`;
- agent notification with an observable `params.sessionId` different from the active segment.

Provider-specific metadata is not a segment signal.

`rooms/segment_started`:

```json
{
  "jsonrpc": "2.0",
  "method": "rooms/segment_started",
  "params": {
    "roomId": "work",
    "segmentId": "seg-2",
    "acpSessionId": "sess-child",
    "openedAt": "2026-05-27T19:00:00Z"
  }
}
```

`rooms/segment_ended`:

```json
{
  "jsonrpc": "2.0",
  "method": "rooms/segment_ended",
  "params": {
    "roomId": "work",
    "segmentId": "seg-1",
    "closedAt": "2026-05-27T19:00:00Z",
    "endReason": "session_load",
    "successorSegmentId": "seg-2"
  }
}
```

`endReason` values:

- `session_load`
- `acp_session_id_changed`

## Rendering guidance

- Use `rooms/turn_started` and `rooms/turn_complete` as turn bookends.
- Use `roomsTurnId`, not ACP `sessionId`, to bracket output chunks into a turn.
- Use `roomId` for room UI and connection state.
- Use `currentAcpSessionId` only in ACP payloads (`session/prompt`, `session/load`, `session/cancel`, etc.).
- Render segment boundaries as compact session-load/session-rotation dividers if useful, but do not treat them as separate conversations.
- If `snapshot.segments.length > 1`, the transcript spans multiple ACP session ids.

## Permission/request handling

Live `session/request_permission` frames are actionable. Replayed `rooms/agent_request_opened` frames are not.

Rules:

1. Show an approval UI only for a live or re-issued raw `session/request_permission` request.
2. Treat `rooms/agent_request_opened` as context for history/audit.
3. Treat `rooms/agent_request_resolved` as a dismissal/update signal.
4. First valid peer reply wins; late replies may be dropped if another peer already answered.

## Streamed attach history

If requesting stream mode:

```jsonc
{
  "historyPolicy": "full_lineage",
  "_meta": { "rooms": { "historyDelivery": "stream" } }
}
```

Check the response:

```jsonc
"_meta": {
  "rooms": {
    "appliedHistoryDelivery": "stream"
  }
}
```

If accepted, the mux brackets streamed history with:

- `rooms/replay_started`
- historical frames
- `rooms/replay_complete`

If the applied delivery is `response`, read `result.history` instead.

## Minimal migration checklist

- [ ] URL uses `room`, not `session`.
- [ ] Client state separates `roomId`, `currentAcpSessionId`, and `activeSegmentId`.
- [ ] Client filters `rooms/*` out of agent conversation rendering.
- [ ] Client handles `rooms/turn_started` / `rooms/turn_complete` / `rooms/turn_cancelled`.
- [ ] Client handles `rooms/segment_started` / `rooms/segment_ended`.
- [ ] Full transcript restore uses `historyPolicy: "full_lineage"`.
- [ ] Approval UI only treats raw live/re-issued `session/request_permission` as actionable.
- [ ] Streamed attach path checks `appliedHistoryDelivery` before assuming stream markers will arrive.
