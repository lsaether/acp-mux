use rooms_client::{ConnectionStatus, InboundMessage, QueueItemStatus, RoomState, TranscriptKind};
use serde_json::json;

#[test]
fn attach_response_seeds_roster_session_snapshot_and_response_history() {
    let mut state = RoomState::default();

    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "id": "rooms-client.attach",
            "result": {
                "sessionId": "sess-1",
                "clientId": "desktop",
                "historyPolicy": "full_lineage",
                "history": [
                    {
                        "jsonrpc": "2.0",
                        "method": "rooms/turn_started",
                        "params": {
                            "roomId": "demo",
                            "roomsTurnId": "at-1",
                            "peerId": "desktop",
                            "peerName": "Desktop",
                            "content": [{ "type": "text", "text": "from history" }]
                        }
                    },
                    {
                        "jsonrpc": "2.0",
                        "method": "session/update",
                        "params": {
                            "sessionId": "sess-1",
                            "update": {
                                "kind": "agent_message_chunk",
                                "content": { "type": "text", "text": "agent chunk" }
                            }
                        }
                    }
                ],
                "_meta": {
                    "rooms": {
                        "connectedClients": [
                            { "peerId": "desktop", "peerName": "Desktop", "role": "primary" },
                            { "peerId": "phone", "peerName": "Phone" }
                        ],
                        "snapshot": {
                            "roomId": "demo",
                            "activeSegmentId": "seg-2"
                        }
                    }
                }
            }
        }))
        .unwrap();

    assert_eq!(state.connection_status, ConnectionStatus::Attached);
    assert_eq!(state.room_id.as_deref(), Some("demo"));
    assert_eq!(state.session_id.as_deref(), Some("sess-1"));
    assert_eq!(state.active_segment_id.as_deref(), Some("seg-2"));
    assert_eq!(state.peers.len(), 2);
    assert_eq!(state.peers[0].peer_id, "desktop");
    assert_eq!(state.peers[0].peer_name.as_deref(), Some("Desktop"));
    assert_eq!(state.peers[0].role.as_deref(), Some("primary"));
    assert_eq!(state.peers[1].peer_id, "phone");

    assert_eq!(state.transcript.len(), 2);
    assert_eq!(state.transcript[0].kind, TranscriptKind::Prompt);
    assert_eq!(state.transcript[0].turn_id.as_deref(), Some("at-1"));
    assert_eq!(state.transcript[0].text, "from history");
    assert!(state.transcript[0].replayed);
    assert_eq!(state.transcript[1].kind, TranscriptKind::AgentUpdate);
    assert_eq!(state.transcript[1].session_id.as_deref(), Some("sess-1"));
    assert_eq!(state.transcript[1].text, "agent chunk");
    assert!(state.transcript[1].replayed);
}

#[test]
fn attach_snapshot_seeds_current_active_turn_queue_and_inert_permissions() {
    let mut state = RoomState::default();

    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "id": "rooms-client.attach",
            "result": {
                "sessionId": "sess-2",
                "clientId": "desktop",
                "_meta": {
                    "rooms": {
                        "snapshot": {
                            "roomId": "demo",
                            "activeSegmentId": "seg-3",
                            "connectedClients": [
                                { "clientId": "desktop", "name": "Desktop" },
                                { "clientId": "phone", "name": "Phone" }
                            ],
                            "selfPeer": { "clientId": "desktop", "name": "Desktop" },
                            "activeTurn": { "roomsTurnId": "at-9", "peerId": "desktop" },
                            "queue": [
                                { "queueItemId": "aq-9", "peerId": "phone", "kind": "prompt", "status": "queued" }
                            ],
                            "pendingPermissions": [
                                { "requestId": "perm-9", "toolName": "fs/write_text_file", "summary": "Write file" }
                            ]
                        }
                    }
                }
            }
        }))
        .unwrap();

    assert_eq!(state.connection_status, ConnectionStatus::Attached);
    assert_eq!(state.peers.len(), 2);
    assert_eq!(state.active_turn.as_ref().unwrap().turn_id, "at-9");
    assert_eq!(state.active_turn.as_ref().unwrap().peer_id, "desktop");
    assert_eq!(state.queue[0].queue_item_id, "aq-9");
    assert_eq!(state.queue[0].peer_id, "phone");
    assert_eq!(state.queue[0].status, QueueItemStatus::Queued);
    assert_eq!(state.pending_permissions[0].request_id, "perm-9");
    assert_eq!(
        state.pending_permissions[0].title.as_deref(),
        Some("Write file")
    );
    assert!(!state.pending_permissions[0].actionable);
}

#[test]
fn live_frames_track_active_turn_queue_permissions_and_peers() {
    let mut state = RoomState::default();

    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "method": "rooms/peer_joined",
            "params": { "roomId": "demo", "peerId": "desktop", "peerName": "Desktop" }
        }))
        .unwrap();
    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "method": "rooms/turn_started",
            "params": {
                "roomId": "demo",
                "roomsTurnId": "at-2",
                "peerId": "desktop",
                "peerName": "Desktop",
                "content": [{ "type": "text", "text": "build reducer" }]
            }
        }))
        .unwrap();
    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "method": "rooms/queue_item_added",
            "params": {
                "roomId": "demo",
                "queueItemId": "aq-1",
                "peerId": "phone",
                "peerName": "Phone",
                "text": "next prompt",
                "status": "queued"
            }
        }))
        .unwrap();
    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "method": "session/request_permission",
            "id": "perm-1",
            "params": {
                "sessionId": "sess-1",
                "toolCall": { "title": "Write file" },
                "options": [{ "optionId": "allow" }, { "optionId": "deny" }]
            }
        }))
        .unwrap();

    assert_eq!(state.peers.len(), 2, "turn/queue attribution upserts peers");
    assert_eq!(state.active_turn.as_ref().unwrap().turn_id, "at-2");
    assert_eq!(state.active_turn.as_ref().unwrap().text, "build reducer");
    assert_eq!(state.queue[0].queue_item_id, "aq-1");
    assert_eq!(state.queue[0].status, QueueItemStatus::Queued);
    assert_eq!(state.pending_permissions[0].request_id, "perm-1");
    assert!(state.pending_permissions[0].actionable);
    assert_eq!(state.pending_permissions[0].options, vec!["allow", "deny"]);

    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "method": "rooms/queue_item_submitted",
            "params": { "roomId": "demo", "queueItemId": "aq-1", "roomsTurnId": "at-3" }
        }))
        .unwrap();
    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "method": "rooms/agent_request_resolved",
            "params": { "roomId": "demo", "requestId": "perm-1", "resolvedBy": "desktop" }
        }))
        .unwrap();
    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "method": "rooms/turn_complete",
            "params": { "roomId": "demo", "roomsTurnId": "at-2", "stopReason": "end_turn" }
        }))
        .unwrap();
    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "method": "rooms/peer_left",
            "params": { "roomId": "demo", "peerId": "phone" }
        }))
        .unwrap();

    assert!(state.active_turn.is_none());
    assert_eq!(state.queue[0].status, QueueItemStatus::Submitted);
    assert_eq!(state.queue[0].turn_id.as_deref(), Some("at-3"));
    assert!(state.pending_permissions.is_empty());
    assert_eq!(state.peers.len(), 1);
    assert_eq!(state.peers[0].peer_id, "desktop");
}

#[test]
fn streamed_replay_marks_frames_replayed_and_does_not_replace_snapshot_state() {
    let mut state = RoomState::default();
    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "id": "rooms-client.attach",
            "result": {
                "sessionId": "sess-current",
                "clientId": "desktop",
                "_meta": {
                    "rooms": {
                        "snapshot": {
                            "roomId": "demo",
                            "connectedClients": [{ "clientId": "desktop", "name": "Desktop" }],
                            "selfPeer": { "clientId": "desktop", "name": "Desktop" },
                            "activeTurn": { "roomsTurnId": "at-current", "peerId": "desktop" },
                            "queue": [],
                            "pendingPermissions": []
                        }
                    }
                }
            }
        }))
        .unwrap();

    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "method": "rooms/replay_started",
            "params": {
                "roomId": "demo",
                "phase": "backfill",
                "replayOrder": "newest_turn_first",
                "generation": 8,
                "replayBoundarySeq": 12,
                "frameCount": 2
            }
        }))
        .unwrap();
    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "method": "rooms/turn_started",
            "params": {
                "roomId": "demo",
                "roomsTurnId": "at-old",
                "peerId": "phone",
                "peerName": "Phone",
                "content": [{ "type": "text", "text": "old turn" }]
            }
        }))
        .unwrap();
    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "method": "session/request_permission",
            "id": "replayed-perm",
            "params": {
                "sessionId": "sess-old",
                "toolCall": { "title": "Old permission" },
                "options": [{ "optionId": "allow" }]
            }
        }))
        .unwrap();
    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "method": "rooms/replay_complete",
            "params": {
                "roomId": "demo",
                "phase": "backfill",
                "replayOrder": "newest_turn_first",
                "generation": 8,
                "replayBoundarySeq": 12,
                "frameCount": 2
            }
        }))
        .unwrap();

    assert_eq!(state.connection_status, ConnectionStatus::Live);
    assert_eq!(state.active_turn.as_ref().unwrap().turn_id, "at-current");
    assert_eq!(state.session_id.as_deref(), Some("sess-current"));
    assert_eq!(
        state.transcript.last().unwrap().turn_id.as_deref(),
        Some("at-old")
    );
    assert!(state.transcript.last().unwrap().replayed);
    assert_eq!(state.pending_permissions[0].request_id, "replayed-perm");
    assert!(!state.pending_permissions[0].actionable);
}

#[test]
fn attach_snapshot_null_active_turn_and_non_attach_responses_do_not_clobber_state() {
    let mut state = RoomState::default();
    state.set_connection_status(ConnectionStatus::Live);
    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "id": "rooms-client.attach",
            "result": {
                "sessionId": "sess-idle",
                "clientId": "desktop",
                "_meta": {
                    "rooms": {
                        "snapshot": {
                            "roomId": "demo",
                            "connectedClients": [{ "clientId": "desktop", "name": "Desktop" }],
                            "selfPeer": { "clientId": "desktop", "name": "Desktop" },
                            "activeTurn": null,
                            "queue": [],
                            "pendingPermissions": []
                        }
                    }
                }
            }
        }))
        .unwrap();

    assert_eq!(state.connection_status, ConnectionStatus::Attached);
    assert!(state.active_turn.is_none());

    state.set_connection_status(ConnectionStatus::Live);
    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "id": "other-command",
            "result": {
                "sessionId": "other-session",
                "clientId": "other-client",
                "history": []
            }
        }))
        .unwrap();

    assert_eq!(state.connection_status, ConnectionStatus::Live);
    assert_eq!(state.session_id.as_deref(), Some("sess-idle"));
}

#[test]
fn protocol_error_frames_are_recorded_without_marking_connection_failed() {
    let mut state = RoomState::default();
    state.set_connection_status(ConnectionStatus::Live);

    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "id": "rooms-client.prompt-1",
            "error": { "code": -32001, "message": "busy" }
        }))
        .unwrap();

    assert_eq!(state.connection_status, ConnectionStatus::Live);
    assert_eq!(state.errors.len(), 1);
    assert!(state.errors[0].contains("busy"));
}

#[test]
fn replay_markers_unknown_frames_and_transport_errors_update_status_and_debug() {
    let mut state = RoomState::default();

    state
        .apply_inbound(&InboundMessage::Frame {
            raw: json!({
                "jsonrpc": "2.0",
                "method": "rooms/replay_started",
                "params": {
                    "roomId": "demo",
                    "phase": "attach_history",
                    "replayOrder": "newest_turn_first",
                    "generation": 7,
                    "replayBoundarySeq": 12,
                    "frameCount": 3
                }
            }),
            event: None,
        })
        .unwrap();

    assert_eq!(state.connection_status, ConnectionStatus::Replaying);
    assert_eq!(state.replay.as_ref().unwrap().phase, "attach_history");
    assert_eq!(state.replay.as_ref().unwrap().frame_count, 3);

    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "method": "rooms/new_future_event",
            "params": { "roomId": "demo" }
        }))
        .unwrap();
    assert_eq!(state.debug_frames[0].method, "rooms/new_future_event");

    state
        .apply_frame(&json!({
            "jsonrpc": "2.0",
            "method": "rooms/replay_complete",
            "params": {
                "roomId": "demo",
                "phase": "attach_history",
                "replayOrder": "newest_turn_first",
                "generation": 7,
                "replayBoundarySeq": 12,
                "frameCount": 3
            }
        }))
        .unwrap();
    assert_eq!(state.connection_status, ConnectionStatus::Live);
    assert!(state.replay.is_none());

    state
        .apply_inbound(&InboundMessage::Error("bad frame".into()))
        .unwrap();
    assert_eq!(state.connection_status, ConnectionStatus::Error);
    assert_eq!(state.errors, vec!["bad frame"]);

    state.apply_inbound(&InboundMessage::Closed).unwrap();
    assert_eq!(state.connection_status, ConnectionStatus::Closed);
}
