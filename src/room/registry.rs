//! Session registry: maps session ids to live session actors.
//!
//! On attach: if the session exists and its actor is still alive, the
//! subscriber joins it; otherwise a fresh agent subprocess is spawned and
//! a new session actor is started. The registry lock is released before
//! awaiting the actor's Attach ack to avoid head-of-line blocking on
//! concurrent attaches to different sessions.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{Mutex, oneshot};
use tokio::time::timeout;

use crate::agent::process::AgentProcess;
use crate::cli::{ClientToolPolicy, ReplayTurns};
use crate::multiplex::subscriber::Subscriber;
use crate::protocol::jsonrpc::{Id, Incoming, JsonRpcError, ParseError};
use crate::room::replay_store::ReplayStore;
use crate::room::state::{
    AttachError, RoomHandle, RoomMsg, RoomOptions, RoomSnapshot, SessionListMetadataIndex,
    spawn_room,
};

const CONTROL_PLANE_AGENT_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Debug, Clone)]
pub struct AgentCmd {
    pub program: String,
    pub args: Vec<String>,
}

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("server has no --agent-cmd configured")]
    AgentCmdMissing,
    #[error("peer_id already attached to this session")]
    PeerIdInUse,
    #[error("agent spawn failed: {0}")]
    AgentSpawn(#[from] anyhow::Error),
    #[error("session actor not reachable")]
    ActorUnreachable,
}

#[derive(Debug, Error)]
pub enum ControlPlaneSessionListError {
    #[error("agent command not configured")]
    AgentCmdMissing,
    #[error("agent process failed: {0}")]
    AgentProcess(#[from] anyhow::Error),
    #[error("json encode/decode failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("agent protocol parse failed: {0}")]
    Protocol(#[from] ParseError),
    #[error("agent returned JSON-RPC error {code}: {message}")]
    AgentJsonRpc {
        code: i64,
        message: String,
        data: Option<Value>,
    },
    #[error("agent did not respond to {method} before timeout")]
    AgentTimeout { method: &'static str },
    #[error("agent exited before responding to {method}")]
    AgentEof { method: &'static str },
}

impl From<JsonRpcError> for ControlPlaneSessionListError {
    fn from(err: JsonRpcError) -> Self {
        Self::AgentJsonRpc {
            code: err.code,
            message: err.message,
            data: err.data,
        }
    }
}

async fn query_transient_session_list(
    agent: &mut AgentProcess,
    cwd: Option<String>,
) -> Result<Value, ControlPlaneSessionListError> {
    let _initialize = request_transient_agent(
        agent,
        1,
        "initialize",
        Some(json!({ "protocolVersion": 1 })),
    )
    .await?;

    let params = cwd.map(|cwd| json!({ "cwd": cwd }));
    request_transient_agent(agent, 2, "session/list", params).await
}

async fn request_transient_agent(
    agent: &mut AgentProcess,
    id: i64,
    method: &'static str,
    params: Option<Value>,
) -> Result<Value, ControlPlaneSessionListError> {
    let mut request = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
    });
    if let Some(params) = params {
        request["params"] = params;
    }
    let bytes = serde_json::to_vec(&request)?;
    agent.send(&bytes).await?;

    for _ in 0..128 {
        let line = timeout(CONTROL_PLANE_AGENT_TIMEOUT, agent.recv_line())
            .await
            .map_err(|_| ControlPlaneSessionListError::AgentTimeout { method })?
            .ok_or(ControlPlaneSessionListError::AgentEof { method })?;
        let incoming = Incoming::parse(&line)?;
        let Incoming::Response(response) = incoming else {
            continue;
        };
        if response.id != Id::Number(id) {
            continue;
        }
        if let Some(error) = response.error {
            return Err(error.into());
        }
        return Ok(response.result.unwrap_or(Value::Null));
    }

    Err(ControlPlaneSessionListError::AgentTimeout { method })
}

pub struct RoomRegistry {
    agent_cmd: Option<AgentCmd>,
    replay_policy: ReplayTurns,
    session_ttl: Duration,
    meta_propagate: bool,
    client_tool_policy: ClientToolPolicy,
    emit_segment_frames: bool,
    hermes_compaction_signals: bool,
    replay_store: Option<Arc<ReplayStore>>,
    session_list_index: Arc<SessionListMetadataIndex>,
    sessions: Mutex<HashMap<String, RoomHandle>>,
}

impl RoomRegistry {
    pub fn new(
        agent_cmd: Option<AgentCmd>,
        replay_policy: ReplayTurns,
        session_ttl: Duration,
    ) -> Arc<Self> {
        Self::new_with_client_tool_policy(
            agent_cmd,
            replay_policy,
            session_ttl,
            false,
            ClientToolPolicy::default(),
        )
    }

    pub fn new_with_meta_propagation(
        agent_cmd: Option<AgentCmd>,
        replay_policy: ReplayTurns,
        session_ttl: Duration,
        meta_propagate: bool,
    ) -> Arc<Self> {
        Self::new_with_client_tool_policy(
            agent_cmd,
            replay_policy,
            session_ttl,
            meta_propagate,
            ClientToolPolicy::default(),
        )
    }

    pub fn new_with_client_tool_policy(
        agent_cmd: Option<AgentCmd>,
        replay_policy: ReplayTurns,
        session_ttl: Duration,
        meta_propagate: bool,
        client_tool_policy: ClientToolPolicy,
    ) -> Arc<Self> {
        Self::new_with_options(
            agent_cmd,
            replay_policy,
            session_ttl,
            meta_propagate,
            client_tool_policy,
            true,
        )
    }

    pub fn new_with_options(
        agent_cmd: Option<AgentCmd>,
        replay_policy: ReplayTurns,
        session_ttl: Duration,
        meta_propagate: bool,
        client_tool_policy: ClientToolPolicy,
        emit_segment_frames: bool,
    ) -> Arc<Self> {
        Self::new_with_replay_store(
            agent_cmd,
            replay_policy,
            session_ttl,
            meta_propagate,
            client_tool_policy,
            emit_segment_frames,
            None,
        )
    }

    pub fn new_with_replay_store(
        agent_cmd: Option<AgentCmd>,
        replay_policy: ReplayTurns,
        session_ttl: Duration,
        meta_propagate: bool,
        client_tool_policy: ClientToolPolicy,
        emit_segment_frames: bool,
        replay_store: Option<Arc<ReplayStore>>,
    ) -> Arc<Self> {
        Self::new_full(
            agent_cmd,
            replay_policy,
            session_ttl,
            meta_propagate,
            client_tool_policy,
            emit_segment_frames,
            false,
            replay_store,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_full(
        agent_cmd: Option<AgentCmd>,
        replay_policy: ReplayTurns,
        session_ttl: Duration,
        meta_propagate: bool,
        client_tool_policy: ClientToolPolicy,
        emit_segment_frames: bool,
        hermes_compaction_signals: bool,
        replay_store: Option<Arc<ReplayStore>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            agent_cmd,
            replay_policy,
            session_ttl,
            meta_propagate,
            client_tool_policy,
            emit_segment_frames,
            hermes_compaction_signals,
            replay_store,
            session_list_index: Arc::new(SessionListMetadataIndex::new()),
            sessions: Mutex::new(HashMap::new()),
        })
    }

    /// Cold-start session discovery: spawn a transient agent subprocess,
    /// initialize it, send `session/list`, return the agent's result, and
    /// tear the subprocess down without registering a live mux session.
    pub async fn list_sessions_control_plane(
        &self,
        cwd: Option<String>,
    ) -> Result<Value, ControlPlaneSessionListError> {
        let cmd = self
            .agent_cmd
            .clone()
            .ok_or(ControlPlaneSessionListError::AgentCmdMissing)?;
        let mut agent = AgentProcess::spawn(&cmd.program, &cmd.args).await?;
        let result = query_transient_session_list(&mut agent, cwd).await;
        if let Err(err) = agent.shutdown(CONTROL_PLANE_AGENT_TIMEOUT).await {
            tracing::warn!(error = %err, "transient session/list agent shutdown failed");
        }
        result
    }

    /// Attach a subscriber to `session_id`. Two paths:
    /// - existing live session: send Attach over the actor channel and wait
    ///   for ack (so peer_id collision turns into PeerIdInUse).
    /// - no live session (or dead handle): spawn the agent, start a new
    ///   actor with `subscriber` as the initial member.
    pub async fn attach(
        self: &Arc<Self>,
        session_id: &str,
        subscriber: Subscriber,
    ) -> Result<RoomHandle, RegistryError> {
        let existing = {
            let mut sessions = self.sessions.lock().await;
            match sessions.get(session_id) {
                Some(h) if h.is_alive() => Some(h.clone()),
                Some(_) => {
                    sessions.remove(session_id);
                    None
                }
                None => None,
            }
        };

        if let Some(handle) = existing {
            // Try to join the live session. If the actor died between the
            // is_alive check and the send, fall through to spawn.
            match self.try_join(&handle, subscriber).await {
                Ok(()) => return Ok(handle),
                Err(RegistryError::ActorUnreachable) => {
                    // Session died after our snapshot; fall through to
                    // spawn a fresh one. We can't recover the subscriber
                    // (it was consumed by try_join), so the WS attach
                    // returns ActorUnreachable in this rare race.
                    return Err(RegistryError::ActorUnreachable);
                }
                Err(other) => return Err(other),
            }
        }

        let mut sessions = self.sessions.lock().await;
        // Re-check under lock: another attach() may have spawned us a
        // session between our snapshot and now. If so, recurse-ish by
        // re-running the try_join path on the now-live handle. But the
        // subscriber has already been consumed if we got here from the
        // first try_join branch — and we didn't, so it's still in hand.
        if let Some(h) = sessions.get(session_id) {
            if h.is_alive() {
                let handle = h.clone();
                drop(sessions);
                self.try_join(&handle, subscriber).await?;
                return Ok(handle);
            }
            sessions.remove(session_id);
        }
        self.spawn_locked(&mut sessions, session_id, subscriber)
            .await
    }

    async fn spawn_locked(
        self: &Arc<Self>,
        sessions: &mut HashMap<String, RoomHandle>,
        session_id: &str,
        subscriber: Subscriber,
    ) -> Result<RoomHandle, RegistryError> {
        let cmd = self
            .agent_cmd
            .as_ref()
            .ok_or(RegistryError::AgentCmdMissing)?;
        let agent_cwd = std::env::current_dir()
            .map(|path| path.to_string_lossy().to_string())
            .unwrap_or_else(|err| {
                tracing::warn!(error = %err, "failed to read current dir for session context");
                String::new()
            });
        let agent = AgentProcess::spawn(&cmd.program, &cmd.args)
            .await
            .map_err(RegistryError::AgentSpawn)?;
        let (handle, _actor) = spawn_room(
            subscriber,
            agent,
            session_id.to_string(),
            RoomOptions {
                replay_policy: self.replay_policy,
                session_ttl: self.session_ttl,
                meta_propagate: self.meta_propagate,
                client_tool_policy: self.client_tool_policy,
                session_list_index: self.session_list_index.clone(),
                agent_cwd,
                emit_segment_frames: self.emit_segment_frames,
                hermes_compaction_signals: self.hermes_compaction_signals,
                replay_store: self.replay_store.clone(),
            },
        );
        sessions.insert(session_id.to_string(), handle.clone());
        tracing::info!(session = %session_id, "spawned session");
        Ok(handle)
    }

    async fn try_join(
        &self,
        handle: &RoomHandle,
        subscriber: Subscriber,
    ) -> Result<(), RegistryError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        handle
            .tx
            .send(RoomMsg::Attach {
                subscriber,
                ack: ack_tx,
            })
            .await
            .map_err(|_| RegistryError::ActorUnreachable)?;
        match ack_rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(AttachError::PeerIdInUse)) => Err(RegistryError::PeerIdInUse),
            Err(_) => Err(RegistryError::ActorUnreachable),
        }
    }

    /// Force-shutdown all sessions. Drops every RoomHandle, which causes
    /// each actor to see its rx closed and exit, which drops subscriber
    /// senders and shuts down the agent subprocess.
    pub async fn shutdown(&self) {
        let mut sessions = self.sessions.lock().await;
        let count = sessions.len();
        sessions.clear();
        tracing::info!(sessions = count, "registry shutdown");
    }

    /// Snapshot every live session for `/debug/sessions`. Sessions whose
    /// actors have exited (handle closed) are skipped. Each live session
    /// gets a `Snapshot` RoomMsg and a short timeout; sessions that
    /// don't reply in time are skipped with a warn.
    pub async fn snapshot(&self) -> Vec<RoomSnapshot> {
        let handles: Vec<(String, RoomHandle)> = {
            let sessions = self.sessions.lock().await;
            sessions
                .iter()
                .filter(|(_, h)| h.is_alive())
                .map(|(id, h)| (id.clone(), h.clone()))
                .collect()
        };

        let mut out = Vec::with_capacity(handles.len());
        for (id, handle) in handles {
            let (ack_tx, ack_rx) = oneshot::channel();
            if handle
                .tx
                .send(RoomMsg::Snapshot { ack: ack_tx })
                .await
                .is_err()
            {
                tracing::debug!(session = %id, "session unreachable during snapshot");
                continue;
            }
            match tokio::time::timeout(std::time::Duration::from_millis(200), ack_rx).await {
                Ok(Ok(snap)) => out.push(snap),
                Ok(Err(_)) => {
                    tracing::debug!(session = %id, "session actor dropped snapshot ack");
                }
                Err(_) => {
                    tracing::warn!(session = %id, "snapshot timed out");
                }
            }
        }
        out
    }

    /// Count of sessions whose actors are still alive. Used by the
    /// integration tests and exposed publicly because they live in
    /// `tests/` (the lib is built without `cfg(test)` when consumed
    /// from an integration test). Safe to expose — it's already
    /// visible via `/debug/sessions` over HTTP.
    pub async fn live_session_count(&self) -> usize {
        let sessions = self.sessions.lock().await;
        sessions.values().filter(|h| h.is_alive()).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_plane_agent_timeout_allows_slow_hermes_mcp_startup() {
        assert!(CONTROL_PLANE_AGENT_TIMEOUT >= Duration::from_secs(8));
    }
}
