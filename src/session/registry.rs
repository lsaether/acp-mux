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

use thiserror::Error;
use tokio::sync::{Mutex, oneshot};

use crate::agent::process::AgentProcess;
use crate::cli::ReplayTurns;
use crate::multiplex::subscriber::Subscriber;
use crate::session::state::{
    AttachError, SessionHandle, SessionListMetadataIndex, SessionMsg, SessionOptions,
    SessionSnapshot, spawn_session,
};

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

pub struct SessionRegistry {
    agent_cmd: Option<AgentCmd>,
    replay_policy: ReplayTurns,
    session_ttl: Duration,
    meta_propagate: bool,
    session_list_index: Arc<SessionListMetadataIndex>,
    sessions: Mutex<HashMap<String, SessionHandle>>,
}

impl SessionRegistry {
    pub fn new(
        agent_cmd: Option<AgentCmd>,
        replay_policy: ReplayTurns,
        session_ttl: Duration,
    ) -> Arc<Self> {
        Self::new_with_meta_propagation(agent_cmd, replay_policy, session_ttl, false)
    }

    pub fn new_with_meta_propagation(
        agent_cmd: Option<AgentCmd>,
        replay_policy: ReplayTurns,
        session_ttl: Duration,
        meta_propagate: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            agent_cmd,
            replay_policy,
            session_ttl,
            meta_propagate,
            session_list_index: Arc::new(SessionListMetadataIndex::new()),
            sessions: Mutex::new(HashMap::new()),
        })
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
    ) -> Result<SessionHandle, RegistryError> {
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
        sessions: &mut HashMap<String, SessionHandle>,
        session_id: &str,
        subscriber: Subscriber,
    ) -> Result<SessionHandle, RegistryError> {
        let cmd = self
            .agent_cmd
            .as_ref()
            .ok_or(RegistryError::AgentCmdMissing)?;
        let agent = AgentProcess::spawn(&cmd.program, &cmd.args)
            .await
            .map_err(RegistryError::AgentSpawn)?;
        let (handle, _actor) = spawn_session(
            subscriber,
            agent,
            session_id.to_string(),
            SessionOptions {
                replay_policy: self.replay_policy,
                session_ttl: self.session_ttl,
                meta_propagate: self.meta_propagate,
                session_list_index: self.session_list_index.clone(),
            },
        );
        sessions.insert(session_id.to_string(), handle.clone());
        tracing::info!(session = %session_id, "spawned session");
        Ok(handle)
    }

    async fn try_join(
        &self,
        handle: &SessionHandle,
        subscriber: Subscriber,
    ) -> Result<(), RegistryError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        handle
            .tx
            .send(SessionMsg::Attach {
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

    /// Force-shutdown all sessions. Drops every SessionHandle, which causes
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
    /// gets a `Snapshot` SessionMsg and a short timeout; sessions that
    /// don't reply in time are skipped with a warn.
    pub async fn snapshot(&self) -> Vec<SessionSnapshot> {
        let handles: Vec<(String, SessionHandle)> = {
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
                .send(SessionMsg::Snapshot { ack: ack_tx })
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
