//! Mux registry: maps mux ids to live mux actors.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{Mutex, oneshot};
use tokio::time::timeout;

use crate::agent::process::AgentProcess;
use crate::cli::{ClientToolPolicy, ReplayTurns};
use crate::extension::{MuxExtension, NoopExtension};
use crate::jsonrpc::{Id, Incoming, JsonRpcError, ParseError};
use crate::mux::actor::{
    AttachError, MuxHandle, MuxMsg, MuxOptions, MuxSnapshot, spawn_mux_with_extension,
};
use crate::replay_store::ReplayStore;
use crate::subscriber::Subscriber;

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
    #[error("peer_id already attached to this mux")]
    PeerIdInUse,
    #[error("agent spawn failed: {0}")]
    AgentSpawn(#[from] anyhow::Error),
    #[error("mux actor not reachable")]
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

pub struct MuxRegistry {
    agent_cmd: Option<AgentCmd>,
    replay_policy: ReplayTurns,
    mux_ttl: Duration,
    client_tool_policy: ClientToolPolicy,
    replay_store: Option<Arc<ReplayStore>>,
    extension_factory: Arc<dyn Fn() -> Box<dyn MuxExtension> + Send + Sync>,
    muxes: Mutex<HashMap<String, MuxHandle>>,
}

impl MuxRegistry {
    pub fn new(
        agent_cmd: Option<AgentCmd>,
        replay_policy: ReplayTurns,
        mux_ttl: Duration,
        client_tool_policy: ClientToolPolicy,
    ) -> Arc<Self> {
        Self::with_extension(
            agent_cmd,
            replay_policy,
            mux_ttl,
            client_tool_policy,
            || Box::new(NoopExtension),
        )
    }

    pub fn with_extension<F>(
        agent_cmd: Option<AgentCmd>,
        replay_policy: ReplayTurns,
        mux_ttl: Duration,
        client_tool_policy: ClientToolPolicy,
        extension_factory: F,
    ) -> Arc<Self>
    where
        F: Fn() -> Box<dyn MuxExtension> + Send + Sync + 'static,
    {
        Self::with_extension_and_replay_store(
            agent_cmd,
            replay_policy,
            mux_ttl,
            client_tool_policy,
            None,
            extension_factory,
        )
    }

    pub fn with_extension_and_replay_store<F>(
        agent_cmd: Option<AgentCmd>,
        replay_policy: ReplayTurns,
        mux_ttl: Duration,
        client_tool_policy: ClientToolPolicy,
        replay_store: Option<Arc<ReplayStore>>,
        extension_factory: F,
    ) -> Arc<Self>
    where
        F: Fn() -> Box<dyn MuxExtension> + Send + Sync + 'static,
    {
        Arc::new(Self {
            agent_cmd,
            replay_policy,
            mux_ttl,
            client_tool_policy,
            replay_store,
            extension_factory: Arc::new(extension_factory),
            muxes: Mutex::new(HashMap::new()),
        })
    }

    pub async fn list_sessions_control_plane(
        &self,
        cwd: Option<String>,
    ) -> Result<Value, ControlPlaneSessionListError> {
        let cmd = self
            .agent_cmd
            .clone()
            .ok_or(ControlPlaneSessionListError::AgentCmdMissing)?;
        let mut agent = AgentProcess::spawn(&cmd.program, &cmd.args).await?;
        if let Some(mut stderr_rx) = agent.take_stderr_rx() {
            tokio::spawn(async move {
                while let Some(line) = stderr_rx.recv().await {
                    let text = String::from_utf8_lossy(&line);
                    tracing::debug!(target: "agent_stderr", control_plane = true, line = %text);
                }
            });
        }
        let result = query_transient_session_list(&mut agent, cwd).await;
        if let Err(err) = agent.shutdown(CONTROL_PLANE_AGENT_TIMEOUT).await {
            tracing::warn!(error = %err, "transient session/list agent shutdown failed");
        }
        result
    }

    pub async fn attach(
        self: &Arc<Self>,
        mux_id: &str,
        subscriber: Subscriber,
    ) -> Result<MuxHandle, RegistryError> {
        let existing = {
            let mut muxes = self.muxes.lock().await;
            match muxes.get(mux_id) {
                Some(h) if h.is_alive() => Some(h.clone()),
                Some(_) => {
                    muxes.remove(mux_id);
                    None
                }
                None => None,
            }
        };

        if let Some(handle) = existing {
            self.try_join(&handle, subscriber).await?;
            return Ok(handle);
        }

        let mut muxes = self.muxes.lock().await;
        if let Some(h) = muxes.get(mux_id) {
            if h.is_alive() {
                let handle = h.clone();
                drop(muxes);
                self.try_join(&handle, subscriber).await?;
                return Ok(handle);
            }
            muxes.remove(mux_id);
        }
        self.spawn_locked(&mut muxes, mux_id, subscriber).await
    }

    async fn spawn_locked(
        self: &Arc<Self>,
        muxes: &mut HashMap<String, MuxHandle>,
        mux_id: &str,
        subscriber: Subscriber,
    ) -> Result<MuxHandle, RegistryError> {
        let cmd = self
            .agent_cmd
            .as_ref()
            .ok_or(RegistryError::AgentCmdMissing)?;
        let agent_cwd = std::env::current_dir()
            .map(|path| path.to_string_lossy().to_string())
            .unwrap_or_else(|err| {
                tracing::warn!(error = %err, "failed to read current dir for mux context");
                String::new()
            });
        let agent = AgentProcess::spawn(&cmd.program, &cmd.args)
            .await
            .map_err(RegistryError::AgentSpawn)?;
        let (handle, _actor) = spawn_mux_with_extension(
            subscriber,
            agent,
            mux_id.to_string(),
            MuxOptions {
                replay_policy: self.replay_policy,
                mux_ttl: self.mux_ttl,
                client_tool_policy: self.client_tool_policy,
                agent_cwd,
                replay_store: self.replay_store.clone(),
            },
            (self.extension_factory)(),
        );
        muxes.insert(mux_id.to_string(), handle.clone());
        tracing::info!(mux = %mux_id, "spawned mux");
        Ok(handle)
    }

    async fn try_join(
        &self,
        handle: &MuxHandle,
        subscriber: Subscriber,
    ) -> Result<(), RegistryError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        handle
            .tx
            .send(MuxMsg::Attach {
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

    pub async fn snapshot(&self) -> Vec<MuxSnapshot> {
        let handles: Vec<(String, MuxHandle)> = {
            let muxes = self.muxes.lock().await;
            muxes
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
                .send(MuxMsg::Snapshot { ack: ack_tx })
                .await
                .is_err()
            {
                tracing::debug!(mux = %id, "mux unreachable during snapshot");
                continue;
            }
            match tokio::time::timeout(Duration::from_millis(200), ack_rx).await {
                Ok(Ok(snap)) => out.push(snap),
                Ok(Err(_)) => tracing::debug!(mux = %id, "mux actor dropped snapshot ack"),
                Err(_) => tracing::warn!(mux = %id, "snapshot timed out"),
            }
        }
        out
    }

    pub async fn shutdown(&self) {
        let mut muxes = self.muxes.lock().await;
        let count = muxes.len();
        muxes.clear();
        tracing::info!(muxes = count, "registry shutdown");
    }

    pub async fn live_mux_count(&self) -> usize {
        let muxes = self.muxes.lock().await;
        muxes.values().filter(|h| h.is_alive()).count()
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
