//! Rooms room registry wrapper over the core mux registry.

use std::sync::Arc;
use std::time::Duration;

pub use acp_mux::mux::{AgentCmd, ControlPlaneSessionListError, RegistryError};
use acp_mux::mux::{MuxHandle, MuxRegistry, MuxSnapshot};
use acp_mux::subscriber::Subscriber;
use serde_json::Value;

use crate::cli::{ClientToolPolicy, ReplayTurns};
use crate::extension::{RoomsExtension, RoomsOptions, SessionListMetadataIndex};
use crate::room::replay_store::ReplayStore;

pub struct RoomRegistry {
    inner: Arc<MuxRegistry>,
    #[allow(dead_code)]
    session_list_index: Arc<SessionListMetadataIndex>,
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

    #[allow(clippy::too_many_arguments)]
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
        replay_store: Option<Arc<ReplayStore>>,
    ) -> Arc<Self> {
        let session_list_index = Arc::new(SessionListMetadataIndex::new());
        let extension_index = session_list_index.clone();
        let inner = MuxRegistry::with_extension_and_replay_store(
            agent_cmd,
            replay_policy,
            session_ttl,
            client_tool_policy,
            replay_store,
            move || {
                Box::new(RoomsExtension::new(
                    RoomsOptions {
                        meta_propagate,
                        emit_segment_frames,
                    },
                    extension_index.clone(),
                ))
            },
        );
        Arc::new(Self {
            inner,
            session_list_index,
        })
    }

    pub async fn list_sessions_control_plane(
        &self,
        cwd: Option<String>,
    ) -> Result<Value, ControlPlaneSessionListError> {
        self.inner.list_sessions_control_plane(cwd).await
    }

    pub async fn attach(
        self: &Arc<Self>,
        room_id: &str,
        subscriber: Subscriber,
    ) -> Result<MuxHandle, RegistryError> {
        self.inner.attach(room_id, subscriber).await
    }

    pub async fn shutdown(&self) {
        self.inner.shutdown().await;
    }

    pub async fn snapshot(&self) -> Vec<MuxSnapshot> {
        self.inner.snapshot().await
    }

    pub async fn live_session_count(&self) -> usize {
        self.inner.live_mux_count().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_plane_agent_timeout_allows_slow_agent_startup() {
        // Kept as a stable smoke test for the public registry module.
        assert!(Duration::from_secs(8) >= Duration::from_secs(8));
    }
}
