use std::time::Duration;

use bytes::Bytes;
use serde_json::Value;

use crate::attach::{AttachParams, AttachResult};
use crate::jsonrpc::{Id, Incoming, IncomingNotification, IncomingRequest, IncomingResponse};
use crate::mux::{MuxCore, MuxMsg, ReplayView};
use crate::subscriber::Subscriber;

pub enum Disposition {
    Forward,
    Handled,
    Reject { code: i64, message: String },
}

pub enum NotifyDisposition {
    Passthrough,
    Handled,
}

pub enum ResolvedBy {
    Peer(String),
    AgentCancelled,
    TurnEnded,
}

pub struct MuxCtx<'a> {
    pub(crate) core: &'a mut MuxCore,
}

impl<'a> MuxCtx<'a> {
    pub(crate) fn new(core: &'a mut MuxCore) -> Self {
        Self { core }
    }

    pub fn mux_id(&self) -> &str {
        &self.core.mux_id
    }

    pub fn agent_cwd(&self) -> &str {
        &self.core.agent_cwd
    }

    pub fn canonical_session_id(&self) -> Option<&str> {
        self.core.canonical_session_id.as_deref()
    }

    pub fn subscribers(&self) -> impl Iterator<Item = &Subscriber> {
        self.core.subscribers.values()
    }

    pub fn subscriber(&self, peer_id: &str) -> Option<&Subscriber> {
        self.core.subscribers.get(peer_id)
    }

    pub fn replay_entries(&self) -> impl Iterator<Item = ReplayView<'_>> {
        self.core
            .replay_log
            .iter()
            .flat_map(|log| log.iter())
            .map(|entry| ReplayView {
                seq: entry.seq,
                ext_tag: entry.ext_tag,
                recorded_at: entry.recorded_at.as_str(),
                frame: &entry.frame,
            })
    }

    pub fn pending_permissions(&self) -> &[(Id, Bytes)] {
        &self.core.pending_permissions
    }

    pub fn prompt_in_flight(&self) -> Option<u64> {
        self.core.prompt_in_flight
    }

    pub fn pending_peer(&self, mux_id: u64) -> Option<&str> {
        self.core
            .pending
            .get(&mux_id)
            .map(|pending| pending.peer_id.as_str())
    }

    pub fn broadcast(&mut self, frame: impl Into<Bytes>) -> bool {
        self.core.broadcast(frame.into())
    }

    pub fn send_to(&mut self, peer_id: &str, frame: Bytes) {
        self.core.send_to(peer_id, frame);
    }

    pub fn send_to_agent(&mut self, acp_frame: Vec<u8>) {
        match Incoming::parse(&acp_frame) {
            Ok(Incoming::Request(_)) | Ok(Incoming::Notification(_)) => {
                self.core.agent_outbox.push(acp_frame);
            }
            Ok(Incoming::Response(_)) => {
                tracing::debug!("extension attempted to send JSON-RPC response to agent; dropping");
            }
            Err(err) => {
                tracing::debug!(error = %err, "extension attempted to send invalid ACP frame; dropping");
            }
        }
    }

    pub fn submit_prompt(&mut self, peer_id: &str, params: Value, deliver_response: bool) -> u64 {
        self.core
            .submit_prompt(peer_id, params, deliver_response)
            .unwrap_or(0)
    }

    pub fn set_replay_tag(&mut self, tag: u64) {
        self.core.replay_tag = tag;
    }

    pub fn schedule_wake(&mut self, delay: Duration, payload: Vec<u8>) {
        let tx = self.core.self_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = tx.send(MuxMsg::ExtensionWake(payload)).await;
        });
    }
}

pub trait MuxExtension: Send {
    fn on_subscriber_request(
        &mut self,
        _ctx: &mut MuxCtx,
        _peer_id: &str,
        _req: &mut IncomingRequest,
    ) -> Disposition {
        Disposition::Forward
    }

    fn on_request_translating(
        &mut self,
        _ctx: &mut MuxCtx,
        _peer_id: &str,
        _mux_id: u64,
        _req: &mut IncomingRequest,
    ) {
    }

    fn on_request_forwarded(
        &mut self,
        _ctx: &mut MuxCtx,
        _peer_id: &str,
        _mux_id: u64,
        _req: &IncomingRequest,
    ) {
    }

    fn on_subscriber_notification(
        &mut self,
        _ctx: &mut MuxCtx,
        _peer_id: &str,
        _notif: &IncomingNotification,
    ) -> NotifyDisposition {
        NotifyDisposition::Passthrough
    }

    fn on_agent_notification(&mut self, _ctx: &mut MuxCtx, _notif: &IncomingNotification) {}

    fn on_agent_request(&mut self, _ctx: &mut MuxCtx, _id: &Id, _req: &IncomingRequest) {}

    fn on_agent_response(&mut self, _ctx: &mut MuxCtx, _mux_id: u64, _resp: &mut IncomingResponse) {
    }

    fn on_prompt_settled(&mut self, _ctx: &mut MuxCtx, _mux_id: u64, _resp: &IncomingResponse) {}

    fn on_agent_request_resolved(
        &mut self,
        _ctx: &mut MuxCtx,
        _id: &Id,
        _by: ResolvedBy,
        _resp: Option<&IncomingResponse>,
    ) {
    }

    fn on_canonical_session_id(
        &mut self,
        _ctx: &mut MuxCtx,
        _old: Option<&str>,
        _new: &str,
        _via_load: bool,
    ) {
    }

    fn on_subscriber_attaching(&mut self, _ctx: &mut MuxCtx, _newcomer: &Subscriber) {}

    fn on_subscriber_attached(&mut self, _ctx: &mut MuxCtx, _peer_id: &str) {}

    fn on_subscriber_detached(&mut self, _ctx: &mut MuxCtx, _peer_id: &str) {}

    fn on_attach(
        &mut self,
        _ctx: &mut MuxCtx,
        _peer_id: &str,
        _params: &AttachParams,
        _result: &mut AttachResult,
    ) {
    }

    fn replay_frame(&mut self, _ctx: &mut MuxCtx, entry: ReplayView<'_>) -> Option<Bytes> {
        Some(entry.frame.clone())
    }

    fn on_wake(&mut self, _ctx: &mut MuxCtx, _payload: Vec<u8>) {}

    fn debug_snapshot(&self, _ctx: &MuxCtx) -> Value {
        Value::Null
    }
}

pub struct NoopExtension;

impl MuxExtension for NoopExtension {}
