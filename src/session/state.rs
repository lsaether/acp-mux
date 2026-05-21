//! Per-session state: agent subprocess + attached subscribers + the
//! actor task that serializes all mutations.
//!
//! All state mutation flows through a single tokio task driven by an mpsc
//! `SessionMsg` queue. Subscribers push inbound frames via `InboundFromSubscriber`
//! and detach via `Detach`. The agent's stdout pump task forwards each NDJSON
//! line as `AgentStdoutLine` and signals exit via `AgentDied`. This avoids
//! interior mutability over the subscriber map and the agent stdin handle.

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::agent::process::AgentProcess;
use crate::multiplex::subscriber::Subscriber;

/// Bound on the SessionMsg queue. Sized so a steady stream of agent stdout
/// lines plus subscriber inbound traffic has room without backpressuring
/// the WS-in tasks under bursts.
const SESSION_QUEUE_CAPACITY: usize = 256;

/// Time we wait for the agent subprocess to exit cleanly before SIGKILL.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

/// Message types delivered to the session actor.
pub enum SessionMsg {
    Attach {
        subscriber: Subscriber,
        ack: oneshot::Sender<Result<(), AttachError>>,
    },
    Detach {
        peer_id: String,
    },
    InboundFromSubscriber {
        peer_id: String,
        bytes: Vec<u8>,
    },
    AgentStdoutLine(Vec<u8>),
    AgentDied,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttachError {
    PeerIdInUse,
}

/// Handle held by the registry. Cheap to clone; sending after the actor
/// task ends is detected via `is_closed`.
#[derive(Clone)]
pub struct SessionHandle {
    pub tx: mpsc::Sender<SessionMsg>,
}

impl SessionHandle {
    pub fn is_alive(&self) -> bool {
        !self.tx.is_closed()
    }
}

/// Spawn a new session: take the first subscriber, the agent process, and
/// return a handle plus the actor's JoinHandle for shutdown coordination.
pub fn spawn_session(
    initial_subscriber: Subscriber,
    mut agent: AgentProcess,
    session_id: String,
) -> (SessionHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel::<SessionMsg>(SESSION_QUEUE_CAPACITY);

    let stdout_rx = agent
        .take_stdout_rx()
        .expect("AgentProcess::take_stdout_rx must succeed on a fresh process");

    let pump_tx = tx.clone();
    let pump_session_id = session_id.clone();
    let pump = tokio::spawn(async move {
        let mut rx = stdout_rx;
        while let Some(line) = rx.recv().await {
            if pump_tx
                .send(SessionMsg::AgentStdoutLine(line))
                .await
                .is_err()
            {
                return;
            }
        }
        let _ = pump_tx.send(SessionMsg::AgentDied).await;
        tracing::debug!(session = %pump_session_id, "stdout pump finished");
    });

    let actor = tokio::spawn(run_session(rx, agent, initial_subscriber, pump, session_id));
    (SessionHandle { tx }, actor)
}

async fn run_session(
    mut rx: mpsc::Receiver<SessionMsg>,
    mut agent: AgentProcess,
    initial_subscriber: Subscriber,
    pump: JoinHandle<()>,
    session_id: String,
) {
    let mut subscribers: HashMap<String, Subscriber> = HashMap::new();
    subscribers.insert(initial_subscriber.peer_id.clone(), initial_subscriber);
    tracing::info!(session = %session_id, subscribers = subscribers.len(), "session started");

    while let Some(msg) = rx.recv().await {
        match msg {
            SessionMsg::Attach { subscriber, ack } => {
                if subscribers.contains_key(&subscriber.peer_id) {
                    let _ = ack.send(Err(AttachError::PeerIdInUse));
                } else {
                    tracing::info!(
                        session = %session_id,
                        peer_id = %subscriber.peer_id,
                        "subscriber joined existing session",
                    );
                    subscribers.insert(subscriber.peer_id.clone(), subscriber);
                    let _ = ack.send(Ok(()));
                }
            }
            SessionMsg::Detach { peer_id } => {
                if subscribers.remove(&peer_id).is_some() {
                    tracing::info!(session = %session_id, %peer_id, "subscriber detached");
                }
                if subscribers.is_empty() {
                    tracing::info!(session = %session_id, "last subscriber gone; ending session");
                    break;
                }
            }
            SessionMsg::InboundFromSubscriber { peer_id, bytes } => {
                if let Err(err) = agent.send(&bytes).await {
                    tracing::warn!(session = %session_id, %peer_id, error = %err, "agent stdin write failed");
                }
            }
            SessionMsg::AgentStdoutLine(line) => {
                // Naive fan-out: every subscriber receives every line.
                // Chunk 4 layers id translation + handshake caching on top.
                subscribers.retain(|peer_id, sub| match sub.outbound.send(line.clone()) {
                    Ok(()) => true,
                    Err(_) => {
                        tracing::debug!(session = %session_id, %peer_id, "outbound channel closed; dropping subscriber");
                        false
                    }
                });
                if subscribers.is_empty() {
                    tracing::info!(session = %session_id, "no live subscribers after fan-out; ending session");
                    break;
                }
            }
            SessionMsg::AgentDied => {
                tracing::warn!(session = %session_id, "agent subprocess exited; ending session");
                break;
            }
        }
    }

    // Tear down: drop all subscriber senders (closes WS-out tasks), then
    // shut down the agent subprocess and abort the stdout pump.
    subscribers.clear();
    if let Err(err) = agent.shutdown(SHUTDOWN_TIMEOUT).await {
        tracing::warn!(session = %session_id, error = %err, "agent shutdown error");
    }
    pump.abort();
    tracing::info!(session = %session_id, "session task exiting");
}
