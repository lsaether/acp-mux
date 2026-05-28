use std::net::SocketAddr;
use std::sync::Arc;

use amux::cli;
use amux::room::registry::{AgentCmd, RoomRegistry};
use amux::room::replay_store::ReplayStore;
use amux::server;
use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = cli::Cli::parse();

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(cli.log_level.as_filter()));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let agent_cmd = cli
        .agent_cmd
        .as_deref()
        .and_then(cli::split_agent_cmd)
        .map(|(program, args)| AgentCmd { program, args });
    if agent_cmd.is_none() {
        tracing::warn!(
            "--agent-cmd not configured; subscriber attaches will be rejected with close 1011",
        );
    }

    let client_tool_policy = cli.client_tool_policy();
    if cli.unsafe_debug_client_tool_broadcast {
        tracing::warn!(
            "UNSAFE: raw-broadcasting agent-initiated fs/* and terminal/* client-tool requests; side effects may duplicate",
        );
    }

    let replay_store = match cli.replay_store.as_ref() {
        Some(path) => {
            let store = ReplayStore::open(path)
                .with_context(|| format!("open --replay-store {}", path.display()))?;
            tracing::info!(path = %path.display(), "replay store enabled");
            if !cli.emit_segment_frames {
                tracing::warn!(
                    "--replay-store is enabled with --emit-segment-frames=false; \
                     restart hydration cannot reconstruct the canonical ACP session id or segment lineage \
                     without persisted amux/segment_started bookends. Late joiners after a restart will \
                     need to drive a fresh session/new or session/load before session/attach.",
                );
            }
            Some(Arc::new(store))
        }
        None => None,
    };

    if cli.hermes_compaction_signals {
        tracing::info!(
            "hermes compaction signal parser enabled; stderr lines matching Hermes \
             context-compression formats will emit amux/context_compaction_* events",
        );
    }
    let registry = RoomRegistry::new_full(
        agent_cmd,
        cli.replay_turns,
        std::time::Duration::from_secs(cli.session_ttl_seconds),
        cli.meta_propagate,
        client_tool_policy,
        cli.emit_segment_frames,
        cli.hermes_compaction_signals,
        replay_store,
    );
    let app = server::router(server::AppState::new(registry));

    let addr = SocketAddr::new(cli.host, cli.port);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    tracing::info!(%addr, "acp-mux listening");

    axum::serve(listener, app).await?;
    Ok(())
}
