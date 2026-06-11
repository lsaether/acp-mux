use std::net::SocketAddr;
use std::sync::Arc;

use acp_mux::agent_config;
use anyhow::{Context, Result};
use clap::Parser;
use rooms::cli;
use rooms::room::registry::RoomRegistry;
use rooms::room::replay_store::ReplayStore;
use rooms::server;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = cli::Cli::parse();

    if cli.list_agents {
        print!("{}", agent_config::list_agents_text(cli.config.as_deref())?);
        return Ok(());
    }

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(cli.log_level.as_filter()));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let agent_cmd = agent_config::resolve_agent_cmd(
        cli.agent.as_deref(),
        cli.agent_cmd.as_deref(),
        cli.config.as_deref(),
    )?;
    if agent_cmd.is_none() {
        tracing::warn!(
            "no agent configured (--agent <name> or --agent-cmd <command>); subscriber attaches will be rejected with close 1011",
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
                     without persisted rooms/segment_started bookends. Late joiners after a restart will \
                     need to drive a fresh session/new or session/load before session/attach.",
                );
            }
            Some(Arc::new(store))
        }
        None => None,
    };

    let registry = RoomRegistry::new_full(
        agent_cmd,
        cli.replay_turns,
        std::time::Duration::from_secs(cli.session_ttl_seconds),
        cli.meta_propagate,
        client_tool_policy,
        cli.emit_segment_frames,
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
