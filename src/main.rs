use std::net::SocketAddr;

use amux::cli;
use amux::server;
use amux::room::registry::{AgentCmd, RoomRegistry};
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

    let registry = RoomRegistry::new_with_options(
        agent_cmd,
        cli.replay_turns,
        std::time::Duration::from_secs(cli.session_ttl_seconds),
        cli.meta_propagate,
        client_tool_policy,
        cli.emit_segment_frames,
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
