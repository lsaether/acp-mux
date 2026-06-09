use std::net::SocketAddr;

use acp_mux::agent_config;
use acp_mux::cli;
use acp_mux::mux::MuxRegistry;
use acp_mux::server;
use anyhow::{Context, Result};
use clap::Parser;
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

    let registry = MuxRegistry::new(
        agent_cmd,
        cli.replay_turns,
        std::time::Duration::from_secs(cli.mux_ttl_seconds),
        client_tool_policy,
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
