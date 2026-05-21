#![allow(dead_code)]

mod agent;
mod cli;
mod multiplex;
mod protocol;
mod server;
mod session;

use std::net::SocketAddr;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = cli::Cli::parse();

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(cli.log_level.as_filter()));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let state = server::AppState::new();
    let app = server::router(state);

    let addr = SocketAddr::new(cli.host, cli.port);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    tracing::info!(%addr, "acp-mux listening");

    axum::serve(listener, app).await?;
    Ok(())
}
