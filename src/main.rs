#![allow(dead_code)]

mod agent;
mod cli;
mod multiplex;
mod protocol;
mod server;
mod session;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let _cli = <cli::Cli as clap::Parser>::parse();
    tracing::info!("acp-mux scaffold; server not yet wired");
    Ok(())
}
