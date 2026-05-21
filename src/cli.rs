use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "acp-mux",
    version,
    about = "Multi-subscriber ACP session multiplexer"
)]
pub struct Cli {}
