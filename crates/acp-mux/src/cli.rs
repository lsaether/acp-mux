use std::net::IpAddr;
use std::path::PathBuf;

use clap::{Parser, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "acp-mux",
    version,
    about = "Standards-oriented ACP session multiplexer"
)]
pub struct Cli {
    /// Bind address for the HTTP/WS listener.
    #[arg(long, default_value = "127.0.0.1")]
    pub host: IpAddr,

    /// TCP port for the HTTP/WS listener.
    #[arg(long, default_value_t = 8765)]
    pub port: u16,

    /// Command (and args, whitespace-separated) used to spawn an agent
    /// subprocess for each new `?mux=`. The raw escape hatch; mutually
    /// exclusive with `--agent`.
    #[arg(long)]
    pub agent_cmd: Option<String>,

    /// Launch a named agent from the agent config file (see `--config`).
    /// Mutually exclusive with `--agent-cmd`.
    #[arg(long, value_name = "NAME")]
    pub agent: Option<String>,

    /// Path to the agent config file (TOML). Defaults to
    /// `$XDG_CONFIG_HOME/acp-mux/agents.toml` (or
    /// `~/.config/acp-mux/agents.toml`).
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// List the agents available in the config file and exit.
    #[arg(long, default_value_t = false)]
    pub list_agents: bool,

    /// Seconds to retain a mux after the last subscriber leaves before
    /// tearing down the subprocess.
    #[arg(long, default_value_t = 60)]
    pub mux_ttl_seconds: u64,

    /// Replay-log policy. "unbounded" (default) keeps the full broadcast
    /// log; N > 0 is currently treated as unbounded with a warning. "0"
    /// disables the log entirely.
    #[arg(long, default_value = "unbounded")]
    pub replay_turns: ReplayTurns,

    /// UNSAFE: raw-broadcast agent-initiated fs/* and terminal/* client-tool
    /// requests to every subscriber. May duplicate local side effects.
    #[arg(long, default_value_t = false)]
    pub unsafe_debug_client_tool_broadcast: bool,

    /// Logging verbosity. Overridden by RUST_LOG when that variable is set.
    #[arg(long, value_enum, default_value_t = LogLevel::Info)]
    pub log_level: LogLevel,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub fn as_filter(&self) -> &'static str {
        match self {
            LogLevel::Trace => "trace",
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientToolMode {
    Block,
    UnsafeDebug,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientToolPolicy {
    pub fs: ClientToolMode,
    pub terminal: ClientToolMode,
}

impl ClientToolPolicy {
    pub fn block_by_default() -> Self {
        Self {
            fs: ClientToolMode::Block,
            terminal: ClientToolMode::Block,
        }
    }

    pub fn unsafe_debug_broadcast() -> Self {
        Self {
            fs: ClientToolMode::UnsafeDebug,
            terminal: ClientToolMode::UnsafeDebug,
        }
    }

    pub fn mode_for_method(&self, method: &str) -> Option<ClientToolMode> {
        if method.starts_with("fs/") {
            Some(self.fs)
        } else if method.starts_with("terminal/") {
            Some(self.terminal)
        } else {
            None
        }
    }
}

impl Default for ClientToolPolicy {
    fn default() -> Self {
        Self::block_by_default()
    }
}

impl Cli {
    pub fn client_tool_policy(&self) -> ClientToolPolicy {
        if self.unsafe_debug_client_tool_broadcast {
            ClientToolPolicy::unsafe_debug_broadcast()
        } else {
            ClientToolPolicy::block_by_default()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayTurns {
    Disabled,
    Bounded(u32),
    Unbounded,
}

impl std::str::FromStr for ReplayTurns {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("unbounded") {
            return Ok(ReplayTurns::Unbounded);
        }
        let n: u32 = s
            .parse()
            .map_err(|_| format!("expected \"unbounded\" or a non-negative integer, got {s:?}"))?;
        Ok(if n == 0 {
            ReplayTurns::Disabled
        } else {
            ReplayTurns::Bounded(n)
        })
    }
}

/// Split `--agent-cmd` into (program, args). Whitespace-only splitting; no
/// shell quote handling. Returns `None` if the string is empty after trim.
pub fn split_agent_cmd(raw: &str) -> Option<(String, Vec<String>)> {
    let mut it = raw.split_whitespace().map(str::to_string);
    let prog = it.next()?;
    Ok::<_, ()>((prog, it.collect())).ok()
}
