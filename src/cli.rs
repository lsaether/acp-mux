use std::net::IpAddr;

use clap::{Parser, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "amux",
    version,
    about = "Multi-subscriber ACP session multiplexer"
)]
pub struct Cli {
    /// Bind address for the HTTP/WS listener.
    #[arg(long, default_value = "127.0.0.1")]
    pub host: IpAddr,

    /// TCP port for the HTTP/WS listener.
    #[arg(long, default_value_t = 8765)]
    pub port: u16,

    /// Command (and args, whitespace-separated) used to spawn an agent
    /// subprocess for each new `?room=`. Required to actually serve
    /// sessions; absent values are caught at the first session attach.
    #[arg(long)]
    pub agent_cmd: Option<String>,

    /// Seconds to retain a session after the last subscriber leaves before
    /// tearing down the subprocess.
    #[arg(long, default_value_t = 60)]
    pub session_ttl_seconds: u64,

    /// Replay-log policy. "unbounded" (default) keeps the full broadcast
    /// log; N > 0 is currently treated as unbounded with a warning (bounded
    /// eviction lands in v0.2). "0" disables the log entirely.
    #[arg(long, default_value = "unbounded")]
    pub replay_turns: ReplayTurns,

    /// Opt into injecting mux-owned trace metadata into subscriber → agent
    /// requests under params._meta.amux.
    #[arg(long, default_value_t = false)]
    pub meta_propagate: bool,

    /// UNSAFE: raw-broadcast agent-initiated fs/* and terminal/* client-tool
    /// requests to every subscriber. May duplicate local side effects.
    #[arg(long, default_value_t = false)]
    pub unsafe_debug_client_tool_broadcast: bool,

    /// Emit `amux/segment_started` and `amux/segment_ended` lifecycle frames
    /// on segment rotation (session/load, hermes compaction). Default on;
    /// disable to keep wire output byte-equivalent with v0.1.x for clients
    /// that haven't picked up the new frame methods yet.
    #[arg(long, default_value_t = true)]
    pub emit_segment_frames: bool,

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_turns_parses_unbounded() {
        assert_eq!(
            "unbounded".parse::<ReplayTurns>().unwrap(),
            ReplayTurns::Unbounded
        );
        assert_eq!(
            "UNBOUNDED".parse::<ReplayTurns>().unwrap(),
            ReplayTurns::Unbounded
        );
    }

    #[test]
    fn replay_turns_parses_zero_as_disabled() {
        assert_eq!("0".parse::<ReplayTurns>().unwrap(), ReplayTurns::Disabled);
    }

    #[test]
    fn replay_turns_parses_positive() {
        assert_eq!(
            "16".parse::<ReplayTurns>().unwrap(),
            ReplayTurns::Bounded(16)
        );
    }

    #[test]
    fn replay_turns_rejects_garbage() {
        assert!("nope".parse::<ReplayTurns>().is_err());
    }

    #[test]
    fn agent_cmd_split() {
        assert_eq!(
            split_agent_cmd("claude-code-acp --port 9090"),
            Some((
                "claude-code-acp".into(),
                vec!["--port".into(), "9090".into()]
            ))
        );
        assert_eq!(split_agent_cmd("   "), None);
    }

    #[test]
    fn meta_propagate_defaults_off() {
        let cli = Cli::try_parse_from(["amux"]).unwrap();
        assert!(!cli.meta_propagate);
    }

    #[test]
    fn meta_propagate_flag_enables_trace_injection() {
        let cli = Cli::try_parse_from(["amux", "--meta-propagate"]).unwrap();
        assert!(cli.meta_propagate);
    }

    #[test]
    fn client_tool_policy_blocks_fs_and_terminal_by_default() {
        let cli = Cli::try_parse_from(["amux"]).unwrap();
        let policy = cli.client_tool_policy();
        assert_eq!(
            policy.mode_for_method("fs/read_text_file"),
            Some(ClientToolMode::Block)
        );
        assert_eq!(
            policy.mode_for_method("terminal/create"),
            Some(ClientToolMode::Block)
        );
        assert_eq!(
            policy.mode_for_method("session/request_permission"),
            None,
            "permission prompts stay on the collaborative request path",
        );
        assert_eq!(
            policy.mode_for_method("vendor/unknown"),
            None,
            "v1 only classifies fs/* and terminal/* namespaces",
        );
    }

    #[test]
    fn unsafe_debug_flag_enables_fs_and_terminal_broadcast() {
        let cli = Cli::try_parse_from(["amux", "--unsafe-debug-client-tool-broadcast"]).unwrap();
        let policy = cli.client_tool_policy();
        assert_eq!(
            policy.mode_for_method("fs/write_text_file"),
            Some(ClientToolMode::UnsafeDebug)
        );
        assert_eq!(
            policy.mode_for_method("terminal/create"),
            Some(ClientToolMode::UnsafeDebug)
        );
    }
}
