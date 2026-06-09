//! Named ACP agent configuration.
//!
//! A small config file maps friendly agent names to the command/args/env used
//! to spawn them, so the CLI can launch an agent by name (`--agent claude`)
//! instead of a raw `--agent-cmd "<command>"`. The shape mirrors Zed's
//! `agent_servers` (command / args / env).
//!
//! Default location: `$XDG_CONFIG_HOME/acp-mux/agents.toml`, falling back to
//! `$HOME/.config/acp-mux/agents.toml`. Override with `--config <path>`.
//!
//! ```toml
//! [agents.claude]
//! command = "npx"
//! args = ["-y", "@agentclientprotocol/claude-agent-acp"]
//! env = { ANTHROPIC_API_KEY = "sk-..." }   # optional; layered over the inherited env
//!
//! [agents.gemini]
//! command = "gemini"
//! args = ["acp"]
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;

use crate::cli::split_agent_cmd;
use crate::mux::AgentCmd;

/// Parsed `agents.toml`.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    #[serde(default)]
    pub agents: BTreeMap<String, AgentEntry>,
}

/// One `[agents.<name>]` entry.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentEntry {
    /// Executable to spawn (resolved via `PATH`).
    pub command: String,
    /// Arguments passed to `command`.
    #[serde(default)]
    pub args: Vec<String>,
    /// Extra environment variables, layered on top of the inherited
    /// environment (the agent already inherits the parent process env).
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

impl AgentEntry {
    fn to_agent_cmd(&self) -> AgentCmd {
        AgentCmd {
            program: self.command.clone(),
            args: self.args.clone(),
            env: self
                .env
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }
}

impl AgentConfig {
    /// Load the config from `explicit` if given, else the default path.
    ///
    /// A missing file at an **explicit** `--config` path is an error; a missing
    /// file at the **default** path yields an empty config, so the feature is
    /// opt-in and its absence is never fatal.
    pub fn load(explicit: Option<&Path>) -> Result<Self> {
        let (path, required) = match explicit {
            Some(p) => (p.to_path_buf(), true),
            None => match default_config_path() {
                Some(p) => (p, false),
                None => return Ok(Self::default()),
            },
        };
        match std::fs::read_to_string(&path) {
            Ok(text) => toml::from_str(&text)
                .with_context(|| format!("parse agent config {}", path.display())),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound && !required => {
                Ok(Self::default())
            }
            Err(err) => Err(err).with_context(|| format!("read agent config {}", path.display())),
        }
    }

    /// Configured agent names, sorted.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.agents.keys().map(String::as_str)
    }
}

fn default_config_path() -> Option<PathBuf> {
    if let Ok(dir) = std::env::var("XDG_CONFIG_HOME")
        && !dir.is_empty()
    {
        return Some(PathBuf::from(dir).join("acp-mux").join("agents.toml"));
    }
    let home = std::env::var("HOME").ok().filter(|h| !h.is_empty())?;
    Some(
        PathBuf::from(home)
            .join(".config")
            .join("acp-mux")
            .join("agents.toml"),
    )
}

/// Resolve the agent command to spawn from the CLI inputs.
///
/// - `--agent` and `--agent-cmd` are mutually exclusive.
/// - `--agent <name>` looks the name up in the config (error if unknown).
/// - `--agent-cmd "<raw>"` is the unconfigured escape hatch.
/// - neither → `Ok(None)` (the server starts but rejects attaches until one is
///   configured).
pub fn resolve_agent_cmd(
    agent: Option<&str>,
    agent_cmd: Option<&str>,
    config_path: Option<&Path>,
) -> Result<Option<AgentCmd>> {
    match (agent, agent_cmd) {
        (Some(_), Some(_)) => bail!("--agent and --agent-cmd are mutually exclusive"),
        (None, Some(raw)) => Ok(split_agent_cmd(raw).map(|(program, args)| AgentCmd {
            program,
            args,
            env: Vec::new(),
        })),
        (None, None) => Ok(None),
        (Some(name), None) => {
            let config = AgentConfig::load(config_path)?;
            let entry = config.agents.get(name).ok_or_else(|| {
                let known: Vec<&str> = config.names().collect();
                let known = if known.is_empty() {
                    "(none configured)".to_string()
                } else {
                    known.join(", ")
                };
                anyhow!("no agent named {name:?} in the agent config; known agents: {known}")
            })?;
            Ok(Some(entry.to_agent_cmd()))
        }
    }
}

/// Human-readable listing for `--list-agents`.
pub fn list_agents_text(config_path: Option<&Path>) -> Result<String> {
    let config = AgentConfig::load(config_path)?;
    if config.agents.is_empty() {
        return Ok("no agents configured\n".to_string());
    }
    let mut out = String::new();
    for (name, entry) in &config.agents {
        let args = entry.args.join(" ");
        if args.is_empty() {
            out.push_str(&format!("{name}\t{}\n", entry.command));
        } else {
            out.push_str(&format!("{name}\t{} {args}\n", entry.command));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const SAMPLE: &str = r#"
[agents.claude]
command = "npx"
args = ["-y", "@agentclientprotocol/claude-agent-acp"]
env = { ANTHROPIC_API_KEY = "sk-test" }

[agents.gemini]
command = "gemini"
"#;

    #[test]
    fn parses_agents_with_args_and_env() {
        let config: AgentConfig = toml::from_str(SAMPLE).unwrap();
        let claude = config.agents.get("claude").expect("claude present");
        assert_eq!(claude.command, "npx");
        assert_eq!(claude.args, ["-y", "@agentclientprotocol/claude-agent-acp"]);
        assert_eq!(claude.env.get("ANTHROPIC_API_KEY").unwrap(), "sk-test");
        let gemini = config.agents.get("gemini").expect("gemini present");
        assert_eq!(gemini.command, "gemini");
        assert!(gemini.args.is_empty());
        assert!(gemini.env.is_empty());
    }

    #[test]
    fn entry_converts_to_agent_cmd_with_env() {
        let config: AgentConfig = toml::from_str(SAMPLE).unwrap();
        let cmd = config.agents.get("claude").unwrap().to_agent_cmd();
        assert_eq!(cmd.program, "npx");
        assert_eq!(
            cmd.env,
            vec![("ANTHROPIC_API_KEY".to_string(), "sk-test".to_string())]
        );
    }

    #[test]
    fn rejects_unknown_fields() {
        let bad = r#"[agents.x]
command = "c"
bogus = true
"#;
        assert!(toml::from_str::<AgentConfig>(bad).is_err());
    }

    #[test]
    fn resolve_agent_and_agent_cmd_are_mutually_exclusive() {
        let err = resolve_agent_cmd(Some("claude"), Some("cat"), None).unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"));
    }

    #[test]
    fn resolve_raw_agent_cmd_has_no_env() {
        let cmd = resolve_agent_cmd(None, Some("claude-agent-acp --port 9090"), None)
            .unwrap()
            .unwrap();
        assert_eq!(cmd.program, "claude-agent-acp");
        assert_eq!(cmd.args, ["--port", "9090"]);
        assert!(cmd.env.is_empty());
    }

    #[test]
    fn resolve_neither_is_none() {
        assert!(resolve_agent_cmd(None, None, None).unwrap().is_none());
    }

    #[test]
    fn explicit_missing_config_is_an_error() {
        let missing = std::env::temp_dir().join("acp-mux-no-such-config-12345.toml");
        let err = AgentConfig::load(Some(&missing)).unwrap_err();
        assert!(err.to_string().contains("read agent config"));
    }

    #[test]
    fn resolve_named_agent_from_explicit_config() {
        let path = write_temp_config(SAMPLE);
        let cmd = resolve_agent_cmd(Some("claude"), None, Some(&path))
            .unwrap()
            .unwrap();
        assert_eq!(cmd.program, "npx");
        assert_eq!(
            cmd.env,
            vec![("ANTHROPIC_API_KEY".to_string(), "sk-test".to_string())]
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn resolve_unknown_named_agent_lists_known() {
        let path = write_temp_config(SAMPLE);
        let err = resolve_agent_cmd(Some("nope"), None, Some(&path)).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("no agent named"), "got: {msg}");
        assert!(
            msg.contains("claude") && msg.contains("gemini"),
            "got: {msg}"
        );
        std::fs::remove_file(&path).ok();
    }

    fn write_temp_config(contents: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "acp-mux-agents-{}-{}.toml",
            std::process::id(),
            // Monotonic-ish unique suffix without pulling in time/rand.
            COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        ));
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        path
    }

    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
}
