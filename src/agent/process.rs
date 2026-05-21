//! Subprocess driver for an ACP agent over NDJSON stdin/stdout.

use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;

/// Bound on the stdout channel. Each item is one NDJSON line.
const STDOUT_CAPACITY: usize = 1024;

pub struct AgentProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout_rx: Option<mpsc::Receiver<Vec<u8>>>,
    stdout_pump: Option<JoinHandle<()>>,
}

impl AgentProcess {
    pub async fn spawn(program: &str, args: &[String]) -> Result<Self> {
        let mut child = Command::new(program)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("spawn agent {program:?}"))?;

        let stdin = child.stdin.take().ok_or_else(|| anyhow!("no stdin pipe"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("no stdout pipe"))?;
        let (tx, rx) = mpsc::channel(STDOUT_CAPACITY);

        let pump = tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            let mut buf = Vec::with_capacity(4096);
            loop {
                buf.clear();
                match reader.read_until(b'\n', &mut buf).await {
                    Ok(0) => break,
                    Ok(_) => {
                        let mut line = std::mem::take(&mut buf);
                        if line.ends_with(b"\n") {
                            line.pop();
                            if line.ends_with(b"\r") {
                                line.pop();
                            }
                        }
                        if line.is_empty() {
                            continue;
                        }
                        if tx.send(line).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "agent stdout read error");
                        break;
                    }
                }
            }
        });

        Ok(AgentProcess {
            child,
            stdin: Some(stdin),
            stdout_rx: Some(rx),
            stdout_pump: Some(pump),
        })
    }

    /// Take ownership of the stdout NDJSON channel. After this returns,
    /// `recv_line` will yield `None`. Used by the session actor task so it
    /// can own the receiver in its own loop while still calling `send` and
    /// `shutdown` on the AgentProcess handle.
    pub fn take_stdout_rx(&mut self) -> Option<mpsc::Receiver<Vec<u8>>> {
        self.stdout_rx.take()
    }

    /// Write one NDJSON frame to the agent. Caller passes the payload
    /// without the trailing newline.
    pub async fn send(&mut self, line: &[u8]) -> Result<()> {
        let stdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| anyhow!("agent stdin already closed"))?;
        stdin.write_all(line).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await?;
        Ok(())
    }

    /// Receive the next stdout line. Returns `None` once the subprocess
    /// closes its stdout (EOF, pump aborted, or after `take_stdout_rx`).
    pub async fn recv_line(&mut self) -> Option<Vec<u8>> {
        self.stdout_rx.as_mut()?.recv().await
    }

    /// Graceful stop: close stdin, wait up to `wait` for the child to exit,
    /// kill on overrun.
    pub async fn shutdown(mut self, wait: Duration) -> Result<()> {
        drop(self.stdin.take());
        match timeout(wait, self.child.wait()).await {
            Ok(Ok(_status)) => {}
            Ok(Err(err)) => return Err(err.into()),
            Err(_) => {
                tracing::warn!("agent did not exit within {:?}; sending kill", wait);
                let _ = self.child.start_kill();
                let _ = self.child.wait().await;
            }
        }
        if let Some(handle) = self.stdout_pump.take() {
            handle.abort();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// `cat` echoes stdin to stdout — a deterministic NDJSON loopback.
    #[tokio::test]
    async fn cat_loopback_roundtrip() {
        let mut proc = AgentProcess::spawn("cat", &[]).await.expect("spawn cat");

        proc.send(br#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#)
            .await
            .unwrap();
        let line = timeout(Duration::from_secs(2), proc.recv_line())
            .await
            .expect("recv timed out")
            .expect("eof before line");
        assert_eq!(line, br#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#);

        proc.send(br#"{"jsonrpc":"2.0","method":"session/update"}"#)
            .await
            .unwrap();
        let line = timeout(Duration::from_secs(2), proc.recv_line())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(line, br#"{"jsonrpc":"2.0","method":"session/update"}"#);

        proc.shutdown(Duration::from_secs(2)).await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_kills_unresponsive_child() {
        // `sleep 30` never exits on its own within our timeout.
        let proc = AgentProcess::spawn("sleep", &["30".into()])
            .await
            .expect("spawn sleep");
        proc.shutdown(Duration::from_millis(200)).await.unwrap();
    }
}
