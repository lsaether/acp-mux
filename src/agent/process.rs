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
/// Bound on the stderr channel. The pump is *lossy*: if the consumer
/// falls behind (or never drains the receiver — e.g. the transient
/// agent spawned for `/acp/sessions`), new lines are dropped with a
/// debug log rather than backpressured. That keeps the child's OS
/// stderr pipe drained, so a chatty agent can never wedge itself on
/// the mux's internal channel.
const STDERR_CAPACITY: usize = 1024;

pub struct AgentProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout_rx: Option<mpsc::Receiver<Vec<u8>>>,
    stdout_pump: Option<JoinHandle<()>>,
    stderr_rx: Option<mpsc::Receiver<Vec<u8>>>,
    stderr_pump: Option<JoinHandle<()>>,
}

impl AgentProcess {
    pub async fn spawn(program: &str, args: &[String]) -> Result<Self> {
        let mut child = Command::new(program)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("spawn agent {program:?}"))?;

        let stdin = child.stdin.take().ok_or_else(|| anyhow!("no stdin pipe"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("no stdout pipe"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow!("no stderr pipe"))?;
        let (stdout_tx, stdout_rx) = mpsc::channel(STDOUT_CAPACITY);
        let (stderr_tx, stderr_rx) = mpsc::channel(STDERR_CAPACITY);

        let stdout_pump = tokio::spawn(pump_lines(stdout, stdout_tx, "stdout", PumpMode::Blocking));
        let stderr_pump = tokio::spawn(pump_lines(stderr, stderr_tx, "stderr", PumpMode::Lossy));

        Ok(AgentProcess {
            child,
            stdin: Some(stdin),
            stdout_rx: Some(stdout_rx),
            stdout_pump: Some(stdout_pump),
            stderr_rx: Some(stderr_rx),
            stderr_pump: Some(stderr_pump),
        })
    }

    /// Take ownership of the stdout NDJSON channel. After this returns,
    /// `recv_line` will yield `None`. Used by the session actor task so it
    /// can own the receiver in its own loop while still calling `send` and
    /// `shutdown` on the AgentProcess handle.
    pub fn take_stdout_rx(&mut self) -> Option<mpsc::Receiver<Vec<u8>>> {
        self.stdout_rx.take()
    }

    /// Take ownership of the stderr line channel. Each item is one
    /// stderr line with the trailing newline stripped. The session
    /// actor consumes these to mirror them into mux logs and parse
    /// recognized Hermes compaction lifecycle signals.
    pub fn take_stderr_rx(&mut self) -> Option<mpsc::Receiver<Vec<u8>>> {
        self.stderr_rx.take()
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
        if let Some(handle) = self.stderr_pump.take() {
            handle.abort();
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum PumpMode {
    /// Awaited `send`. Backpressures when the channel is full — the
    /// only acceptable mode for stdout (NDJSON protocol: dropping a
    /// line corrupts the stream).
    Blocking,
    /// `try_send` with drop-on-full. Acceptable mode for stderr (line
    /// logs only, no protocol invariant). Keeps the child's OS pipe
    /// drained even when the receiver is undrained, e.g. for a
    /// transient subprocess used by `list_sessions_control_plane`.
    Lossy,
}

async fn pump_lines<R>(reader: R, tx: mpsc::Sender<Vec<u8>>, stream: &'static str, mode: PumpMode)
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut reader = BufReader::new(reader);
    let mut buf = Vec::with_capacity(4096);
    let mut dropped: u64 = 0;
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
                match mode {
                    PumpMode::Blocking => {
                        if tx.send(line).await.is_err() {
                            break;
                        }
                    }
                    PumpMode::Lossy => match tx.try_send(line) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            dropped = dropped.saturating_add(1);
                            // Throttle the log so a sustained burst doesn't
                            // spam the operator: log on the first drop and
                            // then every 256th drop.
                            if dropped == 1 || dropped.is_multiple_of(256) {
                                tracing::debug!(
                                    %stream,
                                    dropped,
                                    "agent line dropped: receiver not draining fast enough",
                                );
                            }
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => break,
                    },
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, %stream, "agent read error");
                break;
            }
        }
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
    async fn stderr_burst_does_not_wedge_stdout_when_stderr_undrained() {
        // Regression guard for the deadlock: a transient agent
        // (e.g. spawned by `list_sessions_control_plane`) never drains
        // the stderr receiver. If the pump backpressured on a chatty
        // child, the OS stderr pipe would fill and the child would
        // block, never reading stdin or producing stdout.
        //
        // The shell script bursts way more than STDERR_CAPACITY lines
        // before executing `cat`. With a lossy stderr pump, lines are
        // dropped, the OS pipe stays drained, the child proceeds to
        // `cat`, and our loopback completes.
        let burst = format!(
            "for i in $(seq 1 {}); do echo noise $i >&2; done; exec cat",
            STDERR_CAPACITY * 4,
        );
        let mut proc = AgentProcess::spawn("sh", &["-c".into(), burst])
            .await
            .expect("spawn sh");

        // We intentionally do NOT call take_stderr_rx — leaving the
        // receiver to sit at capacity is the whole point of the test.
        proc.send(b"hello").await.unwrap();
        let line = timeout(Duration::from_secs(5), proc.recv_line())
            .await
            .expect("recv timed out — stderr backpressure wedged stdout")
            .expect("eof before stdout line");
        assert_eq!(line, b"hello");

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
