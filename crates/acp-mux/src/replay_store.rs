//! Opt-in on-disk persistence for the per-mux broadcast replay log.
//!
//! Enabled by higher-level crates that want persistence. One JSONL file per
//! mux (`<DIR>/<mux_id>.jsonl`); every persisted line is one broadcast-tier
//! frame in the same order it flowed through the mux broadcast path.
//!
//! Record schema (v=1):
//!
//! ```json
//! {"v":1,"seq":42,"segment_id":3,"recorded_at":"...","frame":{...}}
//! ```
//!
//! - `seq` and `segment_id` are the same values carried by the in-memory
//!   replay entry. Core treats `segment_id` as an opaque extension tag.
//! - `frame` is the raw broadcast frame as a JSON value (frames are
//!   already valid JSON-RPC). Replay metadata (`recordedAt`, `replaySeq`)
//!   is *not* baked into `frame` here; it is re-injected on read by
//!   the persisted `recorded_at` and `seq`, preserving the contract that
//!   mux-recorded time wins.
//!
//! Concurrency: each mux owns its own `RoomReplayStore` and the mux actor
//! is single-threaded, so writes are serialized by construction.
//! The file is opened with `O_APPEND`, which keeps each in-process
//! write positioned at the current end of file. We do not coordinate
//! cross-process access and do not rely on any filesystem-level
//! atomicity guarantee for concurrent writers — operators running two
//! two mux processes against the same store directory is a
//! configuration error.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;

const RECORD_VERSION: u32 = 1;

/// Process-wide handle to a replay store root directory. Cheap to clone
/// (Arc'd by callers). Vending per-room handles is the only operation
/// — actual file I/O happens through `RoomReplayStore`.
#[derive(Debug)]
pub struct ReplayStore {
    root: PathBuf,
}

impl ReplayStore {
    /// Open (creating if necessary) the replay store rooted at `path`.
    /// Fails only if the directory cannot be created.
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let root = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    /// File path used for `room_id`. Room ids are validated by
    /// `server::is_valid_room_id` to `[A-Za-z0-9_-]+`, so no escaping
    /// is required.
    fn room_path(&self, room_id: &str) -> PathBuf {
        self.root.join(format!("{room_id}.jsonl"))
    }

    /// Open the per-room handle. Reads any existing persisted frames
    /// for `room_id` so the caller can rehydrate the in-memory replay
    /// log; then keeps the file open in append mode for subsequent
    /// `append` calls.
    pub fn open_room(&self, room_id: &str) -> std::io::Result<RoomReplayStore> {
        let path = self.room_path(room_id);

        let loaded = load_existing(&path)?;

        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        Ok(RoomReplayStore {
            path,
            file: Mutex::new(file),
            loaded_on_open: loaded,
        })
    }
}

/// Per-room replay store handle. Owns an open file in append mode.
#[derive(Debug)]
pub struct RoomReplayStore {
    path: PathBuf,
    file: Mutex<File>,
    /// Frames already on disk when the store was opened. Consumed by
    /// the room on construction via `take_loaded`.
    loaded_on_open: Vec<PersistedFrame>,
}

impl RoomReplayStore {
    /// Take the prehydrated frames out of the store. Called once at
    /// room construction. Subsequent calls return an empty Vec.
    pub fn take_loaded(&mut self) -> Vec<PersistedFrame> {
        std::mem::take(&mut self.loaded_on_open)
    }

    /// Append a frame to the on-disk log. Sync I/O on the calling
    /// thread; safe for the room actor because each broadcast is small
    /// (~hundreds of bytes) and the file is held open with `O_APPEND`.
    /// Errors are surfaced to the caller, which logs and drops them —
    /// disk failure does not affect live fan-out.
    pub fn append(
        &self,
        seq: u64,
        segment_id: u64,
        recorded_at: &str,
        frame: &Bytes,
    ) -> std::io::Result<()> {
        // Frames are JSON-RPC bytes; parse so we can re-emit as a
        // structured `frame` field. If a frame is somehow not JSON,
        // skip persistence and warn — we'd lose round-trip otherwise.
        let frame_value: Value = match serde_json::from_slice(frame) {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "replay store: skipping non-JSON broadcast frame",
                );
                return Ok(());
            }
        };

        let record = PersistedFrame {
            v: RECORD_VERSION,
            seq,
            segment_id,
            recorded_at: recorded_at.to_string(),
            frame: frame_value,
        };

        let mut line = serde_json::to_vec(&record).map_err(std::io::Error::other)?;
        line.push(b'\n');

        let mut file = self.file.lock().expect("replay store mutex poisoned");
        file.write_all(&line)?;
        file.flush()?;
        Ok(())
    }

    /// Path on disk. Exposed for diagnostics and tests.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// One persisted broadcast frame. Public so the room module can
/// rebuild `ReplayEntry` and `Segment` structures on hydration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedFrame {
    pub v: u32,
    pub seq: u64,
    pub segment_id: u64,
    pub recorded_at: String,
    pub frame: Value,
}

impl PersistedFrame {
    /// Raw frame bytes as they were when originally broadcast (pre
    /// replay-metadata injection).
    pub fn frame_bytes(&self) -> Bytes {
        // `to_vec` on a Value never fails for values produced by
        // `from_slice` of valid JSON.
        Bytes::from(serde_json::to_vec(&self.frame).unwrap_or_default())
    }
}

fn load_existing(path: &Path) -> std::io::Result<Vec<PersistedFrame>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };
    let reader = BufReader::new(file);
    let mut out = Vec::new();
    for (lineno, line) in reader.lines().enumerate() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<PersistedFrame>(&line) {
            Ok(record) => {
                if record.v != RECORD_VERSION {
                    tracing::warn!(
                        path = %path.display(),
                        lineno = lineno + 1,
                        version = record.v,
                        "replay store: skipping record with unknown version",
                    );
                    continue;
                }
                out.push(record);
            }
            Err(err) => {
                tracing::warn!(
                    path = %path.display(),
                    lineno = lineno + 1,
                    error = %err,
                    "replay store: skipping malformed record",
                );
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_single_frame() {
        let dir = tempdir();
        let store = ReplayStore::open(&dir).unwrap();
        {
            let room = store.open_room("room1").unwrap();
            let frame = Bytes::from(br#"{"jsonrpc":"2.0","method":"x","params":{}}"#.as_ref());
            room.append(1, 0, "2026-01-01T00:00:00Z", &frame).unwrap();
            let frame2 = Bytes::from(br#"{"jsonrpc":"2.0","method":"y","params":{}}"#.as_ref());
            room.append(2, 1, "2026-01-01T00:00:01Z", &frame2).unwrap();
        }

        let store2 = ReplayStore::open(&dir).unwrap();
        let mut room = store2.open_room("room1").unwrap();
        let loaded = room.take_loaded();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].seq, 1);
        assert_eq!(loaded[0].segment_id, 0);
        assert_eq!(loaded[0].recorded_at, "2026-01-01T00:00:00Z");
        assert_eq!(loaded[1].seq, 2);
        assert_eq!(loaded[1].segment_id, 1);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn missing_room_file_loads_empty() {
        let dir = tempdir();
        let store = ReplayStore::open(&dir).unwrap();
        let mut room = store.open_room("never_written").unwrap();
        assert!(room.take_loaded().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn malformed_line_is_skipped() {
        let dir = tempdir();
        let store = ReplayStore::open(&dir).unwrap();
        {
            let room = store.open_room("r").unwrap();
            let frame = Bytes::from(br#"{"jsonrpc":"2.0","method":"x"}"#.as_ref());
            room.append(1, 0, "2026-01-01T00:00:00Z", &frame).unwrap();
        }
        // Corrupt the file by appending garbage.
        {
            let path = dir.join("r.jsonl");
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(b"this is not json\n").unwrap();
            // Valid second record.
            f.write_all(
                br#"{"v":1,"seq":2,"segment_id":1,"recorded_at":"t","frame":{"a":1}}
"#,
            )
            .unwrap();
        }

        let store2 = ReplayStore::open(&dir).unwrap();
        let mut room = store2.open_room("r").unwrap();
        let loaded = room.take_loaded();
        assert_eq!(loaded.len(), 2, "garbage line skipped, valid lines kept");
        assert_eq!(loaded[0].seq, 1);
        assert_eq!(loaded[1].seq, 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    fn tempdir() -> PathBuf {
        let base = std::env::temp_dir();
        let suffix = format!(
            "acp-mux-replay-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        );
        let p = base.join(suffix);
        std::fs::create_dir_all(&p).unwrap();
        p
    }
}
