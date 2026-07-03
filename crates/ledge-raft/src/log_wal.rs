//! Durable, WAL-backed Raft log storage (`WalLogStore`).
//!
//! This is the production counterpart to the in-memory [`crate::log_store::LogStore`]
//! (Task 2). It implements the *same* openraft 0.9.24 trait surface
//! (`RaftLogReader` + `RaftLogStorage`) but persists every mutation to a
//! write-ahead log on disk and replays it on open, so the log/vote/committed
//! state survives a process restart.
//!
//! # Frame format (byte-for-byte the ref-store WAL contract)
//! Every record on disk is a self-describing framed entry, identical to
//! `ledge-ref-store::wal`:
//!
//! ```text
//! | length: u32 LE | crc32: u32 LE | bincode payload (length bytes) |
//! ```
//!
//! On open the file is read sequentially. A frame whose length extends past EOF
//! is a torn tail (interrupted final write): the file is truncated back to the
//! last valid boundary and the prefix recovers. A frame that is fully present
//! but fails its CRC is in-place corruption (bit-rot), not a torn write, so
//! `open` fails loud rather than truncating — silently dropping it would discard
//! every valid record after it, i.e. lose committed Raft entries. These
//! torn-tail / corruption invariants match the ref-store WAL. We do **not**
//! depend on `ledge-ref-store::wal::Wal` directly — its `WalEntry` enum is
//! ref-store-specific; instead this module owns a tiny generic frame codec with
//! the same layout. (A shared `ledge-wal` crate is a deliberate Phase 3.1
//! refactor candidate, not justified by two call sites today.)
//!
//! # Record model
//! The Raft log itself is the entry stream; vote, committed, truncate, and purge
//! are folded in as interleaved control records so a single sequential replay
//! rebuilds the full `RaftLogStorage` state into the same in-memory shape as
//! `LogStore`. Entries / votes / log-ids are stored as opaque pre-serialized
//! bincode blobs so the WAL format is independent of openraft's internal
//! `Entry<C>` layout across patch releases (it is decoded back via the app's
//! bincode config in `try_get_log_entries`).
//!
//! # Durability ordering (correctness)
//! openraft requires a written log entry to be durable *before* the flush
//! callback fires (`append`) and before `save_vote`/`truncate`/`purge` return.
//! Every mutating method therefore writes its frame and `fsync`s the file
//! *before* signalling completion. `append` does its fsync before invoking
//! `callback.log_io_completed(Ok(()))`.
//!
//! # openraft 0.9.24 trait surface (verified against the resolved crate source)
//! Same as `LogStore`: `truncate(log_id)` removes entries with index
//! `>= log_id.index`; `purge(log_id)` removes entries `<= log_id.index` and
//! advances `last_purged`; `append` takes `LogFlushed<C>` whose
//! `.log_io_completed(Ok(()))` signals durability.
//!
//! # Compaction (deferred — Phase 3 TODO)
//! The ref-store WAL exposes a `compact()` that rewrites the file as a single
//! snapshot frame once it grows past a threshold. For `WalLogStore`, openraft's
//! own log purging (`purge`) bounds the live entry set, but the *file* still
//! grows monotonically because purge only appends a marker. A size-triggered
//! rewrite to a single snapshot record set (current entries, vote, committed,
//! and purged id) is the right follow-up. It is intentionally NOT triggered
//! automatically here (the size bound is acceptable for Phase 3). The rewrite
//! itself is implemented in [`WalLogStore::rewrite_snapshot`] and is honored on
//! `open` (a `Snapshot` record bounds replay), so wiring a size-threshold
//! trigger is the only remaining work.

// openraft's storage trait methods return `Result<_, StorageError<NodeId>>` by
// contract; `StorageError` is large (>200 B) and cannot be boxed without
// violating the trait signatures. Our private helpers thread the same error
// type, so we allow the large-Result lint module-wide. The error path is cold
// (only on an I/O or corruption failure), so the Result size is irrelevant.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{Entry, LogId, OptionalSend, RaftLogReader, StorageError, StorageIOError, Vote};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::type_config::TypeConfig;

// ── Frame codec (same layout as ledge-ref-store::wal, generic over payload) ───

/// Byte size of the fixed frame header (length u32 LE + crc32 u32 LE).
const HEADER_LEN: usize = 8;

/// Encode a serializable payload `T` into a complete on-disk frame.
fn encode_frame<T: Serialize>(rec: &T) -> Result<Vec<u8>, String> {
    let payload = bincode::serde::encode_to_vec(rec, bincode::config::standard())
        .map_err(|e| format!("WAL encode: {e}"))?;
    let length = payload.len() as u32;
    let crc = crc32fast::hash(&payload);
    let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
    frame.extend_from_slice(&length.to_le_bytes());
    frame.extend_from_slice(&crc.to_le_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// Outcome of trying to decode one frame at `pos`.
///
/// `Incomplete` vs `Corrupt` is what separates a benign torn-tail write from
/// silent bit-rot: a torn write (interrupted append) is always short, so a frame
/// that is *fully present* yet fails its CRC is in-place corruption. Silently
/// dropping it would also discard every valid Raft record after it — for the
/// authoritative log that means silently losing committed entries, a safety
/// violation. So corruption must fail loud, not truncate.
enum FrameDecode<T> {
    Entry(T, usize),
    Incomplete,
    Corrupt(String),
}

/// Attempt to decode one frame of type `T` from `data` starting at `pos`.
fn decode_frame<T: DeserializeOwned>(data: &[u8], pos: usize) -> FrameDecode<T> {
    if pos + HEADER_LEN > data.len() {
        return FrameDecode::Incomplete; // header torn by an interrupted write
    }
    // SAFETY: bounds checked above; the 4-byte slices make try_into infallible.
    let length = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
    let crc_stored = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap());
    let payload_end = pos + HEADER_LEN + length;
    if payload_end > data.len() {
        return FrameDecode::Incomplete; // payload short — torn tail
    }
    let payload = &data[pos + HEADER_LEN..payload_end];
    if crc32fast::hash(payload) != crc_stored {
        // Full frame present, CRC fails → corruption, not an interrupted write.
        return FrameDecode::Corrupt(format!("CRC mismatch at byte {pos}"));
    }
    match bincode::serde::decode_from_slice(payload, bincode::config::standard()) {
        Ok((rec, _)) => FrameDecode::Entry(rec, payload_end),
        Err(e) => FrameDecode::Corrupt(format!("payload decode error at byte {pos}: {e}")),
    }
}

// ── Persisted record model ────────────────────────────────────────────────────

/// One durable record in the Raft log WAL. A single sequential replay of the
/// record stream rebuilds the full `RaftLogStorage` state.
#[derive(Serialize, Deserialize)]
enum WalRec {
    /// A log entry at `index`. `entry_bytes` is the bincode of `Entry<TypeConfig>`.
    Entry { index: u64, entry_bytes: Vec<u8> },
    /// The persisted hard-state vote. `vote_bytes` is the bincode of `Vote<u64>`.
    Vote { vote_bytes: Vec<u8> },
    /// The persisted committed log id. `committed_bytes` is the bincode of
    /// `Option<LogId<u64>>`.
    Committed { committed_bytes: Vec<u8> },
    /// A truncate marker: on replay, drop all entries with index `>= at`
    /// (`at == log_id.index`, matching openraft 0.9 `truncate` semantics).
    Truncate { at: u64 },
    /// A purge marker: on replay, drop all entries with index `<= up_to` and set
    /// `last_purged_log_id`. `up_to_bytes` is the bincode of `LogId<u64>`.
    Purge { up_to_bytes: Vec<u8> },
    /// A full-state snapshot written by compaction: the entire live state in one
    /// frame. On replay everything *before* the last snapshot is discarded and
    /// this is folded in first. `*_bytes` are bincode blobs as above.
    Snapshot {
        entries: Vec<(u64, Vec<u8>)>,
        vote_bytes: Option<Vec<u8>>,
        committed_bytes: Vec<u8>,
        purged_bytes: Vec<u8>,
    },
}

/// bincode the standard config, used for the opaque blobs inside `WalRec`.
fn cfg() -> bincode::config::Configuration {
    bincode::config::standard()
}

/// Encode any serde value to a bincode blob, mapping errors to a write
/// `StorageError`.
fn blob<T: Serialize>(v: &T) -> Result<Vec<u8>, StorageError<u64>> {
    bincode::serde::encode_to_vec(v, cfg())
        .map_err(|e| StorageIOError::write_logs(&io_err(format!("bincode encode: {e}"))).into())
}

/// Decode a bincode blob back to `T`, mapping errors to a read `StorageError`.
fn unblob<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError<u64>> {
    bincode::serde::decode_from_slice(bytes, cfg())
        .map(|(v, _)| v)
        .map_err(|e| StorageIOError::read_logs(&io_err(format!("bincode decode: {e}"))).into())
}

/// Build a generic `io::Error` for wrapping in a `StorageIOError`.
fn io_err(msg: String) -> std::io::Error {
    std::io::Error::other(msg)
}

// ── In-memory state (mirrors LogStore::Inner) + file handle ───────────────────

/// Mutable state guarded by an async mutex so a cloned reader shares it. Mirrors
/// `LogStore`'s `Inner` plus the durable file handle, always positioned at EOF.
struct WalInner {
    /// index -> entry (the live, un-purged log).
    log: BTreeMap<u64, Entry<TypeConfig>>,
    /// Last purged log id (entries `<=` this are gone).
    last_purged: Option<LogId<u64>>,
    /// Persisted hard-state vote.
    vote: Option<Vote<u64>>,
    /// Last committed log id.
    committed: Option<LogId<u64>>,
    /// The WAL file, positioned at EOF for appends.
    file: std::fs::File,
    /// Path to the WAL file, needed for the crash-atomic snapshot rewrite
    /// (temp file + rename + reopen).
    path: PathBuf,
}

impl WalInner {
    /// Encode + write a frame, then `fsync`, mapping all errors to a write
    /// `StorageError`. This is the single durability primitive: it returns only
    /// after the bytes are on stable storage.
    fn write_frame(&mut self, rec: &WalRec) -> Result<(), StorageError<u64>> {
        let frame = encode_frame(rec).map_err(|e| StorageIOError::write_logs(&io_err(e)))?;
        self.file
            .write_all(&frame)
            .map_err(|e| StorageIOError::write_logs(&io_err(format!("WAL write: {e}"))))?;
        self.file
            .sync_data()
            .map_err(|e| StorageIOError::write_logs(&io_err(format!("WAL fsync: {e}"))))?;
        Ok(())
    }
}

/// Durable Raft log storage. Cloning yields a handle sharing the same state +
/// file (via `Arc<Mutex<_>>`), which is exactly what `get_log_reader` needs.
#[derive(Clone)]
pub struct WalLogStore {
    inner: Arc<Mutex<WalInner>>,
}

impl WalLogStore {
    /// Open (or create) the log WAL at `dir/raft-log.wal`, replaying its records
    /// to rebuild the full in-memory state. A torn tail frame is truncated back
    /// to the last valid boundary.
    ///
    /// # Errors
    /// Propagates OS I/O errors as `StorageError` (write-logs subject).
    pub fn open(dir: PathBuf) -> Result<Self, StorageError<u64>> {
        std::fs::create_dir_all(&dir)
            .map_err(|e| StorageIOError::write_logs(&io_err(format!("create WAL dir: {e}"))))?;
        let path = dir.join("raft-log.wal");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| StorageIOError::write_logs(&io_err(format!("open WAL: {e}"))))?;

        // Persist the WAL's directory entry: create(true) may have just created
        // the file, and a rename/create is not durable until the parent dir is
        // fsynced. Without this a crash could lose a newly-created log file whose
        // first entries were already fsynced and acked. Best-effort (dir fsync is
        // rejected on some filesystems); once per open, negligible cost.
        if let Ok(dir_file) = std::fs::File::open(&dir) {
            let _ = dir_file.sync_all();
        }

        // Read the whole file; Raft logs are bounded by purge + (future) snapshot
        // compaction, so a full in-memory read is acceptable.
        let mut data = Vec::new();
        file.read_to_end(&mut data)
            .map_err(|e| StorageIOError::read_logs(&io_err(format!("read WAL: {e}"))))?;

        let mut log: BTreeMap<u64, Entry<TypeConfig>> = BTreeMap::new();
        let mut last_purged: Option<LogId<u64>> = None;
        let mut vote: Option<Vote<u64>> = None;
        let mut committed: Option<LogId<u64>> = None;

        let mut pos = 0usize;
        let mut last_valid = 0usize;
        // Collect records first so we can honor a Snapshot record (which discards
        // everything logically prior) without re-reading the file.
        let mut recs: Vec<WalRec> = Vec::new();
        while pos < data.len() {
            match decode_frame::<WalRec>(&data, pos) {
                FrameDecode::Entry(rec, new_pos) => {
                    recs.push(rec);
                    last_valid = new_pos;
                    pos = new_pos;
                }
                // Torn tail: the final append was interrupted. Stop and truncate
                // the partial bytes below — openraft tolerates losing un-acked
                // tail entries (they were never confirmed durable).
                FrameDecode::Incomplete => break,
                // A fully-written frame is corrupt (bit-rot). Refuse to open:
                // silently truncating here would discard every valid record after
                // it, which for the authoritative Raft log means silently dropping
                // committed entries. Fail loud so the node recovers from a peer
                // snapshot instead of serving a short, lossy log.
                FrameDecode::Corrupt(why) => {
                    return Err(StorageIOError::read_logs(&io_err(format!(
                        "Raft log WAL {}: {why}; {} record(s) recovered before it. \
                         Refusing to truncate — recover this node from a peer.",
                        path.display(),
                        recs.len()
                    )))
                    .into());
                }
            }
        }

        // Truncate any partial tail frame so the next append starts cleanly.
        if last_valid < data.len() {
            file.set_len(last_valid as u64).map_err(|e| {
                StorageIOError::write_logs(&io_err(format!("truncate torn tail: {e}")))
            })?;
        }
        // Position the write cursor at EOF.
        file.seek(SeekFrom::End(0))
            .map_err(|e| StorageIOError::write_logs(&io_err(format!("seek EOF: {e}"))))?;

        // Replay only from the last Snapshot onward (earlier records are folded
        // into that snapshot), mirroring the ref-store checkpoint semantics.
        let start = recs
            .iter()
            .rposition(|r| matches!(r, WalRec::Snapshot { .. }))
            .unwrap_or(0);

        for rec in &recs[start..] {
            match rec {
                WalRec::Entry { index, entry_bytes } => {
                    let entry: Entry<TypeConfig> = unblob(entry_bytes)?;
                    log.insert(*index, entry);
                }
                WalRec::Vote { vote_bytes } => {
                    vote = Some(unblob(vote_bytes)?);
                }
                WalRec::Committed { committed_bytes } => {
                    committed = unblob(committed_bytes)?;
                }
                WalRec::Truncate { at } => {
                    log.split_off(at);
                }
                WalRec::Purge { up_to_bytes } => {
                    let up_to: LogId<u64> = unblob(up_to_bytes)?;
                    last_purged = Some(up_to);
                    log = log.split_off(&(up_to.index + 1));
                }
                WalRec::Snapshot {
                    entries,
                    vote_bytes,
                    committed_bytes,
                    purged_bytes,
                } => {
                    log.clear();
                    for (i, eb) in entries {
                        log.insert(*i, unblob(eb)?);
                    }
                    vote = match vote_bytes {
                        Some(b) => Some(unblob(b)?),
                        None => None,
                    };
                    committed = unblob(committed_bytes)?;
                    last_purged = unblob(purged_bytes)?;
                }
            }
        }

        Ok(WalLogStore {
            inner: Arc::new(Mutex::new(WalInner {
                log,
                last_purged,
                vote,
                committed,
                file,
                path,
            })),
        })
    }

    /// Rewrite the WAL as a single `Snapshot` frame capturing the current live
    /// state, discarding the historical record stream. Used for compaction; not
    /// yet wired to a size trigger (Phase 3 TODO — see module docs). Exposed for
    /// completeness and tested for correctness.
    ///
    /// # Crash atomicity
    /// The snapshot is written to a sibling temp file, fsynced, then atomically
    /// `rename`d over the live WAL (parent directory fsynced), so a crash leaves
    /// either the intact old log or the intact new snapshot — never a torn frame
    /// at offset 0. An in-place `seek(0); write; set_len` cannot offer this: a
    /// crash after the header lands but before the payload does corrupts frame 0,
    /// and on the next `open` that now fails loud, refusing to serve the log —
    /// losing the entire Raft log for this node. After the rename the old fd
    /// refers to the unlinked inode, so the handle is reopened at EOF.
    ///
    /// # Errors
    /// Propagates encode / I/O errors as `StorageError`.
    pub async fn rewrite_snapshot(&self) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        let mut entries = Vec::with_capacity(inner.log.len());
        for (i, e) in &inner.log {
            entries.push((*i, blob(e)?));
        }
        let vote_bytes = match &inner.vote {
            Some(v) => Some(blob(v)?),
            None => None,
        };
        let committed_bytes = blob(&inner.committed)?;
        let purged_bytes = blob(&inner.last_purged)?;
        let rec = WalRec::Snapshot {
            entries,
            vote_bytes,
            committed_bytes,
            purged_bytes,
        };
        let frame = encode_frame(&rec).map_err(|e| StorageIOError::write_logs(&io_err(e)))?;

        // Sibling temp path (append ".compact" to the full filename so we never
        // clobber a real extension), in the same directory so the rename is a
        // same-filesystem atomic op.
        let tmp_path = {
            let mut p = inner.path.clone().into_os_string();
            p.push(".compact");
            PathBuf::from(p)
        };
        {
            let mut tmp = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)
                .map_err(|e| StorageIOError::write_logs(&io_err(format!("open temp: {e}"))))?;
            tmp.write_all(&frame)
                .map_err(|e| StorageIOError::write_logs(&io_err(format!("temp write: {e}"))))?;
            tmp.sync_all()
                .map_err(|e| StorageIOError::write_logs(&io_err(format!("temp fsync: {e}"))))?;
        }
        std::fs::rename(&tmp_path, &inner.path)
            .map_err(|e| StorageIOError::write_logs(&io_err(format!("rename: {e}"))))?;
        // POSIX: the rename is not durable until the parent directory is fsynced.
        // Best-effort — some filesystems reject an fsync on a directory fd.
        if let Some(dir) = inner.path.parent() {
            if let Ok(dir_file) = std::fs::File::open(dir) {
                let _ = dir_file.sync_all();
            }
        }
        // The old fd now points at the unlinked pre-rename inode; reopen the live
        // file and position at EOF so subsequent appends land after the snapshot.
        let mut new_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .open(&inner.path)
            .map_err(|e| StorageIOError::write_logs(&io_err(format!("reopen: {e}"))))?;
        new_file
            .seek(SeekFrom::End(0))
            .map_err(|e| StorageIOError::write_logs(&io_err(format!("seek EOF: {e}"))))?;
        inner.file = new_file;
        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for WalLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let inner = self.inner.lock().await;
        let entries = inner
            .log
            .range(range)
            .map(|(_, e)| e.clone())
            .collect::<Vec<_>>();
        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for WalLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let inner = self.inner.lock().await;
        let last = inner
            .log
            .iter()
            .next_back()
            .map(|(_, e)| e.log_id)
            // No present entry: the last log id is the last purged id (if any).
            .or(inner.last_purged);
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id: last,
        })
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let vote_bytes = blob(vote)?;
        let mut inner = self.inner.lock().await;
        // Durable before return: write + fsync, then update memory.
        inner.write_frame(&WalRec::Vote { vote_bytes })?;
        inner.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().await.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        let committed_bytes = blob(&committed)?;
        let mut inner = self.inner.lock().await;
        inner.write_frame(&WalRec::Committed { committed_bytes })?;
        inner.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().await.committed)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut inner = self.inner.lock().await;
            for entry in entries {
                let index = entry.log_id.index;
                // Serialize the opaque entry blob, then write+fsync the frame
                // BEFORE inserting into memory, so a crash mid-append never
                // leaves an in-memory entry that is not on disk.
                let entry_bytes = blob(&entry)?;
                inner.write_frame(&WalRec::Entry { index, entry_bytes })?;
                inner.log.insert(index, entry);
            }
        }
        // All entries are now durable (fsync'd above) — signal completion. This
        // is the openraft durability contract: the callback fires only after the
        // log IO has hit stable storage.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        // Persist the marker (durable) before mutating memory.
        inner.write_frame(&WalRec::Truncate { at: log_id.index })?;
        // Remove conflicting entries since `log_id`, inclusive.
        inner.log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let up_to_bytes = blob(&log_id)?;
        let mut inner = self.inner.lock().await;
        inner.write_frame(&WalRec::Purge { up_to_bytes })?;
        inner.last_purged = Some(log_id);
        // Retain only entries strictly after `log_id.index`.
        inner.log = inner.log.split_off(&(log_id.index + 1));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::op::LedgeOp;
    use openraft::storage::RaftLogStorage;
    use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, RaftLogReader, Vote};
    use tempfile::tempdir;

    fn entry(index: u64) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(LedgeOp::RefUpdate {
                name: "refs/heads/main".into(),
                target_bytes: [index as u8; 32],
                expected_bytes: None,
                hlc: index,
            }),
        }
    }

    fn log_id(index: u64) -> LogId<u64> {
        LogId::new(CommittedLeaderId::new(1, 1), index)
    }

    /// A `LogFlushed` cannot be constructed outside openraft, so `append` tests
    /// drive the durable path via a tiny local helper that mirrors `append`'s
    /// write loop (the real callback wiring is exercised by the cluster harness).
    /// We instead exercise `append`'s logic through the public trait by feeding a
    /// callback we obtain from a 1-node Raft is overkill here; the durable write
    /// path is identical to `truncate`/`purge`/`save_vote` which we test directly,
    /// plus we test the entry frame round-trip through reopen.
    async fn append_durable(store: &WalLogStore, entries: Vec<Entry<TypeConfig>>) {
        let mut inner = store.inner.lock().await;
        for entry in entries {
            let index = entry.log_id.index;
            let entry_bytes = blob(&entry).unwrap();
            inner
                .write_frame(&WalRec::Entry { index, entry_bytes })
                .unwrap();
            inner.log.insert(index, entry);
        }
    }

    #[tokio::test]
    async fn append_and_read_back() {
        let dir = tempdir().unwrap();
        let store = WalLogStore::open(dir.path().to_path_buf()).unwrap();
        append_durable(&store, vec![entry(1), entry(2), entry(3)]).await;
        let mut reader = store.clone();
        let got = reader.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(
            got.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        // Payload survives the blob round-trip.
        match &got[1].payload {
            EntryPayload::Normal(LedgeOp::RefUpdate {
                hlc, target_bytes, ..
            }) => {
                assert_eq!(*hlc, 2);
                assert_eq!(target_bytes[0], 2);
            }
            other => panic!("expected RefUpdate, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn vote_persistence() {
        let dir = tempdir().unwrap();
        let v = Vote::new(2, 1);
        {
            let mut store = WalLogStore::open(dir.path().to_path_buf()).unwrap();
            assert_eq!(store.read_vote().await.unwrap(), None);
            store.save_vote(&v).await.unwrap();
            assert_eq!(store.read_vote().await.unwrap(), Some(v));
        }
        // Reopen: vote survives.
        let mut store = WalLogStore::open(dir.path().to_path_buf()).unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(v));
    }

    #[tokio::test]
    async fn save_and_read_committed() {
        let dir = tempdir().unwrap();
        let c = log_id(5);
        {
            let mut store = WalLogStore::open(dir.path().to_path_buf()).unwrap();
            assert_eq!(store.read_committed().await.unwrap(), None);
            store.save_committed(Some(c)).await.unwrap();
            assert_eq!(store.read_committed().await.unwrap(), Some(c));
        }
        let mut store = WalLogStore::open(dir.path().to_path_buf()).unwrap();
        assert_eq!(store.read_committed().await.unwrap(), Some(c));
    }

    #[tokio::test]
    async fn truncate_drops_suffix() {
        let dir = tempdir().unwrap();
        let mut store = WalLogStore::open(dir.path().to_path_buf()).unwrap();
        append_durable(&store, (1..=5).map(entry).collect()).await;
        store.truncate(log_id(3)).await.unwrap();
        let st = store.get_log_state().await.unwrap();
        assert_eq!(st.last_log_id.unwrap().index, 2);
        let mut reader = store.clone();
        assert!(reader.try_get_log_entries(3..=5).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn purge_drops_prefix() {
        let dir = tempdir().unwrap();
        let mut store = WalLogStore::open(dir.path().to_path_buf()).unwrap();
        append_durable(&store, (1..=5).map(entry).collect()).await;
        store.purge(log_id(2)).await.unwrap();
        let st = store.get_log_state().await.unwrap();
        assert_eq!(st.last_purged_log_id.unwrap().index, 2);
        let mut reader = store.clone();
        assert!(reader.try_get_log_entries(1..=2).await.unwrap().is_empty());
        assert_eq!(
            reader
                .try_get_log_entries(3..=5)
                .await
                .unwrap()
                .iter()
                .map(|e| e.log_id.index)
                .collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
    }

    #[tokio::test]
    async fn reopen_replays() {
        let dir = tempdir().unwrap();
        let v = Vote::new(7, 1);
        {
            let mut store = WalLogStore::open(dir.path().to_path_buf()).unwrap();
            append_durable(&store, (1..=4).map(entry).collect()).await;
            store.save_vote(&v).await.unwrap();
        }
        let mut store = WalLogStore::open(dir.path().to_path_buf()).unwrap();
        let got = store.try_get_log_entries(1..=4).await.unwrap();
        assert_eq!(
            got.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert_eq!(store.read_vote().await.unwrap(), Some(v));
    }

    #[tokio::test]
    async fn truncated_tail_recovery() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("raft-log.wal");
        {
            let store = WalLogStore::open(dir.path().to_path_buf()).unwrap();
            append_durable(&store, (1..=5).map(entry).collect()).await;
        }
        // Lop 3 bytes off the tail to simulate a torn final write.
        {
            let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            let l = f.metadata().unwrap().len();
            f.set_len(l - 3).unwrap();
        }
        let mut store = WalLogStore::open(dir.path().to_path_buf()).unwrap();
        let got = store.try_get_log_entries(1..=5).await.unwrap();
        // At least the first 4 entries survive (the torn 5th is dropped).
        assert!(got.len() >= 4, "recovered {} entries", got.len());
        // Store is still usable: a fresh append at the recovered next index works.
        let next = got.last().unwrap().log_id.index + 1;
        append_durable(&store, vec![entry(next)]).await;
        let got2 = store.try_get_log_entries(next..=next).await.unwrap();
        assert_eq!(got2.len(), 1);
    }

    #[tokio::test]
    async fn mid_stream_corruption_fails_loud_not_silent() {
        // Bit-rot in an EARLY frame must not silently drop the committed records
        // after it. Write several entries, flip a byte in the first, and require
        // open() to error rather than return a truncated, lossy log.
        let dir = tempdir().unwrap();
        let path = dir.path().join("raft-log.wal");
        {
            let store = WalLogStore::open(dir.path().to_path_buf()).unwrap();
            append_durable(&store, (1..=5).map(entry).collect()).await;
        }
        {
            // Byte 9 is inside the first frame's payload (header is bytes 0..8).
            let mut data = std::fs::read(&path).unwrap();
            data[9] ^= 0xFF;
            std::fs::write(&path, &data).unwrap();
        }
        assert!(
            WalLogStore::open(dir.path().to_path_buf()).is_err(),
            "mid-stream corruption must fail open, not silently drop committed records"
        );
    }

    #[tokio::test]
    async fn rewrite_snapshot_compacts_and_replays() {
        let dir = tempdir().unwrap();
        let v = Vote::new(3, 1);
        {
            let mut store = WalLogStore::open(dir.path().to_path_buf()).unwrap();
            append_durable(&store, (1..=5).map(entry).collect()).await;
            store.purge(log_id(2)).await.unwrap();
            store.save_vote(&v).await.unwrap();
            store.save_committed(Some(log_id(4))).await.unwrap();
            store.rewrite_snapshot().await.unwrap();
            // Append after the snapshot to prove post-snapshot records replay too.
            append_durable(&store, vec![entry(6)]).await;
        }
        let mut store = WalLogStore::open(dir.path().to_path_buf()).unwrap();
        let st = store.get_log_state().await.unwrap();
        assert_eq!(st.last_purged_log_id.unwrap().index, 2);
        assert_eq!(st.last_log_id.unwrap().index, 6);
        assert_eq!(store.read_vote().await.unwrap(), Some(v));
        assert_eq!(store.read_committed().await.unwrap(), Some(log_id(4)));
        let got = store.try_get_log_entries(3..=6).await.unwrap();
        assert_eq!(
            got.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![3, 4, 5, 6]
        );
    }

    #[tokio::test]
    async fn rewrite_snapshot_leaves_no_temp_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("raft-log.wal");
        let mut store = WalLogStore::open(dir.path().to_path_buf()).unwrap();
        append_durable(&store, (1..=3).map(entry).collect()).await;
        store.rewrite_snapshot().await.unwrap();
        let mut tmp = path.clone().into_os_string();
        tmp.push(".compact");
        assert!(
            !std::path::Path::new(&tmp).exists(),
            "temp file must not survive a successful rewrite"
        );
        // The reopened handle still appends + recovers cleanly.
        append_durable(&store, vec![entry(4)]).await;
        let got = store.try_get_log_entries(1..=4).await.unwrap();
        assert_eq!(got.len(), 4);
    }

    #[tokio::test]
    async fn stale_snapshot_temp_does_not_corrupt_live_log() {
        // Models a crash mid-rewrite: a partial ".compact" temp is left behind
        // while the live WAL is still the pre-rewrite file. Recovery must read the
        // intact live log and ignore the orphaned temp — the invariant the atomic
        // rename buys (never a torn frame 0, never a lost log).
        let dir = tempdir().unwrap();
        let path = dir.path().join("raft-log.wal");
        {
            let store = WalLogStore::open(dir.path().to_path_buf()).unwrap();
            append_durable(&store, (1..=3).map(entry).collect()).await;
        }
        let mut tmp = path.clone().into_os_string();
        tmp.push(".compact");
        std::fs::write(&tmp, b"\xde\xad\xbe\xef partial snapshot").unwrap();

        let mut store = WalLogStore::open(dir.path().to_path_buf()).unwrap();
        let got = store.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(got.len(), 3, "live log must survive intact");
    }
}
