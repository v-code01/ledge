//! Write-Ahead Log (WAL) for the ledge ref store.
//!
//! # Frame format
//! Every record on disk is a self-describing framed entry:
//!
//! ```text
//! | length: u32 LE | crc32: u32 LE | bincode payload (length bytes) |
//! ```
//!
//! * `length`  – byte length of the bincode payload that follows.
//! * `crc32`   – CRC-32 of the payload bytes (IEEE polynomial via crc32fast).
//! * payload   – bincode-encoded `WalEntry` using the standard config.
//!
//! On open, the file is read sequentially.  A frame whose declared length
//! runs past EOF is a torn tail (an interrupted final write): the file is
//! truncated back to the last valid boundary and the prefix recovers. A frame
//! that is fully present but fails its CRC is in-place corruption (bit-rot),
//! not a torn write, so `open` fails loudly rather than silently truncating —
//! silently dropping it would also discard every valid frame that follows.
//!
//! # Checkpoint compaction
//! `compact()` writes a single `Checkpoint` frame to a sibling temp file,
//! fsyncs it, and atomically `rename`s it over the live WAL (then fsyncs the
//! directory), so a crash leaves either the intact old WAL or the intact new
//! checkpoint — never a torn frame at offset 0.  On the next `open()` only
//! entries at or after the last checkpoint are replayed.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use ledge_core::{LedgeError, RefEntry, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

// ── Frame layout ─────────────────────────────────────────────────────────────

/// Byte size of the fixed frame header (length u32 + crc32 u32).
const HEADER_LEN: usize = 8;

/// Encode a `WalEntry` into a complete on-disk frame.
///
/// # Errors
/// Returns `LedgeError::Corruption` if bincode serialization fails.
fn encode_frame(entry: &WalEntry) -> Result<Vec<u8>> {
    let payload = bincode::serde::encode_to_vec(entry, bincode::config::standard())
        .map_err(|e| LedgeError::Corruption(format!("WAL encode: {e}")))?;
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
/// The distinction between `Incomplete` and `Corrupt` is what lets recovery
/// tell a benign torn-tail write apart from silent bit-rot:
/// * `Incomplete` — there are not enough bytes for the frame the header claims,
///   i.e. the last append was interrupted mid-write. Safe to truncate the tail.
/// * `Corrupt` — a frame that is *fully present* on disk fails its CRC (or its
///   payload will not decode). A torn write is always short, never full-length-
///   with-bad-CRC, so this is in-place corruption. It must NOT be silently
///   dropped: doing so would also discard every valid frame after it.
enum FrameDecode {
    Entry(WalEntry, usize),
    Incomplete,
    Corrupt(String),
}

/// Attempt to decode one frame from `data` starting at `pos`.
fn decode_frame(data: &[u8], pos: usize) -> FrameDecode {
    if pos + HEADER_LEN > data.len() {
        return FrameDecode::Incomplete; // header torn by an interrupted write
    }
    // SAFETY: slice bounds checked above; try_into on exactly-4-byte slice is
    // infallible — the unwrap() below will never panic.
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
        Ok((entry, _)) => FrameDecode::Entry(entry, payload_end),
        // CRC matched but the bytes will not decode — corrupt payload structure.
        Err(e) => FrameDecode::Corrupt(format!("payload decode error at byte {pos}: {e}")),
    }
}

// ── Public types ──────────────────────────────────────────────────────────────

/// A WAL entry describing a single ref-store mutation or a compaction snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WalEntry {
    /// A ref was created or updated.
    Update { name: String, entry: RefEntry },
    /// A ref was deleted; `hlc` is the timestamp of the deletion.
    Delete { name: String, hlc: u64 },
    /// A compaction checkpoint containing the full ref-store snapshot at the
    /// time of compaction.  On recovery, only entries at or after the last
    /// checkpoint are replayed.
    Checkpoint { leaves: Vec<(String, RefEntry)> },
    /// An atomic multi-ref commit: all `updates` were published to the store in
    /// a single CoW swap, so they must recover all-or-nothing. Persisting them
    /// as one frame gives that guarantee for free — the frame's length+CRC guard
    /// means a torn tail write drops the whole batch, never a partial prefix.
    ///
    /// Appended as the last variant so bincode's enum discriminants for the
    /// pre-existing variants are unchanged and older WAL files still replay.
    Batch { updates: Vec<(String, RefEntry)> },
}

/// Append-only WAL backed by a single flat file.
///
/// All `append` / `compact` calls are serialised through an async `Mutex`
/// so concurrent tokio tasks share a single `Wal` safely.  The underlying
/// `std::fs::File` performs synchronous I/O; for the ref-store's write path
/// this is acceptable because the mutex already serialises writers.
pub struct Wal {
    /// Mutex-protected file handle, always positioned at EOF for appends.
    file: Mutex<std::fs::File>,
    /// Path to the WAL file on disk (used by `file_size_bytes`).
    pub path: PathBuf,
    /// Test-only fault injection: when set, the next `append` returns an I/O
    /// error without writing, so durability-failure paths can be exercised
    /// deterministically. Compiled out of non-test builds.
    #[cfg(test)]
    fail_next_append: std::sync::atomic::AtomicBool,
}

impl Wal {
    /// Open (or create) the WAL at `path` and replay its contents.
    ///
    /// If the file ends with a partial / corrupt frame the file is truncated
    /// back to the last valid frame boundary before returning.
    ///
    /// # Returns
    /// A tuple of:
    /// * the open `Wal` instance, ready for further appends, and
    /// * the slice of entries to replay — everything from the last
    ///   `Checkpoint` to the end of the file (or the full file if there
    ///   is no checkpoint).
    ///
    /// # Errors
    /// Propagates any OS-level I/O error encountered while opening or reading.
    pub fn open(path: PathBuf) -> Result<(Self, Vec<WalEntry>)> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            // Do not truncate on open — we read existing entries and then append.
            .truncate(false)
            .open(&path)
            .map_err(LedgeError::Io)?;

        // Persist the file's own directory entry: create(true) may have just
        // created the file, and POSIX does not guarantee that a new dir entry is
        // durable until the parent directory is fsynced. Without this, the first
        // append's fsync makes the DATA durable while the file itself could still
        // vanish on power loss, losing an acked write. Once per open — negligible.
        // Best-effort: some filesystems reject an fsync on a directory fd.
        if let Some(dir) = path.parent() {
            if let Ok(dir_file) = std::fs::File::open(dir) {
                let _ = dir_file.sync_all();
            }
        }

        // Read the entire file into memory.  WALs are typically small (tens of
        // MiB at most before compaction kicks in).
        let mut data = Vec::new();
        file.read_to_end(&mut data).map_err(LedgeError::Io)?;

        // Decode all valid frames, tracking the byte offset of the last valid
        // frame boundary so we can truncate any torn tail write.
        let mut all: Vec<WalEntry> = Vec::new();
        let mut pos = 0usize;
        let mut last_valid = 0usize;
        while pos < data.len() {
            match decode_frame(&data, pos) {
                FrameDecode::Entry(entry, new_pos) => {
                    all.push(entry);
                    last_valid = new_pos;
                    pos = new_pos;
                }
                // Torn tail: the final append was interrupted. Stop and truncate
                // the partial bytes below — no valid data is lost.
                FrameDecode::Incomplete => break,
                // A fully-written frame is corrupt (bit-rot). Refuse to open:
                // silently truncating here would also discard every valid frame
                // after this point, turning one flipped bit into wholesale loss.
                FrameDecode::Corrupt(why) => {
                    return Err(LedgeError::Corruption(format!(
                        "WAL {}: {why}; {} frame(s) recovered before it. Refusing to \
                         truncate — restore from a replica or backup.",
                        path.display(),
                        all.len()
                    )));
                }
            }
        }

        // Truncate any partial tail frame so the next append starts cleanly.
        if last_valid < data.len() {
            file.set_len(last_valid as u64).map_err(LedgeError::Io)?;
        }
        // Position write cursor at EOF so appends land at the right offset.
        file.seek(SeekFrom::End(0)).map_err(LedgeError::Io)?;

        // Only replay from the last checkpoint onward.  Earlier entries have
        // already been incorporated into that checkpoint's snapshot.
        let last_cp = all
            .iter()
            .rposition(|e| matches!(e, WalEntry::Checkpoint { .. }));
        let replay: Vec<WalEntry> = match last_cp {
            Some(i) => all[i..].to_vec(),
            None => all,
        };

        Ok((
            Wal {
                file: Mutex::new(file),
                path,
                #[cfg(test)]
                fail_next_append: std::sync::atomic::AtomicBool::new(false),
            },
            replay,
        ))
    }

    /// Test-only: arm a one-shot failure so the next `append` returns an I/O
    /// error without writing. Used to drive `CommitBatchError::NotDurable` and
    /// the swallow/propagate paths deterministically.
    #[cfg(test)]
    pub fn fail_next_append(&self) {
        self.fail_next_append
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Append a single `WalEntry` to the WAL and fsync it before returning.
    ///
    /// The frame is written with one `write_all` and then `sync_data`d, so a
    /// return of `Ok(())` means the entry is on stable storage: it survives not
    /// just a process crash but a power loss or kernel panic. This is the
    /// property that lets the ref store ack a write only once it is durable —
    /// without the fsync, `write_all` leaves the frame in the OS page cache and
    /// an acked ref can vanish on power loss.
    ///
    /// `sync_data` (fdatasync) is used rather than `sync_all` (fsync): the frame
    /// is appended at EOF so the file's size metadata does change, but on the
    /// platforms we target fdatasync flushes the size along with the data, and
    /// it avoids the extra inode-timestamp flush that full fsync forces.
    ///
    /// # Errors
    /// Propagates bincode encode errors as `LedgeError::Corruption` and OS
    /// write / sync errors as `LedgeError::Io`.
    pub async fn append(&self, entry: &WalEntry) -> Result<()> {
        #[cfg(test)]
        if self
            .fail_next_append
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(LedgeError::Io(std::io::Error::other(
                "injected WAL append failure",
            )));
        }
        let frame = encode_frame(entry)?;
        let mut file = self.file.lock().await;
        file.write_all(&frame).map_err(LedgeError::Io)?;
        file.sync_data().map_err(LedgeError::Io)
    }

    /// Compact the WAL by replacing all existing content with a single
    /// `Checkpoint` frame containing `leaves`, then positioning the write
    /// cursor at EOF ready for subsequent appends.
    ///
    /// # Crash atomicity
    /// The checkpoint is written to a sibling temp file, fsynced, and then
    /// atomically `rename`d over the live WAL; the parent directory is fsynced
    /// so the rename itself is durable. A crash at any instant therefore
    /// leaves *either* the intact old WAL (full history) *or* the intact new
    /// checkpoint on disk — never a torn frame at offset 0.
    ///
    /// An in-place overwrite (`seek(0); write; set_len`) cannot offer this: a
    /// crash after the header but before the payload lands corrupts frame 0,
    /// whose failed CRC on the next `open` truncates the file to empty and
    /// loses every ref. That is the bug this method exists to avoid.
    ///
    /// After the rename the pre-existing file descriptor refers to the now
    /// unlinked old inode, so the handle is reopened against the live path and
    /// seeked to EOF; subsequent `append`s land after the checkpoint.
    ///
    /// # Errors
    /// Propagates encode or I/O errors.
    pub async fn compact(&self, leaves: Vec<(String, RefEntry)>) -> Result<()> {
        self.compact_with(move || leaves).await
    }

    /// Like [`compact`], but the checkpoint payload is produced by `snapshot`
    /// **while the WAL lock is held**, closing a compaction/append race.
    ///
    /// Whole-file compaction discards every frame not in the checkpoint. If the
    /// payload were snapshotted before the lock (as a plain `compact(leaves)`
    /// caller must), a writer could `append` a new ref into that window and
    /// have its frame erased by the replacement — the ref survives in memory
    /// but is lost on the next `open()`. Taking the snapshot under the lock
    /// removes the window: a writer whose `append` has not completed is blocked
    /// behind this lock and lands *after* the new checkpoint, while a writer
    /// that already appended has, by the store's append-after-publish ordering,
    /// already made its ref visible to `snapshot`. Either way nothing is lost.
    ///
    /// `snapshot` runs synchronously under the lock (an O(refs) walk), briefly
    /// stalling appends for its duration — acceptable since compaction is rare.
    ///
    /// # Errors
    /// Propagates encode or I/O errors.
    pub async fn compact_with<F>(&self, snapshot: F) -> Result<()>
    where
        F: FnOnce() -> Vec<(String, RefEntry)>,
    {
        // Hold the WAL lock across snapshot → write → rename → reopen so no
        // append can interleave (see the race analysis above).
        let mut file = self.file.lock().await;
        let frame = encode_frame(&WalEntry::Checkpoint { leaves: snapshot() })?;

        // Sibling temp path: append ".compact" to the full filename so we never
        // clobber a real extension (wal → wal.compact), keeping it in the same
        // directory as the WAL so the rename is a same-filesystem atomic op.
        let tmp_path = {
            let mut p = self.path.clone().into_os_string();
            p.push(".compact");
            PathBuf::from(p)
        };

        {
            let mut tmp = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)
                .map_err(LedgeError::Io)?;
            tmp.write_all(&frame).map_err(LedgeError::Io)?;
            // Durable before the rename publishes it.
            tmp.sync_all().map_err(LedgeError::Io)?;
        }

        std::fs::rename(&tmp_path, &self.path).map_err(LedgeError::Io)?;

        // POSIX: a rename is not guaranteed persisted until the parent
        // directory entry is fsynced. Best-effort — some platforms/filesystems
        // reject fsync on a directory fd; a failure there does not lose data.
        if let Some(dir) = self.path.parent() {
            if let Ok(dir_file) = std::fs::File::open(dir) {
                let _ = dir_file.sync_all();
            }
        }

        // Reopen against the live (post-rename) inode; the old fd is stale.
        let mut new_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .open(&self.path)
            .map_err(LedgeError::Io)?;
        new_file.seek(SeekFrom::End(0)).map_err(LedgeError::Io)?;
        *file = new_file;
        Ok(())
    }

    /// Return the current on-disk byte size of the WAL file.
    ///
    /// Used by higher-level compaction triggers to decide when to compact.
    /// Returns 0 if the metadata call fails (e.g., file deleted externally).
    pub fn file_size_bytes(&self) -> u64 {
        std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledge_core::{ObjectId, RefEntry};
    use tempfile::tempdir;

    fn make_entry(byte: u8, version: u64) -> RefEntry {
        RefEntry {
            target: ObjectId::from_bytes([byte; 32]),
            hlc: version,
            version,
        }
    }

    #[tokio::test]
    async fn append_and_recover_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let (wal, entries) = Wal::open(path.clone()).unwrap();
        assert!(entries.is_empty());
        drop(wal);
        let (_, entries2) = Wal::open(path).unwrap();
        assert!(entries2.is_empty());
    }

    #[tokio::test]
    async fn append_update_and_recover() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let (wal, _) = Wal::open(path.clone()).unwrap();
        wal.append(&WalEntry::Update {
            name: "refs/heads/main".to_string(),
            entry: make_entry(1, 1),
        })
        .await
        .unwrap();
        drop(wal);
        let (_, entries) = Wal::open(path).unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            WalEntry::Update { name, entry } => {
                assert_eq!(name, "refs/heads/main");
                assert_eq!(entry.version, 1);
            }
            _ => panic!("expected Update"),
        }
    }

    #[tokio::test]
    async fn recovery_from_checkpoint() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let (wal, _) = Wal::open(path.clone()).unwrap();
        for i in 0u8..3 {
            wal.append(&WalEntry::Update {
                name: format!("refs/heads/b{i}"),
                entry: make_entry(i, i as u64),
            })
            .await
            .unwrap();
        }
        wal.compact(vec![("refs/heads/main".to_string(), make_entry(42, 10))])
            .await
            .unwrap();
        wal.append(&WalEntry::Update {
            name: "refs/heads/post".to_string(),
            entry: make_entry(99, 20),
        })
        .await
        .unwrap();
        drop(wal);
        let (_, entries) = Wal::open(path).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(matches!(&entries[0], WalEntry::Checkpoint { .. }));
        assert!(matches!(&entries[1], WalEntry::Update { .. }));
    }

    #[tokio::test]
    async fn truncated_tail_recovery() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let (wal, _) = Wal::open(path.clone()).unwrap();
        for i in 0u8..5 {
            wal.append(&WalEntry::Update {
                name: format!("refs/heads/b{i}"),
                entry: make_entry(i, i as u64 + 1),
            })
            .await
            .unwrap();
        }
        drop(wal);
        {
            let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            let l = f.metadata().unwrap().len();
            f.set_len(l - 3).unwrap();
        }
        let (_, entries) = Wal::open(path).unwrap();
        assert!(entries.len() >= 4);
    }

    #[tokio::test]
    async fn delete_entry_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let (wal, _) = Wal::open(path.clone()).unwrap();
        wal.append(&WalEntry::Delete {
            name: "refs/heads/gone".to_string(),
            hlc: 999,
        })
        .await
        .unwrap();
        drop(wal);
        let (_, entries) = Wal::open(path).unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            WalEntry::Delete { name, hlc } => {
                assert_eq!(name, "refs/heads/gone");
                assert_eq!(*hlc, 999);
            }
            _ => panic!("expected Delete"),
        }
    }

    #[tokio::test]
    async fn compact_leaves_no_temp_file() {
        // After a successful compaction the sibling temp must be renamed away,
        // not left behind as garbage.
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let (wal, _) = Wal::open(path.clone()).unwrap();
        wal.append(&WalEntry::Update {
            name: "refs/heads/main".to_string(),
            entry: make_entry(1, 1),
        })
        .await
        .unwrap();
        wal.compact(vec![("refs/heads/main".to_string(), make_entry(1, 1))])
            .await
            .unwrap();
        let mut tmp = path.clone().into_os_string();
        tmp.push(".compact");
        assert!(
            !std::path::Path::new(&tmp).exists(),
            "temp file must not survive a successful compaction"
        );
        // The reopened handle must still accept appends that recover cleanly.
        wal.append(&WalEntry::Update {
            name: "refs/heads/after".to_string(),
            entry: make_entry(2, 2),
        })
        .await
        .unwrap();
        drop(wal);
        let (_, entries) = Wal::open(path).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(matches!(&entries[0], WalEntry::Checkpoint { .. }));
    }

    #[tokio::test]
    async fn stale_compact_temp_does_not_corrupt_live_wal() {
        // Models a crash mid-compaction: a partial ".compact" temp is left on
        // disk while the live WAL is still the pre-compaction file. Recovery
        // must read the intact live WAL and ignore the orphaned temp entirely —
        // this is the invariant the atomic rename buys us (no total ref loss).
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let (wal, _) = Wal::open(path.clone()).unwrap();
        for i in 0u8..3 {
            wal.append(&WalEntry::Update {
                name: format!("refs/heads/b{i}"),
                entry: make_entry(i, i as u64 + 1),
            })
            .await
            .unwrap();
        }
        drop(wal);
        // Simulate the interrupted temp write.
        let mut tmp = path.clone().into_os_string();
        tmp.push(".compact");
        std::fs::write(&tmp, b"\xde\xad\xbe\xef partial checkpoint").unwrap();

        let (_, entries) = Wal::open(path.clone()).unwrap();
        assert_eq!(entries.len(), 3, "live WAL must survive intact");
        assert!(entries.iter().all(|e| matches!(e, WalEntry::Update { .. })));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn compact_with_snapshot_runs_under_wal_lock() {
        // The whole compaction-race fix rests on one invariant: the closure that
        // produces the checkpoint payload runs while the WAL lock is held, so no
        // append can complete between the snapshot and the file replacement. Prove
        // it deterministically — a concurrent append must NOT finish while the
        // snapshot closure is executing. Regresses if compact_with is ever
        // refactored to snapshot outside the lock.
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let wal = Arc::new(Wal::open(path.clone()).unwrap().0);

        let appended = Arc::new(AtomicBool::new(false));
        let in_closure = Arc::new(tokio::sync::Notify::new());

        let w2 = Arc::clone(&wal);
        let appended2 = Arc::clone(&appended);
        let in_closure2 = Arc::clone(&in_closure);
        let compactor = tokio::spawn(async move {
            w2.compact_with(move || {
                // Lock is held here. Signal the test, then hold long enough that a
                // concurrent append would surely complete if the lock were NOT held.
                in_closure2.notify_one();
                std::thread::sleep(std::time::Duration::from_millis(100));
                assert!(
                    !appended2.load(Ordering::SeqCst),
                    "an append completed while the snapshot closure ran — the WAL \
                     lock is not held across the snapshot"
                );
                Vec::new()
            })
            .await
            .unwrap();
        });

        in_closure.notified().await;
        // Must block behind the compaction lock until the closure returns.
        wal.append(&WalEntry::Update {
            name: "refs/x".to_string(),
            entry: make_entry(1, 1),
        })
        .await
        .unwrap();
        appended.store(true, Ordering::SeqCst);
        compactor.await.unwrap();
    }

    #[tokio::test]
    async fn crc32_corruption_detected() {
        // A fully-written frame whose payload is corrupted (bit-rot) must be
        // surfaced as an error, not silently dropped: silently discarding it
        // would also discard every valid frame that follows.
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let (wal, _) = Wal::open(path.clone()).unwrap();
        wal.append(&WalEntry::Update {
            name: "refs/heads/main".to_string(),
            entry: make_entry(1, 1),
        })
        .await
        .unwrap();
        drop(wal);
        {
            let mut data = std::fs::read(&path).unwrap();
            if data.len() > 10 {
                data[9] ^= 0xFF; // flip a payload byte → CRC mismatch
            }
            std::fs::write(&path, &data).unwrap();
        }
        assert!(
            Wal::open(path).is_err(),
            "a corrupt full frame must fail open loudly, not recover as empty"
        );
    }

    #[tokio::test]
    async fn mid_stream_corruption_fails_loud_not_silent() {
        // Regression for the bit-rot case: corrupting an EARLY frame must not
        // silently discard the valid frames after it. Write several frames,
        // flip a byte in the first, and require open() to error rather than
        // return a truncated prefix.
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let (wal, _) = Wal::open(path.clone()).unwrap();
        for i in 0u8..5 {
            wal.append(&WalEntry::Update {
                name: format!("refs/heads/b{i}"),
                entry: make_entry(i, i as u64 + 1),
            })
            .await
            .unwrap();
        }
        drop(wal);
        {
            // Byte 9 is inside the FIRST frame's payload (header is bytes 0..8).
            let mut data = std::fs::read(&path).unwrap();
            data[9] ^= 0xFF;
            std::fs::write(&path, &data).unwrap();
        }
        match Wal::open(path) {
            Err(LedgeError::Corruption(_)) => {}
            Err(other) => panic!("expected a Corruption error, got {other:?}"),
            Ok(_) => panic!("mid-stream corruption must fail open, not recover a truncated prefix"),
        }
    }

    #[tokio::test]
    async fn torn_tail_after_valid_frames_still_recovers() {
        // Contrast with corruption: a SHORT final frame (interrupted write) is a
        // torn tail, not bit-rot, and must recover the valid prefix silently.
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let (wal, _) = Wal::open(path.clone()).unwrap();
        for i in 0u8..4 {
            wal.append(&WalEntry::Update {
                name: format!("refs/heads/b{i}"),
                entry: make_entry(i, i as u64 + 1),
            })
            .await
            .unwrap();
        }
        drop(wal);
        {
            // Shear a few bytes off the end → the last frame is now short.
            let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            let len = f.metadata().unwrap().len();
            f.set_len(len - 3).unwrap();
        }
        let (_, entries) = Wal::open(path).expect("a torn tail must recover, not error");
        assert_eq!(
            entries.len(),
            3,
            "the three intact frames survive the torn tail"
        );
    }
}
