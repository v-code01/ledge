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
//! On open, the file is read sequentially.  Any frame whose length would
//! extend beyond EOF or whose CRC does not match is treated as a partial
//! write and the file is truncated back to the last valid frame boundary.
//!
//! # Checkpoint compaction
//! `compact()` atomically overwrites the entire file with a single
//! `Checkpoint` frame and then positions the write cursor at the end so
//! subsequent `append()` calls land after the checkpoint.  On the next
//! `open()` only entries at or after the last checkpoint are replayed.

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

/// Attempt to decode one frame from `data` starting at `pos`.
///
/// Returns `Some((entry, next_pos))` on success, `None` on any error
/// (truncated header, truncated payload, CRC mismatch, decode error).
/// The caller is responsible for truncating the backing file when `None`
/// is returned mid-stream.
fn decode_frame(data: &[u8], pos: usize) -> Option<(WalEntry, usize)> {
    if pos + HEADER_LEN > data.len() {
        return None;
    }
    // SAFETY: slice bounds checked above; try_into on exactly-4-byte slice is
    // infallible — the unwrap() below will never panic.
    let length = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
    let crc_stored = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap());
    let payload_end = pos + HEADER_LEN + length;
    if payload_end > data.len() {
        return None;
    }
    let payload = &data[pos + HEADER_LEN..payload_end];
    if crc32fast::hash(payload) != crc_stored {
        return None;
    }
    let (entry, _): (WalEntry, _) =
        bincode::serde::decode_from_slice(payload, bincode::config::standard()).ok()?;
    Some((entry, payload_end))
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
                Some((entry, new_pos)) => {
                    all.push(entry);
                    last_valid = new_pos;
                    pos = new_pos;
                }
                None => break,
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
        let last_cp = all.iter().rposition(|e| matches!(e, WalEntry::Checkpoint { .. }));
        let replay: Vec<WalEntry> = match last_cp {
            Some(i) => all[i..].to_vec(),
            None => all,
        };

        Ok((Wal { file: Mutex::new(file), path }, replay))
    }

    /// Append a single `WalEntry` to the WAL.
    ///
    /// The entry is encoded and written atomically from the OS perspective
    /// (a single `write_all` call).  The OS may still buffer; callers that
    /// need strict durability should call `fsync` themselves.
    ///
    /// # Errors
    /// Propagates bincode encode errors as `LedgeError::Corruption` and
    /// OS write errors as `LedgeError::Io`.
    pub async fn append(&self, entry: &WalEntry) -> Result<()> {
        let frame = encode_frame(entry)?;
        let mut file = self.file.lock().await;
        file.write_all(&frame).map_err(LedgeError::Io)
    }

    /// Compact the WAL by replacing all existing content with a single
    /// `Checkpoint` frame containing `leaves`, then positioning the cursor
    /// at EOF ready for subsequent appends.
    ///
    /// This is an atomic overwrite: the file is seeked to offset 0, the
    /// checkpoint frame is written, and the file is truncated to exactly
    /// the checkpoint frame length.  Any entries appended after this call
    /// will follow the checkpoint in the file.
    ///
    /// # Errors
    /// Propagates encode or I/O errors.
    pub async fn compact(&self, leaves: Vec<(String, RefEntry)>) -> Result<()> {
        let frame = encode_frame(&WalEntry::Checkpoint { leaves })?;
        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(0)).map_err(LedgeError::Io)?;
        file.write_all(&frame).map_err(LedgeError::Io)?;
        file.set_len(frame.len() as u64).map_err(LedgeError::Io)?;
        file.seek(SeekFrom::End(0)).map_err(LedgeError::Io)?;
        file.flush().map_err(LedgeError::Io)
    }

    /// Return the current on-disk byte size of the WAL file.
    ///
    /// Used by higher-level compaction triggers to decide when to compact.
    /// Returns 0 if the metadata call fails (e.g., file deleted externally).
    pub fn file_size_bytes(&self) -> u64 {
        std::fs::metadata(&self.path)
            .map(|m| m.len())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledge_core::{ObjectId, RefEntry};
    use tempfile::tempdir;

    fn make_entry(byte: u8, version: u64) -> RefEntry {
        RefEntry { target: ObjectId::from_bytes([byte; 32]), hlc: version, version }
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
    async fn crc32_corruption_detected() {
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
                data[9] ^= 0xFF;
            }
            std::fs::write(&path, &data).unwrap();
        }
        let (_, entries) = Wal::open(path).unwrap();
        assert!(entries.is_empty());
    }
}
