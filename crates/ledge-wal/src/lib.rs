//! Shared write-ahead-log primitives for the ledge stores.
//!
//! Five stores (ref-store, raft-log, lease, auth, webhook) independently grew
//! the *same* framed WAL — same on-disk format, same replay scan, same
//! checkpoint compaction — and, inevitably, the same latent bugs. Each was
//! fixed five times. This crate is the single, tested home for that logic so it
//! can neither drift nor regress again.
//!
//! # Frame format
//! ```text
//! | length: u32 LE | crc32: u32 LE | bincode payload (length bytes) |
//! ```
//! `length` is the payload byte count; `crc32` is the IEEE CRC of the payload
//! (via `crc32fast`); the payload is `bincode`-encoded with the standard config.
//!
//! # Durability contract (what the primitives guarantee)
//! - [`append_record`] writes one frame and `fsync`s it before returning, so an
//!   acked write survives power loss, not merely a process crash.
//! - [`open_replay`] scans frames and distinguishes a **torn tail** (an
//!   interrupted final write, always short) from **in-place corruption** (a
//!   fully-present frame with a bad CRC). A torn tail is truncated and the valid
//!   prefix recovered; corruption fails loud, because silently dropping a
//!   corrupt frame would also discard every valid frame after it.
//! - [`write_checkpoint`] replaces the file crash-atomically (temp + fsync +
//!   rename + directory fsync + reopen), so a crash mid-compaction leaves either
//!   the intact old file or the intact new checkpoint — never a torn frame 0
//!   (which `open_replay` would now reject, losing the whole file).
//! - [`open_replay`] and [`write_checkpoint`] fsync the parent directory so a
//!   newly created file and a rename are themselves durable.
//!
//! # What this crate does NOT own
//! Concurrency (each store keeps its own mutex / async model) and replay
//! *semantics* (this returns the raw record stream; the store folds it — e.g.
//! replaying only from the last checkpoint). That keeps the crate a pure,
//! lock-free set of primitives usable from both sync and async call sites.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::Serialize;

/// Byte size of the fixed frame header (length u32 LE + crc32 u32 LE).
pub const HEADER_LEN: usize = 8;

/// Errors surfaced by the WAL primitives. Callers map these into their own
/// error type (`LedgeError`, openraft `StorageError`, …) at the boundary.
#[derive(Debug)]
pub enum WalError {
    /// An OS-level I/O error (open, read, write, fsync, rename).
    Io(std::io::Error),
    /// The payload could not be serialized.
    Encode(String),
    /// A fully-present frame failed its CRC or would not decode — bit-rot, not a
    /// torn write. The message names the byte offset and how many records were
    /// recovered before it.
    Corruption(String),
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::Io(e) => write!(f, "WAL I/O error: {e}"),
            WalError::Encode(s) => write!(f, "WAL encode error: {s}"),
            WalError::Corruption(s) => write!(f, "WAL corruption: {s}"),
        }
    }
}

impl std::error::Error for WalError {}

impl From<std::io::Error> for WalError {
    fn from(e: std::io::Error) -> Self {
        WalError::Io(e)
    }
}

// ── Frame codec ────────────────────────────────────────────────────────────

/// Encode a serializable record into a complete on-disk frame.
pub fn encode_frame<T: Serialize>(rec: &T) -> Result<Vec<u8>, WalError> {
    let payload = bincode::serde::encode_to_vec(rec, bincode::config::standard())
        .map_err(|e| WalError::Encode(e.to_string()))?;
    let length = payload.len() as u32;
    let crc = crc32fast::hash(&payload);
    let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
    frame.extend_from_slice(&length.to_le_bytes());
    frame.extend_from_slice(&crc.to_le_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// Outcome of decoding one frame at a byte offset.
///
/// `Incomplete` (torn tail — safe to truncate) is deliberately distinct from
/// `Corrupt` (bit-rot — must fail loud). A torn write is always short, so a
/// frame that is fully present yet fails its CRC is in-place corruption.
pub enum FrameDecode<T> {
    /// A complete, CRC-valid frame plus the offset one past its end.
    Entry(T, usize),
    /// Not enough bytes for the frame the header claims — an interrupted write.
    Incomplete,
    /// A fully-present frame that failed its CRC or would not decode.
    Corrupt(String),
}

/// Decode one frame of type `T` from `data` starting at `pos`.
pub fn decode_frame<T: DeserializeOwned>(data: &[u8], pos: usize) -> FrameDecode<T> {
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
        return FrameDecode::Corrupt(format!("CRC mismatch at byte {pos}"));
    }
    match bincode::serde::decode_from_slice(payload, bincode::config::standard()) {
        Ok((rec, _)) => FrameDecode::Entry(rec, payload_end),
        Err(e) => FrameDecode::Corrupt(format!("payload decode error at byte {pos}: {e}")),
    }
}

/// Scan every frame in `data`, returning the decoded records and the byte offset
/// of the last valid frame boundary (so a torn tail can be truncated).
///
/// Fails loud on in-place corruption: silently dropping a corrupt frame would
/// also discard every valid frame after it.
pub fn replay<T: DeserializeOwned>(data: &[u8]) -> Result<(Vec<T>, usize), WalError> {
    let mut records: Vec<T> = Vec::new();
    let mut pos = 0usize;
    let mut last_valid = 0usize;
    while pos < data.len() {
        match decode_frame::<T>(data, pos) {
            FrameDecode::Entry(rec, new_pos) => {
                records.push(rec);
                last_valid = new_pos;
                pos = new_pos;
            }
            FrameDecode::Incomplete => break, // torn tail — caller truncates
            FrameDecode::Corrupt(why) => {
                return Err(WalError::Corruption(format!(
                    "{why}; {} record(s) recovered before it; refusing to truncate",
                    records.len()
                )));
            }
        }
    }
    Ok((records, last_valid))
}

// ── File-level primitives ──────────────────────────────────────────────────

/// Best-effort fsync of `path`'s parent directory, so a create or rename of the
/// file is itself durable (POSIX does not guarantee a directory entry persists
/// until the directory is synced). Some filesystems reject a directory fsync;
/// that is not data loss, so the error is ignored.
pub fn fsync_parent_dir(path: &Path) {
    if let Some(dir) = path.parent() {
        // An empty parent means the current directory.
        let dir = if dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir
        };
        if let Ok(dir_file) = File::open(dir) {
            let _ = dir_file.sync_all();
        }
    }
}

/// Open (creating if absent) the WAL at `path`, replay its frames, truncate any
/// torn tail, and return the file positioned at EOF plus the decoded records.
///
/// The parent directory is fsynced so a freshly created file is durable before
/// its first append. Fails loud (does not truncate) on in-place corruption.
pub fn open_replay<T: DeserializeOwned>(path: &Path) -> Result<(File, Vec<T>), WalError> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;

    // Persist the (possibly just-created) file's directory entry.
    fsync_parent_dir(path);

    let mut data = Vec::new();
    file.read_to_end(&mut data)?;

    let (records, last_valid) = replay::<T>(&data)?;

    // Truncate any partial tail frame so the next append starts cleanly.
    if last_valid < data.len() {
        file.set_len(last_valid as u64)?;
    }
    file.seek(SeekFrom::End(0))?;
    Ok((file, records))
}

/// Append one already-encoded frame to `file` and fsync it before returning, so
/// an acked write is durable. Prefer [`append_record`] unless you already hold
/// the encoded bytes.
pub fn append_frame(file: &mut File, frame: &[u8]) -> Result<(), WalError> {
    file.write_all(frame)?;
    file.sync_data()?;
    Ok(())
}

/// Encode `rec` and durably append it (write + fsync).
pub fn append_record<T: Serialize>(file: &mut File, rec: &T) -> Result<(), WalError> {
    let frame = encode_frame(rec)?;
    append_frame(file, &frame)
}

/// Crash-atomically replace the entire contents of `path` with `frame`.
///
/// Writes to a sibling temp file, fsyncs it, atomically renames it over `path`,
/// fsyncs the parent directory, then opens and returns a fresh handle at EOF
/// (the caller's previous handle refers to the unlinked pre-rename inode and
/// must be replaced with the returned one). A crash at any point leaves either
/// the intact old file or the intact new one — never a torn frame 0.
pub fn crash_atomic_replace(path: &Path, frame: &[u8]) -> Result<File, WalError> {
    // Sibling temp: append ".compact" to the full filename so a real extension
    // is never clobbered, and it shares the directory for a same-fs rename.
    let tmp_path = {
        let mut p = path.as_os_str().to_owned();
        p.push(".compact");
        std::path::PathBuf::from(p)
    };
    {
        let mut tmp = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        tmp.write_all(frame)?;
        tmp.sync_all()?; // durable before the rename publishes it
    }
    std::fs::rename(&tmp_path, path)?;
    fsync_parent_dir(path);

    let mut new_file = OpenOptions::new()
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)?;
    new_file.seek(SeekFrom::End(0))?;
    Ok(new_file)
}

/// Encode `checkpoint` and crash-atomically replace `path` with it, returning
/// the fresh handle at EOF.
pub fn write_checkpoint<T: Serialize>(path: &Path, checkpoint: &T) -> Result<File, WalError> {
    let frame = encode_frame(checkpoint)?;
    crash_atomic_replace(path, &frame)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use tempfile::tempdir;

    #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
    enum Rec {
        Put(u64, String),
        Checkpoint(Vec<u64>),
    }

    #[test]
    fn frame_roundtrip() {
        let r = Rec::Put(7, "refs/heads/main".into());
        let frame = encode_frame(&r).unwrap();
        match decode_frame::<Rec>(&frame, 0) {
            FrameDecode::Entry(got, end) => {
                assert_eq!(got, r);
                assert_eq!(end, frame.len());
            }
            _ => panic!("expected Entry"),
        }
    }

    #[test]
    fn replay_recovers_all_records() {
        let mut data = Vec::new();
        for i in 0..5 {
            data.extend_from_slice(&encode_frame(&Rec::Put(i, format!("r{i}"))).unwrap());
        }
        let (recs, last_valid) = replay::<Rec>(&data).unwrap();
        assert_eq!(recs.len(), 5);
        assert_eq!(last_valid, data.len());
    }

    #[test]
    fn torn_tail_is_incomplete_not_corrupt() {
        let mut data = encode_frame(&Rec::Put(1, "a".into())).unwrap();
        data.extend_from_slice(&encode_frame(&Rec::Put(2, "b".into())).unwrap());
        let full = data.len();
        data.truncate(full - 3); // shear the last frame short
        let (recs, last_valid) = replay::<Rec>(&data).unwrap();
        assert_eq!(recs.len(), 1, "only the first, intact frame recovers");
        assert!(last_valid < data.len() || last_valid > 0);
    }

    #[test]
    fn mid_stream_corruption_fails_loud() {
        let mut data = Vec::new();
        for i in 0..4 {
            data.extend_from_slice(&encode_frame(&Rec::Put(i, format!("r{i}"))).unwrap());
        }
        data[9] ^= 0xFF; // flip a payload byte in the first frame
        let err = replay::<Rec>(&data).expect_err("must fail loud, not truncate");
        assert!(matches!(err, WalError::Corruption(_)), "got {err:?}");
    }

    #[test]
    fn open_replay_then_append_then_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let (mut file, recs) = open_replay::<Rec>(&path).unwrap();
            assert!(recs.is_empty());
            append_record(&mut file, &Rec::Put(1, "a".into())).unwrap();
            append_record(&mut file, &Rec::Put(2, "b".into())).unwrap();
        }
        let (_file, recs) = open_replay::<Rec>(&path).unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0], Rec::Put(1, "a".into()));
    }

    #[test]
    fn checkpoint_is_crash_atomic_and_leaves_no_temp() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let (mut file, _) = open_replay::<Rec>(&path).unwrap();
        append_record(&mut file, &Rec::Put(1, "a".into())).unwrap();

        // Replace with a checkpoint; the returned handle must accept appends.
        let mut file = write_checkpoint(&path, &Rec::Checkpoint(vec![1, 2, 3])).unwrap();
        let mut tmp = path.clone().into_os_string();
        tmp.push(".compact");
        assert!(
            !Path::new(&tmp).exists(),
            "temp must be renamed away, not left behind"
        );
        append_record(&mut file, &Rec::Put(9, "post".into())).unwrap();
        drop(file);

        let (_f, recs) = open_replay::<Rec>(&path).unwrap();
        assert_eq!(recs.len(), 2);
        assert!(matches!(recs[0], Rec::Checkpoint(_)));
        assert_eq!(recs[1], Rec::Put(9, "post".into()));
    }

    #[test]
    fn stale_temp_is_ignored_by_open_replay() {
        // A crash mid-compaction can leave a partial ".compact" temp while the
        // live WAL is intact. open_replay must read the live WAL and ignore it.
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let (mut file, _) = open_replay::<Rec>(&path).unwrap();
            append_record(&mut file, &Rec::Put(1, "a".into())).unwrap();
        }
        let mut tmp = path.clone().into_os_string();
        tmp.push(".compact");
        std::fs::write(&tmp, b"\xde\xad\xbe\xef partial").unwrap();

        let (_f, recs) = open_replay::<Rec>(&path).unwrap();
        assert_eq!(recs.len(), 1, "live WAL survives; temp ignored");
    }
}
