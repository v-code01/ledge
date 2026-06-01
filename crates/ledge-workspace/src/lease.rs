//! Durable lease store for ephemeral workspaces.
//!
//! Mirrors the Phase 1 ref-store WAL (`ledge-ref-store/src/wal.rs`) exactly:
//! CRC32 + bincode framed entries, checkpoint compaction, truncated-tail
//! recovery. Differs only in (a) the entry enum (`LeaseWalEntry`) and (b) an
//! in-memory `HashMap<WorkspaceId, Lease>` of live state rebuilt on `open`.
//!
//! # Frame format (identical to the ref WAL)
//! ```text
//! | length: u32 LE | crc32: u32 LE | bincode(LeaseWalEntry) |
//! ```

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, RwLock, Weak};

use ledge_core::{LedgeError, Result, HLC};
use tokio::sync::Mutex;

use crate::id::WorkspaceId;

/// Byte size of the fixed frame header (length u32 + crc32 u32).
const HEADER_LEN: usize = 8;

/// A durable lease over a workspace's ref namespace.
///
/// `source_refs` are stored as plain `String`s (not `RefName`) because the
/// WAL is bincode-serialised and `String` is the simplest stable encoding.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Lease {
    pub id: WorkspaceId,
    pub source_refs: Vec<String>,
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
    pub hlc: u64,
    pub generation: u64,
}

/// One WAL record: a lease upsert, a tombstone, or a compaction checkpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum LeaseWalEntry {
    /// Create or update a lease (last-writer-wins by replay order).
    Put(Lease),
    /// Mark a lease dead; `hlc` records when. Removes it from the live index.
    Tombstone { id: WorkspaceId, hlc: u64 },
    /// Full live snapshot written by `compact()`. On replay, clears the index
    /// then inserts every lease.
    Checkpoint { leases: Vec<Lease> },
}

/// Encode a `LeaseWalEntry` into a complete on-disk frame.
fn encode_frame(entry: &LeaseWalEntry) -> Result<Vec<u8>> {
    let payload = bincode::serde::encode_to_vec(entry, bincode::config::standard())
        .map_err(|e| LedgeError::Corruption(format!("lease WAL encode: {e}")))?;
    let length = payload.len() as u32;
    let crc = crc32fast::hash(&payload);
    let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
    frame.extend_from_slice(&length.to_le_bytes());
    frame.extend_from_slice(&crc.to_le_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// Attempt to decode one frame from `data` at `pos`. `None` on any
/// truncation / CRC mismatch / decode error (caller truncates the file).
fn decode_frame(data: &[u8], pos: usize) -> Option<(LeaseWalEntry, usize)> {
    if pos + HEADER_LEN > data.len() {
        return None;
    }
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
    let (entry, _): (LeaseWalEntry, _) =
        bincode::serde::decode_from_slice(payload, bincode::config::standard()).ok()?;
    Some((entry, payload_end))
}

/// Durable, WAL-backed store of workspace leases with an in-memory live index.
pub struct LeaseStore {
    /// Mutex-protected WAL file, always positioned at EOF for appends.
    file: Mutex<std::fs::File>,
    /// Path to the WAL file (`<data_dir>/leases/wal`).
    path: PathBuf,
    /// Live in-memory state: tombstoned ids are absent.
    index: RwLock<HashMap<WorkspaceId, Lease>>,
    /// Shared clock used to HLC-stamp tombstones.
    hlc: Arc<HLC>,
}

impl LeaseStore {
    /// Open (or create) `<data_dir>/leases/wal`, replay it, and rebuild the
    /// live index. A torn tail frame is truncated, exactly like the ref WAL.
    pub fn open(data_dir: PathBuf, hlc: Arc<HLC>) -> Result<Self> {
        let dir = data_dir.join("leases");
        std::fs::create_dir_all(&dir).map_err(LedgeError::Io)?;
        let path = dir.join("wal");

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(LedgeError::Io)?;

        let mut data = Vec::new();
        file.read_to_end(&mut data).map_err(LedgeError::Io)?;

        // Decode every valid frame, tracking the last valid boundary.
        let mut all: Vec<LeaseWalEntry> = Vec::new();
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
        if last_valid < data.len() {
            file.set_len(last_valid as u64).map_err(LedgeError::Io)?;
        }
        file.seek(SeekFrom::End(0)).map_err(LedgeError::Io)?;

        // Rebuild the index by applying replay entries in order. Unlike the ref
        // WAL we replay the *whole* file: Checkpoint clears the index, so any
        // pre-checkpoint Put/Tombstone is correctly superseded. (Replaying from
        // the last checkpoint only would also be correct; replaying all is
        // simplest and equally cheap for the small lease set.)
        let mut index: HashMap<WorkspaceId, Lease> = HashMap::new();
        for entry in &all {
            match entry {
                LeaseWalEntry::Put(l) => {
                    index.insert(l.id, l.clone());
                }
                LeaseWalEntry::Tombstone { id, .. } => {
                    index.remove(id);
                }
                LeaseWalEntry::Checkpoint { leases } => {
                    index.clear();
                    for l in leases {
                        index.insert(l.id, l.clone());
                    }
                }
            }
        }

        Ok(LeaseStore {
            file: Mutex::new(file),
            path,
            index: RwLock::new(index),
            hlc,
        })
    }

    /// Append a `Put` frame and upsert the live index. Create-or-update.
    pub async fn put(&self, lease: Lease) -> Result<()> {
        let frame = encode_frame(&LeaseWalEntry::Put(lease.clone()))?;
        {
            let mut file = self.file.lock().await;
            file.write_all(&frame).map_err(LedgeError::Io)?;
        }
        self.index.write().unwrap().insert(lease.id, lease);
        Ok(())
    }

    /// Read a live lease by id (tombstoned ids return `None`).
    pub async fn get(&self, id: WorkspaceId) -> Result<Option<Lease>> {
        Ok(self.index.read().unwrap().get(&id).cloned())
    }

    /// Append a `Tombstone` frame and remove the lease from the live index.
    /// Idempotent: tombstoning an absent id is a no-op write + no-op remove.
    pub async fn tombstone(&self, id: WorkspaceId) -> Result<()> {
        let hlc = self.hlc.tick();
        let frame = encode_frame(&LeaseWalEntry::Tombstone { id, hlc })?;
        {
            let mut file = self.file.lock().await;
            file.write_all(&frame).map_err(LedgeError::Io)?;
        }
        self.index.write().unwrap().remove(&id);
        Ok(())
    }

    /// Live leases (`expires_at_ms > now_ms`), unsorted.
    pub async fn live(&self, now_ms: u64) -> Result<Vec<Lease>> {
        let idx = self.index.read().unwrap();
        Ok(idx
            .values()
            .filter(|l| l.expires_at_ms > now_ms)
            .cloned()
            .collect())
    }

    /// Expired leases (`expires_at_ms <= now_ms`) still present in the index
    /// (i.e. not yet tombstoned), unsorted.
    pub async fn expired(&self, now_ms: u64) -> Result<Vec<Lease>> {
        let idx = self.index.read().unwrap();
        Ok(idx
            .values()
            .filter(|l| l.expires_at_ms <= now_ms)
            .cloned()
            .collect())
    }

    /// Compact the WAL: overwrite it with a single `Checkpoint` frame holding
    /// every live lease, then truncate to exactly that frame and seek to EOF
    /// for subsequent appends. Mirrors `Wal::compact`. The live snapshot is
    /// taken from the in-memory index (tombstoned ids are already absent).
    pub async fn compact(&self) -> Result<()> {
        let leases: Vec<Lease> = {
            let idx = self.index.read().unwrap();
            idx.values().cloned().collect()
        };
        let frame = encode_frame(&LeaseWalEntry::Checkpoint { leases })?;
        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(0)).map_err(LedgeError::Io)?;
        file.write_all(&frame).map_err(LedgeError::Io)?;
        file.set_len(frame.len() as u64).map_err(LedgeError::Io)?;
        file.seek(SeekFrom::End(0)).map_err(LedgeError::Io)?;
        file.flush().map_err(LedgeError::Io)
    }

    /// Path to the backing WAL file. Useful for diagnostics and future
    /// size-based compaction triggers (mirrors `Wal::path`).
    pub fn wal_path(&self) -> &std::path::Path {
        &self.path
    }

    /// Spawn a background tokio task that checks the WAL size every 60 seconds
    /// and calls [`compact`](Self::compact) whenever it exceeds `threshold_bytes`.
    ///
    /// Mirrors the ref store's `RefStoreImpl::spawn_compaction_task`: holds a
    /// [`Weak`] reference so the task exits automatically once the store is
    /// dropped — no explicit cancellation handle required. In production the
    /// server holds the `Arc` for its whole lifetime, so the task runs forever.
    ///
    /// The interval uses [`MissedTickBehavior::Skip`](tokio::time::MissedTickBehavior::Skip)
    /// so a long compaction (or a stalled runtime) never piles up backlogged
    /// ticks; the next tick simply fires on schedule.
    ///
    /// Complexity per tick: one `stat(2)` (O(1)); compaction is O(live leases)
    /// and runs only above the threshold.
    pub fn spawn_compaction_task(self: &Arc<Self>, threshold_bytes: u64) {
        let weak: Weak<Self> = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                // Drop the strong ref before the next await so the store can be
                // freed while this task sleeps; re-upgrade each tick.
                let Some(store) = weak.upgrade() else {
                    // Store has been dropped; exit the task.
                    break;
                };
                // A failed stat (file vanished) is treated as size 0: nothing to
                // compact, never a panic in the unsupervised task.
                let size = std::fs::metadata(store.wal_path())
                    .map(|m| m.len())
                    .unwrap_or(0);
                if size > threshold_bytes {
                    match store.compact().await {
                        Ok(()) => {
                            let post = std::fs::metadata(store.wal_path())
                                .map(|m| m.len())
                                .unwrap_or(0);
                            tracing::info!(
                                pre_bytes = size,
                                post_bytes = post,
                                "lease WAL compacted"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "lease WAL compaction failed");
                        }
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn hlc() -> Arc<HLC> {
        Arc::new(HLC::new())
    }

    /// Wall-clock helper — TEST ONLY. Production code never reads SystemTime:
    /// `expires_at_ms` is supplied by the caller and `live`/`expired` take
    /// `now_ms` as a parameter, so `LeaseStore` is fully deterministic.
    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    fn lease(id: WorkspaceId, expires: u64) -> Lease {
        Lease {
            id,
            source_refs: vec!["refs/heads/main".to_string()],
            created_at_ms: 1000,
            expires_at_ms: expires,
            hlc: 1,
            generation: 0,
        }
    }

    #[tokio::test]
    async fn open_empty_has_no_leases() {
        let dir = tempdir().unwrap();
        let store = LeaseStore::open(dir.path().to_path_buf(), hlc()).unwrap();
        assert!(store.live(now_ms()).await.unwrap().is_empty());
        assert!(store.expired(now_ms()).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn put_then_get() {
        let dir = tempdir().unwrap();
        let h = hlc();
        let store = LeaseStore::open(dir.path().to_path_buf(), h.clone()).unwrap();
        let id = WorkspaceId::generate(&h);
        store.put(lease(id, now_ms() + 10_000)).await.unwrap();
        let got = store.get(id).await.unwrap().expect("present");
        assert_eq!(got.id, id);
        assert_eq!(got.generation, 0);
        assert!(store
            .get(WorkspaceId::generate(&h))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn put_updates_existing_same_id() {
        let dir = tempdir().unwrap();
        let h = hlc();
        let store = LeaseStore::open(dir.path().to_path_buf(), h.clone()).unwrap();
        let id = WorkspaceId::generate(&h);
        store.put(lease(id, now_ms() + 1000)).await.unwrap();
        let mut updated = lease(id, now_ms() + 99_000);
        updated.generation = 7;
        store.put(updated).await.unwrap();
        let got = store.get(id).await.unwrap().unwrap();
        assert_eq!(got.generation, 7);
        // exactly one live lease for this id
        assert_eq!(store.live(now_ms()).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn tombstone_removes_from_index() {
        let dir = tempdir().unwrap();
        let h = hlc();
        let store = LeaseStore::open(dir.path().to_path_buf(), h.clone()).unwrap();
        let id = WorkspaceId::generate(&h);
        store.put(lease(id, now_ms() + 10_000)).await.unwrap();
        store.tombstone(id).await.unwrap();
        assert!(store.get(id).await.unwrap().is_none());
        assert!(store.live(now_ms()).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn tombstone_is_idempotent() {
        let dir = tempdir().unwrap();
        let h = hlc();
        let store = LeaseStore::open(dir.path().to_path_buf(), h.clone()).unwrap();
        let id = WorkspaceId::generate(&h);
        store.tombstone(id).await.unwrap(); // never existed
        store.tombstone(id).await.unwrap(); // twice
        assert!(store.get(id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn live_and_expired_partition_by_now() {
        let dir = tempdir().unwrap();
        let h = hlc();
        let store = LeaseStore::open(dir.path().to_path_buf(), h.clone()).unwrap();
        let now = 1_000_000u64;
        let a = WorkspaceId::generate(&h);
        let b = WorkspaceId::generate(&h);
        let c = WorkspaceId::generate(&h);
        store.put(lease(a, now + 1)).await.unwrap(); // live
        store.put(lease(b, now)).await.unwrap(); // expired (<=)
        store.put(lease(c, now - 1)).await.unwrap(); // expired
        let live: std::collections::HashSet<_> = store
            .live(now)
            .await
            .unwrap()
            .into_iter()
            .map(|l| l.id)
            .collect();
        let exp: std::collections::HashSet<_> = store
            .expired(now)
            .await
            .unwrap()
            .into_iter()
            .map(|l| l.id)
            .collect();
        assert_eq!(live, std::collections::HashSet::from([a]));
        assert_eq!(exp, std::collections::HashSet::from([b, c]));
        assert!(live.is_disjoint(&exp));
    }

    #[tokio::test]
    async fn reopen_replays_all_puts() {
        let dir = tempdir().unwrap();
        let h = hlc();
        let ids: Vec<_> = {
            let store = LeaseStore::open(dir.path().to_path_buf(), h.clone()).unwrap();
            let mut ids = Vec::new();
            for _ in 0..3 {
                let id = WorkspaceId::generate(&h);
                store.put(lease(id, now_ms() + 10_000)).await.unwrap();
                ids.push(id);
            }
            ids
        }; // store dropped → file closed
        let store = LeaseStore::open(dir.path().to_path_buf(), h.clone()).unwrap();
        for id in ids {
            assert!(
                store.get(id).await.unwrap().is_some(),
                "lease {id} lost on reopen"
            );
        }
        assert_eq!(store.live(now_ms()).await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn tombstone_survives_reopen() {
        let dir = tempdir().unwrap();
        let h = hlc();
        let (live_id, dead_id) = {
            let store = LeaseStore::open(dir.path().to_path_buf(), h.clone()).unwrap();
            let live_id = WorkspaceId::generate(&h);
            let dead_id = WorkspaceId::generate(&h);
            store.put(lease(live_id, now_ms() + 10_000)).await.unwrap();
            store.put(lease(dead_id, now_ms() + 10_000)).await.unwrap();
            store.tombstone(dead_id).await.unwrap();
            (live_id, dead_id)
        };
        let store = LeaseStore::open(dir.path().to_path_buf(), h.clone()).unwrap();
        assert!(store.get(live_id).await.unwrap().is_some());
        assert!(
            store.get(dead_id).await.unwrap().is_none(),
            "tombstone did not survive reopen"
        );
    }

    #[tokio::test]
    async fn truncated_tail_recovery() {
        let dir = tempdir().unwrap();
        let h = hlc();
        let path = dir.path().join("leases").join("wal");
        let ids: Vec<_> = {
            let store = LeaseStore::open(dir.path().to_path_buf(), h.clone()).unwrap();
            let mut ids = Vec::new();
            for _ in 0..5 {
                let id = WorkspaceId::generate(&h);
                store.put(lease(id, now_ms() + 10_000)).await.unwrap();
                ids.push(id);
            }
            ids
        };
        // Corrupt the tail: drop the last 3 bytes (partial final frame).
        {
            let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            let len = f.metadata().unwrap().len();
            f.set_len(len - 3).unwrap();
        }
        let store = LeaseStore::open(dir.path().to_path_buf(), h.clone()).unwrap();
        // At least the first 4 of 5 survive (the 5th frame's tail was cut).
        let mut surviving = 0;
        for id in &ids {
            if store.get(*id).await.unwrap().is_some() {
                surviving += 1;
            }
        }
        assert!(surviving >= 4, "expected >= 4 survivors, got {surviving}");
    }

    #[tokio::test]
    async fn compact_then_reopen_yields_same_live_set() {
        let dir = tempdir().unwrap();
        let h = hlc();
        let now = now_ms();
        let live_set: std::collections::HashSet<_> = {
            let store = LeaseStore::open(dir.path().to_path_buf(), h.clone()).unwrap();
            // 3 live, 1 tombstoned (must not reappear after checkpoint).
            let mut live = std::collections::HashSet::new();
            for _ in 0..3 {
                let id = WorkspaceId::generate(&h);
                store.put(lease(id, now + 60_000)).await.unwrap();
                live.insert(id);
            }
            let dead = WorkspaceId::generate(&h);
            store.put(lease(dead, now + 60_000)).await.unwrap();
            store.tombstone(dead).await.unwrap();
            store.compact().await.unwrap();
            // post-compact append still works and survives.
            let extra = WorkspaceId::generate(&h);
            store.put(lease(extra, now + 60_000)).await.unwrap();
            live.insert(extra);
            live
        };
        let store = LeaseStore::open(dir.path().to_path_buf(), h.clone()).unwrap();
        let reopened: std::collections::HashSet<_> = store
            .live(now)
            .await
            .unwrap()
            .into_iter()
            .map(|l| l.id)
            .collect();
        assert_eq!(reopened, live_set);
    }

    /// `compact()` must shrink an append-grown WAL while preserving every live
    /// lease across a reopen. We grow the WAL with many puts (each appends a
    /// frame), capture its size, compact to a single checkpoint, and assert the
    /// on-disk file is strictly smaller AND all live leases survive.
    #[tokio::test]
    async fn compaction_truncates_wal() {
        let dir = tempdir().unwrap();
        let h = hlc();
        let now = now_ms();
        let path = dir.path().join("leases").join("wal");

        let mut live_ids = std::collections::HashSet::new();
        let pre_size;
        {
            let store = LeaseStore::open(dir.path().to_path_buf(), h.clone()).unwrap();
            // Grow the WAL: 200 distinct puts ⇒ 200 appended frames.
            for _ in 0..200 {
                let id = WorkspaceId::generate(&h);
                store.put(lease(id, now + 60_000)).await.unwrap();
                live_ids.insert(id);
            }
            pre_size = std::fs::metadata(&path).unwrap().len();
            // Compact directly (the spawned task is just a timer wrapper around this).
            store.compact().await.unwrap();
        } // drop ⇒ flush + close before reopen.

        let post_size = std::fs::metadata(&path).unwrap().len();
        assert!(
            post_size < pre_size,
            "compaction must shrink the WAL: post {post_size} !< pre {pre_size}"
        );

        // Every live lease survives the checkpoint round-trip.
        let store = LeaseStore::open(dir.path().to_path_buf(), h.clone()).unwrap();
        let reopened: std::collections::HashSet<_> = store
            .live(now)
            .await
            .unwrap()
            .into_iter()
            .map(|l| l.id)
            .collect();
        assert_eq!(reopened, live_ids, "all live leases must survive compaction");
    }
}
