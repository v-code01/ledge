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
use std::path::PathBuf;
use std::sync::{Arc, RwLock, Weak};

use ledge_core::{LedgeError, Result, HLC};
use tokio::sync::Mutex;

use crate::id::WorkspaceId;

/// Map a shared-WAL error into this crate's error, tagging corruption with the
/// store name and the fail-loud recovery hint.
fn map_wal(e: ledge_wal::WalError) -> LedgeError {
    match e {
        ledge_wal::WalError::Io(io) => LedgeError::Io(io),
        ledge_wal::WalError::Encode(s) => LedgeError::Corruption(format!("lease WAL: {s}")),
        ledge_wal::WalError::Corruption(s) => LedgeError::Corruption(format!(
            "lease WAL: {s}. Refusing to truncate — restore from a backup."
        )),
    }
}

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
    /// Owning tenant (Phase 4d-2). `#[serde(default)]` keeps the bincode WAL
    /// frame-compatible: a pre-4d-2 frame decodes with `tenant_id == ""`, which
    /// the manager normalizes to `root`/global. New leases always store a
    /// normalized tenant ("root", "acme", …), never "".
    #[serde(default)]
    pub tenant_id: String,
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

        // Shared primitive: opens (dir-fsync on create), replays frames, recovers
        // a torn tail, and fails loud on in-place corruption.
        let (file, all) = ledge_wal::open_replay::<LeaseWalEntry>(&path).map_err(map_wal)?;

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
    ///
    /// The WAL append and the index update happen under a single hold of the file
    /// lock, so the on-disk frame and the in-memory entry move together with
    /// respect to `compact` (which also snapshots the index under that lock).
    /// Without this, a concurrent compaction could snapshot the index after the
    /// frame was appended but before it was indexed, then erase the frame in the
    /// whole-file replacement — losing an acked lease on the next reopen.
    pub async fn put(&self, lease: Lease) -> Result<()> {
        let mut file = self.file.lock().await;
        // Durable (write + fsync) before returning so an acked lease survives
        // power loss, not just a process crash.
        ledge_wal::append_record(&mut file, &LeaseWalEntry::Put(lease.clone())).map_err(map_wal)?;
        self.index.write().unwrap().insert(lease.id, lease);
        Ok(())
    }

    /// Read a live lease by id (tombstoned ids return `None`).
    pub async fn get(&self, id: WorkspaceId) -> Result<Option<Lease>> {
        Ok(self.index.read().unwrap().get(&id).cloned())
    }

    /// Append a `Tombstone` frame and remove the lease from the live index.
    /// Idempotent: tombstoning an absent id is a no-op write + no-op remove.
    ///
    /// Single-node path: stamps the tombstone with a freshly ticked hlc. The
    /// replicated Raft apply path must use [`tombstone_with_hlc`](Self::tombstone_with_hlc)
    /// instead, so every replica records the identical tombstone frame.
    pub async fn tombstone(&self, id: WorkspaceId) -> Result<()> {
        let hlc = self.hlc.tick();
        self.tombstone_with_hlc(id, hlc).await
    }

    /// Tombstone with a caller-supplied hlc (deterministic — used by the Raft
    /// apply path so all replicas record the identical tombstone frame). Mirrors
    /// [`tombstone`](Self::tombstone) but does NOT call `self.hlc.tick()`.
    ///
    /// Idempotent: tombstoning an absent id is a no-op write + no-op remove.
    pub async fn tombstone_with_hlc(&self, id: WorkspaceId, hlc: u64) -> Result<()> {
        // Append + index removal under one file-lock hold, consistent with
        // `put`/`compact` (see `put` for the compaction-race rationale).
        let mut file = self.file.lock().await;
        // Durable before returning: a tombstone lost on power loss would
        // resurrect a released lease on restart.
        ledge_wal::append_record(&mut file, &LeaseWalEntry::Tombstone { id, hlc })
            .map_err(map_wal)?;
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

    /// Live leases (`expires_at_ms > now_ms`) owned by `tenant`, unsorted.
    ///
    /// Tenant comparison normalizes the empty string to `root` (a legacy lease
    /// decoded without `tenant_id` is global/root — see [`Lease`]). Used by
    /// `WorkspaceManager::list` so a tenant lists ONLY its own workspaces
    /// (Phase 4d-2 spec §3.2 / §6). The unscoped [`live`](Self::live) is retained
    /// for GC, which roots every live workspace regardless of tenant.
    pub async fn live_for_tenant(&self, now_ms: u64, tenant: &str) -> Result<Vec<Lease>> {
        let want = if tenant.is_empty() { "root" } else { tenant };
        let idx = self.index.read().unwrap();
        Ok(idx
            .values()
            .filter(|l| l.expires_at_ms > now_ms)
            .filter(|l| {
                let have = if l.tenant_id.is_empty() {
                    "root"
                } else {
                    l.tenant_id.as_str()
                };
                have == want
            })
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

    /// Compact the WAL: replace it with a single `Checkpoint` frame holding every
    /// live lease. The live snapshot is taken from the in-memory index
    /// (tombstoned ids are already absent).
    ///
    /// Crash-atomic via the shared `ledge_wal::write_checkpoint` (temp + fsync +
    /// rename + dir fsync + reopen): a crash mid-rewrite leaves either the intact
    /// old log or the new checkpoint, never a torn frame 0 (which open() now
    /// rejects — losing every lease).
    ///
    /// The index snapshot is taken UNDER the file lock, and `put`/`tombstone`
    /// update the index under that same lock, so the checkpoint payload always
    /// matches the frames it replaces — a concurrent write is either fully
    /// captured in the snapshot or lands as a fresh frame after the new
    /// checkpoint, never erased. This closes the compaction/append race.
    pub async fn compact(&self) -> Result<()> {
        let mut file = self.file.lock().await;
        let leases: Vec<Lease> = self.index.read().unwrap().values().cloned().collect();
        *file = ledge_wal::write_checkpoint(&self.path, &LeaseWalEntry::Checkpoint { leases })
            .map_err(map_wal)?;
        Ok(())
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
            tenant_id: "root".to_string(),
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
    async fn mid_stream_corruption_fails_loud_not_silent() {
        // Bit-rot in an early frame must fail open, not silently drop the leases
        // after it (which would let GC reclaim live workspaces).
        let h = hlc();
        let dir = tempdir().unwrap();
        let path = dir.path().join("leases").join("wal");
        {
            let store = LeaseStore::open(dir.path().to_path_buf(), h.clone()).unwrap();
            for _ in 0..5 {
                store
                    .put(lease(WorkspaceId::generate(&h), now_ms() + 10_000))
                    .await
                    .unwrap();
            }
        }
        {
            // Byte 9 is inside the first frame's payload (header is bytes 0..8).
            let mut data = std::fs::read(&path).unwrap();
            data[9] ^= 0xFF;
            std::fs::write(&path, &data).unwrap();
        }
        assert!(
            LeaseStore::open(dir.path().to_path_buf(), h).is_err(),
            "mid-stream corruption must fail open, not silently drop leases"
        );
    }

    /// Compaction/append race guard: many concurrent `put`s against repeated
    /// `compact`s, then reopen from the WAL alone — every acked lease must
    /// survive. The fix (append + index update + checkpoint snapshot all under
    /// one file-lock hold) makes a put either fully captured by a compaction's
    /// snapshot or written as a fresh frame after it, never erased.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_puts_survive_compaction() {
        const WRITERS: usize = 8;
        const PER_WRITER: usize = 40;
        const COMPACTORS: usize = 3;
        let h = hlc();
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let store = Arc::new(LeaseStore::open(path.clone(), h.clone()).unwrap());

        let mut writers = Vec::new();
        for _ in 0..WRITERS {
            let s = Arc::clone(&store);
            let hh = h.clone();
            writers.push(tokio::spawn(async move {
                let mut ids = Vec::new();
                for _ in 0..PER_WRITER {
                    let id = WorkspaceId::generate(&hh);
                    s.put(lease(id, now_ms() + 10_000)).await.unwrap();
                    ids.push(id);
                    tokio::task::yield_now().await;
                }
                ids
            }));
        }
        let mut compactors = Vec::new();
        for _ in 0..COMPACTORS {
            let s = Arc::clone(&store);
            compactors.push(tokio::spawn(async move {
                for _ in 0..(PER_WRITER * 2) {
                    s.compact().await.unwrap();
                    tokio::task::yield_now().await;
                }
            }));
        }
        let mut all_ids = Vec::new();
        for w in writers {
            all_ids.extend(w.await.unwrap());
        }
        for c in compactors {
            c.await.unwrap();
        }

        // Reopen from the WAL only — no in-memory state carries over.
        drop(store);
        let store = LeaseStore::open(path, h).unwrap();
        for id in all_ids {
            assert!(
                store.get(id).await.unwrap().is_some(),
                "lease {id:?} lost across compaction + reopen"
            );
        }
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
        assert_eq!(
            reopened, live_ids,
            "all live leases must survive compaction"
        );
    }

    /// A lease serialized WITHOUT a `tenant_id` field must decode with
    /// `tenant_id == ""` via `#[serde(default)]`, proving the field is optional
    /// on the wire and a legacy/global lease is treated as `root`.
    ///
    /// We assert this with a `serde_json` round-trip of a `tenant_id`-less object,
    /// which unambiguously honors `#[serde(default)]` for the missing trailing
    /// field. (bincode's positional `standard()` config requires every field to
    /// be physically present, so the bincode WAL itself never omits the trailing
    /// field — but the WAL is greenfield, so no legacy frames exist on disk. The
    /// `#[serde(default)]` attribute is the load-bearing back-compat guarantee.)
    #[tokio::test]
    async fn legacy_lease_without_tenant_decodes_as_root_default() {
        let h = hlc();
        let id = WorkspaceId::generate(&h);
        // A legacy JSON object: every field EXCEPT tenant_id.
        let json = serde_json::json!({
            "id": id,
            "source_refs": ["refs/heads/main"],
            "created_at_ms": 1,
            "expires_at_ms": 2,
            "hlc": 3,
            "generation": 4,
        })
        .to_string();
        let decoded: Lease = serde_json::from_str(&json).unwrap();
        assert_eq!(
            decoded.tenant_id, "",
            "missing tenant_id defaults to empty (root)"
        );
        assert_eq!(decoded.generation, 4, "every other field round-trips");
        assert_eq!(decoded.id, id);
    }

    /// `live_for_tenant` partitions live leases by owner; the empty string and
    /// "root" are the SAME tenant (back-compat).
    #[tokio::test]
    async fn live_for_tenant_filters_by_owner() {
        let dir = tempdir().unwrap();
        let h = hlc();
        let store = LeaseStore::open(dir.path().to_path_buf(), h.clone()).unwrap();
        let now = 1_000_000u64;

        let mk = |id, tenant: &str| Lease {
            id,
            source_refs: vec![],
            created_at_ms: 0,
            expires_at_ms: now + 60_000,
            hlc: 1,
            generation: 1,
            tenant_id: tenant.to_string(),
        };
        let acme = WorkspaceId::generate(&h);
        let globex = WorkspaceId::generate(&h);
        let legacy = WorkspaceId::generate(&h); // tenant_id == "" ⇒ root
        store.put(mk(acme, "acme")).await.unwrap();
        store.put(mk(globex, "globex")).await.unwrap();
        store.put(mk(legacy, "")).await.unwrap();

        let acme_ids: std::collections::HashSet<_> = store
            .live_for_tenant(now, "acme")
            .await
            .unwrap()
            .into_iter()
            .map(|l| l.id)
            .collect();
        assert_eq!(acme_ids, std::collections::HashSet::from([acme]));

        // "" and "root" name the same tenant ⇒ the legacy lease is rooted.
        let root_ids: std::collections::HashSet<_> = store
            .live_for_tenant(now, "root")
            .await
            .unwrap()
            .into_iter()
            .map(|l| l.id)
            .collect();
        assert_eq!(root_ids, std::collections::HashSet::from([legacy]));
        // The unscoped live() still returns ALL three (GC roots every tenant).
        assert_eq!(store.live(now).await.unwrap().len(), 3);
    }

    /// A lease with a real tenant survives a WAL reopen with its tenant intact.
    #[tokio::test]
    async fn tenant_id_survives_reopen() {
        let dir = tempdir().unwrap();
        let h = hlc();
        let id = WorkspaceId::generate(&h);
        {
            let store = LeaseStore::open(dir.path().to_path_buf(), h.clone()).unwrap();
            let mut l = lease(id, now_ms() + 60_000);
            l.tenant_id = "acme".to_string();
            store.put(l).await.unwrap();
        }
        let store = LeaseStore::open(dir.path().to_path_buf(), h.clone()).unwrap();
        let got = store.get(id).await.unwrap().expect("present");
        assert_eq!(got.tenant_id, "acme", "tenant_id survives WAL replay");
    }
}
