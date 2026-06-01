//! `RefStoreImpl` — the production ref store.
//!
//! # Architecture
//! The live ref namespace is held in an `ArcSwap<Option<Arc<ArtNode>>>`.
//! Every mutation executes a CAS loop:
//!
//! 1. Load the current root (snapshot).
//! 2. Validate optimistic-concurrency preconditions against the snapshot.
//! 3. Produce a new root via the ART's pure copy-on-write insert/delete helpers.
//! 4. Attempt to swap the root atom; on failure (concurrent writer), restart.
//! 5. Durably log the committed entry to the WAL.
//!
//! # O(1) snapshot
//! `snapshot()` calls `ArcSwap::load_full()` — one atomic load — and wraps
//! the root in an `ArtSnapshot`.  No copying, no locking.
//!
//! # WAL replay on open
//! `open()` reads the WAL, replays `Checkpoint` + subsequent `Update`/`Delete`
//! entries in order, and arrives at the correct in-memory state before
//! accepting any new writes.

use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use ledge_core::{HLC, LedgeError, ObjectId, RefEntry, RefName, RefSnapshot, RefStore, Result};
use tracing::{debug, instrument};

use crate::art::{art_delete, art_insert, art_lookup, art_prefix_iter, ArtNode};
use crate::snapshot::ArtSnapshot;
use crate::wal::{Wal, WalEntry};

// ---------------------------------------------------------------------------
// RefStoreImpl
// ---------------------------------------------------------------------------

/// Production ref store implementation.
///
/// Thread-safe and lock-free for reads.  Writes serialise their WAL appends
/// through `Wal`'s internal `tokio::sync::Mutex`, but the ART swap itself is
/// always attempted lock-free via the CAS loop.
pub struct RefStoreImpl {
    /// Current ART root, wrapped in `Option` (None ⟺ empty namespace).
    root: ArcSwap<Option<Arc<ArtNode>>>,
    /// Shared hybrid logical clock for stamping new entries.
    hlc: Arc<HLC>,
    /// Write-ahead log for durability.
    wal: Arc<Wal>,
    /// Data directory (unused after open, kept for diagnostics).
    #[allow(dead_code)]
    data_dir: PathBuf,
}

impl RefStoreImpl {
    /// Open (or create) the ref store rooted at `data_dir`.
    ///
    /// Creates `data_dir/refs/` if absent, opens the WAL at
    /// `data_dir/refs/wal`, replays it to reconstruct in-memory state,
    /// and returns a ready-to-use store.
    ///
    /// # Errors
    /// Propagates any WAL I/O or corruption error.
    pub fn open(data_dir: PathBuf, hlc: Arc<HLC>) -> Result<Self> {
        let refs_dir = data_dir.join("refs");
        std::fs::create_dir_all(&refs_dir).map_err(LedgeError::Io)?;

        let (wal, entries) = Wal::open(refs_dir.join("wal"))?;

        // Replay WAL entries to reconstruct the in-memory ART.
        let mut root: Option<Arc<ArtNode>> = None;
        for entry in entries {
            match entry {
                WalEntry::Checkpoint { leaves } => {
                    // Checkpoint replaces all prior state with its full snapshot.
                    root = None;
                    for (name, ref_entry) in leaves {
                        root = Some(art_insert(root, name.as_bytes(), ref_entry, 0));
                    }
                }
                WalEntry::Update { name, entry: ref_entry } => {
                    root = Some(art_insert(root, name.as_bytes(), ref_entry, 0));
                }
                WalEntry::Delete { name, .. } => {
                    if let Some(r) = root.take() {
                        root = art_delete(r, name.as_bytes(), 0);
                    }
                }
            }
        }

        Ok(RefStoreImpl {
            root: ArcSwap::new(Arc::new(root)),
            hlc,
            wal: Arc::new(wal),
            data_dir,
        })
    }
}

// ---------------------------------------------------------------------------
// RefStore implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl RefStore for RefStoreImpl {
    /// Return the current entry for `name`, or `None` if it does not exist.
    ///
    /// Lock-free single atomic load + O(k) ART traversal.
    async fn get(&self, name: &RefName) -> Result<Option<RefEntry>> {
        let root_guard = self.root.load();
        Ok(match root_guard.as_ref() {
            None => None,
            Some(root) => art_lookup(root, name.as_str().as_bytes(), 0).cloned(),
        })
    }

    /// Atomically create or update `name` under optimistic CAS.
    ///
    /// - `expected = None`: create new ref; errors with `Conflict` if it
    ///   already exists with any target.
    /// - `expected = Some(id)`: update only if current target equals `id`;
    ///   errors with `Conflict` or `NotFound` otherwise.
    ///
    /// Version starts at 1 for new refs and increments by 1 on each update.
    /// `debug_assert!` guards the version ≥ 1 invariant on every write path.
    #[instrument(skip(self), fields(ref_name = %name))]
    async fn update(&self, name: &RefName, new: ObjectId, expected: Option<ObjectId>) -> Result<RefEntry> {
        let key = name.as_str().as_bytes().to_vec();

        loop {
            // Snapshot the current root for this CAS attempt.
            let current_arc = self.root.load_full();
            let current_root = current_arc.as_ref();

            // Read the entry that exists right now (if any).
            let current_entry: Option<RefEntry> = match current_root {
                None => None,
                Some(root) => art_lookup(root, &key, 0).cloned(),
            };

            // Validate optimistic preconditions.
            match (&current_entry, &expected) {
                // create (expected = None) but ref already exists → Conflict.
                (Some(existing), None) => {
                    return Err(LedgeError::Conflict { current: existing.clone() });
                }
                // update (expected = Some) but ref does not exist → NotFound.
                (None, Some(_)) => {
                    return Err(LedgeError::NotFound(new));
                }
                // update but wrong target → Conflict.
                (Some(existing), Some(exp_oid)) if existing.target != *exp_oid => {
                    return Err(LedgeError::Conflict { current: existing.clone() });
                }
                // All other cases are valid (create with None, update with matching id).
                _ => {}
            }

            // Compute the new version (1-based; new ref starts at 1).
            let new_version = current_entry.as_ref().map(|e| e.version + 1).unwrap_or(1);
            // Per spec: version must always be >= 1 on the write path.
            debug_assert!(new_version >= 1, "version invariant violated: version must be >= 1");

            let new_entry = RefEntry {
                target: new,
                hlc: self.hlc.tick(),
                version: new_version,
            };

            // Produce the new root via CoW ART insert.
            let new_root = art_insert(current_root.clone(), &key, new_entry.clone(), 0);
            let new_root_arc = Arc::new(Some(new_root));

            // Attempt the atomic swap.
            let prev = self.root.compare_and_swap(&current_arc, Arc::clone(&new_root_arc));
            if Arc::ptr_eq(&prev, &current_arc) {
                // CAS succeeded — commit to WAL then return.
                self.wal.append(&WalEntry::Update {
                    name: name.as_str().to_string(),
                    entry: new_entry.clone(),
                }).await?;
                debug!(version = new_entry.version, "ref committed");
                return Ok(new_entry);
            }
            // CAS lost to a concurrent writer — reload and retry.
        }
    }

    /// Atomically delete `name`, verifying the current target equals `expected`.
    ///
    /// # Errors
    /// - `NotFound` if the ref does not exist.
    /// - `Conflict` if the current target differs from `expected`.
    #[instrument(skip(self), fields(ref_name = %name))]
    async fn delete(&self, name: &RefName, expected: ObjectId) -> Result<()> {
        let key = name.as_str().as_bytes().to_vec();

        loop {
            let current_arc = self.root.load_full();
            let current_root = current_arc.as_ref();

            // Read and validate the current entry.
            let current_entry: RefEntry = match current_root {
                None => return Err(LedgeError::NotFound(expected)),
                Some(root) => match art_lookup(root, &key, 0) {
                    None => return Err(LedgeError::NotFound(expected)),
                    Some(e) => e.clone(),
                },
            };

            if current_entry.target != expected {
                return Err(LedgeError::Conflict { current: current_entry });
            }

            // Produce the new root without the deleted key.
            let new_root_opt = match current_root {
                None => None,
                Some(root) => art_delete(Arc::clone(root), &key, 0),
            };
            let new_root_arc = Arc::new(new_root_opt);

            let prev = self.root.compare_and_swap(&current_arc, Arc::clone(&new_root_arc));
            if Arc::ptr_eq(&prev, &current_arc) {
                self.wal.append(&WalEntry::Delete {
                    name: name.as_str().to_string(),
                    hlc: self.hlc.tick(),
                }).await?;
                return Ok(());
            }
            // CAS lost — retry.
        }
    }

    /// Return all refs whose name starts with `prefix`, in unspecified order.
    ///
    /// Lock-free single atomic load + O(n_matches * k) ART prefix scan.
    async fn list(&self, prefix: &str) -> Result<Vec<(RefName, RefEntry)>> {
        let root_guard = self.root.load();
        match root_guard.as_ref() {
            None => Ok(Vec::new()),
            Some(root) => {
                let pairs = art_prefix_iter(root, prefix.as_bytes(), 0);
                let mut results = Vec::with_capacity(pairs.len());
                for (key_bytes, entry) in pairs {
                    if let Ok(s) = std::str::from_utf8(&key_bytes) {
                        if let Ok(n) = RefName::new(s) {
                            results.push((n, entry));
                        }
                    }
                }
                Ok(results)
            }
        }
    }

    /// Capture a consistent, point-in-time snapshot of all refs.
    ///
    /// O(1): one atomic load, no copying, no locking.
    fn snapshot(&self) -> Arc<dyn RefSnapshot> {
        let root_arc = self.root.load_full();
        // Unwrap the `Arc<Option<…>>` to get `Option<Arc<ArtNode>>`.
        let inner: Option<Arc<ArtNode>> = root_arc.as_ref().clone();
        Arc::new(ArtSnapshot { root: inner })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::tempdir;
    use ledge_core::{HLC, ObjectId, RefName};

    fn make_store() -> RefStoreImpl {
        let dir = tempdir().unwrap();
        // Keep the TempDir so the path survives for the duration of the test.
        let path = dir.keep();
        RefStoreImpl::open(path, Arc::new(HLC::new())).unwrap()
    }

    fn oid(byte: u8) -> ObjectId {
        ObjectId::from_bytes([byte; 32])
    }

    fn name(s: &str) -> RefName {
        RefName::new(s).unwrap()
    }

    // ── 1. create then get ───────────────────────────────────────────────────

    #[tokio::test]
    async fn create_and_get() {
        let store = make_store();
        let n = name("refs/heads/main");
        let t = oid(1);
        let entry = store.update(&n, t, None).await.unwrap();
        assert_eq!(entry.target, t);
        assert_eq!(entry.version, 1, "first write must be version 1");
        let got = store.get(&n).await.unwrap().unwrap();
        assert_eq!(got, entry);
    }

    // ── 2. version increments on each update ────────────────────────────────

    #[tokio::test]
    async fn version_increments() {
        let store = make_store();
        let n = name("refs/heads/ver");
        let t1 = oid(1);
        let t2 = oid(2);
        let e1 = store.update(&n, t1, None).await.unwrap();
        let e2 = store.update(&n, t2, Some(t1)).await.unwrap();
        assert_eq!(e1.version, 1);
        assert_eq!(e2.version, 2);
    }

    // ── 3. create conflict (expected = None but ref exists) ─────────────────

    #[tokio::test]
    async fn create_conflict() {
        let store = make_store();
        let n = name("refs/heads/conflict");
        let t = oid(1);
        store.update(&n, t, None).await.unwrap();
        // Second create with expected = None must fail.
        let res = store.update(&n, oid(2), None).await;
        assert!(matches!(res, Err(LedgeError::Conflict { .. })), "expected Conflict, got {res:?}");
    }

    // ── 4. update with wrong expected → Conflict ────────────────────────────

    #[tokio::test]
    async fn update_wrong_expected() {
        let store = make_store();
        let n = name("refs/heads/cas");
        store.update(&n, oid(1), None).await.unwrap();
        let res = store.update(&n, oid(2), Some(oid(99))).await;
        assert!(matches!(res, Err(LedgeError::Conflict { .. })));
    }

    // ── 5. delete then get returns None ─────────────────────────────────────

    #[tokio::test]
    async fn delete_and_get() {
        let store = make_store();
        let n = name("refs/heads/del");
        let t = oid(0xdd);
        store.update(&n, t, None).await.unwrap();
        store.delete(&n, t).await.unwrap();
        assert!(store.get(&n).await.unwrap().is_none());
    }

    // ── 6. delete with wrong expected → Conflict ────────────────────────────

    #[tokio::test]
    async fn delete_wrong_expected() {
        let store = make_store();
        let n = name("refs/heads/delwrong");
        let t = oid(0xaa);
        store.update(&n, t, None).await.unwrap();
        let res = store.delete(&n, oid(0xbb)).await;
        assert!(matches!(res, Err(LedgeError::Conflict { .. })));
        assert!(store.get(&n).await.unwrap().is_some(), "ref must survive a failed delete");
    }

    // ── 7. list by prefix ───────────────────────────────────────────────────

    #[tokio::test]
    async fn list_prefix() {
        let store = make_store();
        for r in ["refs/heads/main", "refs/heads/dev", "refs/tags/v1"] {
            store.update(&name(r), oid(1), None).await.unwrap();
        }
        let heads = store.list("refs/heads/").await.unwrap();
        assert_eq!(heads.len(), 2);
        let tags = store.list("refs/tags/").await.unwrap();
        assert_eq!(tags.len(), 1);
        let all = store.list("refs/").await.unwrap();
        assert_eq!(all.len(), 3);
    }

    // ── 8. snapshot isolation ───────────────────────────────────────────────

    #[tokio::test]
    async fn snapshot_isolation() {
        let store = make_store();
        let n = name("refs/heads/snap");
        let t1 = oid(1);
        let t2 = oid(2);
        store.update(&n, t1, None).await.unwrap();
        let snap = store.snapshot();
        // Mutate the live store.
        store.update(&n, t2, Some(t1)).await.unwrap();
        // Snapshot must still reflect t1.
        assert_eq!(snap.get(&n).unwrap().target, t1, "snapshot must be isolated from writes");
        // Live store reflects t2.
        assert_eq!(store.get(&n).await.unwrap().unwrap().target, t2);
    }

    // ── 9. WAL replay durability ─────────────────────────────────────────────

    #[tokio::test]
    async fn wal_replay() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let n = name("refs/heads/persist");
        let t = oid(0xfe);

        // Write to the first store instance, then drop it.
        {
            let store = RefStoreImpl::open(data_dir.clone(), Arc::new(HLC::new())).unwrap();
            store.update(&n, t, None).await.unwrap();
        }

        // Open a fresh instance from the same directory and verify state is restored.
        let store2 = RefStoreImpl::open(data_dir, Arc::new(HLC::new())).unwrap();
        let entry = store2.get(&n).await.unwrap().expect("ref must survive a store reopen");
        assert_eq!(entry.target, t);
        assert_eq!(entry.version, 1);
    }
}
