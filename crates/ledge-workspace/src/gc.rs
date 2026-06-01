//! Mark-and-sweep garbage collection for the object store (spec §6).
//!
//! GC reclaims content-addressed objects that are no longer reachable from any
//! root. Roots are (a) durable refs `refs/heads/*` + `refs/tags/*` and (b) the
//! refs of every *live-lease* workspace `refs/workspaces/<id>/*`. The pass is
//! crash-safe and race-safe via a candidate-set freeze (§6 safety argument):
//! the set of deletion candidates is snapshotted *before* marking, so any object
//! written after the snapshot is structurally excluded from this pass and can
//! never be wrongly deleted.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ledge_core::{ObjectId, RefStore, Result};
use ledge_object_store::{graph, DiskObjectStore};
use ledge_ref_store::RefStoreImpl;

use crate::lease::LeaseStore;

/// Mark-and-sweep GC engine. Holds shared handles only; no per-pass state.
pub struct Gc {
    refs: Arc<RefStoreImpl>,
    leases: Arc<LeaseStore>,
    objects: Arc<DiskObjectStore>,
}

/// Per-pass GC accounting (spec §4.3).
///
/// - `scanned`   — objects in the frozen candidate set (`list_all_ids` at start).
/// - `reachable` — objects reachable from the snapshotted root set.
/// - `reclaimed` — candidate objects deleted this pass.
/// - `bytes_freed` — sum of on-disk file sizes of the reclaimed objects.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GcStats {
    pub scanned: usize,
    pub reachable: usize,
    pub reclaimed: usize,
    pub bytes_freed: u64,
}

/// Wall-clock milliseconds since the Unix epoch, used for lease-liveness checks.
///
/// GC tolerates clock skew here: a lease that is "live" at `now_ms` is treated
/// as a root and its objects are kept. Over-keeping is always safe (objects are
/// reclaimed on a later pass); under-keeping would be a correctness bug, so the
/// liveness predicate is intentionally inclusive.
fn now_ms() -> u64 {
    // A pre-1970 clock yields `Err`; we map that to 0 rather than panicking so a
    // skewed clock can never crash the unsupervised GC task. now_ms == 0 makes
    // every lease appear live (conservative: GC never reclaims a live workspace's
    // objects), which is the fail-safe direction.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl Gc {
    pub fn new(
        refs: Arc<RefStoreImpl>,
        leases: Arc<LeaseStore>,
        objects: Arc<DiskObjectStore>,
    ) -> Self {
        Self {
            refs,
            leases,
            objects,
        }
    }

    /// Run one mark-and-sweep pass (spec §6). Implements the four steps exactly:
    /// snapshot roots → freeze candidates → mark → sweep.
    #[tracing::instrument(skip(self))]
    pub async fn run(&self) -> Result<GcStats> {
        // Monotonic timer for the pass duration (Instant is clock-skew-immune).
        let start = std::time::Instant::now();
        // ── 1. Snapshot roots ────────────────────────────────────────────────
        // Durable refs are unconditional roots.
        let mut roots: Vec<ObjectId> = Vec::new();
        for prefix in ["refs/heads/", "refs/tags/"] {
            for (_name, entry) in self.refs.list(prefix).await? {
                roots.push(entry.target);
            }
        }
        // Live-lease workspaces contribute their workspace refs as roots.
        // A *tombstoned or expired* lease is excluded by `live()`, so its
        // workspace refs are NOT roots even if those refs still physically
        // exist (the expiry sweeper removes them out of band — see §6 note).
        let now = now_ms();
        for lease in self.leases.live(now).await? {
            let prefix = format!("refs/workspaces/{}/", lease.id.to_hex());
            for (_name, entry) in self.refs.list(&prefix).await? {
                roots.push(entry.target);
            }
        }

        // ── 2. Freeze candidates ─────────────────────────────────────────────
        // Snapshot of every object that exists *now*. Anything written after
        // this line is never a candidate this pass (safety argument, §6).
        let candidates = self.objects.list_all_ids().await?;
        let scanned = candidates.len();

        // ── 3. Mark ──────────────────────────────────────────────────────────
        // Capture the root count before `reachable_from` consumes the Vec.
        let root_count = roots.len();
        let reachable: HashSet<ObjectId> = graph::reachable_from(&self.objects, roots).await?;
        let reachable_count = reachable.len();

        // ── 4. Sweep ─────────────────────────────────────────────────────────
        // Delete every candidate that is not reachable. Deletes are idempotent;
        // a crash mid-sweep is harmless — the next pass re-derives and continues.
        let mut reclaimed = 0usize;
        let mut bytes_freed = 0u64;
        for id in candidates {
            if reachable.contains(&id) {
                continue;
            }
            // Stat the file *before* deleting to attribute freed bytes.
            // A missing file (concurrent delete) contributes 0 bytes and the
            // delete remains a no-op — both idempotent.
            let path = self.objects.object_path(&id);
            if let Ok(meta) = tokio::fs::metadata(&path).await {
                bytes_freed += meta.len();
            }
            self.objects.delete(id).await?;
            reclaimed += 1;
        }

        let stats = GcStats {
            scanned,
            reachable: reachable_count,
            reclaimed,
            bytes_freed,
        };

        // Structured pass summary (spec §9): roots, candidates, reachable,
        // reclaimed, bytes freed, and wall duration.
        tracing::info!(
            roots = root_count,
            candidates = stats.scanned,
            reachable = stats.reachable,
            reclaimed = stats.reclaimed,
            bytes_freed = stats.bytes_freed,
            duration_ms = start.elapsed().as_millis(),
            "gc pass complete"
        );

        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use bytes::Bytes;
    use tempfile::TempDir;

    use crate::id::WorkspaceId;
    use crate::lease::{Lease, LeaseStore};
    use ledge_core::{ObjectId, ObjectStore, RefName, RefStore, HLC};
    use ledge_object_store::DiskObjectStore;
    use ledge_ref_store::RefStoreImpl;

    /// Build a `Gc` over a single shared `data_dir` so refs/objects/leases
    /// coexist (objects under `objects/`, refs under `refs/`, leases under
    /// `leases/` — disjoint subtrees). Returns the tempdir (kept alive by the
    /// caller), the shared clock, the three Arcs, and the Gc.
    struct Harness {
        _dir: TempDir,
        hlc: Arc<HLC>,
        refs: Arc<RefStoreImpl>,
        leases: Arc<LeaseStore>,
        objects: Arc<DiskObjectStore>,
        gc: Gc,
    }

    fn setup() -> Harness {
        let dir = TempDir::new().unwrap();
        let root = dir.path().to_path_buf();
        let hlc = Arc::new(HLC::new());
        let refs = Arc::new(RefStoreImpl::open(root.clone(), hlc.clone()).unwrap());
        let leases = Arc::new(LeaseStore::open(root.clone(), hlc.clone()).unwrap());
        let objects = Arc::new(DiskObjectStore::new(root.clone()).unwrap());
        let gc = Gc::new(refs.clone(), leases.clone(), objects.clone());
        Harness {
            _dir: dir,
            hlc,
            refs,
            leases,
            objects,
            gc,
        }
    }

    fn now_ms_test() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    // ── object-graph builders (git-canonical wire formats) ───────────────────

    /// Write a blob; return its ObjectId.
    async fn write_blob(store: &DiskObjectStore, content: &[u8]) -> ObjectId {
        store
            .write_git_object(3, Bytes::copy_from_slice(content))
            .await
            .unwrap()
    }

    /// Write a tree with a single entry `name -> blob_id` and return the tree's
    /// ObjectId. Git tree-entry format is:
    ///   "100644 <name>\0" ++ <20-byte raw SHA-1 of the blob>
    /// The blob's SHA-1 is the canonical git id recorded in its header, read via
    /// `sha1_of` (NOT the BLAKE3 ObjectId), because `reachable_from` resolves
    /// children by git SHA-1.
    async fn write_tree(store: &DiskObjectStore, name: &str, blob_id: ObjectId) -> ObjectId {
        let blob_sha1 = store.sha1_of(blob_id).await.unwrap(); // [u8; 20]
        let mut body = Vec::new();
        body.extend_from_slice(format!("100644 {name}\0").as_bytes());
        body.extend_from_slice(&blob_sha1);
        store.write_git_object(2, Bytes::from(body)).await.unwrap()
    }

    /// Write a commit pointing at `tree_id` and return the commit's ObjectId.
    async fn write_commit(store: &DiskObjectStore, tree_id: ObjectId) -> ObjectId {
        let tree_sha1 = store.sha1_of(tree_id).await.unwrap();
        let tree_hex = hex_lower(&tree_sha1);
        let body = format!(
            "tree {tree_hex}\n\
             author a <a@b> 0 +0000\n\
             committer a <a@b> 0 +0000\n\
             \n\
             msg\n"
        );
        store
            .write_git_object(1, Bytes::from(body.into_bytes()))
            .await
            .unwrap()
    }

    fn hex_lower(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            use std::fmt::Write as _;
            let _ = write!(s, "{b:02x}");
        }
        s
    }

    /// Build blob -> tree -> commit; return (blob, tree, commit).
    async fn build_graph(store: &DiskObjectStore) -> (ObjectId, ObjectId, ObjectId) {
        let blob = write_blob(store, b"hello reachable world").await;
        let tree = write_tree(store, "file.txt", blob).await;
        let commit = write_commit(store, tree).await;
        (blob, tree, commit)
    }

    /// Point a durable/workspace ref at `target` (create: expected = None).
    async fn set_ref(refs: &RefStoreImpl, name: &str, target: ObjectId) {
        let rn = RefName::new(name).unwrap();
        refs.update(&rn, target, None).await.unwrap();
    }

    /// Construct a Lease with the given id and expiry; other fields are filler.
    fn lease(id: WorkspaceId, expires_at_ms: u64) -> Lease {
        Lease {
            id,
            source_refs: Vec::new(),
            created_at_ms: 0,
            expires_at_ms,
            hlc: 0,
            generation: 0,
        }
    }

    // 1 ───────────────────────────────────────────────────────────────────────
    #[tokio::test]
    async fn gc_reclaims_orphan() {
        let h = setup();
        let orphan = write_blob(&h.objects, b"orphan").await;
        let stats = h.gc.run().await.unwrap();
        assert_eq!(stats.reclaimed, 1, "single orphan must be reclaimed");
        assert_eq!(stats.scanned, 1);
        assert!(
            !h.objects.exists(orphan).await.unwrap(),
            "orphan must be gone after sweep"
        );
    }

    // 2 ───────────────────────────────────────────────────────────────────────
    #[tokio::test]
    async fn gc_keeps_object_reachable_from_durable_ref() {
        let h = setup();
        let (blob, tree, commit) = build_graph(&h.objects).await;
        set_ref(&h.refs, "refs/heads/main", commit).await;
        let orphan = write_blob(&h.objects, b"discard me").await;

        let stats = h.gc.run().await.unwrap();

        // Only the orphan is reclaimed; the reachable trio survives.
        assert_eq!(stats.reclaimed, 1, "only the orphan is unreachable");
        assert_eq!(stats.reachable, 3, "commit + tree + blob are reachable");
        assert!(h.objects.exists(commit).await.unwrap());
        assert!(h.objects.exists(tree).await.unwrap());
        assert!(h.objects.exists(blob).await.unwrap());
        assert!(!h.objects.exists(orphan).await.unwrap());
    }

    // 3 ───────────────────────────────────────────────────────────────────────
    #[tokio::test]
    async fn gc_keeps_object_held_by_live_workspace() {
        let h = setup();
        let (blob, tree, commit) = build_graph(&h.objects).await;

        let id = WorkspaceId::generate(&h.hlc);
        let ref_name = format!("refs/workspaces/{}/heads/main", id.to_hex());
        set_ref(&h.refs, &ref_name, commit).await;

        // Live lease: far-future expiry.
        let far_future = now_ms_test() + 1_000_000_000;
        h.leases.put(lease(id, far_future)).await.unwrap();

        let orphan = write_blob(&h.objects, b"discard me").await;

        let stats = h.gc.run().await.unwrap();

        assert_eq!(stats.reclaimed, 1, "only the orphan is reclaimed");
        assert_eq!(stats.reachable, 3, "live workspace keeps its trio");
        assert!(h.objects.exists(commit).await.unwrap());
        assert!(h.objects.exists(tree).await.unwrap());
        assert!(h.objects.exists(blob).await.unwrap());
        assert!(!h.objects.exists(orphan).await.unwrap());
    }

    // 4 ───────────────────────────────────────────────────────────────────────
    #[tokio::test]
    async fn gc_reclaims_expired_workspace_objects() {
        let h = setup();
        let (blob, tree, commit) = build_graph(&h.objects).await;

        let id = WorkspaceId::generate(&h.hlc);
        // Workspace refs still physically exist...
        let ref_name = format!("refs/workspaces/{}/heads/main", id.to_hex());
        set_ref(&h.refs, &ref_name, commit).await;

        // ...but the lease is EXPIRED, and there are NO durable refs.
        let past = now_ms_test().saturating_sub(60_000);
        h.leases.put(lease(id, past)).await.unwrap();

        let stats = h.gc.run().await.unwrap();

        // `live()` excludes the expired lease, so its workspace refs are NOT
        // roots → the whole graph is unreachable → all three reclaimed.
        assert_eq!(stats.reachable, 0, "expired lease contributes no roots");
        assert_eq!(stats.reclaimed, 3, "commit + tree + blob all reclaimed");
        assert!(!h.objects.exists(commit).await.unwrap());
        assert!(!h.objects.exists(tree).await.unwrap());
        assert!(!h.objects.exists(blob).await.unwrap());
    }

    // 5 ───────────────────────────────────────────────────────────────────────
    #[tokio::test]
    async fn gc_zero_roots_reclaims_all() {
        let h = setup();
        write_blob(&h.objects, b"a").await;
        write_blob(&h.objects, b"b").await;
        write_blob(&h.objects, b"c").await;
        let stats = h.gc.run().await.unwrap();
        assert_eq!(stats.scanned, 3);
        assert_eq!(stats.reachable, 0);
        assert_eq!(stats.reclaimed, 3, "no roots ⇒ everything reclaimed");
    }

    // 6 ───────────────────────────────────────────────────────────────────────
    #[tokio::test]
    async fn gc_all_reachable_reclaims_nothing() {
        let h = setup();
        let (_b, _t, commit) = build_graph(&h.objects).await;
        set_ref(&h.refs, "refs/heads/main", commit).await;
        let stats = h.gc.run().await.unwrap();
        assert_eq!(stats.scanned, 3);
        assert_eq!(stats.reachable, 3);
        assert_eq!(stats.reclaimed, 0, "fully-reachable graph keeps everything");
        assert_eq!(stats.bytes_freed, 0);
    }

    // 7 ───────────────────────────────────────────────────────────────────────
    #[tokio::test]
    async fn gc_idempotent_second_run_noop() {
        let h = setup();
        write_blob(&h.objects, b"x").await;
        write_blob(&h.objects, b"y").await;
        let first = h.gc.run().await.unwrap();
        assert_eq!(first.reclaimed, 2);
        // Second pass: list_all_ids no longer returns the deleted objects, so
        // they are not even candidates → nothing to reclaim. Crash-mid-sweep
        // idempotency reduces to this same property (§6).
        let second = h.gc.run().await.unwrap();
        assert_eq!(second.scanned, 0, "deleted objects are no longer candidates");
        assert_eq!(second.reclaimed, 0);
        assert_eq!(second.bytes_freed, 0);
    }

    // 8 ───────────────────────────────────────────────────────────────────────
    // Freeze guard (§6 safety argument). True concurrency (a write landing
    // *during* the sweep loop) is non-deterministic to unit-test, so we assert
    // the structural property that makes it safe instead: the candidate set is
    // frozen by `list_all_ids` BEFORE marking/sweeping, so any object written
    // after that snapshot is, by construction, NOT in `candidates` and thus can
    // never be swept this pass.
    //
    // Part A — end-to-end ordering: GC over an empty store reclaims nothing,
    // then an orphan written *after* run() returns still exists.
    // Part B — replicate run()'s freeze step manually: capture `candidates` via
    // list_all_ids, write a NEW orphan AFTER capture, and assert the new orphan
    // is absent from the captured candidate set (so the sweep would never touch
    // it). This is an honest, deterministic stand-in for the within-sweep guard.
    #[tokio::test]
    async fn gc_object_written_after_freeze_survives() {
        let h = setup();

        // Part A: GC finishes on an empty store, *then* an orphan is written.
        let stats = h.gc.run().await.unwrap();
        assert_eq!(stats.reclaimed, 0);
        let late = write_blob(&h.objects, b"written after GC finished").await;
        assert!(
            h.objects.exists(late).await.unwrap(),
            "object written after a completed GC pass must survive"
        );

        // Part B: replicate the freeze step and prove post-freeze writes are
        // excluded from the candidate snapshot.
        let candidates_before: HashSet<ObjectId> =
            h.objects.list_all_ids().await.unwrap().into_iter().collect();
        let post_freeze = write_blob(&h.objects, b"written after the freeze").await;
        assert!(
            !candidates_before.contains(&post_freeze),
            "an object written after list_all_ids is never a sweep candidate (§6 freeze guard)"
        );
        // And it would never be deleted: a real run() built from
        // `candidates_before` only ever calls delete() on members of that set.
    }
}
