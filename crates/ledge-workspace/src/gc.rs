//! Mark-and-sweep garbage collection for the object store (spec §6).
//!
//! GC reclaims content-addressed objects that are no longer reachable from any
//! root. Roots are (a) every durable ref — any ref NOT under `refs/workspaces/*`
//! (covers `refs/heads/*`, `refs/tags/*`, and per-tenant `refs/tenants/<t>/*`,
//! Phase 4d-2 R6) — and (b) the refs of every *live-lease* workspace
//! `refs/workspaces/<id>/*`. The pass is
//! crash-safe and race-safe via a candidate-set freeze (§6 safety argument):
//! the set of deletion candidates is snapshotted *before* marking, so any object
//! written after the snapshot is structurally excluded from this pass and can
//! never be wrongly deleted.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ledge_core::{tenant_of_ref, ObjectId, RefStore, Result};
use ledge_object_store::{graph, DiskObjectStore};

use crate::lease::LeaseStore;
use crate::quota::{TenantUsage, UsageMap};

/// Mark-and-sweep GC engine. Holds shared handles only; no per-pass state.
///
/// `refs` is an `Arc<dyn RefStore>` (GC only *lists* refs to build the root set,
/// a trait method), so it works against either the single-node `RefStoreImpl` or
/// the clustered `ClusterRefStore`. `objects` stays a concrete
/// [`DiskObjectStore`] because mark-and-sweep needs `list_all_ids`/`delete`/
/// on-disk sizing, which are not on the [`ledge_core::ObjectStore`] trait.
///
/// # Distributed GC is per-node-local in Phase 3
/// In cluster mode each node runs GC against *its own* on-disk
/// [`DiskObjectStore`] using *its own* applied ref state (read through the
/// `dyn RefStore`) as the root set. This is correct because objects are
/// content-addressed and a node's applied ref set is a superset-safe root set
/// for that node's local objects (over-keeping is always safe; a later pass
/// reclaims). Cluster-wide GC coordination is intentionally out of scope.
pub struct Gc {
    refs: Arc<dyn RefStore>,
    leases: Arc<LeaseStore>,
    objects: Arc<DiskObjectStore>,
    /// The shared per-tenant usage snapshot this pass refreshes (Phase 4d-3,
    /// R Q4). `ArcSwap::store`d at the end of `run`; read by the manager's commit
    /// gate. The SAME `Arc` the server + manager hold.
    usage: Arc<UsageMap>,
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
    /// Candidates retained THIS pass solely because they are younger than the
    /// grace window (distributed GC only; the single-node `Gc` has no grace
    /// fence and always leaves this `0`). Observability so operators can see the
    /// grace window's effect (spec §4.1).
    pub skipped_grace: usize,
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
        refs: Arc<dyn RefStore>,
        leases: Arc<LeaseStore>,
        objects: Arc<DiskObjectStore>,
        usage: Arc<UsageMap>,
    ) -> Self {
        Self {
            refs,
            leases,
            objects,
            usage,
        }
    }

    /// Run one mark-and-sweep pass (spec §6). Implements the four steps exactly:
    /// snapshot roots → freeze candidates → mark → sweep.
    #[tracing::instrument(skip(self))]
    pub async fn run(&self) -> Result<GcStats> {
        // Monotonic timer for the pass duration (Instant is clock-skew-immune).
        let start = std::time::Instant::now();
        // ── 1. Snapshot roots ────────────────────────────────────────────────
        // Durable refs are unconditional roots. Phase 4d-2 (R6): a real tenant's
        // durable refs live under refs/tenants/<t>/heads|tags/*, so we root EVERY
        // ref that is NOT a workspace ref — mirroring the cluster GC filter
        // `!starts_with("refs/workspaces/")` (ledge-cluster/src/ref_store.rs:618).
        // Root-tenant refs (refs/heads/*, refs/tags/*) are a SUBSET of this widened
        // filter, so single-tenant/root behavior is byte-identical; workspace refs
        // stay lease-gated (rooted only when their lease is live, added below).
        let mut roots: Vec<ObjectId> = Vec::new();
        for (name, entry) in self.refs.list("refs/").await? {
            if !name.as_str().starts_with("refs/workspaces/") {
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
        // `reachable` counts objects reached by the git-graph walk from the roots.
        // The delta-base closure below only ADDS bases that are otherwise
        // unreachable; reporting the ref-reachable count keeps `stats.reachable`
        // stable for callers/tests (non-delta graphs add nothing to the closure).
        let reachable_count = reachable.len();

        // Close the keep-set under the delta-base relation: a kept delta's base
        // must be kept too, else the delta becomes unreadable (data loss). Task 3.
        // Header-only `delta_base_of` walk; the closure terminates because every
        // step either inserts a NEW id into `keep` (a finite set bounded by the
        // candidate population) or stops — no id is revisited.
        let keep = self.close_under_delta_bases(reachable).await?;

        // ── 4. Sweep ─────────────────────────────────────────────────────────
        // Delete every candidate that is not in the keep-set. Deletes are
        // idempotent; a crash mid-sweep is harmless — the next pass re-derives.
        let mut reclaimed = 0usize;
        let mut bytes_freed = 0u64;
        for id in candidates {
            if keep.contains(&id) {
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

        // ── Per-tenant usage measurement (Phase 4d-3, side-product of the mark).
        // Group the DURABLE roots (the non-workspace refs already enumerated above)
        // by owning tenant (tenant_of_ref), then compute each tenant's reachable
        // set INDEPENDENTLY: an object reachable from two tenants counts in BOTH
        // (dedup-correct, you-pay-for-your-reachable-set — spec §3.4/§6, R Q8).
        // This NEVER changes which objects were reclaimed above (R Q10); it only
        // refreshes the shared UsageMap the commit soft-gate reads.
        let mut groups: std::collections::HashMap<String, Vec<ObjectId>> =
            std::collections::HashMap::new();
        for (name, entry) in self.refs.list("refs/").await? {
            if name.as_str().starts_with("refs/workspaces/") {
                continue; // workspace refs are ephemeral, not durable usage
            }
            let tenant = tenant_of_ref(name.as_str()).to_string();
            groups.entry(tenant).or_default().push(entry.target);
        }
        let mut usage: std::collections::HashMap<String, TenantUsage> =
            std::collections::HashMap::with_capacity(groups.len());
        for (tenant, tenant_roots) in groups {
            let reachable = graph::reachable_from(&self.objects, tenant_roots).await?;
            // Close under delta-bases: a base backing a kept delta is real on-disk
            // bytes GC retains for this tenant, so it must count toward the
            // tenant's measured usage (and is retained by the closure, Task 3).
            let reachable = self.close_under_delta_bases(reachable).await?;
            let mut bytes = 0u64;
            let objects = reachable.len() as u64;
            for id in &reachable {
                let path = self.objects.object_path(id);
                if let Ok(meta) = tokio::fs::metadata(&path).await {
                    bytes += meta.len();
                }
            }
            usage.insert(tenant, TenantUsage { bytes, objects });
        }
        self.usage.store(Arc::new(usage));

        let stats = GcStats {
            scanned,
            reachable: reachable_count,
            reclaimed,
            bytes_freed,
            // The single-node Gc has no grace fence: every unreachable candidate
            // is swept immediately, so none is ever retained by grace.
            skipped_grace: 0,
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

    /// Expand `set` to its closure under the delta-base relation: for every id in
    /// the set that is stored as a delta, transitively include its base (Task 3,
    /// delta retention). A kept delta whose base is reclaimed becomes unreadable —
    /// the closure prevents that data loss.
    ///
    /// Complexity: O(|closure|) header-only reads (`delta_base_of` reads only the
    /// 56-byte object header). Termination: each iteration either inserts a NEW id
    /// (the `keep` set grows monotonically and is bounded by the finite on-disk
    /// object population) or stops; no id is pushed onto the frontier twice.
    async fn close_under_delta_bases(&self, set: HashSet<ObjectId>) -> Result<HashSet<ObjectId>> {
        let mut keep = set;
        let mut frontier: Vec<ObjectId> = keep.iter().copied().collect();
        while let Some(id) = frontier.pop() {
            if let Some(base) = self.objects.delta_base_of(id).await? {
                // `insert` returns true only the first time `base` is seen, so a
                // base is enqueued at most once even with diamond delta chains.
                if keep.insert(base) {
                    frontier.push(base);
                }
            }
        }
        Ok(keep)
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
    use crate::quota::{TenantUsage, UsageMap};
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
        usage: Arc<UsageMap>,
        gc: Gc,
    }

    fn setup() -> Harness {
        let dir = TempDir::new().unwrap();
        let root = dir.path().to_path_buf();
        let hlc = Arc::new(HLC::new());
        let refs = Arc::new(RefStoreImpl::open(root.clone(), hlc.clone()).unwrap());
        let leases = Arc::new(LeaseStore::open(root.clone(), hlc.clone()).unwrap());
        let objects = Arc::new(DiskObjectStore::new(root.clone()).unwrap());
        let usage = Arc::new(UsageMap::default());
        let gc = Gc::new(refs.clone(), leases.clone(), objects.clone(), usage.clone());
        Harness {
            _dir: dir,
            hlc,
            refs,
            leases,
            objects,
            usage,
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
            tenant_id: "root".to_string(),
        }
    }

    // 0 ───────────────────────────────────────────────────────────────────────
    #[test]
    fn gc_stats_has_skipped_grace_default_zero() {
        let s = GcStats::default();
        assert_eq!(s.skipped_grace, 0, "skipped_grace defaults to 0");
        // The single-node Gc never sets it, so a clone preserves the default.
        let back = s.clone();
        assert_eq!(back.skipped_grace, 0);
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

    // 9 ───────────────────────────────────────────────────────────────────────
    /// Phase 4d-2 GC interaction (R6): a tenant's DURABLE ref
    /// (refs/tenants/<t>/heads/…) is a root — its object survives GC — while a
    /// RELEASED workspace's exclusive object is reclaimed once its lease is
    /// tombstoned. This proves the widened durable-root filter keeps tenant
    /// durable refs AND the lease-gated workspace reclamation intact: too narrow
    /// would lose tenant data; too broad would never reclaim workspace garbage.
    #[tokio::test]
    async fn tenant_durable_survives_gc_released_workspace_reclaims() {
        let h = setup();
        // Two blobs: one reachable from a tenant durable ref, one only from a
        // soon-to-be-released workspace ref.
        let durable_obj = write_blob(&h.objects, b"durable payload").await;
        let ws_obj = write_blob(&h.objects, b"workspace only payload").await;

        // Tenant acme's PHYSICAL durable ref roots durable_obj. Note this is
        // neither refs/heads/* nor refs/tags/* — under the OLD two-prefix filter
        // it would have been IGNORED and durable_obj wrongly reclaimed.
        set_ref(&h.refs, "refs/tenants/acme/heads/main", durable_obj).await;

        // A workspace (live lease) roots ws_obj via a workspace ref.
        let id = WorkspaceId::generate(&h.hlc);
        set_ref(
            &h.refs,
            &format!("refs/workspaces/{}/heads/wip", id.to_hex()),
            ws_obj,
        )
        .await;
        // Stamp the live lease as acme-owned (GC roots ALL live workspaces via the
        // unscoped `live()`, regardless of tenant).
        let mut l = lease(id, now_ms_test() + 600_000); // live
        l.tenant_id = "acme".to_string();
        h.leases.put(l).await.unwrap();

        // While the lease is live, BOTH survive.
        h.gc.run().await.unwrap();
        assert!(
            h.objects.exists(durable_obj).await.unwrap(),
            "tenant durable survives (live)"
        );
        assert!(
            h.objects.exists(ws_obj).await.unwrap(),
            "live workspace object survives"
        );

        // Release the workspace (tombstone the lease + delete its refs).
        h.leases.tombstone(id).await.unwrap();
        for (name, entry) in h
            .refs
            .list(&format!("refs/workspaces/{}/", id.to_hex()))
            .await
            .unwrap()
        {
            h.refs.delete(&name, entry.target).await.unwrap();
        }

        // After release, the workspace-only object is reclaimed; the tenant
        // durable object STILL survives (it is a durable root, R6).
        h.gc.run().await.unwrap();
        assert!(
            h.objects.exists(durable_obj).await.unwrap(),
            "tenant durable persists after release"
        );
        assert!(
            !h.objects.exists(ws_obj).await.unwrap(),
            "released workspace's exclusive object must be reclaimed"
        );
    }

    // 10 ──────────────────────────────────────────────────────────────────────
    /// Phase 4d-3: a GC pass measures per-tenant durable usage. Two tenants each
    /// root a distinct commit→tree→blob graph (3 objects, ~known bytes); plus a
    /// shared blob reachable from BOTH tenants' refs counts in EACH (dedup-correct,
    /// overlap-counts-per-tenant — the total is NOT the sum, R Q8).
    #[tokio::test]
    async fn gc_measures_per_tenant_usage_with_overlap() {
        let h = setup();
        // acme's durable graph (commit→tree→blob = 3 objects).
        let (a_blob, a_tree, a_commit) = build_graph(&h.objects).await;
        set_ref(&h.refs, "refs/tenants/acme/heads/main", a_commit).await;
        // globex's durable graph (a DIFFERENT 3-object graph).
        let g_blob = write_blob(&h.objects, b"globex unique blob").await;
        let g_tree = write_tree(&h.objects, "g.txt", g_blob).await;
        let g_commit = write_commit(&h.objects, g_tree).await;
        set_ref(&h.refs, "refs/tenants/globex/heads/main", g_commit).await;

        // A blob reachable from BOTH tenants: give each a SECOND ref pointing at a
        // shared blob (a blob is a leaf, so the ref roots exactly it). The shared
        // blob counts in acme's AND globex's reachable sets.
        let shared = write_blob(&h.objects, b"shared dedup blob").await;
        set_ref(&h.refs, "refs/tenants/acme/heads/shared", shared).await;
        set_ref(&h.refs, "refs/tenants/globex/heads/shared", shared).await;

        h.gc.run().await.unwrap();

        let snap = h.usage.load();
        let acme = snap.get("acme").copied().expect("acme measured");
        let globex = snap.get("globex").copied().expect("globex measured");
        // Each tenant: its own 3-object graph + the shared blob = 4 objects.
        assert_eq!(acme.objects, 4, "acme: commit+tree+blob+shared");
        assert_eq!(globex.objects, 4, "globex: commit+tree+blob+shared");
        assert!(acme.bytes > 0 && globex.bytes > 0, "bytes are summed from file sizes");
        // Overlap-counts-per-tenant: the shared blob is in BOTH counts, so the
        // sum of per-tenant objects (8) exceeds the physical distinct object count
        // (3 + 3 + 1 = 7). This is the documented dedup-crosses-tenants semantics.
        assert_eq!(acme.objects + globex.objects, 8);
        // Root tenant has NO durable refs here ⇒ absent or zero.
        assert_eq!(snap.get("root").copied().unwrap_or_default(), TenantUsage::default());
        let _ = (a_blob, a_tree, g_blob, g_tree); // silence unused (ids are wired via refs)
    }

    // 12 ──────────────────────────────────────────────────────────────────────
    /// DATA-LOSS guard (delta retention, Task 3): if a reachable object `a` is
    /// stored as a delta against `base`, GC MUST keep `base` even though `base`
    /// is not independently reachable — otherwise `a` becomes unreadable.
    ///
    /// Reachability is established the SAME way `gc_keeps_object_reachable_from_durable_ref`
    /// does it: `a` is the blob inside a tree referenced by a commit that a durable
    /// ref (`refs/heads/main`) points at, so the git-graph walk
    /// (commit→tree→blob-by-SHA-1) reaches `a`. `base` is a BARE blob in no tree and
    /// under no ref, so it is NOT independently reachable — it survives ONLY via the
    /// delta-base closure.
    #[tokio::test]
    async fn gc_retains_base_of_a_kept_delta() {
        let h = setup();

        // `base`: a 400-line blob. `a`: the same content with one line changed,
        // so `a` is highly compressible as a delta against `base`.
        let base_content: Vec<u8> = (0..400).flat_map(|i| format!("l{i}\n").into_bytes()).collect();
        let edited: Vec<u8> = String::from_utf8(base_content.clone())
            .unwrap()
            .replace("l200\n", "EDIT\n")
            .into_bytes();
        let base = h
            .objects
            .write_git_object(3, Bytes::from(base_content))
            .await
            .unwrap();
        let a = h
            .objects
            .write_git_object(3, Bytes::from(edited.clone()))
            .await
            .unwrap();
        assert!(
            h.objects.deltify(a, base).await.unwrap(),
            "a is now a delta on base"
        );

        // Make ONLY `a` reachable: tree -> a, commit -> tree, durable ref -> commit.
        // `base` is referenced by no tree and no ref, so the graph walk never
        // reaches it (it is reachable solely through the delta-base relation).
        let tree = write_tree(&h.objects, "a.txt", a).await;
        let commit = write_commit(&h.objects, tree).await;
        set_ref(&h.refs, "refs/heads/main", commit).await;

        let stats = h.gc.run().await.unwrap();

        assert!(
            h.objects.exists(base).await.unwrap(),
            "GC MUST retain the delta base (a needs it)"
        );
        assert_eq!(
            ledge_core::ObjectStore::read(&*h.objects, a).await.unwrap().as_ref(),
            edited.as_slice(),
            "a still resolves after GC"
        );
        // Nothing was reclaimed: commit + tree + a are ref-reachable, and base is
        // kept by the delta-base closure.
        assert_eq!(stats.reclaimed, 0, "base must not be reclaimed");
        assert!(h.objects.exists(commit).await.unwrap());
        assert!(h.objects.exists(tree).await.unwrap());
    }

    // 11 ──────────────────────────────────────────────────────────────────────
    /// Root-tenant durable refs (refs/heads/*) are measured under "root".
    #[tokio::test]
    async fn gc_measures_root_tenant_usage() {
        let h = setup();
        let (_b, _t, commit) = build_graph(&h.objects).await;
        set_ref(&h.refs, "refs/heads/main", commit).await;
        h.gc.run().await.unwrap();
        let snap = h.usage.load();
        let root = snap.get("root").copied().expect("root measured");
        assert_eq!(root.objects, 3, "root: commit+tree+blob");
        assert!(root.bytes > 0);
    }
}
