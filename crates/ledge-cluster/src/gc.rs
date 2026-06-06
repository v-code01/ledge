//! `ClusterGc` — node-local distributed garbage collection (Phase 4c).
//!
//! The cluster-aware analogue of [`ledge_workspace::Gc`]. Each node independently
//! marks from the union of its HOSTED shards' committed-and-prepared roots and
//! sweeps ONLY its own local [`DiskObjectStore`]. This is sound because objects
//! are content-addressed (over-keeping is always safe) and the write-locality
//! invariant (spec §3) guarantees every object physically present on a node
//! belongs to a shard that node hosts — so a node's hosted-shard root set is a
//! sound liveness lower bound for its own store. No global live-set gather is
//! required, which is what makes this decentralized.
//!
//! Roots (spec §4.2) = committed ref targets (leader-linearized) ∪ prepared 2PC
//! staged targets (pins in-flight cross-shard commits — the load-bearing 4b
//! interaction) ∪ live-lease workspace ref targets. Sweep safety (spec §4.4) =
//! the existing freeze guard (`list_all_ids()` snapshot taken before the root
//! read) composed with a NEW grace fence (a candidate is swept only if it is
//! unreachable AND older than `grace`, by file mtime), which closes the
//! object-resurrection race under bounded clock skew.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use ledge_core::{ObjectId, RefStore, Result};
use ledge_object_store::{graph, DiskObjectStore};
use ledge_workspace::lease::LeaseStore;
use ledge_workspace::GcStats;

use crate::ref_store::ClusterRefStore;

/// Node-local distributed-GC driver. Holds shared handles only; no per-pass state.
pub struct ClusterGc {
    /// Hosted-shard committed refs + prepared 2PC locks (the cross-shard roots).
    cluster_refs: Arc<ClusterRefStore>,
    /// Live-lease roots (workspace refs of leases that pass `live(now)`).
    leases: Arc<LeaseStore>,
    /// THIS node's local store — the sweep target (never a peer's).
    objects: Arc<DiskObjectStore>,
    /// Resurrection-race fence: a candidate younger than this is never swept.
    grace: Duration,
}

impl ClusterGc {
    /// Construct over a node's view of the cluster + its local store.
    pub fn new(
        cluster_refs: Arc<ClusterRefStore>,
        leases: Arc<LeaseStore>,
        objects: Arc<DiskObjectStore>,
        grace: Duration,
    ) -> Self {
        Self {
            cluster_refs,
            leases,
            objects,
            grace,
        }
    }

    /// Run one cluster-aware mark-and-sweep pass over this node's local store.
    ///
    /// `now_unix_secs` is the pass clock: it gates lease liveness (× 1000 for the
    /// ms-based lease store) and anchors the mtime-based grace comparison. Taking
    /// it as a parameter (rather than reading the wall clock) makes the grace
    /// fence deterministically testable.
    ///
    /// Ordering (spec §4.4): freeze candidates → snapshot lease clock → collect
    /// roots (linearizable) → mark → grace-fenced sweep. Freezing BEFORE the root
    /// read ensures any ref committed before the (later) root read that points at
    /// a frozen candidate is observed by the mark.
    ///
    /// # Complexity
    /// O(C) freeze + O(R) per-hosted-shard linearized ref reads + O(closure) mark
    /// + O(C) sweep, for C candidates and R total roots.
    #[tracing::instrument(skip(self), fields(now = now_unix_secs))]
    pub async fn run(&self, now_unix_secs: u64) -> Result<GcStats> {
        let start = std::time::Instant::now();

        // ── 1. Freeze candidates ─────────────────────────────────────────────
        // Snapshot every object that exists NOW. Anything written after this line
        // is never a candidate this pass (freeze guard, spec §4.4).
        let candidates = self.objects.list_all_ids().await?;
        let scanned = candidates.len();

        // ── 2. Snapshot the lease clock ──────────────────────────────────────
        // Lease store is millisecond-based; the pass clock is seconds.
        let now_ms = now_unix_secs.saturating_mul(1000);

        // ── 3. Collect roots (committed ∪ prepared ∪ live-lease) ─────────────
        let mut roots: Vec<ObjectId> = Vec::new();
        let mut committed_roots = 0usize;
        let mut prepared_roots = 0usize;
        let mut lease_roots = 0usize;

        // (a) Committed ref targets across every hosted shard (leader-linearized).
        for (_shard, targets) in self.cluster_refs.committed_targets_by_shard().await? {
            committed_roots += targets.len();
            roots.extend(targets);
        }

        // (b) Prepared 2PC staged targets — pin objects staged by in-flight
        //     cross-shard commits that no committed ref yet references (spec §4.2
        //     source 2; the load-bearing 4b interaction).
        for (_shard, locks) in self.cluster_refs.prepared_locks_by_shard().await? {
            for (_name, intent) in locks {
                roots.push(intent.staged_target);
                prepared_roots += 1;
            }
        }

        // (c) Live-lease workspace roots. A tombstoned/expired lease is excluded
        //     by `live()`, so its workspace refs are NOT roots (spec §4.2 source 3).
        for lease in self.leases.live(now_ms).await? {
            let prefix = format!("refs/workspaces/{}/", lease.id.to_hex());
            for (_name, entry) in self.cluster_refs.list(&prefix).await? {
                roots.push(entry.target);
                lease_roots += 1;
            }
        }

        // ── 4. Mark ──────────────────────────────────────────────────────────
        let reachable: HashSet<ObjectId> = graph::reachable_from(&self.objects, roots).await?;
        let reachable_count = reachable.len();

        // ── 5. Grace-fenced sweep ────────────────────────────────────────────
        // Delete each candidate that is unreachable AND older than `grace`. A
        // younger unreachable candidate is RETAINED (counted in skipped_grace),
        // closing the resurrection race. Deletes are idempotent; a crash
        // mid-sweep is harmless (the next pass re-derives candidates).
        let grace_secs = self.grace.as_secs();
        let mut reclaimed = 0usize;
        let mut bytes_freed = 0u64;
        let mut skipped_grace = 0usize;
        for id in candidates {
            if reachable.contains(&id) {
                continue;
            }
            let path = self.objects.object_path(&id);
            // Read mtime as seconds-since-epoch; on any metadata error treat the
            // object as too-young-to-sweep (fail-safe: never delete on a stat
            // error — over-keeping is always safe).
            let mtime_secs = match tokio::fs::metadata(&path).await {
                Ok(meta) => match meta.modified() {
                    Ok(m) => m
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(now_unix_secs), // pre-epoch mtime ⇒ treat as "now" (kept)
                    Err(_) => now_unix_secs, // platform without mtime ⇒ keep
                },
                Err(_) => {
                    // Missing/raced file: nothing to free, nothing to retain.
                    continue;
                }
            };
            // age = now − mtime, saturating so a future mtime (skew) ⇒ age 0 ⇒ kept.
            let age_secs = now_unix_secs.saturating_sub(mtime_secs);
            if age_secs < grace_secs {
                skipped_grace += 1;
                continue;
            }
            // Past grace and unreachable → sweep. Stat the size first to attribute
            // freed bytes (a concurrent delete contributes 0 — idempotent).
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
            skipped_grace,
        };

        tracing::info!(
            committed_roots,
            prepared_roots,
            lease_roots,
            candidates = stats.scanned,
            reachable = stats.reachable,
            reclaimed = stats.reclaimed,
            bytes_freed = stats.bytes_freed,
            skipped_grace = stats.skipped_grace,
            grace_secs,
            duration_ms = start.elapsed().as_millis(),
            "cluster gc pass complete"
        );

        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use tempfile::TempDir;

    use ledge_core::{ObjectStore, HLC};
    use ledge_object_store::DiskObjectStore;
    use ledge_workspace::lease::LeaseStore;

    use crate::router::ShardId;
    use crate::shard_map::{Replica, ShardMap};
    use crate::testkit::MultiShardCluster;

    /// A 2-shard × 3-node all-on-all cluster + node 1's ref store, plus a
    /// FRESH node-local disk object store and lease store (the testkit replicas
    /// carry no DiskObjectStore — Reconciliation #6). Returns everything the
    /// ClusterGc needs.
    async fn gc_harness() -> (
        TempDir,
        MultiShardCluster,
        Arc<crate::ref_store::ClusterRefStore>,
        Arc<DiskObjectStore>,
        Arc<LeaseStore>,
        Arc<HLC>,
    ) {
        let cluster = MultiShardCluster::start(2, &[1, 2, 3]).await;
        let map = ShardMap::from_entries([
            (
                ShardId(0),
                vec![
                    Replica { node_id: 1, addr: "mem://1".into() },
                    Replica { node_id: 2, addr: "mem://2".into() },
                    Replica { node_id: 3, addr: "mem://3".into() },
                ],
            ),
            (
                ShardId(1),
                vec![
                    Replica { node_id: 1, addr: "mem://1".into() },
                    Replica { node_id: 2, addr: "mem://2".into() },
                    Replica { node_id: 3, addr: "mem://3".into() },
                ],
            ),
        ])
        .unwrap();
        let fwd = Arc::new(crate::forward::InMemoryForwarder::new());
        fwd.set_map(map.clone());
        let store1 = cluster.cluster_ref_store_hosting(1, &map, fwd.clone());
        fwd.register(1, Arc::new(crate::ref_store::StoreApplier(store1.clone())));

        let dir = TempDir::new().unwrap();
        let hlc = Arc::new(HLC::new());
        let objects = Arc::new(DiskObjectStore::new(dir.path().to_path_buf()).unwrap());
        let leases = Arc::new(LeaseStore::open(dir.path().to_path_buf(), hlc.clone()).unwrap());
        (dir, cluster, store1, objects, leases, hlc)
    }

    async fn write_blob(store: &DiskObjectStore, content: &[u8]) -> ledge_core::ObjectId {
        store
            .write_git_object(3, Bytes::copy_from_slice(content))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn cluster_gc_reclaims_orphan() {
        let (_dir, _cluster, store1, objects, leases, _hlc) = gc_harness().await;
        let orphan = write_blob(&objects, b"orphan").await;

        // Grace = 0 so the just-written orphan is immediately sweepable.
        let gc = ClusterGc::new(store1.clone(), leases.clone(), objects.clone(), Duration::ZERO);
        // now far in the future so mtime age >= grace(0) trivially.
        let stats = gc.run(4_000_000_000).await.unwrap();

        assert_eq!(stats.reclaimed, 1, "the lone orphan is reclaimed");
        assert_eq!(stats.scanned, 1);
        assert_eq!(stats.skipped_grace, 0);
        assert!(!objects.exists(orphan).await.unwrap(), "orphan gone after sweep");
    }
}
