//! Phase 4c distributed-GC safety suite (spec §6) over the in-process
//! MultiShardCluster. Deterministic: no fixed sleeps for correctness — the
//! cluster's leader election is awaited by `start`, and ref commits are
//! synchronous through the leader, so the root set is observable immediately.
#![cfg(feature = "testkit")]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tempfile::TempDir;

use ledge_cluster::forward::{ClusterOp, InMemoryForwarder, RefOpForwarder, RefOpResponse};
use ledge_cluster::gc::ClusterGc;
use ledge_cluster::ref_store::{ClusterRefStore, StoreApplier};
use ledge_cluster::router::ShardId;
use ledge_cluster::shard_map::{Replica, ShardMap};
use ledge_cluster::testkit::MultiShardCluster;
use ledge_core::{ObjectId, ObjectStore, RefName, RefStore, HLC};
use ledge_object_store::DiskObjectStore;
use ledge_raft::TxnId;
use ledge_workspace::id::WorkspaceId;
use ledge_workspace::lease::{Lease, LeaseStore};

/// Lowercase hex (ledge-cluster has no `hex` dep; mirror gc.rs's local helper).
fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// 2 shards × 3 nodes, all-on-all placement (node 1 hosts BOTH shards), node 1's
/// ref store, plus a fresh node-local disk + lease store.
async fn harness() -> (
    TempDir,
    MultiShardCluster,
    Arc<ClusterRefStore>,
    Arc<DiskObjectStore>,
    Arc<LeaseStore>,
    Arc<HLC>,
) {
    let cluster = MultiShardCluster::start(2, &[1, 2, 3]).await;
    let map = ShardMap::from_entries([
        (
            ShardId(0),
            vec![
                Replica {
                    node_id: 1,
                    addr: "mem://1".into(),
                },
                Replica {
                    node_id: 2,
                    addr: "mem://2".into(),
                },
                Replica {
                    node_id: 3,
                    addr: "mem://3".into(),
                },
            ],
        ),
        (
            ShardId(1),
            vec![
                Replica {
                    node_id: 1,
                    addr: "mem://1".into(),
                },
                Replica {
                    node_id: 2,
                    addr: "mem://2".into(),
                },
                Replica {
                    node_id: 3,
                    addr: "mem://3".into(),
                },
            ],
        ),
    ])
    .unwrap();
    let fwd = Arc::new(InMemoryForwarder::new());
    fwd.set_map(map.clone());
    let store1 = cluster.cluster_ref_store_hosting(1, &map, fwd.clone());
    fwd.register(1, Arc::new(StoreApplier(store1.clone())));

    let dir = TempDir::new().unwrap();
    let hlc = Arc::new(HLC::new());
    let objects = Arc::new(DiskObjectStore::new(dir.path().to_path_buf()).unwrap());
    let leases = Arc::new(LeaseStore::open(dir.path().to_path_buf(), hlc.clone()).unwrap());
    (dir, cluster, store1, objects, leases, hlc)
}

async fn write_blob(store: &DiskObjectStore, content: &[u8]) -> ObjectId {
    store
        .write_git_object(3, Bytes::copy_from_slice(content))
        .await
        .unwrap()
}

/// blob -> tree -> commit (git on-disk wire formats); returns (blob, tree, commit).
/// Verbatim from ledge-workspace/src/gc.rs builders (reachable_from resolves
/// children by git SHA-1, so the exact bytes matter).
async fn build_graph(store: &DiskObjectStore) -> (ObjectId, ObjectId, ObjectId) {
    let blob = write_blob(store, b"hello reachable world").await;
    let blob_sha1 = store.sha1_of(blob).await.unwrap();
    let mut body = Vec::new();
    body.extend_from_slice(b"100644 file.txt\0");
    body.extend_from_slice(&blob_sha1);
    let tree = store.write_git_object(2, Bytes::from(body)).await.unwrap();
    let tree_sha1 = store.sha1_of(tree).await.unwrap();
    let commit_body = format!(
        "tree {}\nauthor a <a@b> 0 +0000\ncommitter a <a@b> 0 +0000\n\nmsg\n",
        hex_lower(&tree_sha1)
    );
    let commit = store
        .write_git_object(1, Bytes::from(commit_body.into_bytes()))
        .await
        .unwrap();
    (blob, tree, commit)
}

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

// 1 ── Cross-shard liveness: an object reachable from EITHER co-hosted shard's
//      refs survives. A single-shard-only mark would delete the other shard's
//      objects — this test discriminates.
#[tokio::test]
async fn cross_shard_liveness_keeps_objects_from_either_shard() {
    let (_dir, cluster, store1, objects, leases, _hlc) = harness().await;
    // Two graphs; one committed on each distinct shard.
    let (b0, t0, c0) = build_graph(&objects).await;
    // A SECOND, distinct graph for shard 1.
    let blob1 = write_blob(&objects, b"second graph blob").await;
    let blob1_sha1 = objects.sha1_of(blob1).await.unwrap();
    let mut tbody = Vec::new();
    tbody.extend_from_slice(b"100644 other.txt\0");
    tbody.extend_from_slice(&blob1_sha1);
    let tree1 = objects
        .write_git_object(2, Bytes::from(tbody))
        .await
        .unwrap();
    let tree1_sha1 = objects.sha1_of(tree1).await.unwrap();
    let cbody = format!(
        "tree {}\nauthor a <a@b> 0 +0000\ncommitter a <a@b> 0 +0000\n\nmsg2\n",
        hex_lower(&tree1_sha1)
    );
    let c1 = objects
        .write_git_object(1, Bytes::from(cbody.into_bytes()))
        .await
        .unwrap();

    // An orphan referenced by NO shard.
    let orphan = write_blob(&objects, b"orphan").await;

    let (a, b) = cluster.two_durable_names_on_distinct_shards();
    // Sanity: the two refs really do live on distinct shards, so this test
    // genuinely exercises the cross-shard root union (not two refs on one shard).
    assert_ne!(
        cluster.router().shard_for(a.as_str()),
        cluster.router().shard_for(b.as_str()),
        "test setup must place refs on distinct shards"
    );
    store1.update(&a, c0, None).await.unwrap(); // shard A ref → first graph
    store1.update(&b, c1, None).await.unwrap(); // shard B ref → second graph

    let gc = ClusterGc::new(
        store1.clone(),
        leases.clone(),
        objects.clone(),
        Duration::ZERO,
        Arc::new(ledge_workspace::UsageMap::default()),
    );
    let stats = gc.run(4_000_000_000).await.unwrap();

    // BOTH graphs survive; only the orphan is reclaimed. If GC marked only ONE
    // shard, one of these objects would be gone and this loop would fail.
    for id in [b0, t0, c0, blob1, tree1, c1] {
        assert!(
            objects.exists(id).await.unwrap(),
            "{id:?} reachable from a hosted shard must survive"
        );
    }
    assert!(
        !objects.exists(orphan).await.unwrap(),
        "the cross-shard orphan is reclaimed"
    );
    assert_eq!(stats.reclaimed, 1, "exactly the orphan");
}

// 2 ── Prepared-intent pinning (the 4b interaction): a staged target with no
//      committed referrer is NOT deleted; after commit it is still present.
#[tokio::test]
async fn prepared_intent_pins_staged_object() {
    let (_dir, cluster, store1, objects, leases, _hlc) = harness().await;
    // The staged object exists on disk (a push wrote it) but no committed ref
    // points at it yet — only a prepared lock does.
    let staged = write_blob(&objects, b"staged-by-prepare").await;
    let (a, _b) = cluster.two_durable_names_on_distinct_shards();
    let a_shard = cluster.router().shard_for(a.as_str());
    let coord_shard = ShardId(0);
    let txn = TxnId::from_bytes([7u8; 16]);

    let vote = store1
        .op_on_shard(
            a_shard,
            ClusterOp::Prepare {
                txn_id: txn,
                coord_shard: coord_shard.0,
                name: a.as_str().to_string(),
                target_bytes: *staged.as_bytes(),
                expected_bytes: None,
            },
        )
        .await
        .unwrap();
    assert!(matches!(vote, RefOpResponse::Vote(true)));

    // Discriminator: at this point NO committed ref names `staged` — only the
    // prepared lock does. A GC that ignored prepared intents would free it.
    assert!(
        store1.get(&a).await.unwrap().is_none(),
        "no committed ref yet — staged target is rooted SOLELY by the prepared lock"
    );

    // GC while prepared-but-not-committed: the staged object is rooted by the
    // prepared intent → NOT deleted.
    let gc = ClusterGc::new(
        store1.clone(),
        leases.clone(),
        objects.clone(),
        Duration::ZERO,
        Arc::new(ledge_workspace::UsageMap::default()),
    );
    gc.run(4_000_000_000).await.unwrap();
    assert!(
        objects.exists(staged).await.unwrap(),
        "prepared staged target must be pinned"
    );

    // Commit the intent; the now-committed object is still present and reachable.
    store1
        .op_on_shard(
            a_shard,
            ClusterOp::CommitPrepared {
                txn_id: txn,
                name: a.as_str().to_string(),
            },
        )
        .await
        .unwrap();
    assert_eq!(store1.get(&a).await.unwrap().unwrap().target, staged);
    let gc2 = ClusterGc::new(
        store1.clone(),
        leases.clone(),
        objects.clone(),
        Duration::ZERO,
        Arc::new(ledge_workspace::UsageMap::default()),
    );
    gc2.run(4_000_000_001).await.unwrap();
    assert!(
        objects.exists(staged).await.unwrap(),
        "committed target still present"
    );
}

// 3 ── Abort then reclaim: prepare → abort → a GC pass past grace reclaims the
//      now-unreferenced staged object.
#[tokio::test]
async fn abort_then_reclaim_releases_staged_object() {
    let (_dir, cluster, store1, objects, leases, _hlc) = harness().await;
    let staged = write_blob(&objects, b"staged-then-aborted").await;
    let (a, _b) = cluster.two_durable_names_on_distinct_shards();
    let a_shard = cluster.router().shard_for(a.as_str());
    let coord_shard = ShardId(0);
    let txn = TxnId::from_bytes([8u8; 16]);

    store1
        .op_on_shard(
            a_shard,
            ClusterOp::Prepare {
                txn_id: txn,
                coord_shard: coord_shard.0,
                name: a.as_str().to_string(),
                target_bytes: *staged.as_bytes(),
                expected_bytes: None,
            },
        )
        .await
        .unwrap();
    store1
        .op_on_shard(
            a_shard,
            ClusterOp::AbortPrepared {
                txn_id: txn,
                name: a.as_str().to_string(),
            },
        )
        .await
        .unwrap();

    // Now no committed ref AND no prepared lock references `staged`. A grace-0
    // pass reclaims it.
    let gc = ClusterGc::new(
        store1.clone(),
        leases.clone(),
        objects.clone(),
        Duration::ZERO,
        Arc::new(ledge_workspace::UsageMap::default()),
    );
    let stats = gc.run(4_000_000_000).await.unwrap();
    assert!(
        !objects.exists(staged).await.unwrap(),
        "aborted staged target is reclaimed"
    );
    assert_eq!(stats.reclaimed, 1);
}

// 4 ── Grace fence: a fresh-mtime unreferenced object is KEPT; the same object
//      with an old mtime is SWEPT. Inject now_unix_secs to drive both sides.
#[tokio::test]
async fn grace_fence_keeps_fresh_sweeps_old() {
    let (_dir, _cluster, store1, objects, leases, _hlc) = harness().await;
    let orphan = write_blob(&objects, b"fresh orphan").await;

    // mtime ≈ real now. With now_unix_secs ≈ real now and grace 1h, age < grace.
    let real_now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let one_hour = Duration::from_secs(3600);

    let gc = ClusterGc::new(
        store1.clone(),
        leases.clone(),
        objects.clone(),
        one_hour,
        Arc::new(ledge_workspace::UsageMap::default()),
    );
    let kept = gc.run(real_now).await.unwrap();
    assert_eq!(kept.reclaimed, 0, "a fresh orphan is within grace and kept");
    assert_eq!(kept.skipped_grace, 1, "retained solely by grace");
    assert!(objects.exists(orphan).await.unwrap());

    // Advance the pass clock far past grace (mtime now ≪ now_unix_secs).
    let swept = gc.run(real_now + 10_000).await.unwrap();
    assert_eq!(swept.reclaimed, 1, "past grace, the orphan is reclaimed");
    assert_eq!(swept.skipped_grace, 0);
    assert!(!objects.exists(orphan).await.unwrap());
}

// 5 ── Freeze guard: an object written AFTER the candidate freeze is never swept
//      by that pass. Deterministic structural stand-in (same shape as the
//      single-node gc_object_written_after_freeze_survives test).
#[tokio::test]
async fn freeze_guard_excludes_post_freeze_writes() {
    let (_dir, _cluster, store1, objects, leases, _hlc) = harness().await;

    // Replicate run()'s freeze step: capture candidates, then write a NEW orphan.
    let candidates_before: HashSet<ObjectId> =
        objects.list_all_ids().await.unwrap().into_iter().collect();
    let post_freeze = write_blob(&objects, b"written after the freeze").await;
    assert!(
        !candidates_before.contains(&post_freeze),
        "an object written after list_all_ids is never a sweep candidate (freeze guard)"
    );

    // And an end-to-end pass that began before the write leaves it intact: run a
    // grace-0 GC, THEN write — the just-written object survives.
    let gc = ClusterGc::new(
        store1.clone(),
        leases.clone(),
        objects.clone(),
        Duration::ZERO,
        Arc::new(ledge_workspace::UsageMap::default()),
    );
    gc.run(4_000_000_000).await.unwrap();
    let late = write_blob(&objects, b"written after a completed pass").await;
    assert!(
        objects.exists(late).await.unwrap(),
        "post-pass write survives"
    );
}

// 6 ── Crash idempotence: a re-run equals a single pass (no double-delete error,
//      no live loss). Re-running after a clean pass reclaims nothing new.
#[tokio::test]
async fn crash_idempotent_rerun_equals_single_pass() {
    let (_dir, cluster, store1, objects, leases, _hlc) = harness().await;
    let (b, t, c) = build_graph(&objects).await;
    let orphan = write_blob(&objects, b"orphan").await;
    let (a, _b) = cluster.two_durable_names_on_distinct_shards();
    store1.update(&a, c, None).await.unwrap();

    let gc = ClusterGc::new(
        store1.clone(),
        leases.clone(),
        objects.clone(),
        Duration::ZERO,
        Arc::new(ledge_workspace::UsageMap::default()),
    );
    let first = gc.run(4_000_000_000).await.unwrap();
    assert_eq!(first.reclaimed, 1, "the orphan is reclaimed once");

    // Second pass: deleted objects are no longer candidates → nothing new.
    let second = gc.run(4_000_000_001).await.unwrap();
    assert_eq!(
        second.reclaimed, 0,
        "re-run reclaims nothing new (idempotent)"
    );
    // Reachable graph intact across both passes.
    for id in [b, t, c] {
        assert!(objects.exists(id).await.unwrap());
    }
    assert!(!objects.exists(orphan).await.unwrap());
}

// 7 ── Live-lease workspace roots keep their objects; an expired lease does not.
#[tokio::test]
async fn live_lease_workspace_roots_kept_expired_reclaimed() {
    let (_dir, _cluster, store1, objects, leases, hlc) = harness().await;
    let (b, t, c) = build_graph(&objects).await;
    let id = WorkspaceId::generate(&hlc);
    let ref_name = format!("refs/workspaces/{}/heads/main", id.to_hex());
    store1
        .update(&RefName::new(&ref_name).unwrap(), c, None)
        .await
        .unwrap();

    // Live lease: GC at now_secs whose ms is < expiry keeps the trio.
    let now_secs = 1_000_000u64;
    leases
        .put(lease(id, now_secs * 1000 + 1_000_000))
        .await
        .unwrap();
    let gc = ClusterGc::new(
        store1.clone(),
        leases.clone(),
        objects.clone(),
        Duration::ZERO,
        Arc::new(ledge_workspace::UsageMap::default()),
    );
    let kept = gc.run(now_secs).await.unwrap();
    assert_eq!(kept.reclaimed, 0, "live-lease workspace refs root the trio");
    for id in [b, t, c] {
        assert!(objects.exists(id).await.unwrap());
    }

    // Expire the lease (expiry ≤ now_ms) → its refs are not roots → trio reclaimed.
    leases.put(lease(id, now_secs * 1000 - 1)).await.unwrap();
    let gc2 = ClusterGc::new(
        store1.clone(),
        leases.clone(),
        objects.clone(),
        Duration::ZERO,
        Arc::new(ledge_workspace::UsageMap::default()),
    );
    let swept = gc2.run(now_secs).await.unwrap();
    assert_eq!(swept.reclaimed, 3, "expired lease contributes no roots");
}
