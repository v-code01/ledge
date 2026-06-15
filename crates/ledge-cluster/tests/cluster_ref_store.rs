//! Task 4 integration tests: `ClusterRefStore` + `ClusterLeaseStore` over a
//! 2-shard × 3-node in-process Raft cluster (6 Raft groups, one shared network
//! registry). Each test asserts an end-to-end property of the clustered seam:
//! routing, linearizable get, shard isolation, CAS, delete, cross-shard list,
//! merged snapshot, `dyn RefStore` object-safety, lease lifecycle, and
//! replication completeness.
//!
//! Determinism: reads use linearizable mode (a metrics-driven `ensure_linearizable`
//! barrier) and replica convergence is observed via bounded metrics polling —
//! no fixed sleeps gate correctness.

use ledge_cluster::testkit::MultiShardCluster;
use ledge_cluster::ConsistencyMode;
use ledge_core::{LedgeError, ObjectId, RefName, RefStore};
use ledge_workspace::{id::WorkspaceId, lease::Lease};

/// Content-addressed-ish object id from a byte fill (cheap, distinct per byte).
fn oid(b: u8) -> ObjectId {
    ObjectId::from_bytes([b; 32])
}

fn name(s: &str) -> RefName {
    RefName::new(s).unwrap()
}

// ---------------------------------------------------------------------------
// Step 4.1 — construct & route
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_ref_store_routes_update_to_owning_shard() {
    let h = MultiShardCluster::start(2, &[1, 2, 3]).await;
    let store = h.cluster_ref_store(1);

    let n = name("refs/workspaces/acme/main");
    let target = oid(0xa1);
    let entry = store.update(&n, target, None).await.unwrap();
    assert_eq!(entry.target, target);

    let owning = h.router().shard_for(n.as_str());
    let other = if owning.0 == 0 {
        ledge_cluster::ShardId(1)
    } else {
        ledge_cluster::ShardId(0)
    };
    assert!(
        h.shard_sm_has_ref(owning, &n).await,
        "owning shard must have the ref"
    );
    assert!(
        !h.shard_sm_has_ref(other, &n).await,
        "other shard must NOT have the ref"
    );
}

// ---------------------------------------------------------------------------
// Step 4.2 — linearizable get reflects a committed update on every node
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_get_reflects_update_on_all_nodes_of_shard() {
    let h = MultiShardCluster::start(2, &[1, 2, 3]).await;
    let n = name("refs/workspaces/acme/main");
    let target = oid(0xa2);

    h.cluster_ref_store(1)
        .update(&n, target, None)
        .await
        .unwrap();

    for node in [1, 2, 3] {
        let got = h
            .cluster_ref_store(node)
            .get(&n)
            .await
            .unwrap()
            .expect("ref must be visible");
        assert_eq!(got.target, target, "node {node} must see committed ref");
    }
}

// ---------------------------------------------------------------------------
// Step 4.3 — shard isolation
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_in_shard_a_absent_from_shard_b() {
    let h = MultiShardCluster::start(2, &[1, 2, 3]).await;
    let (name_a, name_b) = h.two_names_on_distinct_shards();

    h.cluster_ref_store(1)
        .update(&name_a, oid(0x11), None)
        .await
        .unwrap();

    let shard_a = h.router().shard_for(name_a.as_str());
    let shard_b = h.router().shard_for(name_b.as_str());
    assert_ne!(shard_a, shard_b);
    assert!(h.shard_sm_has_ref(shard_a, &name_a).await);
    assert!(
        !h.shard_sm_has_ref(shard_b, &name_a).await,
        "shard B's log/SM must never see shard A's ref"
    );
}

// ---------------------------------------------------------------------------
// Step 4.4 — CAS conflict surfaces through the cluster store
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_cas_conflict_surfaces() {
    let h = MultiShardCluster::start(2, &[1, 2, 3]).await;
    let n = name("refs/workspaces/acme/main");
    let a = oid(0xaa);
    let b = oid(0xbb);
    let store = h.cluster_ref_store(1);

    let e1 = store.update(&n, a, None).await.unwrap();
    // CAS with the WRONG expected -> Conflict carrying the current entry.
    let err = store.update(&n, b, Some(b)).await.unwrap_err();
    match err {
        LedgeError::Conflict { current } => assert_eq!(current.target, e1.target),
        other => panic!("expected Conflict, got {other:?}"),
    }
    // Correct CAS succeeds.
    let e2 = store.update(&n, b, Some(a)).await.unwrap();
    assert_eq!(e2.target, b);
}

// ---------------------------------------------------------------------------
// Step 4.5 — delete
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_delete_removes_ref() {
    let h = MultiShardCluster::start(2, &[1, 2, 3]).await;
    let n = name("refs/workspaces/acme/topic");
    let a = oid(0x55);
    let store = h.cluster_ref_store(1);

    let e = store.update(&n, a, None).await.unwrap();
    // wrong expected -> error, ref not deleted.
    assert!(store.delete(&n, oid(0x99)).await.is_err());
    assert!(store.get(&n).await.unwrap().is_some());
    // correct expected -> deleted.
    store.delete(&n, e.target).await.unwrap();
    assert!(store.get(&n).await.unwrap().is_none());
}

// ---------------------------------------------------------------------------
// Step 4.6 — list merges across shards
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_list_merges_across_shards() {
    let h = MultiShardCluster::start(2, &[1, 2, 3]).await;
    let store = h.cluster_ref_store(1);
    let names = h.names_spanning_both_shards(6);
    for n in &names {
        store.update(n, oid(0x77), None).await.unwrap();
    }

    // Broad prefix -> fan out to all shards, merged + sorted.
    let mut listed: Vec<String> = store
        .list("refs/")
        .await
        .unwrap()
        .into_iter()
        .map(|(n, _)| n.as_str().to_string())
        .collect();
    listed.sort();
    let mut expected: Vec<String> = names.iter().map(|n| n.as_str().to_string()).collect();
    expected.sort();
    assert_eq!(listed, expected, "broad list must merge all shards");

    // Pinned prefix -> single shard, subset only, all under the prefix.
    let pinned = {
        let s = names[0].as_str();
        // refs/workspaces/<tenant>/ pins one shard.
        let parts: Vec<&str> = s.split('/').collect();
        format!("refs/workspaces/{}/", parts[2])
    };
    let sub = store.list(&pinned).await.unwrap();
    assert!(!sub.is_empty(), "pinned list must find the workspace ref");
    assert!(sub.iter().all(|(n, _)| n.as_str().starts_with(&pinned)));
}

// ---------------------------------------------------------------------------
// Step 4.7 — snapshot merges all shards
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_snapshot_merges_all_shards() {
    let h = MultiShardCluster::start(2, &[1, 2, 3]).await;
    let store = h.cluster_ref_store(1);
    let names = h.names_spanning_both_shards(4);
    for n in &names {
        store.update(n, oid(0x33), None).await.unwrap();
    }
    // Ensure node 1's local replicas have applied before the (local) snapshot.
    for n in &names {
        let shard = h.router().shard_for(n.as_str());
        h.await_applied(shard, n).await;
    }

    let snap = store.snapshot();
    for n in &names {
        assert!(
            snap.get(n).is_some(),
            "snapshot must contain {}",
            n.as_str()
        );
    }
    // list on the snapshot also merges both shards.
    assert_eq!(snap.list("refs/").len(), names.len());
}

// ---------------------------------------------------------------------------
// Step 4.8 — ClusterRefStore satisfies dyn RefStore (the §2.2 seam)
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_ref_store_is_dyn_ref_store() {
    let h = MultiShardCluster::start(2, &[1, 2, 3]).await;
    let dyn_store: std::sync::Arc<dyn RefStore> = std::sync::Arc::new(h.cluster_ref_store(1));
    let n = name("refs/workspaces/acme/main");
    // Exercises the trait object: absent ref -> None.
    assert!(dyn_store.get(&n).await.unwrap().is_none());
    // And a mutation + read through the trait object round-trips.
    dyn_store.update(&n, oid(0x42), None).await.unwrap();
    assert_eq!(dyn_store.get(&n).await.unwrap().unwrap().target, oid(0x42));
}

// ---------------------------------------------------------------------------
// Step 4.9 — ClusterLeaseStore put/get/tombstone + co-location + live/expired
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_lease_put_get_tombstone() {
    let h = MultiShardCluster::start(2, &[1, 2, 3]).await;
    let leases = h.cluster_lease_store(1);
    let ws = WorkspaceId::from_bytes([7u8; 16]);
    let lease = Lease {
        id: ws,
        source_refs: vec!["refs/heads/main".into()],
        created_at_ms: 1,
        expires_at_ms: 10_000,
        hlc: 5,
        generation: 1,
        tenant_id: "root".to_string(),
    };

    leases.put(lease.clone()).await.unwrap();
    let got = leases.get(&ws).await.unwrap().expect("lease present");
    assert_eq!(got.id, ws);

    // Lease co-locates with the workspace's refs on the SAME shard (D5):
    // refs under refs/workspaces/<hex>/... route to shard_for_workspace(ws).
    let ref_name = name(&format!("refs/workspaces/{}/main", ws.to_hex()));
    assert_eq!(
        h.router().shard_for_workspace(&ws),
        h.router().shard_for(ref_name.as_str()),
        "lease and workspace refs must share a shard"
    );

    leases.tombstone(&ws).await.unwrap();
    assert!(leases.get(&ws).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_lease_live_and_expired() {
    let h = MultiShardCluster::start(2, &[1, 2, 3]).await;
    let leases = h.cluster_lease_store(1);

    // Two workspaces; pick ids so they may land on either shard — live/expired
    // fan out across all shards, so placement does not matter.
    let ws_live = WorkspaceId::from_bytes([1u8; 16]);
    let ws_dead = WorkspaceId::from_bytes([2u8; 16]);
    leases
        .put(Lease {
            id: ws_live,
            source_refs: vec![],
            created_at_ms: 0,
            expires_at_ms: 1_000,
            hlc: 1,
            generation: 1,
            tenant_id: "root".to_string(),
        })
        .await
        .unwrap();
    leases
        .put(Lease {
            id: ws_dead,
            source_refs: vec![],
            created_at_ms: 0,
            expires_at_ms: 100,
            hlc: 1,
            generation: 1,
            tenant_id: "root".to_string(),
        })
        .await
        .unwrap();

    // now = 500: ws_live (exp 1000) live, ws_dead (exp 100) expired.
    let live: Vec<_> = leases
        .live(500)
        .await
        .unwrap()
        .into_iter()
        .map(|l| l.id)
        .collect();
    let expired: Vec<_> = leases
        .expired(500)
        .await
        .unwrap()
        .into_iter()
        .map(|l| l.id)
        .collect();
    assert!(live.contains(&ws_live), "ws_live must be live at now=500");
    assert!(!live.contains(&ws_dead));
    assert!(
        expired.contains(&ws_dead),
        "ws_dead must be expired at now=500"
    );
    assert!(!expired.contains(&ws_live));
}

// ---------------------------------------------------------------------------
// Step 4.10 — replication completeness end-to-end
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_update_replicates_to_all_replicas_of_shard() {
    let h = MultiShardCluster::start(2, &[1, 2, 3]).await;
    let n = name("refs/workspaces/acme/main");
    let target = oid(0xab);
    h.cluster_ref_store(1)
        .update(&n, target, None)
        .await
        .unwrap();

    let shard = h.router().shard_for(n.as_str());
    h.await_applied(shard, &n).await;
    for replica in h.replicas_of(shard) {
        assert_eq!(
            replica.sm.applied_ref(n.as_str()).await.unwrap().target,
            target,
            "replica {} missing committed ref",
            replica.node
        );
    }
}

// ---------------------------------------------------------------------------
// Stale-mode read: local SM, no linearizability barrier (D2 toggle).
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Phase 4c Task 1 — committed_targets_by_shard: leader-linearized committed
// ref targets for every LOCALLY-hosted shard; non-hosted shards excluded.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_targets_by_shard_returns_hosted_shard_targets() {
    use ledge_cluster::forward::RefOpForwarder;
    use ledge_cluster::router::ShardId;
    use ledge_cluster::shard_map::{Replica, ShardMap};
    use ledge_cluster::testkit::MultiShardCluster;
    use ledge_core::{ObjectId, RefStore};

    fn oid(n: u8) -> ObjectId {
        let mut b = [0u8; 32];
        b[31] = n;
        ObjectId::from_bytes(b)
    }

    // 2 shards × 3 nodes; node 1 hosts BOTH shards, node 2 hosts ONLY shard 0.
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
                    node_id: 3,
                    addr: "mem://3".into(),
                },
                // node 2 is intentionally NOT a member of shard 1
            ],
        ),
    ])
    .unwrap();
    let fwd = std::sync::Arc::new(ledge_cluster::forward::InMemoryForwarder::new());
    fwd.set_map(map.clone());

    // node 1 hosts both shards.
    let store1 = cluster.cluster_ref_store_hosting(1, &map, fwd.clone());
    fwd.register(
        1,
        std::sync::Arc::new(ledge_cluster::ref_store::StoreApplier(store1.clone())),
    );
    // node 2 hosts only shard 0.
    let store2 = cluster.cluster_ref_store_hosting(2, &map, fwd.clone());
    fwd.register(
        2,
        std::sync::Arc::new(ledge_cluster::ref_store::StoreApplier(store2.clone())),
    );

    // Pick two DURABLE names on DISTINCT shards so we commit one ref per shard.
    // committed_targets_by_shard returns durable roots only (workspace refs are
    // lease-gated, deliberately excluded), so the fixtures must be durable refs.
    let (a, b) = cluster.two_durable_names_on_distinct_shards();
    let a_shard = cluster.router().shard_for(a.as_str());
    let b_shard = cluster.router().shard_for(b.as_str());
    // Commit a ref on each shard through node 1 (which hosts both).
    store1.update(&a, oid(10), None).await.unwrap();
    store1.update(&b, oid(20), None).await.unwrap();

    // node 1 hosts BOTH shards → sees BOTH committed targets.
    let by_shard1 = store1.committed_targets_by_shard().await.unwrap();
    let all1: std::collections::HashSet<ObjectId> = by_shard1
        .iter()
        .flat_map(|(_, t)| t.iter().copied())
        .collect();
    assert!(all1.contains(&oid(10)), "shard-{a_shard:?} target present");
    assert!(all1.contains(&oid(20)), "shard-{b_shard:?} target present");

    // node 2 hosts ONLY shard 0 → it sees ONLY shard 0's target, never shard 1's.
    let shards2: std::collections::HashSet<ShardId> = store2
        .committed_targets_by_shard()
        .await
        .unwrap()
        .into_iter()
        .map(|(s, _)| s)
        .collect();
    assert!(shards2.contains(&ShardId(0)));
    assert!(
        !shards2.contains(&ShardId(1)),
        "node 2 must not report a non-hosted shard"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_stale_get_reads_local_replica() {
    let h = MultiShardCluster::start(2, &[1, 2, 3]).await;
    let n = name("refs/workspaces/acme/main");
    let target = oid(0xcd);
    h.cluster_ref_store(1)
        .update(&n, target, None)
        .await
        .unwrap();

    let shard = h.router().shard_for(n.as_str());
    h.await_applied(shard, &n).await; // all replicas applied

    let stale = h.cluster_ref_store(2).with_mode(ConsistencyMode::Stale);
    let got = stale
        .get(&n)
        .await
        .unwrap()
        .expect("stale read sees applied ref");
    assert_eq!(got.target, target);
}
