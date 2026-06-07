//! Task 9.1 — end-to-end integration over a 2-shard × 3-node in-process cluster,
//! driven entirely through `Arc<dyn RefStore>` (the §2.2 clustering seam) plus a
//! `WorkspaceManager` layered on top of the replicated store.
//!
//! These tests are the close of Phase 3: they exercise the *composed* system
//! (router + per-shard Raft + ClusterRefStore + WorkspaceManager) rather than any
//! single component, asserting the §1 goals — linearizable sharded replication,
//! survival of leader failure, no committed-write loss — at the trait-object
//! boundary the server actually depends on.
//!
//! # Why the in-memory network is the safety-proof vehicle
//! Per spec §6, the in-memory `RaftNetwork` (Task 3) is the deterministic vehicle
//! for the consensus safety proof: no sockets, no serialization jitter, RPCs are
//! direct handle calls, and a "crashed" node is a registry removal that peers see
//! as `Unreachable`. That makes election/replication/failover *reproducible*,
//! which a real socket cluster cannot guarantee in-process. The HTTP transport is
//! covered by the per-RPC round-trip tests (Task 6) and the documented server
//! smoke (Task 9.2); correctness lives here.
//!
//! # Determinism discipline
//! No fixed sleep ever gates a correctness assertion. Leadership and replica
//! convergence are observed by bounded polling on the openraft metrics watch /
//! applied-state read handle (see the `testkit` helpers). The only sleeps are the
//! poll backoffs inside those bounded loops, which fail loudly on timeout.

use std::sync::Arc;
use std::time::Duration;

use ledge_cluster::shard_map::{Replica, ShardMap};
use ledge_cluster::testkit::MultiShardCluster;
use ledge_cluster::ShardId;
use ledge_core::{LedgeError, ObjectId, RefName, RefStore, HLC};
use ledge_workspace::{CommitOutcome, LeaseStore, WorkspaceManager};
use tempfile::TempDir;

/// Deterministic, distinct `ObjectId` from a byte fill (cheap, unique per byte).
fn oid(b: u8) -> ObjectId {
    ObjectId::from_bytes([b; 32])
}

fn name(s: &str) -> RefName {
    RefName::new(s).unwrap()
}

/// The shard NOT equal to `s` in a 2-shard cluster.
fn other_shard(s: ShardId) -> ShardId {
    if s.0 == 0 {
        ShardId(1)
    } else {
        ShardId(0)
    }
}

// ---------------------------------------------------------------------------
// 9.1 (1) — full ref-ops workflow across BOTH shards via `Arc<dyn RefStore>`
// ---------------------------------------------------------------------------
//
// Create several refs that route across both shards (assert distinct shards),
// CAS-update them, exercise the CAS-conflict path, delete one — all through the
// trait object — and assert every node converges to the committed state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_ref_ops_workflow_through_dyn_refstore() {
    let h = MultiShardCluster::start(2, &[1, 2, 3]).await;
    let store: Arc<dyn RefStore> = Arc::new(h.cluster_ref_store(1));

    // Two names the router places on DISTINCT shards.
    let (name_a, name_b) = h.two_names_on_distinct_shards();
    let shard_a = h.router().shard_for(name_a.as_str());
    let shard_b = h.router().shard_for(name_b.as_str());
    assert_ne!(shard_a, shard_b, "workflow must span both shards");

    // --- create (expected = None) on each shard via the trait object ---
    let ea = store.update(&name_a, oid(0xa0), None).await.unwrap();
    let eb = store.update(&name_b, oid(0xb0), None).await.unwrap();
    assert_eq!(ea.target, oid(0xa0));
    assert_eq!(eb.target, oid(0xb0));

    // The refs landed on their owning shards and NOT on the sibling shard.
    assert!(h.shard_sm_has_ref(shard_a, &name_a).await);
    assert!(!h.shard_sm_has_ref(other_shard(shard_a), &name_a).await);
    assert!(h.shard_sm_has_ref(shard_b, &name_b).await);
    assert!(!h.shard_sm_has_ref(other_shard(shard_b), &name_b).await);

    // --- CAS-update each ref to a new target (correct expected) ---
    let ea2 = store
        .update(&name_a, oid(0xa1), Some(oid(0xa0)))
        .await
        .unwrap();
    assert_eq!(ea2.target, oid(0xa1));
    store
        .update(&name_b, oid(0xb1), Some(oid(0xb0)))
        .await
        .unwrap();

    // --- CAS-conflict path: stale `expected` is rejected, no clobber ---
    let err = store
        .update(&name_a, oid(0xff), Some(oid(0xa0))) // stale: current is 0xa1
        .await
        .unwrap_err();
    match err {
        LedgeError::Conflict { current } => assert_eq!(current.target, oid(0xa1)),
        other => panic!("expected Conflict carrying the live entry, got {other:?}"),
    }
    // The rejected write never moved the ref.
    assert_eq!(store.get(&name_a).await.unwrap().unwrap().target, oid(0xa1));

    // --- linearizable read on EVERY node converges to the committed state ---
    for node in [1, 2, 3] {
        let s = h.cluster_ref_store(node);
        assert_eq!(
            s.get(&name_a).await.unwrap().unwrap().target,
            oid(0xa1),
            "node {node} disagrees on name_a"
        );
        assert_eq!(
            s.get(&name_b).await.unwrap().unwrap().target,
            oid(0xb1),
            "node {node} disagrees on name_b"
        );
    }

    // --- delete name_b (correct expected) through the trait object ---
    store.delete(&name_b, oid(0xb1)).await.unwrap();
    for node in [1, 2, 3] {
        assert!(
            h.cluster_ref_store(node).get(&name_b).await.unwrap().is_none(),
            "node {node} still sees deleted name_b"
        );
    }
    // name_a is untouched by the delete on the other shard (isolation).
    assert_eq!(store.get(&name_a).await.unwrap().unwrap().target, oid(0xa1));
}

// ---------------------------------------------------------------------------
// 9.1 (1, linearizability) — concurrent CAS at the ClusterRefStore level
// ---------------------------------------------------------------------------
//
// Two concurrent `update`s with the SAME `expected` against one cluster store:
// Raft serializes them into a single log, so exactly one applies (Ok) and the
// other observes the moved value (Conflict). This lifts the Task-3 raw-Raft
// CAS-linearizability proof up to the `Arc<dyn RefStore>` seam.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_cas_is_linearizable_through_dyn_refstore() {
    let h = MultiShardCluster::start(2, &[1, 2, 3]).await;
    let store: Arc<dyn RefStore> = Arc::new(h.cluster_ref_store(1));
    let n = name("refs/workspaces/acme/main");

    // Seed the contended ref at a known base.
    let base = oid(0x01);
    store.update(&n, base, None).await.unwrap();

    // Two contending CAS updates, identical `expected = base`, distinct targets.
    let s1 = Arc::clone(&store);
    let s2 = Arc::clone(&store);
    let n1 = n.clone();
    let n2 = n.clone();
    let (a, b) = tokio::join!(
        async move { s1.update(&n1, oid(0xaa), Some(base)).await },
        async move { s2.update(&n2, oid(0xbb), Some(base)).await },
    );

    let oks = [&a, &b].iter().filter(|r| r.is_ok()).count();
    let conflicts = [&a, &b]
        .iter()
        .filter(|r| matches!(r, Err(LedgeError::Conflict { .. })))
        .count();
    assert_eq!(oks, 1, "exactly one CAS winner: a={a:?} b={b:?}");
    assert_eq!(conflicts, 1, "exactly one CAS loser: a={a:?} b={b:?}");

    // The committed value is whichever winner applied; both contenders agree.
    let winner = store.get(&n).await.unwrap().unwrap().target;
    assert!(
        winner == oid(0xaa) || winner == oid(0xbb),
        "committed value must be one of the two contenders, got {winner:?}"
    );
}

// ---------------------------------------------------------------------------
// 9.1 (2) — leader failover mid-workload, no committed-write loss
// ---------------------------------------------------------------------------
//
// Drive a stream of updates on shard 0 through the ClusterRefStore; mid-stream,
// crash shard 0's leader; confirm a NEW leader is elected, every previously
// `Ok`-acknowledged ref survives on the survivors (leader-completeness /
// durability), post-failover writes succeed, and shard 1 is entirely
// undisturbed (per-shard fault independence).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failover_mid_workload_no_data_loss() {
    let h = MultiShardCluster::start(2, &[1, 2, 3]).await;
    // Build the store on a node we will NOT kill, so its local reads stay valid
    // and `leader_of` can resolve the post-failover leader from a live handle.
    let store = h.cluster_ref_store(2);

    // Pick names that all route to shard 0 (the shard we will fail over) and one
    // name on shard 1 (the bystander that must stay untouched).
    let mut s0_names: Vec<RefName> = Vec::new();
    let mut s1_name: Option<RefName> = None;
    let mut i = 0u32;
    while s0_names.len() < 5 || s1_name.is_none() {
        let n = name(&format!("refs/workspaces/fo{i}/main"));
        match h.router().shard_for(n.as_str()) {
            ShardId(0) if s0_names.len() < 5 => s0_names.push(n),
            ShardId(1) if s1_name.is_none() => s1_name = Some(n),
            _ => {}
        }
        i += 1;
    }
    let s1_name = s1_name.unwrap();

    // Seed shard 1's bystander ref BEFORE the failover so we can prove it is
    // unaffected afterward.
    store.update(&s1_name, oid(0x5a), None).await.unwrap();

    // Identify and confirm shard 0's current leader, then write the first half of
    // the workload and record every Ok-acknowledged target.
    let old_leader = h.wait_for_leader(ShardId(0)).await;
    let mut committed: Vec<(RefName, ObjectId)> = Vec::new();
    for (k, n) in s0_names.iter().take(3).enumerate() {
        let t = oid(0x10 + k as u8);
        store.update(n, t, None).await.unwrap();
        committed.push((n.clone(), t));
    }

    // --- crash shard 0's leader mid-workload (hard partition) ---
    // Never kill the node our store reads from; if the leader happens to be node
    // 2, the failover semantics are identical, but we keep node 2 alive so the
    // store's local handle survives. Re-elect onto node 1 or 3 in that case by
    // killing the actual leader regardless and rebuilding the store on a survivor.
    let (store, old_leader) = if old_leader == 2 {
        // Rebuild the store on node 1 (a guaranteed survivor) and kill node 2.
        (h.cluster_ref_store(1), old_leader)
    } else {
        (store, old_leader)
    };
    h.kill_replica(ShardId(0), old_leader).await;

    // A NEW leader must emerge among shard 0's survivors.
    let new_leader = h.wait_for_new_leader(ShardId(0), old_leader).await;
    assert_ne!(new_leader, old_leader, "a new leader must be elected");

    // --- durability: every previously-Ok ref survives on a live replica ---
    for (n, t) in &committed {
        assert!(
            h.surviving_replica_has_ref(ShardId(0), old_leader, n).await,
            "committed ref {} lost after failover",
            n.as_str()
        );
        // And it reads back through the ClusterRefStore with the right target.
        assert_eq!(
            store.get(n).await.unwrap().unwrap().target,
            *t,
            "ref {} read back wrong after failover",
            n.as_str()
        );
    }

    // --- liveness: post-failover writes succeed on the new leader ---
    for (k, n) in s0_names.iter().skip(3).enumerate() {
        let t = oid(0x40 + k as u8);
        store.update(n, t, None).await.unwrap();
        assert_eq!(store.get(n).await.unwrap().unwrap().target, t);
    }

    // --- isolation: shard 1's bystander ref is entirely undisturbed ---
    assert_eq!(
        store.get(&s1_name).await.unwrap().unwrap().target,
        oid(0x5a),
        "shard 1 must be untouched by shard 0's failover"
    );
    // And shard 1 can still take a fresh write (it never lost its leader).
    let s1_fresh = {
        let mut j = i;
        loop {
            let n = name(&format!("refs/workspaces/fo{j}/main"));
            if h.router().shard_for(n.as_str()) == ShardId(1) {
                break n;
            }
            j += 1;
        }
    };
    store.update(&s1_fresh, oid(0x5b), None).await.unwrap();
    assert_eq!(store.get(&s1_fresh).await.unwrap().unwrap().target, oid(0x5b));
}

// ---------------------------------------------------------------------------
// Task 6.1 — per-shard failover RESPECTS PLACEMENT (distinct node subsets)
// ---------------------------------------------------------------------------
//
// Unlike the failover test above (every node hosts every shard), this builds a
// PLACED cluster where the two shards live on DISTINCT node subsets:
//   shard0 = {1,2,3}, shard1 = {2,3,4}   (4 nodes; node 1 hosts only shard0,
//   node 4 only shard1, nodes 2 & 3 host both).
//
// It proves PER-SHARD FAULT INDEPENDENCE under real placement: killing shard0's
// leader (always a {1,2,3} member) forces a re-election strictly among shard0's
// surviving members, the committed shard0 ref survives on a survivor, and
// shard1 — whose quorum {2,3,4} loses at most one member (2 or 3) and so still
// holds majority — keeps a member-local leader and its own committed ref.
// `kill_replica` deregisters only the (shard0, leader) Raft handle, so if the
// killed node also hosts shard1 (node 2 or 3), that node's INDEPENDENT shard1
// handle is untouched: placement isolation, not just shard isolation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_shard_failover_respects_placement() {
    let map = ShardMap::from_entries([
        (
            ShardId(0),
            vec![
                Replica { node_id: 1, addr: "inproc-1".into() },
                Replica { node_id: 2, addr: "inproc-2".into() },
                Replica { node_id: 3, addr: "inproc-3".into() },
            ],
        ),
        (
            ShardId(1),
            vec![
                Replica { node_id: 2, addr: "inproc-2".into() },
                Replica { node_id: 3, addr: "inproc-3".into() },
                Replica { node_id: 4, addr: "inproc-4".into() },
            ],
        ),
    ])
    .unwrap();
    let h = MultiShardCluster::start_placed(&map).await;

    // Confirm the placement the rest of the test relies on.
    assert_eq!(h.member_ids(ShardId(0)), vec![1, 2, 3]);
    assert_eq!(h.member_ids(ShardId(1)), vec![2, 3, 4]);

    // Node 3 hosts BOTH shards, so both writes below take the local-shard path
    // (the cross-host forward path is proven separately in the forward tests).
    let store = h.cluster_ref_store(3);
    let router = *store.router();

    // Two ref names the router places on distinct shards; bind each to its shard.
    let (a, b) = h.two_names_on_distinct_shards();
    let (name0, name1) = if router.shard_for(a.as_str()) == ShardId(0) {
        (a, b)
    } else {
        (b, a)
    };
    assert_eq!(router.shard_for(name0.as_str()), ShardId(0));
    assert_eq!(router.shard_for(name1.as_str()), ShardId(1));

    // Commit one ref to each shard through the cluster ref store, then wait for
    // each to replicate to ALL of that shard's members (deterministic barrier).
    store.update(&name0, oid(0xa0), None).await.unwrap();
    store.update(&name1, oid(0xb1), None).await.unwrap();
    h.await_applied(ShardId(0), &name0).await;
    h.await_applied(ShardId(1), &name1).await;

    // Record shard1's pre-failover leader so we can assert it stays member-local
    // (and confirm placement isolation regardless of which shard0 node dies).
    let s1_leader_before = h.wait_for_leader(ShardId(1)).await;
    assert!([2, 3, 4].contains(&s1_leader_before));

    // --- kill shard0's CURRENT leader (a {1,2,3} member; a hard partition) ---
    let s0_leader = h.wait_for_leader(ShardId(0)).await;
    assert!([1, 2, 3].contains(&s0_leader));
    h.kill_replica(ShardId(0), s0_leader).await;

    // shard0 re-elects a NEW leader strictly among its surviving members.
    let new_leader = h.wait_for_new_leader(ShardId(0), s0_leader).await;
    assert_ne!(new_leader, s0_leader);
    assert!(
        [1, 2, 3].contains(&new_leader),
        "new shard0 leader {new_leader} must be a shard0 member"
    );

    // shard0's committed ref survived the failover on a live replica, and reads
    // back through the cluster store (node 3 always survives — it is in {1,2,3}
    // but is never the killed leader unless it WAS the leader, in which case the
    // store still resolves a survivor leader via the registry).
    assert!(
        h.surviving_replica_has_ref(ShardId(0), s0_leader, &name0).await,
        "committed shard0 ref must survive re-election"
    );
    assert_eq!(
        store.get(&name0).await.unwrap().unwrap().target,
        oid(0xa0),
        "shard0 ref reads back through the cluster store after failover"
    );

    // --- shard1 is UNAFFECTED: its quorum {2,3,4} never dropped below majority ---
    // Even if s0_leader was node 2 or 3 (a shard1 member too), shard1 retains 2/3
    // and keeps/elects a member-local leader; its committed ref is intact.
    let s1_leader = h.wait_for_leader(ShardId(1)).await;
    assert!(
        [2, 3, 4].contains(&s1_leader),
        "shard1 leader {s1_leader} must remain a shard1 member"
    );
    assert!(
        h.surviving_replica_has_ref(ShardId(1), s0_leader, &name1).await,
        "shard1 ref must be intact (its quorum was never lost)"
    );
    assert_eq!(
        store.get(&name1).await.unwrap().unwrap().target,
        oid(0xb1),
        "shard1 ref reads back unchanged through the cluster store"
    );

    // And shard1 still accepts a fresh write after shard0's failover (liveness
    // of the bystander shard is preserved — its Raft group never lost quorum).
    let s1_fresh = {
        let mut i = 0u32;
        loop {
            let n = name(&format!("refs/workspaces/p{i}/main"));
            if router.shard_for(n.as_str()) == ShardId(1) {
                break n;
            }
            i += 1;
        }
    };
    store.update(&s1_fresh, oid(0xb2), None).await.unwrap();
    assert_eq!(store.get(&s1_fresh).await.unwrap().unwrap().target, oid(0xb2));
}

// ---------------------------------------------------------------------------
// 9.1 (3) — WorkspaceManager composed over the ClusterRefStore
// ---------------------------------------------------------------------------
//
// `WorkspaceManager::new(Arc<dyn RefStore>, Arc<LeaseStore>, Arc<HLC>)`: the ref
// store is the cluster (every fork/commit/get ref op flows through Raft across
// both shards); the lease store is node-local (the manager takes a concrete
// `Arc<LeaseStore>`, not a trait object, so cluster leases cannot be injected —
// this is the plan-sanctioned fallback: prove the Phase-2a workspace ref model
// runs unchanged on the replicated ref store). Runs fork → commit → get.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workspace_manager_over_cluster_ref_store() {
    let h = MultiShardCluster::start(2, &[1, 2, 3]).await;
    let dir = TempDir::new().unwrap();
    let hlc = Arc::new(HLC::new());

    // Ref store = the cluster; lease store = node-local WAL. Keep the concrete
    // `Arc<ClusterRefStore>` so the atomic-commit seam (TxnCoordinator) can be
    // built over the SAME store the manager reads/writes; up-cast a clone for the
    // `dyn RefStore` the manager and the test's direct ref ops use.
    let cluster_refs = Arc::new(h.cluster_ref_store(1));
    let refs: Arc<dyn RefStore> = cluster_refs.clone();
    let leases = Arc::new(LeaseStore::open(dir.path().join("leases"), hlc.clone()).unwrap());
    let coordinator: Arc<dyn ledge_ref_store::AtomicCommit> =
        Arc::new(ledge_cluster::TxnCoordinator::new(cluster_refs));
    let mgr = WorkspaceManager::new(Arc::clone(&refs), leases, hlc, coordinator, ledge_workspace::QuotaLimits::default(), std::sync::Arc::new(ledge_workspace::UsageMap::default()));

    // Seed two durable source refs that route to DISTINCT shards, so the forked
    // workspace's refs (re-rooted under refs/workspaces/<hex>/...) land on one
    // shard while the durable promote target spans the cluster.
    let main = name("refs/heads/main");
    let dev = name("refs/heads/dev");
    refs.update(&main, oid(0x01), None).await.unwrap();
    refs.update(&dev, oid(0x02), None).await.unwrap();

    // --- fork: copies ref deltas through the cluster (n cluster CAS-creates) ---
    let view = mgr
        .fork(&[main.clone(), dev.clone()], Duration::from_secs(3600), "root")
        .await
        .unwrap();
    assert_eq!(view.refs.len(), 2, "fork copied both source refs");
    // The workspace refs are physically present in the cluster, on the shard the
    // router assigns the workspace namespace.
    let ws_main = name(&format!("refs/workspaces/{}/heads/main", view.id.to_hex()));
    let ws_shard = h.router().shard_for(ws_main.as_str());
    assert!(
        h.shard_sm_has_ref(ws_shard, &ws_main).await,
        "forked workspace ref must be committed in the cluster"
    );

    // --- commit: CAS-promote the workspace's main onto a NEW durable ref ---
    let durable = name("refs/heads/release");
    let ws_dev = name(&format!("refs/workspaces/{}/heads/dev", view.id.to_hex()));
    let outcomes = mgr
        .commit(view.id, &[(ws_main.clone(), durable.clone())], "root")
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 1);
    match &outcomes[0] {
        CommitOutcome::Ok { target, entry } => {
            assert_eq!(target, "refs/heads/release");
            assert_eq!(entry.target, oid(0x01), "promoted main's target");
        }
        other => panic!("expected Ok commit outcome, got {other:?}"),
    }
    // The durable ref now resolves through the cluster to the promoted target.
    assert_eq!(
        refs.get(&durable).await.unwrap().unwrap().target,
        oid(0x01)
    );

    // --- get: resolve the workspace view back through the cluster store ---
    let got = mgr.get(view.id, "root").await.unwrap().expect("workspace present");
    let mut got_names: Vec<&str> = got.refs.iter().map(|(n, _)| n.as_str()).collect();
    got_names.sort_unstable();
    assert_eq!(
        got_names,
        vec!["refs/heads/dev", "refs/heads/main"],
        "get must present client-facing names for both forked refs"
    );
    // The workspace dev ref is still durably committed (commit does not release).
    assert!(refs.get(&ws_dev).await.unwrap().is_some());

    // --- release: tears down workspace refs across the cluster, idempotently ---
    mgr.release(view.id, "root").await.unwrap();
    assert!(mgr.get(view.id, "root").await.unwrap().is_none());
    assert!(
        refs.get(&ws_main).await.unwrap().is_none(),
        "workspace refs must be deleted from the cluster on release"
    );
    // Durable source + promoted refs are untouched by release.
    assert_eq!(refs.get(&main).await.unwrap().unwrap().target, oid(0x01));
    assert_eq!(refs.get(&durable).await.unwrap().unwrap().target, oid(0x01));
}
