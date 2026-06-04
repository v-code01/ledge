//! TxnCoordinator atomicity over a 2-shard × 3-node in-process cluster.
#![cfg(feature = "testkit")]

use std::sync::Arc;

use ledge_cluster::forward::InMemoryForwarder;
use ledge_cluster::ref_store::StoreApplier;
use ledge_cluster::router::ShardId;
use ledge_cluster::shard_map::{Replica, ShardMap};
use ledge_cluster::testkit::MultiShardCluster;
use ledge_cluster::txn::{AtomicCommit, AtomicCommitResult, TxnCoordinator};
use ledge_core::{ObjectId, RefName, RefStore};

fn oid(n: u8) -> ObjectId {
    let mut b = [0u8; 32];
    b[31] = n;
    ObjectId::from_bytes(b)
}

/// 2 shards × 3 nodes, all-on-all placement, every node hosts both shards (so
/// the coordinator node always hosts min(shards)=0). Returns the cluster, the
/// shard map, and node 1's `Arc<ClusterRefStore>` wired to a shared forwarder.
async fn two_shard_cluster() -> (
    MultiShardCluster,
    ShardMap,
    Arc<ledge_cluster::ref_store::ClusterRefStore>,
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
    let fwd = Arc::new(InMemoryForwarder::new());
    fwd.set_map(map.clone());
    let store1 = cluster.cluster_ref_store_hosting(1, &map, fwd.clone());
    fwd.register(1, Arc::new(StoreApplier(store1.clone())));
    (cluster, map, store1)
}

#[tokio::test]
async fn multi_shard_commit_promotes_both_refs() {
    let (cluster, _map, store1) = two_shard_cluster().await;
    // Two names on DISTINCT shards (forces the 2PC path).
    let (a, b) = cluster.two_names_on_distinct_shards();
    assert_ne!(
        cluster.router().shard_for(a.as_str()),
        cluster.router().shard_for(b.as_str())
    );

    let coord = TxnCoordinator::new(store1.clone());
    let res = coord
        .commit_atomic(vec![(a.clone(), oid(1), None), (b.clone(), oid(2), None)])
        .await
        .unwrap();
    assert!(
        matches!(res, AtomicCommitResult::Committed(ref v) if v.len() == 2),
        "expected Committed(2), got {res:?}"
    );

    // BOTH durable refs advanced, readable on their shards (the store routes each
    // get to the owning shard's leader).
    assert_eq!(store1.get(&a).await.unwrap().unwrap().target, oid(1));
    assert_eq!(store1.get(&b).await.unwrap().unwrap().target, oid(2));
}

#[tokio::test]
async fn conflicting_precondition_aborts_both_no_lock_left() {
    let (cluster, _map, store1) = two_shard_cluster().await;
    let (a, b) = cluster.two_names_on_distinct_shards();

    // Seed a so its committed target is oid(5). The commit will demand
    // expected = oid(9) on a (stale) ⇒ VOTE-NO ⇒ abort BOTH.
    store1.update(&a, oid(5), None).await.unwrap();
    // b is absent; the commit demands create-only (None) on b, which WOULD pass,
    // but a's NO must abort b too.

    let coord = TxnCoordinator::new(store1.clone());
    let res = coord
        .commit_atomic(vec![
            (a.clone(), oid(7), Some(oid(9))), // stale expected ⇒ NO
            (b.clone(), oid(8), None),
        ])
        .await
        .unwrap();
    match res {
        AtomicCommitResult::Aborted { conflicts, .. } => {
            assert!(conflicts.contains(&a), "a is the conflicting ref");
        }
        other => panic!("expected Aborted, got {other:?}"),
    }

    // ATOMICITY: a's old value intact, b never created.
    assert_eq!(store1.get(&a).await.unwrap().unwrap().target, oid(5));
    assert!(store1.get(&b).await.unwrap().is_none());

    // NO PREPARED LOCK LEFT: a follow-up plain update on a (with correct
    // expected) succeeds — proving a is not still locked by the aborted txn.
    let after = store1.update(&a, oid(6), Some(oid(5))).await.unwrap();
    assert_eq!(after.target, oid(6));
    // And b is writable too (never locked).
    store1.update(&b, oid(8), None).await.unwrap();
}

#[tokio::test]
async fn single_shard_mapping_uses_batch_and_is_atomic() {
    let (cluster, _map, store1) = two_shard_cluster().await;
    // Two names that the router places on the SAME shard (so the fast path runs).
    let router = cluster.router();
    let mut same: Option<(RefName, RefName)> = None;
    let mut first: Option<(RefName, ShardId)> = None;
    for i in 0..10_000u32 {
        let n = RefName::new(&format!("refs/heads/x{i}")).unwrap();
        let s = router.shard_for(n.as_str());
        match &first {
            None => first = Some((n, s)),
            Some((f, fs)) if *fs == s => {
                same = Some((f.clone(), n));
                break;
            }
            _ => {}
        }
    }
    let (a, b) = same.expect("a co-located pair exists");
    assert_eq!(router.shard_for(a.as_str()), router.shard_for(b.as_str()));

    let coord = TxnCoordinator::new(store1.clone());
    let res = coord
        .commit_atomic(vec![(a.clone(), oid(1), None), (b.clone(), oid(2), None)])
        .await
        .unwrap();
    assert!(matches!(res, AtomicCommitResult::Committed(ref v) if v.len() == 2));
    assert_eq!(store1.get(&a).await.unwrap().unwrap().target, oid(1));
    assert_eq!(store1.get(&b).await.unwrap().unwrap().target, oid(2));

    // Atomic batch failure: one ref's precondition stale ⇒ NEITHER advances.
    let res2 = coord
        .commit_atomic(vec![
            (a.clone(), oid(3), Some(oid(9))), // stale ⇒ batch conflict
            (b.clone(), oid(4), Some(oid(2))),
        ])
        .await
        .unwrap();
    assert!(matches!(res2, AtomicCommitResult::Aborted { .. }));
    assert_eq!(store1.get(&a).await.unwrap().unwrap().target, oid(1));
    assert_eq!(store1.get(&b).await.unwrap().unwrap().target, oid(2));
}

#[tokio::test]
async fn two_concurrent_cross_shard_txns_sharing_a_ref_exactly_one_commits() {
    let (cluster, _map, store1) = two_shard_cluster().await;
    // Three names: `shared` plus one distinct ref per txn on the OTHER shard, so
    // each txn spans two shards and both contend on `shared`.
    let (shared, other) = cluster.two_names_on_distinct_shards();
    let router = cluster.router();
    let other_shard = router.shard_for(other.as_str());
    let mut other2 = None;
    for i in 0..10_000u32 {
        let n = RefName::new(&format!("refs/heads/y{i}")).unwrap();
        if router.shard_for(n.as_str()) == other_shard && n != other {
            other2 = Some(n);
            break;
        }
    }
    let other2 = other2.unwrap();

    let coord = Arc::new(TxnCoordinator::new(store1.clone()));
    let c1 = coord.clone();
    let c2 = coord.clone();
    let (s1, o1) = (shared.clone(), other.clone());
    let (s2, o2) = (shared.clone(), other2.clone());

    // Two cross-shard commits racing on `shared` (both create-only on it).
    let t1 = tokio::spawn(async move {
        c1.commit_atomic(vec![(s1, oid(1), None), (o1, oid(11), None)])
            .await
    });
    let t2 = tokio::spawn(async move {
        c2.commit_atomic(vec![(s2, oid(2), None), (o2, oid(22), None)])
            .await
    });
    let r1 = t1.await.unwrap().unwrap();
    let r2 = t2.await.unwrap().unwrap();

    // Exactly one Committed, the other Aborted (no-wait ⇒ one prepare on `shared`
    // wins, the other votes NO and aborts cleanly).
    let committed = [&r1, &r2]
        .iter()
        .filter(|r| matches!(r, AtomicCommitResult::Committed(_)))
        .count();
    assert_eq!(committed, 1, "exactly one txn commits: r1={r1:?} r2={r2:?}");

    // `shared` holds exactly one of the two candidate targets (no partial state).
    let shared_target = store1.get(&shared).await.unwrap().unwrap().target;
    assert!(shared_target == oid(1) || shared_target == oid(2));

    // The aborted txn left NO lock: a plain update on `shared` with the correct
    // expected succeeds.
    store1
        .update(&shared, oid(3), Some(shared_target))
        .await
        .unwrap();
}
