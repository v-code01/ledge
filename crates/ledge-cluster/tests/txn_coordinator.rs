//! TxnCoordinator atomicity over a 2-shard × 3-node in-process cluster.
#![cfg(feature = "testkit")]

use std::sync::Arc;

use ledge_cluster::forward::InMemoryForwarder;
use ledge_cluster::ref_store::StoreApplier;
use ledge_cluster::router::ShardId;
use ledge_cluster::shard_map::{Replica, ShardMap};
use ledge_cluster::testkit::MultiShardCluster;
use ledge_cluster::txn::{AtomicCommit, AtomicCommitResult, TxnCoordinator, TxnResolver};
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

// ── Idempotent apply (the resolver's foundation, Task 4) ────────────────────

/// Driving `CommitPrepared` through `op_on_shard` when the prepared lock has
/// already vanished (the slot was removed by a prior AbortPrepared / GC) must be
/// a BENIGN idempotent ack — NOT an `infra` error. This is the precondition for
/// safe resolver retries: a resolver that re-issues CommitPrepared after the ref
/// was already resolved must not blow up.
#[tokio::test]
async fn commit_prepared_on_vanished_lock_is_benign() {
    use ledge_cluster::forward::{ClusterOp, RefOpResponse};
    use ledge_raft::TxnId;

    let (cluster, _map, store1) = two_shard_cluster().await;
    let (a, _b) = cluster.two_names_on_distinct_shards();
    let a_shard = cluster.router().shard_for(a.as_str());
    let coord_shard = ShardId(0);
    let txn = TxnId::from_bytes([11u8; 16]);

    // Prepare an ABSENT ref (creates a version-0 sentinel slot + lock), then
    // AbortPrepared: an absent-ref abort REMOVES the slot entirely, so a
    // subsequent CommitPrepared finds no slot ⇒ store returns AbortedPrepared.
    let vote = store1
        .op_on_shard(
            a_shard,
            ClusterOp::Prepare {
                txn_id: txn,
                coord_shard: coord_shard.0,
                name: a.as_str().to_string(),
                target_bytes: *oid(7).as_bytes(),
                expected_bytes: None,
            },
        )
        .await
        .unwrap();
    assert!(matches!(vote, RefOpResponse::Vote(true)));

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

    // CommitPrepared on the now-vanished lock: must be a benign success, NOT err.
    let resp = store1
        .op_on_shard(
            a_shard,
            ClusterOp::CommitPrepared {
                txn_id: txn,
                name: a.as_str().to_string(),
            },
        )
        .await
        .expect("CommitPrepared on a vanished lock must be a benign idempotent ack");
    assert!(
        matches!(
            resp,
            RefOpResponse::AbortedPrepared | RefOpResponse::CommittedPrepared(_)
        ),
        "expected benign ack, got {resp:?}"
    );
}

/// Applying `CommitPrepared` TWICE on the same (txn, ref) is idempotent: the
/// second apply is a benign success returning the already-committed entry, and
/// the ref's committed value is unchanged.
#[tokio::test]
async fn commit_prepared_twice_is_idempotent() {
    use ledge_cluster::forward::{ClusterOp, RefOpResponse};
    use ledge_raft::TxnId;

    let (cluster, _map, store1) = two_shard_cluster().await;
    let (a, _b) = cluster.two_names_on_distinct_shards();
    let a_shard = cluster.router().shard_for(a.as_str());
    let coord_shard = ShardId(0);
    let txn = TxnId::from_bytes([12u8; 16]);

    // Prepare `a` staging oid(7).
    store1
        .op_on_shard(
            a_shard,
            ClusterOp::Prepare {
                txn_id: txn,
                coord_shard: coord_shard.0,
                name: a.as_str().to_string(),
                target_bytes: *oid(7).as_bytes(),
                expected_bytes: None,
            },
        )
        .await
        .unwrap();

    // First CommitPrepared rolls forward to oid(7).
    let first = store1
        .op_on_shard(
            a_shard,
            ClusterOp::CommitPrepared {
                txn_id: txn,
                name: a.as_str().to_string(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(first, RefOpResponse::CommittedPrepared(ref e) if e.target == oid(7)));
    assert_eq!(store1.get(&a).await.unwrap().unwrap().target, oid(7));

    // Second CommitPrepared: lock already gone ⇒ benign success, ref unchanged.
    let second = store1
        .op_on_shard(
            a_shard,
            ClusterOp::CommitPrepared {
                txn_id: txn,
                name: a.as_str().to_string(),
            },
        )
        .await
        .expect("a duplicate CommitPrepared must be a benign success, not an error");
    assert!(
        matches!(
            second,
            RefOpResponse::CommittedPrepared(_) | RefOpResponse::AbortedPrepared
        ),
        "expected benign idempotent ack, got {second:?}"
    );
    assert_eq!(
        store1.get(&a).await.unwrap().unwrap().target,
        oid(7),
        "ref committed value must be unchanged by the duplicate"
    );
}

// ── TxnResolver: coordinator-crash recovery (Task 4, spec §3.4) ─────────────

/// Coordinator died AFTER the durable `TxnDecide{commit:true}` but BEFORE the
/// CommitPrepared sweep: the lock exists with a COMMITTED decision. The resolver
/// must roll the ref FORWARD (presumed-commit, authorized by the durable
/// decision) and end the txn.
#[tokio::test]
async fn resolver_rolls_forward_after_decision_commit() {
    use ledge_cluster::forward::{ClusterOp, RefOpResponse};
    use ledge_raft::TxnId;

    let (cluster, _map, store1) = two_shard_cluster().await;
    let (a, _b) = cluster.two_names_on_distinct_shards();
    let a_shard = cluster.router().shard_for(a.as_str());
    let coord_shard = ShardId(0);
    let txn = TxnId::from_bytes([42u8; 16]);

    // Simulate the crash window: durable Begin + Decide{commit}, lock taken on
    // `a` staging oid(7), but NO CommitPrepared sent.
    store1
        .apply_txn_record_op(
            coord_shard,
            ledge_raft::LedgeOp::TxnBegin {
                txn_id: txn,
                participants: vec![a_shard.0],
            },
        )
        .await
        .unwrap();
    store1
        .apply_txn_record_op(
            coord_shard,
            ledge_raft::LedgeOp::TxnDecide {
                txn_id: txn,
                commit: true,
            },
        )
        .await
        .unwrap();
    let vote = store1
        .op_on_shard(
            a_shard,
            ClusterOp::Prepare {
                txn_id: txn,
                coord_shard: coord_shard.0,
                name: a.as_str().to_string(),
                target_bytes: *oid(7).as_bytes(),
                expected_bytes: None,
            },
        )
        .await
        .unwrap();
    assert!(matches!(vote, RefOpResponse::Vote(true)));
    // Staged, not yet committed.
    assert!(store1.get(&a).await.unwrap().is_none());

    // Resolve: must find the lock, read COMMIT, roll forward.
    let resolver = TxnResolver::new(store1.clone());
    let resolved = resolver.resolve_once().await.unwrap();
    assert!(resolved >= 1, "resolver must resolve at least the one lock");

    // ATOMICITY HOLDS: `a` is now committed to the staged target.
    assert_eq!(store1.get(&a).await.unwrap().unwrap().target, oid(7));
    // The txn record is ended (gone) once resolved.
    assert!(store1
        .op_on_shard(
            coord_shard,
            ClusterOp::TxnStatus {
                txn_id: txn,
                coord_shard: coord_shard.0,
            },
        )
        .await
        .unwrap()
        .eq(&RefOpResponse::TxnDecisionResp(None)));
}

/// Coordinator died BEFORE `TxnDecide`: a lock exists but the decision is
/// PENDING / unresolvable. Past the TTL this is a PRESUMED ABORT — the resolver
/// releases the lock and advances NO ref. Safe: no participant could have been
/// told to commit, because the durable commit point was never reached.
#[tokio::test]
async fn resolver_presumed_aborts_with_no_decision() {
    use ledge_cluster::forward::ClusterOp;
    use ledge_raft::TxnId;

    let (cluster, _map, store1) = two_shard_cluster().await;
    let (a, _b) = cluster.two_names_on_distinct_shards();
    let a_shard = cluster.router().shard_for(a.as_str());
    let coord_shard = ShardId(0);
    let txn = TxnId::from_bytes([99u8; 16]);

    // Crash BEFORE TxnDecide: Begin only (PENDING) + a Prepare lock.
    store1
        .apply_txn_record_op(
            coord_shard,
            ledge_raft::LedgeOp::TxnBegin {
                txn_id: txn,
                participants: vec![a_shard.0],
            },
        )
        .await
        .unwrap();
    store1
        .op_on_shard(
            a_shard,
            ClusterOp::Prepare {
                txn_id: txn,
                coord_shard: coord_shard.0,
                name: a.as_str().to_string(),
                target_bytes: *oid(7).as_bytes(),
                expected_bytes: None,
            },
        )
        .await
        .unwrap();

    // ZERO TTL ⇒ the PENDING lock is immediately presumed-abort.
    let resolver = TxnResolver::new(store1.clone()).with_ttl(std::time::Duration::ZERO);
    let resolved = resolver.resolve_once().await.unwrap();
    assert!(resolved >= 1);

    // PRESUMED ABORT: lock released, NO ref advanced.
    assert!(store1.get(&a).await.unwrap().is_none());
    // And `a` is writable again (lock gone).
    store1.update(&a, oid(1), None).await.unwrap();
}

/// Running the resolver TWICE on the same already-resolved (committed) txn is
/// safe: the second pass is a no-op (it finds no lock to resolve) and the ref is
/// unchanged. Proves resolver retry-safety on top of idempotent apply.
#[tokio::test]
async fn resolver_idempotent_re_resolve() {
    use ledge_cluster::forward::ClusterOp;
    use ledge_raft::TxnId;

    let (cluster, _map, store1) = two_shard_cluster().await;
    let (a, _b) = cluster.two_names_on_distinct_shards();
    let a_shard = cluster.router().shard_for(a.as_str());
    let coord_shard = ShardId(0);
    let txn = TxnId::from_bytes([55u8; 16]);

    store1
        .apply_txn_record_op(
            coord_shard,
            ledge_raft::LedgeOp::TxnBegin {
                txn_id: txn,
                participants: vec![a_shard.0],
            },
        )
        .await
        .unwrap();
    store1
        .apply_txn_record_op(
            coord_shard,
            ledge_raft::LedgeOp::TxnDecide {
                txn_id: txn,
                commit: true,
            },
        )
        .await
        .unwrap();
    store1
        .op_on_shard(
            a_shard,
            ClusterOp::Prepare {
                txn_id: txn,
                coord_shard: coord_shard.0,
                name: a.as_str().to_string(),
                target_bytes: *oid(7).as_bytes(),
                expected_bytes: None,
            },
        )
        .await
        .unwrap();

    let resolver = TxnResolver::new(store1.clone());
    let first = resolver.resolve_once().await.unwrap();
    assert!(first >= 1, "first pass resolves the lock");
    assert_eq!(store1.get(&a).await.unwrap().unwrap().target, oid(7));

    // Second pass: no lock remains ⇒ a no-op (0 resolved), ref unchanged.
    let second = resolver.resolve_once().await.unwrap();
    assert_eq!(second, 0, "re-resolve is a no-op");
    assert_eq!(store1.get(&a).await.unwrap().unwrap().target, oid(7));
}
