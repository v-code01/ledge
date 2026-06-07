//! Deterministic assertions that the `ledge_txn_*` 2PC-lifecycle metrics
//! (spec §7, Phase 4b Task 7) advance exactly as the protocol dictates.
//!
//! # Why a thread-local `DebuggingRecorder`
//! `metrics` emits to a process-global recorder; in tests we instead install a
//! thread-local [`DebuggingRecorder`] via [`metrics::set_default_local_recorder`]
//! and hold its guard for the whole test body. Each test runs on the
//! `current_thread` tokio flavor so every awaited coordinator/resolver step stays
//! on the one thread the guard covers — no cross-thread emission is missed, and
//! tests do not contend over a shared global recorder. We then snapshot the
//! recorder and read the exact counter/histogram values back.

use std::sync::Arc;

use ledge_cluster::forward::{ClusterOp, InMemoryForwarder, RefOpForwarder, RefOpResponse};
use ledge_cluster::ref_store::StoreApplier;
use ledge_cluster::router::ShardId;
use ledge_cluster::shard_map::{Replica, ShardMap};
use ledge_cluster::testkit::MultiShardCluster;
use ledge_cluster::txn::{AtomicCommit, AtomicCommitResult, TxnCoordinator, TxnResolver};
use ledge_core::{ObjectId, RefName, RefStore};

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use metrics_util::{CompositeKey, MetricKind};

fn oid(n: u8) -> ObjectId {
    let mut b = [0u8; 32];
    b[0] = n;
    ObjectId::from_bytes(b)
}

/// One materialized snapshot of every emitted series.
///
/// IMPORTANT: `Snapshotter::snapshot()` *drains* histograms (`clear_with`), so a
/// histogram's samples are visible only to the FIRST snapshot taken after they
/// are recorded. Tests therefore take exactly ONE snapshot per test and query
/// this captured `Vec` repeatedly — never re-snapshot.
struct MetricSnap(Vec<(CompositeKey, DebugValue)>);

impl MetricSnap {
    fn capture(snap: &Snapshotter) -> Self {
        Self(
            snap.snapshot()
                .into_vec()
                .into_iter()
                .map(|(ck, _u, _d, v)| (ck, v))
                .collect(),
        )
    }

    /// Sum of a counter series whose name == `name` and whose labels are a
    /// superset of `want_labels` (an empty `want_labels` matches every label-set
    /// of that name). Returns `0` if the series was never touched.
    fn counter_sum(&self, name: &str, want_labels: &[(&str, &str)]) -> u64 {
        self.0
            .iter()
            .filter_map(|(ck, v)| {
                if ck.kind() != MetricKind::Counter || ck.key().name() != name {
                    return None;
                }
                let labels: Vec<(&str, &str)> =
                    ck.key().labels().map(|l| (l.key(), l.value())).collect();
                let matches = want_labels
                    .iter()
                    .all(|(k, val)| labels.iter().any(|(lk, lv)| lk == k && lv == val));
                match (matches, v) {
                    (true, DebugValue::Counter(c)) => Some(*c),
                    _ => None,
                }
            })
            .sum()
    }

    /// Number of samples recorded into the histogram series `name`.
    fn histogram_count(&self, name: &str) -> usize {
        self.0
            .iter()
            .filter_map(|(ck, v)| {
                if ck.kind() != MetricKind::Histogram || ck.key().name() != name {
                    return None;
                }
                match v {
                    DebugValue::Histogram(samples) => Some(samples.len()),
                    _ => None,
                }
            })
            .sum()
    }
}

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

/// A co-located pair (same shard) the router places together, for the fast path.
fn co_located_pair(router: &ledge_cluster::router::ShardRouter) -> (RefName, RefName) {
    let mut first: Option<(RefName, ShardId)> = None;
    for i in 0..10_000u32 {
        let n = RefName::new(&format!("refs/heads/x{i}")).unwrap();
        let s = router.shard_for(n.as_str());
        match &first {
            None => first = Some((n, s)),
            Some((f, fs)) if *fs == s => return (f.clone(), n),
            _ => {}
        }
    }
    panic!("a co-located pair must exist within 10k names");
}

/// begin → commit on a multi-shard txn: `ledge_txn_started_total` +1,
/// `ledge_txn_committed_total` +1, two `yes` prepare votes, one duration sample,
/// and NO abort.
#[tokio::test(flavor = "current_thread")]
async fn commit_increments_started_committed_yes_votes_and_duration() {
    let (cluster, _map, store1) = two_shard_cluster().await;
    let (a, b) = cluster.two_names_on_distinct_shards();
    assert_ne!(
        cluster.router().shard_for(a.as_str()),
        cluster.router().shard_for(b.as_str())
    );
    let coord = TxnCoordinator::new(store1.clone());

    let recorder = DebuggingRecorder::new();
    let snap = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);

    let res = coord
        .commit_atomic(vec![(a.clone(), oid(1), None), (b.clone(), oid(2), None)])
        .await
        .unwrap();
    assert!(matches!(res, AtomicCommitResult::Committed(_)), "expected commit, got {res:?}");

    let msnap = MetricSnap::capture(&snap);

    assert_eq!(msnap.counter_sum("ledge_txn_started_total", &[]), 1);
    assert_eq!(msnap.counter_sum("ledge_txn_committed_total", &[]), 1);
    assert_eq!(
        msnap.counter_sum("ledge_txn_prepare_votes_total", &[("vote", "yes")]),
        2,
        "both refs voted yes"
    );
    assert_eq!(
        msnap.counter_sum("ledge_txn_aborted_total", &[]),
        0,
        "a commit must not increment any abort series"
    );
    assert_eq!(
        msnap.histogram_count("ledge_txn_duration_seconds"),
        1,
        "exactly one duration sample per multi-shard txn"
    );
}

/// A precondition conflict ⇒ VOTE-NO ⇒ abort: `ledge_txn_aborted_total{reason="prepare_no"}`
/// +1, a `no` prepare vote recorded, and NO committed increment. Also proves the
/// no-wait NO short-circuits (the conflicting ref is first in canonical order).
#[tokio::test(flavor = "current_thread")]
async fn conflict_increments_aborted_prepare_no_and_no_vote() {
    let (cluster, _map, store1) = two_shard_cluster().await;
    let (a, b) = cluster.two_names_on_distinct_shards();
    // Seed `a` committed at oid(5); the commit demands stale expected oid(9) ⇒ NO.
    store1.update(&a, oid(5), None).await.unwrap();
    let coord = TxnCoordinator::new(store1.clone());

    let recorder = DebuggingRecorder::new();
    let snap = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);

    let res = coord
        .commit_atomic(vec![
            (a.clone(), oid(7), Some(oid(9))), // stale expected ⇒ NO
            (b.clone(), oid(8), None),
        ])
        .await
        .unwrap();
    assert!(matches!(res, AtomicCommitResult::Aborted { .. }), "expected abort, got {res:?}");

    let msnap = MetricSnap::capture(&snap);

    assert_eq!(msnap.counter_sum("ledge_txn_started_total", &[]), 1);
    assert_eq!(msnap.counter_sum("ledge_txn_committed_total", &[]), 0);
    assert_eq!(
        msnap.counter_sum(
            "ledge_txn_aborted_total",
            &[("reason", "prepare_no")]
        ),
        1,
        "a vote-NO abort is reason=prepare_no"
    );
    assert_eq!(
        msnap.counter_sum("ledge_txn_prepare_votes_total", &[("vote", "no")]),
        1
    );
    // Duration is recorded even on abort (begin → end span).
    assert_eq!(msnap.histogram_count("ledge_txn_duration_seconds"), 1);
}

/// The single-shard fast path is a `RefBatch`, NOT 2PC: it must emit NONE of the
/// `ledge_txn_*` series (those count multi-shard transactions only, spec §7).
#[tokio::test(flavor = "current_thread")]
async fn single_shard_fast_path_emits_no_txn_metrics() {
    let (cluster, _map, store1) = two_shard_cluster().await;
    let (a, b) = co_located_pair(&cluster.router());
    assert_eq!(
        cluster.router().shard_for(a.as_str()),
        cluster.router().shard_for(b.as_str())
    );
    let coord = TxnCoordinator::new(store1.clone());

    let recorder = DebuggingRecorder::new();
    let snap = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);

    let res = coord
        .commit_atomic(vec![(a.clone(), oid(1), None), (b.clone(), oid(2), None)])
        .await
        .unwrap();
    assert!(matches!(res, AtomicCommitResult::Committed(_)), "fast path commit, got {res:?}");

    let msnap = MetricSnap::capture(&snap);

    assert_eq!(msnap.counter_sum("ledge_txn_started_total", &[]), 0);
    assert_eq!(msnap.counter_sum("ledge_txn_committed_total", &[]), 0);
    assert_eq!(
        msnap.counter_sum("ledge_txn_prepare_votes_total", &[]),
        0,
        "no prepare phase on the fast path"
    );
    assert_eq!(msnap.histogram_count("ledge_txn_duration_seconds"), 0);
}

/// The crash-recovery resolver rolling a lock FORWARD (durable Commit decision)
/// increments `ledge_txn_recovered_total` once per resolved lock.
#[tokio::test(flavor = "current_thread")]
async fn resolver_roll_forward_increments_recovered() {
    use ledge_raft::TxnId;

    let (cluster, _map, store1) = two_shard_cluster().await;
    let (a, _b) = cluster.two_names_on_distinct_shards();
    let a_shard = cluster.router().shard_for(a.as_str());
    let coord_shard = ShardId(0);
    let txn = TxnId::from_bytes([42u8; 16]);

    // Crash window: durable Begin + Decide{commit}, lock taken, no CommitPrepared.
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

    let recorder = DebuggingRecorder::new();
    let snap = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);

    let resolver = TxnResolver::new(store1.clone());
    let resolved = resolver.resolve_once().await.unwrap();
    let msnap = MetricSnap::capture(&snap);
    assert!(resolved >= 1);

    assert_eq!(
        msnap.counter_sum("ledge_txn_recovered_total", &[]),
        resolved as u64,
        "one recovered increment per resolved lock"
    );
    // Roll-forward actually committed the ref (sanity).
    assert_eq!(store1.get(&a).await.unwrap().unwrap().target, oid(7));
}

/// The resolver doing a PRESUMED ABORT (no durable decision, past TTL) also
/// counts as a recovery: `ledge_txn_recovered_total` advances for the released
/// lock.
#[tokio::test(flavor = "current_thread")]
async fn resolver_presumed_abort_increments_recovered() {
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

    let recorder = DebuggingRecorder::new();
    let snap = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);

    let resolver = TxnResolver::new(store1.clone()).with_ttl(std::time::Duration::ZERO);
    let resolved = resolver.resolve_once().await.unwrap();
    let msnap = MetricSnap::capture(&snap);
    assert!(resolved >= 1);

    assert_eq!(
        msnap.counter_sum("ledge_txn_recovered_total", &[]),
        resolved as u64
    );
    // Presumed-abort released the lock; NO ref advanced.
    assert!(store1.get(&a).await.unwrap().is_none());
}

