//! Deterministic assertions that `ClusterGc::run` emits the `ledge_gc_*` series
//! (spec §7) via the thread-local DebuggingRecorder (the txn_metrics.rs pattern).
#![cfg(feature = "testkit")]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tempfile::TempDir;

use ledge_cluster::forward::{InMemoryForwarder, RefOpForwarder};
use ledge_cluster::gc::ClusterGc;
use ledge_cluster::ref_store::{ClusterRefStore, StoreApplier};
use ledge_cluster::router::ShardId;
use ledge_cluster::shard_map::{Replica, ShardMap};
use ledge_cluster::testkit::MultiShardCluster;
use ledge_core::{ObjectId, HLC};
use ledge_object_store::DiskObjectStore;
use ledge_workspace::lease::LeaseStore;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use metrics_util::{CompositeKey, MetricKind};

fn oid(n: u8) -> ObjectId {
    let mut b = [0u8; 32];
    b[31] = n;
    ObjectId::from_bytes(b)
}

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
    fn counter_sum(&self, name: &str) -> u64 {
        self.0
            .iter()
            .filter_map(|(ck, v)| {
                if ck.kind() != MetricKind::Counter || ck.key().name() != name {
                    return None;
                }
                match v {
                    DebugValue::Counter(c) => Some(*c),
                    _ => None,
                }
            })
            .sum()
    }
    fn gauge_with_label(&self, name: &str, want: (&str, &str)) -> Option<f64> {
        self.0.iter().find_map(|(ck, v)| {
            if ck.kind() != MetricKind::Gauge || ck.key().name() != name {
                return None;
            }
            let has = ck
                .key()
                .labels()
                .any(|l| l.key() == want.0 && l.value() == want.1);
            match (has, v) {
                // `DebugValue::Gauge` wraps `OrderedFloat<f64>`; `.0` is the f64.
                (true, DebugValue::Gauge(g)) => Some(g.0),
                _ => None,
            }
        })
    }
    fn histogram_count(&self, name: &str) -> usize {
        self.0
            .iter()
            .filter_map(|(ck, v)| {
                if ck.kind() != MetricKind::Histogram || ck.key().name() != name {
                    return None;
                }
                match v {
                    DebugValue::Histogram(s) => Some(s.len()),
                    _ => None,
                }
            })
            .sum()
    }
}

async fn harness() -> (
    TempDir,
    MultiShardCluster,
    Arc<ClusterRefStore>,
    Arc<DiskObjectStore>,
    Arc<LeaseStore>,
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
    let dir = TempDir::new().unwrap();
    let hlc = Arc::new(HLC::new());
    let objects = Arc::new(DiskObjectStore::new(dir.path().to_path_buf()).unwrap());
    let leases = Arc::new(LeaseStore::open(dir.path().to_path_buf(), hlc).unwrap());
    (dir, cluster, store1, objects, leases)
}

#[tokio::test(flavor = "current_thread")]
async fn cluster_gc_run_emits_gc_series() {
    let (_dir, _cluster, store1, objects, leases) = harness().await;
    // One fresh orphan kept by grace (so grace_retained = 1, reclaimed = 0).
    objects
        .write_git_object(3, Bytes::copy_from_slice(b"fresh orphan"))
        .await
        .unwrap();
    let _ = oid(0); // silence unused on some toolchains

    let real_now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let gc = ClusterGc::new(
        store1.clone(),
        leases.clone(),
        objects.clone(),
        Duration::from_secs(3600),
        Arc::new(ledge_workspace::UsageMap::default()),
    );

    let recorder = DebuggingRecorder::new();
    let snap = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);

    let stats = gc.run(real_now).await.unwrap();
    assert_eq!(stats.skipped_grace, 1, "fresh orphan retained by grace");

    let m = MetricSnap::capture(&snap);
    assert_eq!(m.counter_sum("ledge_gc_runs_total"), 1, "one run");
    assert_eq!(
        m.counter_sum("ledge_gc_objects_reclaimed_total"),
        0,
        "nothing reclaimed (grace)"
    );
    assert_eq!(
        m.histogram_count("ledge_gc_duration_seconds"),
        1,
        "one duration sample"
    );
    assert_eq!(
        m.gauge_with_label("ledge_gc_roots", ("kind", "committed")),
        Some(0.0)
    );
    assert_eq!(
        m.gauge_with_label("ledge_gc_roots", ("kind", "prepared")),
        Some(0.0)
    );
    assert_eq!(
        m.gauge_with_label("ledge_gc_roots", ("kind", "lease")),
        Some(0.0)
    );
    // grace_retained gauge reflects the single retained candidate.
    assert!(m.0.iter().any(|(ck, v)| ck.key().name() == "ledge_gc_grace_retained"
        && matches!(v, DebugValue::Gauge(g) if (g.0 - 1.0).abs() < f64::EPSILON)));
}
