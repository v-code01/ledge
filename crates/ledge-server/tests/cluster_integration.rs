//! Cluster control-plane route tests (Phase 3, Task 7B + Task 9.2 decision).
//!
//! Covers three properties:
//! 1. `cluster_routes_503_when_disabled` — a default (single-node) `AppState`
//!    (`raft_shards = None`) returns `503` from `/cluster/status` and `/raft/*`,
//!    proving the cluster routes are inert in single-node mode.
//! 2. `cluster_status_shape_when_enabled` — an `AppState` carrying a 1-shard
//!    in-process `Raft<TypeConfig>` (initialized + elected) returns a
//!    `/cluster/status` with one shard, `leader == Some(node)`, `term >= 1`.
//! 3. `raft_append_endpoint_feeds_local_raft` — a bincode heartbeat
//!    `AppendEntriesRequest` POSTed to `/raft/0/append` round-trips a decodable
//!    `AppendEntriesResponse` (the server side of the Task 6 HTTP transport).
//!
//! # Task 9.2 — server-level localhost HTTP cluster smoke (decision)
//! The plan calls for a 2–3 node localhost HTTP cluster smoke (bootstrap → elect
//! → one replicated write over the real transport). That smoke IS implemented and
//! runs by default — it lives in `ledge-cluster`'s `net_http` test module as
//! `localhost_http_cluster_bootstrap_elect_replicate`: three Axum servers, each
//! serving the identical `POST /raft/{shard}/{kind}` route shape these handlers
//! serve, with each node's `Raft` driven over `HttpRaftNetworkFactory`. It elects
//! a leader purely over HTTP and replicates a committed `client_write` to all
//! three state machines. It is deterministic in-process (bounded metrics polling,
//! no fixed sleeps), so it is NOT `#[ignore]`d.
//!
//! This file keeps the server-level coverage at the *route-handler* granularity
//! (the three tests above) because a multi-process `ledge-server` cluster would
//! add no consensus coverage beyond that smoke: the route handlers here are thin
//! wrappers over `handle_rpc` / `Raft::metrics` / `Raft::initialize`, each tested
//! directly, and the end-to-end HTTP consensus path is the cluster-crate smoke.
//! Together — in-memory safety proof (Tasks 3 / 9.1) + per-RPC serde + live HTTP
//! RPC + the 3-node HTTP smoke + these route tests — the HTTP surface is fully
//! exercised without a flaky multi-process harness.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tempfile::TempDir;
use tower::ServiceExt; // oneshot

use ledge_cluster::testkit::MultiShardCluster;
use ledge_cluster::ShardId;
use ledge_raft::{NodeId, TypeConfig};
use ledge_server::routes::{AppState, ClusterHandles};
use ledge_server::{build_app, build_workspace_stack};

use ledge_core::HLC;
use ledge_object_store::DiskObjectStore;
use ledge_ref_store::RefStoreImpl;

/// Build an `AppState` over fresh tempdir stores. `raft_shards` controls cluster
/// mode: `None` = single-node, `Some(map)` = clustered with those shard handles.
fn state_with_shards(dir: &TempDir, raft_shards: Option<Arc<ClusterHandles>>) -> AppState {
    let p = dir.path().to_path_buf();
    let hlc = Arc::new(HLC::new());
    let objects = Arc::new(DiskObjectStore::new(p.clone()).unwrap());
    let refs = Arc::new(RefStoreImpl::open(p.clone(), hlc.clone()).unwrap());
    let (workspaces, leases, gc) =
        build_workspace_stack(p.clone(), objects.clone(), refs.clone(), hlc).unwrap();
    AppState {
        objects: objects.clone() as Arc<dyn ledge_core::ObjectStore>,
        objects_disk: objects.clone(),
        refs: refs.clone() as Arc<dyn ledge_core::RefStore>,
        workspaces,
        leases,
        gc,
        default_ttl_secs: 3600,
        data_dir: p,
        raft_shards,
        cluster_refs: None,
        shard_map: None,
    }
}

#[tokio::test]
async fn cluster_routes_503_when_disabled() {
    let dir = TempDir::new().unwrap();
    let app = build_app(state_with_shards(&dir, None));

    let status = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/cluster/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        status.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "single-node /cluster/status must be 503 (not clustered)"
    );

    let vote = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/raft/0/vote")
                .body(Body::from(vec![0u8; 4]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        vote.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "single-node /raft/*/vote must be 503 (not clustered)"
    );
}

/// Extract a `ShardId -> Raft<TypeConfig>` map from a started in-process cluster:
/// for each shard pick this node's replica's Raft handle (clone — `Raft` is
/// `Arc`-backed and cheap to clone).
fn handles_for_node(cluster: &MultiShardCluster, node: NodeId) -> ClusterHandles {
    let mut map: ClusterHandles = BTreeMap::new();
    for s in 0..cluster.num_shards {
        let shard = ShardId(s);
        let raft = cluster
            .replicas_of(shard)
            .iter()
            .find(|r| r.node == node)
            .expect("node hosts this shard")
            .raft
            .clone();
        map.insert(shard, raft);
    }
    map
}

#[tokio::test]
async fn cluster_status_shape_when_enabled() {
    // A single-node, single-shard in-process cluster (initialized + elected).
    let cluster = MultiShardCluster::start(1, &[1]).await;
    let handles = Arc::new(handles_for_node(&cluster, 1));

    let dir = TempDir::new().unwrap();
    let app = build_app(state_with_shards(&dir, Some(handles)));

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/cluster/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let b = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let j: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let shards = j["shards"].as_array().expect("shards array");
    assert_eq!(shards.len(), 1, "exactly one shard");
    assert_eq!(shards[0]["shard"].as_u64(), Some(0));
    assert_eq!(
        shards[0]["leader"].as_u64(),
        Some(1),
        "the sole node must be its own leader"
    );
    assert!(
        shards[0]["term"].as_u64().unwrap() >= 1,
        "an elected term is >= 1"
    );
    let members: Vec<u64> = shards[0]["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    assert_eq!(members, vec![1]);
}

/// Build a single, *uninitialized* (no leader) `Raft<TypeConfig>` for shard 0,
/// so an external heartbeat is a valid append (mirrors `net_http`'s test peer).
async fn one_uninitialized_raft() -> openraft::Raft<TypeConfig> {
    use ledge_cluster::net_mem::{MemNetworkFactory, Registry};
    use ledge_raft::{LogStore, StateMachineStore};
    let log = LogStore::default();
    let sm = StateMachineStore::new_temp().await;
    let net = MemNetworkFactory::new(ShardId(0), Registry::new());
    let cfg = Arc::new(
        openraft::Config {
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    openraft::Raft::new(1, cfg, net, log, sm)
        .await
        .expect("Raft::new")
}

#[tokio::test]
async fn raft_append_endpoint_feeds_local_raft() {
    use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse};
    use openraft::Vote;

    // An uninitialized node: it has no leader, so an external heartbeat from a
    // term-1 leader is accepted (Success). Using an already-elected leader here
    // would be an invalid append (a leader does not receive heartbeats for its
    // own term) and openraft would reject it — not what this route test covers.
    let raft = one_uninitialized_raft().await;
    let mut map: ClusterHandles = BTreeMap::new();
    map.insert(ShardId(0), raft.clone());
    let dir = TempDir::new().unwrap();
    let app = build_app(state_with_shards(&dir, Some(Arc::new(map))));

    let req = AppendEntriesRequest::<TypeConfig> {
        vote: Vote::new_committed(1, 1),
        prev_log_id: None,
        entries: vec![],
        leader_commit: None,
    };
    let body = bincode::serde::encode_to_vec(&req, bincode::config::standard()).unwrap();

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/raft/0/append")
                .header("content-type", "application/octet-stream")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // The 200 body is the bincode `WireResult` envelope (private to net_http).
    // bincode-standard encodes the enum discriminant as a leading varint u32
    // (0 = Ok, 1 = Err) followed by the payload `Vec<u8>`; decode that public
    // shape, then decode the inner `AppendEntriesResponse`.
    let out = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let (env, _len): ((u32, Vec<u8>), usize) =
        bincode::serde::decode_from_slice(&out, bincode::config::standard())
            .expect("decode WireResult envelope as (tag, payload)");
    assert_eq!(env.0, 0, "WireResult::Ok discriminant (a served heartbeat)");
    let resp: AppendEntriesResponse<NodeId> =
        bincode::serde::decode_from_slice(&env.1, bincode::config::standard())
            .expect("decode AppendEntriesResponse")
            .0;
    assert!(
        matches!(
            resp,
            AppendEntriesResponse::Success | AppendEntriesResponse::PartialSuccess(_)
        ),
        "external heartbeat to an uninitialized node should succeed, got {resp:?}"
    );

    raft.shutdown().await.ok();
}

// ── `/cluster/ref-op` end-to-end through the real server handler ──────────────

use ledge_cluster::{ClusterOp, ClusterRefStore, RefOpResponse, Replica, ShardMap};

/// Build a cluster-mode `AppState` over a started in-process cluster: wires the
/// per-shard Raft handles, the concrete `ClusterRefStore` (for `apply_local_op`),
/// and the shard map (placement). Node `node`'s store hosts only the shards the
/// map assigns it; remote shards are 421'd by the handler (it never forwards a
/// shard-targeted op that landed on the wrong host).
fn cluster_state(
    dir: &TempDir,
    cluster: &MultiShardCluster,
    node: NodeId,
    map: &ShardMap,
) -> AppState {
    let p = dir.path().to_path_buf();
    let hlc = Arc::new(HLC::new());
    let objects = Arc::new(DiskObjectStore::new(p.clone()).unwrap());
    let refs_disk = Arc::new(RefStoreImpl::open(p.clone(), hlc.clone()).unwrap());
    let (workspaces, leases, gc) =
        build_workspace_stack(p.clone(), objects.clone(), refs_disk.clone(), hlc).unwrap();
    // The node's clustered ref store (hosts only its mapped shards); the handler
    // never forwards, so an inert in-memory forwarder is fine.
    let cluster_refs: Arc<ClusterRefStore> = cluster.cluster_ref_store_hosting(
        node,
        map,
        Arc::new(ledge_cluster::InMemoryForwarder::new()),
    );
    // Only the shards `node` actually hosts (node 1 hosts shard 0, not shard 1).
    let mut hosted: ClusterHandles = BTreeMap::new();
    for s in 0..cluster.num_shards {
        let shard = ShardId(s);
        if let Some(rep) = cluster.replicas_of(shard).iter().find(|r| r.node == node) {
            hosted.insert(shard, rep.raft.clone());
        }
    }
    let handles = Arc::new(hosted);
    AppState {
        objects: objects.clone() as Arc<dyn ledge_core::ObjectStore>,
        objects_disk: objects.clone(),
        refs: cluster_refs.clone() as Arc<dyn ledge_core::RefStore>,
        workspaces,
        leases,
        gc,
        default_ttl_secs: 3600,
        data_dir: p,
        raft_shards: Some(handles),
        cluster_refs: Some(cluster_refs),
        shard_map: Some(map.clone()),
    }
}

/// The 2-shard distinct-subset map used by the ref-op tests: shard0={1}, so node
/// 1 hosts shard 0 only; shard1={2,3} (node 1 does NOT host it → 421).
fn two_shard_map() -> ShardMap {
    ShardMap::from_entries([
        (
            ShardId(0),
            vec![Replica {
                node_id: 1,
                addr: "inproc-1".into(),
            }],
        ),
        (
            ShardId(1),
            vec![
                Replica {
                    node_id: 2,
                    addr: "http://n2".into(),
                },
                Replica {
                    node_id: 3,
                    addr: "http://n3".into(),
                },
            ],
        ),
    ])
    .unwrap()
}

#[tokio::test]
async fn cluster_ref_op_applies_to_hosted_shard() {
    let map = two_shard_map();
    let cluster = MultiShardCluster::start_placed(&map).await;
    let dir = TempDir::new().unwrap();
    let app = build_app(cluster_state(&dir, &cluster, 1, &map));

    // A ref-op targeted at shard 0 (which node 1 hosts) applies and returns the
    // bincode `RefOpResponse::Updated`. Wire body = forwarder's `(ShardId, op)`.
    let cfg = bincode::config::standard();
    let op = ClusterOp::Update {
        name: "refs/heads/applied".into(),
        target_bytes: [0x42; 32],
        expected_bytes: None,
    };
    let body = bincode::serde::encode_to_vec((ShardId(0), &op), cfg).unwrap();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/cluster/ref-op")
                .header("content-type", "application/octet-stream")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let out = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let (decoded, _): (RefOpResponse, usize) =
        bincode::serde::decode_from_slice(&out, cfg).unwrap();
    match decoded {
        RefOpResponse::Updated(e) => {
            assert_eq!(e.target, ledge_core::ObjectId::from_bytes([0x42; 32]));
        }
        other => panic!("expected Updated, got {other:?}"),
    }
}

#[tokio::test]
async fn cluster_ref_op_misdirected_returns_421_with_members() {
    let map = two_shard_map();
    let cluster = MultiShardCluster::start_placed(&map).await;
    let dir = TempDir::new().unwrap();
    // Node 1 does NOT host shard 1 → a shard-1 op must 421 with shard 1's members.
    let app = build_app(cluster_state(&dir, &cluster, 1, &map));

    let cfg = bincode::config::standard();
    let op = ClusterOp::Get {
        name: "refs/heads/elsewhere".into(),
    };
    let body = bincode::serde::encode_to_vec((ShardId(1), &op), cfg).unwrap();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/cluster/ref-op")
                .header("content-type", "application/octet-stream")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::MISDIRECTED_REQUEST,
        "a shard this node does not host must 421"
    );
    let out = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let (members, _): (Vec<(u64, String)>, usize) =
        bincode::serde::decode_from_slice(&out, cfg).unwrap();
    // The 421 body carries shard 1's declared hosting members so the caller can
    // retry against a real host.
    assert_eq!(
        members,
        vec![
            (2u64, "http://n2".to_string()),
            (3u64, "http://n3".to_string())
        ]
    );
}

#[tokio::test]
async fn cluster_ref_op_503_when_single_node() {
    // Single-node AppState (no cluster_refs/shard_map) → the route is inert (503).
    let dir = TempDir::new().unwrap();
    let app = build_app(state_with_shards(&dir, None));
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/cluster/ref-op")
                .body(Body::from(vec![0u8; 4]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}
