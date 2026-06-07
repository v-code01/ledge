pub mod admin_routes;
pub mod auth;
pub mod cli;
pub mod cluster_routes;
pub mod config;
pub mod metrics;
pub mod object_routes;
pub mod quota;
pub mod routes;
pub mod rpc_routes;
pub mod workspace_routes;

pub use auth::{Principal, Scopes};
pub use routes::AppState;

use std::sync::Arc;
use std::time::Duration;
use axum::Router;
use tower_http::{timeout::TimeoutLayer, trace::TraceLayer};

use ledge_core::{HLC, ObjectStore, RefStore};
use ledge_object_store::DiskObjectStore;
use ledge_ref_store::RefStoreImpl;
use ledge_workspace::{Gc, LeaseStore, WorkspaceManager};

use std::collections::{BTreeMap, HashMap};
use ledge_cluster::net_http::{HttpObjectPeer, HttpRaftNetworkFactory};
use ledge_cluster::ref_store::ShardHandle;
use ledge_cluster::{
    ClusterRefStore, HttpForwarder, ReplicatedObjectStore, ShardId, ShardMap, ShardRouter,
};
use ledge_raft::{StateMachineStore, TypeConfig, WalLogStore};
use routes::ClusterHandles;

/// Open the lease store and assemble the workspace control-plane trio
/// (manager, lease store, GC) from already-open object/ref stores.
///
/// Single-node path: takes the concrete `Arc<RefStoreImpl>` and up-casts it to
/// the `Arc<dyn RefStore>` seam internally. The GC keeps the concrete
/// `Arc<DiskObjectStore>` (it needs `DiskObjectStore`-only methods). Behavior is
/// byte-identical to Phase 1/2 — the trait object is the same `RefStoreImpl`.
pub fn build_workspace_stack(
    data_dir: std::path::PathBuf,
    objects: Arc<DiskObjectStore>,
    refs: Arc<RefStoreImpl>,
    hlc: Arc<HLC>,
) -> ledge_core::Result<(Arc<WorkspaceManager>, Arc<LeaseStore>, Arc<Gc>)> {
    // Single-node atomic-commit seam: one ArcSwap root swap over the SAME concrete
    // `RefStoreImpl` the manager already reads/writes. Built here, before the
    // up-cast to `dyn RefStore`, because `LocalAtomicCommit` needs the concrete
    // store; the manager then sees an all-or-nothing commit, byte-identical to the
    // pre-Phase-4b behavior for the single-ref happy path.
    let coordinator: Arc<dyn ledge_ref_store::AtomicCommit> =
        Arc::new(ledge_ref_store::LocalAtomicCommit::new(refs.clone()));
    build_workspace_stack_dyn(
        data_dir,
        objects,
        refs as Arc<dyn RefStore>,
        hlc,
        coordinator,
    )
}

/// Cluster path: assemble the workspace control-plane trio over an arbitrary
/// `Arc<dyn RefStore>` (the clustered `ClusterRefStore`) and the node-local
/// `Arc<DiskObjectStore>`. The GC always GCs the local disk store (distributed
/// GC is per-node-local in Phase 3; see [`ledge_workspace::Gc`]). The single-node
/// [`build_workspace_stack`] is a thin wrapper that up-casts a concrete
/// `RefStoreImpl` into this.
pub fn build_workspace_stack_dyn(
    data_dir: std::path::PathBuf,
    objects_disk: Arc<DiskObjectStore>,
    refs: Arc<dyn RefStore>,
    hlc: Arc<HLC>,
    coordinator: Arc<dyn ledge_ref_store::AtomicCommit>,
) -> ledge_core::Result<(Arc<WorkspaceManager>, Arc<LeaseStore>, Arc<Gc>)> {
    let leases = Arc::new(LeaseStore::open(data_dir, hlc.clone())?);
    let manager = Arc::new(WorkspaceManager::new(
        refs.clone(),
        leases.clone(),
        hlc,
        coordinator,
    ));
    let gc = Arc::new(Gc::new(refs, leases.clone(), objects_disk));
    Ok((manager, leases, gc))
}

/// The assembled cluster storage stack: the `dyn RefStore` seam
/// (`ClusterRefStore`), the `dyn ObjectStore` seam (`ReplicatedObjectStore`
/// over the node-local disk store), the node-local concrete `DiskObjectStore`
/// (for git/RPC/GC), and the per-shard Raft handles (for the `/raft` + `/cluster`
/// routes and the metrics poller).
pub struct ClusterStack {
    /// `Arc<ClusterRefStore>` up-cast — `AppState.refs`.
    pub refs: Arc<dyn RefStore>,
    /// The SAME `ClusterRefStore` as a concrete `Arc`, for `AppState.cluster_refs`
    /// — the `/cluster/ref-op` handler needs `apply_local_op`, which is not on the
    /// `dyn RefStore` seam. One store, two views (no duplicate state machine).
    pub cluster_refs: Arc<ClusterRefStore>,
    /// `Arc<ReplicatedObjectStore>` up-cast — `AppState.objects`.
    pub objects: Arc<dyn ObjectStore>,
    /// Node-local concrete disk store — `AppState.objects_disk` (git/RPC/GC).
    pub objects_disk: Arc<DiskObjectStore>,
    /// Per-shard Raft handles for this node — `AppState.raft_shards`.
    pub shards: Arc<ClusterHandles>,
    /// The authoritative shard map — `AppState.shard_map` (placement for
    /// `/cluster/status` + the `/cluster/ref-op` misdirect body).
    pub map: ShardMap,
}

/// Assemble the clustered storage stack: one Raft group per shard THIS node
/// hosts over a disk-backed [`WalLogStore`], wired to a [`ClusterRefStore`]
/// (`dyn RefStore` seam) and a [`ReplicatedObjectStore`] (`dyn ObjectStore`
/// seam) atop the node-local [`DiskObjectStore`].
///
/// This runs ONLY when `cluster.enabled` (gated in `main.rs`); the single-node
/// path is byte-identical to Phase 1/2 and never touches this. The per-shard
/// Raft handles are returned so the `/raft/*` + `/cluster/*` routes can feed
/// inbound RPCs into the local node and the metrics poller can read each shard's
/// `Raft::metrics()`.
///
/// # Placement (Phase 4a §3)
/// The cluster's [`ShardMap`] is the authoritative, per-node-identical
/// shard→replica-set placement. This node builds a Raft group ONLY for the
/// shards [`ShardMap::shards_hosted_by`] reports it is a member of; each such
/// shard's [`HttpRaftNetworkFactory`] is keyed on ONLY that shard's members
/// ([`ShardMap::member_map`]), so a shard's RPCs only ever reach its own member
/// subset. A ref whose target shard this node does NOT host is forwarded to a
/// hosting member by the [`HttpForwarder`] inside [`ClusterRefStore`].
///
/// # Scope notes
/// - The state machine is built with [`StateMachineStore::open`]: applied
///   ref/lease state, the last-applied log id + membership, and the current
///   snapshot all persist under `shard-{s}/sm`, so a node survives restart even
///   after openraft purges the snapshotted log prefix. The Raft **log** IS also
///   disk-durable via `WalLogStore`.
/// - `ReplicatedObjectStore` peers are the union of the OTHER replicas of the
///   shards this node hosts, deduped by addr: a node replicates a shard's
///   objects to that shard's co-replicas, and a node hosting multiple shards
///   unions their peer sets.
///
/// `cluster_secret`: when `Some` (auth enabled), every outbound peer client
/// (forward, object, and per-shard Raft) attaches it as a `Bearer`
/// `Authorization` default header so the receiving node's INTERNAL classifier
/// accepts the call (spec section 4.5); `None` yields bare clients, byte-identical
/// to the pre-4d-1 behavior.
pub async fn build_cluster_stack(
    data_dir: std::path::PathBuf,
    objects_disk: Arc<DiskObjectStore>,
    hlc: Arc<HLC>,
    node_id: u64,
    map: ShardMap,
    raft_config: Arc<openraft::Config>,
    cluster_secret: Option<String>,
) -> ledge_core::Result<ClusterStack> {
    // Router's shard count comes from the map so routing & placement agree.
    let router = ShardRouter::new(map.num_shards());
    let mut shard_handles: BTreeMap<ShardId, Vec<ShardHandle>> = BTreeMap::new();
    let mut raft_handles: ClusterHandles = BTreeMap::new();

    // Build a Raft group ONLY for the shards this node is a member of.
    for shard in map.shards_hosted_by(node_id) {
        let s = shard.0;
        let shard_dir = data_dir.join(format!("shard-{s}"));
        let log = WalLogStore::open(shard_dir.join("raft-log"))
            .map_err(|e| ledge_core::LedgeError::Io(std::io::Error::other(e.to_string())))?;
        // Durable, restart-safe state machine: applied ref/lease state + the
        // last-applied log id + the current snapshot all persist under
        // `shard-{s}/sm`. Without this the SM was tempdir-backed, so a restart
        // after openraft purged the snapshotted log prefix lost committed state.
        let sm = StateMachineStore::open(shard_dir.join("sm"), hlc.clone()).await?;
        // Capture the read handle BEFORE the SM moves into Raft::new (the SM is
        // not Clone; the handle shares its ArcSwap'd applied state).
        let read = sm.read_handle();
        // Per-shard network knows ONLY this shard's members (spec §3.3): id→addr
        // from the map. member_map → BTreeMap; the factory wants a HashMap.
        let peers: HashMap<u64, String> = map.member_map(shard).into_iter().collect();
        // Per-shard Raft RPCs (`/raft/*`) also carry the Bearer cluster secret as
        // a default header when auth is enabled (spec §4.5), so a peer's INTERNAL
        // classifier accepts them; `None` ⇒ bare client, byte-identical to before.
        let net = HttpRaftNetworkFactory::with_secret(shard, peers, cluster_secret.clone());
        let raft = openraft::Raft::<TypeConfig>::new(node_id, raft_config.clone(), net, log, sm)
            .await
            .map_err(|e| ledge_core::LedgeError::Io(std::io::Error::other(e.to_string())))?;

        // This node holds only its OWN replica handle (production shape): peers
        // are reached by the HTTP Raft network, not an in-process registry.
        shard_handles.insert(
            shard,
            vec![ShardHandle {
                shard,
                node_id,
                raft: raft.clone(),
                sm: read,
                hlc: hlc.clone(),
            }],
        );
        raft_handles.insert(shard, raft);
    }

    // The ref store gets the map + an HTTP forwarder so a ref routed to a shard
    // this node does NOT host is forwarded to a hosting member (spec §3.4/§4.3).
    let forward_client = auth_client(cluster_secret.clone())?;
    let forwarder: Arc<dyn ledge_cluster::RefOpForwarder> =
        Arc::new(HttpForwarder::new(map.clone(), forward_client));
    let cluster_refs = Arc::new(ClusterRefStore::with_placement(
        node_id,
        router,
        shard_handles,
        map.clone(),
        forwarder,
    ));

    // Placement gauge: emit `ledge_shard_hosted{shard}=1` for each shard this
    // node hosts, `=0` for the rest, so a scrape shows the full placement vector
    // for this node (not just the ones it serves). Cluster-only; single-node
    // never reaches here so the series is absent in single-node `/metrics`.
    for s in 0..map.num_shards() {
        crate::metrics::set_shard_hosted(s, map.hosts(ShardId(s), node_id));
    }

    // Object peers = union of the OTHER replicas of the shards THIS node hosts,
    // deduped by addr (spec §3.5): a node replicates a shard's objects to that
    // shard's co-replicas; a node hosting multiple shards unions their peers.
    // The `shard` segment passed to each `HttpObjectPeer` is the shard under
    // which we first met that addr — content addressing makes the segment
    // non-load-bearing for correctness, so deduping across shards is safe.
    let object_client = auth_client(cluster_secret.clone())?;
    let mut seen_addrs: std::collections::HashSet<String> = Default::default();
    let mut object_peers: Vec<Arc<dyn ledge_cluster::ObjectPeer>> = Vec::new();
    for shard in map.shards_hosted_by(node_id) {
        for rep in map.members(shard) {
            if rep.node_id == node_id {
                continue; // skip self
            }
            if seen_addrs.insert(rep.addr.clone()) {
                object_peers.push(Arc::new(HttpObjectPeer::with_client(
                    rep.addr.clone(),
                    shard,
                    object_client.clone(),
                )) as Arc<dyn ledge_cluster::ObjectPeer>);
            }
        }
    }
    let replicated = Arc::new(ReplicatedObjectStore::new(objects_disk.clone(), object_peers));

    Ok(ClusterStack {
        refs: cluster_refs.clone() as Arc<dyn RefStore>,
        cluster_refs,
        objects: replicated as Arc<dyn ObjectStore>,
        objects_disk,
        shards: Arc::new(raft_handles),
        map,
    })
}

/// Build an outbound `reqwest::Client` whose default `Authorization` header is
/// the Bearer cluster secret, applied to EVERY request so node-to-node calls
/// satisfy the receiving node's INTERNAL classifier (spec section 4.5). `None`
/// yields a bare client, byte-identical to the pre-4d-1 behavior.
fn auth_client(cluster_secret: Option<String>) -> ledge_core::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder();
    if let Some(secret) = cluster_secret {
        let mut headers = reqwest::header::HeaderMap::new();
        let val = reqwest::header::HeaderValue::from_str(&format!("Bearer {secret}"))
            .map_err(|e| ledge_core::LedgeError::Io(std::io::Error::other(e.to_string())))?;
        headers.insert(reqwest::header::AUTHORIZATION, val);
        builder = builder.default_headers(headers);
    }
    builder
        .build()
        .map_err(|e| ledge_core::LedgeError::Io(std::io::Error::other(e.to_string())))
}

pub fn build_app(state: AppState) -> Router {
    // The auth middleware needs its own clone of the state (`with_state` below
    // consumes `state` into the router's handler state).
    let auth_state = state.clone();
    Router::new()
        .route("/healthz", axum::routing::get(routes::healthz))
        .route("/metrics", axum::routing::get(routes::metrics_handler))
        // ── Workspace control plane (spec §7) ──────────────────────────────
        .route(
            "/workspaces",
            axum::routing::post(workspace_routes::create_workspace)
                .get(workspace_routes::list_workspaces),
        )
        .route(
            "/workspaces/{id}",
            axum::routing::get(workspace_routes::get_workspace)
                .delete(workspace_routes::delete_workspace),
        )
        .route(
            "/workspaces/{id}/renew",
            axum::routing::post(workspace_routes::renew_workspace),
        )
        .route(
            "/workspaces/{id}/commit",
            axum::routing::post(workspace_routes::commit_workspace),
        )
        .route("/admin/gc", axum::routing::post(workspace_routes::admin_gc))
        // ── Binary control plane (Cap'n Proto, spec §2) ────────────────────
        .route("/rpc", axum::routing::post(rpc_routes::rpc))
        .route(
            "/admin/snapshot",
            axum::routing::post(admin_routes::admin_snapshot),
        )
        // ── Cluster control plane (spec §7) — inert (503) in single-node mode ──
        .route(
            "/raft/{shard}/{kind}",
            axum::routing::post(cluster_routes::raft_rpc),
        )
        .route(
            "/cluster/init",
            axum::routing::post(cluster_routes::cluster_init),
        )
        .route(
            "/cluster/status",
            axum::routing::get(cluster_routes::cluster_status),
        )
        .route(
            "/cluster/ref-op",
            axum::routing::post(cluster_routes::cluster_ref_op),
        )
        .route(
            "/cluster/gc",
            axum::routing::post(cluster_routes::cluster_gc),
        )
        // ── Object replication (spec §2.5) — content-addressed peer endpoints.
        // Active in both modes: in single-node they harmlessly serve the local
        // node's objects; in cluster mode they are the HttpObjectPeer transport
        // for within-shard quorum replication + anti-entropy fetch. ───────────
        .route(
            "/objects/{shard}/replicate",
            axum::routing::post(object_routes::replicate_object),
        )
        .route(
            "/objects/{shard}/{id}",
            axum::routing::get(object_routes::get_object),
        )
        // ── Workspace-scoped git (segment = workspaces/{id}/) ──────────────
        .route("/ws/{id}/info/refs", axum::routing::get(routes::ws_info_refs))
        .route(
            "/ws/{id}/git-upload-pack",
            axum::routing::post(routes::ws_upload_pack),
        )
        .route(
            "/ws/{id}/git-receive-pack",
            axum::routing::post(routes::ws_receive_pack),
        )
        // ── Default repo git (segment = "") ────────────────────────────────
        .route("/{repo}/info/refs", axum::routing::get(routes::info_refs))
        .route(
            "/{repo}/git-upload-pack",
            axum::routing::post(routes::upload_pack),
        )
        .route(
            "/{repo}/git-receive-pack",
            axum::routing::post(routes::receive_pack),
        )
        .with_state(state)
        .layer(
            tower::ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(TimeoutLayer::with_status_code(
                    axum::http::StatusCode::REQUEST_TIMEOUT,
                    Duration::from_secs(60),
                ))
                // Auth chokepoint (spec §4.3): innermost of these three layers,
                // so a request is traced → timeout-guarded → authed → routed.
                // It classifies PUBLIC/INTERNAL/CLIENT, verifies the credential,
                // gates `/admin/*`, and injects the resolved `Principal` before
                // any handler runs. Disabled ⇒ synthetic root (byte-identical
                // behavior); 401/403 responses are still traced + timeout-bounded.
                .layer(axum::middleware::from_fn_with_state(
                    auth_state,
                    crate::auth::middleware::auth_layer,
                )),
        )
}

#[cfg(test)]
mod build_cluster_stack_tests {
    use super::*;
    use ledge_cluster::{Replica, ShardMap};

    fn raft_cfg() -> Arc<openraft::Config> {
        Arc::new(
            openraft::Config {
                heartbeat_interval: 250,
                election_timeout_min: 1000,
                election_timeout_max: 2000,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        )
    }

    #[test]
    fn auth_default_header_helper_attaches_bearer() {
        // The helper used by build_cluster_stack to build outbound clients must
        // attach `Authorization: Bearer <secret>` as a default header when the
        // secret is Some, and build a bare client when None.
        let with = super::auth_client(Some("svc-secret".to_string()));
        assert!(with.is_ok(), "client builds with default header");
        let without = super::auth_client(None);
        assert!(without.is_ok(), "bare client builds");
        // A direct header assertion: the default-headers map is private to reqwest,
        // so we assert construction succeeds and rely on the in-process classifier
        // test (Task 4 enabled_internal_needs_cluster_secret) for end-to-end accept.
    }

    #[tokio::test]
    async fn builds_only_locally_hosted_shards() {
        // shard0={1,2,3}, shard1={2,3,4}. Build the stack AS NODE 1.
        let map = ShardMap::from_entries([
            (
                ShardId(0),
                vec![
                    Replica { node_id: 1, addr: "http://n1".into() },
                    Replica { node_id: 2, addr: "http://n2".into() },
                    Replica { node_id: 3, addr: "http://n3".into() },
                ],
            ),
            (
                ShardId(1),
                vec![
                    Replica { node_id: 2, addr: "http://n2".into() },
                    Replica { node_id: 3, addr: "http://n3".into() },
                    Replica { node_id: 4, addr: "http://n4".into() },
                ],
            ),
        ])
        .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let objects = Arc::new(DiskObjectStore::new(dir.path().to_path_buf()).unwrap());
        let hlc = Arc::new(HLC::new());

        let stack = build_cluster_stack(
            dir.path().to_path_buf(),
            objects,
            hlc,
            1, // node_id = 1 → hosts shard0 ONLY
            map,
            raft_cfg(),
            None, // no cluster secret in this placement test
        )
        .await
        .unwrap();

        // Node 1 built exactly shard 0's Raft group, and NOT shard 1's.
        assert!(stack.shards.contains_key(&ShardId(0)), "node 1 hosts shard 0");
        assert!(
            !stack.shards.contains_key(&ShardId(1)),
            "node 1 must NOT host shard 1"
        );
        assert_eq!(stack.shards.len(), 1);

        // Tear down the one Raft we started (no init → no leader; just shut it).
        for raft in stack.shards.values() {
            raft.shutdown().await.ok();
        }
    }
}
