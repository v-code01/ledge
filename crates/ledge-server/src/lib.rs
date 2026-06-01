pub mod admin_routes;
pub mod cluster_routes;
pub mod config;
pub mod metrics;
pub mod object_routes;
pub mod routes;
pub mod rpc_routes;
pub mod workspace_routes;

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
use ledge_cluster::{ClusterRefStore, ReplicatedObjectStore, ShardId, ShardRouter};
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
    build_workspace_stack_dyn(data_dir, objects, refs as Arc<dyn RefStore>, hlc)
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
) -> ledge_core::Result<(Arc<WorkspaceManager>, Arc<LeaseStore>, Arc<Gc>)> {
    let leases = Arc::new(LeaseStore::open(data_dir, hlc.clone())?);
    let manager = Arc::new(WorkspaceManager::new(refs.clone(), leases.clone(), hlc));
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
    /// `Arc<ReplicatedObjectStore>` up-cast — `AppState.objects`.
    pub objects: Arc<dyn ObjectStore>,
    /// Node-local concrete disk store — `AppState.objects_disk` (git/RPC/GC).
    pub objects_disk: Arc<DiskObjectStore>,
    /// Per-shard Raft handles for this node — `AppState.raft_shards`.
    pub shards: Arc<ClusterHandles>,
}

/// Assemble the clustered storage stack: one Raft group per shard over a
/// disk-backed [`WalLogStore`], wired to a [`ClusterRefStore`] (`dyn RefStore`
/// seam) and a [`ReplicatedObjectStore`] (`dyn ObjectStore` seam) atop the
/// node-local [`DiskObjectStore`].
///
/// This runs ONLY when `cluster.enabled` (gated in `main.rs`); the single-node
/// path is byte-identical to Phase 1/2 and never touches this. The per-shard
/// Raft handles are returned so the `/raft/*` + `/cluster/*` routes can feed
/// inbound RPCs into the local node and the metrics poller can read each shard's
/// `Raft::metrics()`.
///
/// # Phase 3 scope notes
/// - The state machine is built with [`StateMachineStore::open`]: applied
///   ref/lease state, the last-applied log id + membership, and the current
///   snapshot all persist under `shard-{s}/sm`, so a node survives restart even
///   after openraft purges the snapshotted log prefix. The Raft **log** IS also
///   disk-durable via `WalLogStore`.
/// - `ReplicatedObjectStore` is constructed with **no peers** here (a node knows
///   its own local store; cross-node object peering is wired with the same HTTP
///   transport as the ref Raft in a follow-up). The git/object wire path is
///   node-local in Phase 3 (see [`AppState`]).
#[allow(clippy::too_many_arguments)]
pub async fn build_cluster_stack(
    data_dir: std::path::PathBuf,
    objects_disk: Arc<DiskObjectStore>,
    hlc: Arc<HLC>,
    node_id: u64,
    num_shards: u32,
    peers: HashMap<u64, String>,
    raft_config: Arc<openraft::Config>,
) -> ledge_core::Result<ClusterStack> {
    let router = ShardRouter::new(num_shards);
    let mut shard_handles: BTreeMap<ShardId, Vec<ShardHandle>> = BTreeMap::new();
    let mut raft_handles: ClusterHandles = BTreeMap::new();

    for s in 0..num_shards {
        let shard = ShardId(s);
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
        let net = HttpRaftNetworkFactory::new(shard, peers.clone());
        let raft = openraft::Raft::<TypeConfig>::new(node_id, raft_config.clone(), net, log, sm)
            .await
            .map_err(|e| ledge_core::LedgeError::Io(std::io::Error::other(e.to_string())))?;

        // Each node holds only its OWN replica handle here (production shape):
        // peers are reached by the HTTP Raft network, not an in-process registry.
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

    let cluster_refs = Arc::new(ClusterRefStore::new(node_id, router, shard_handles));
    // Within-shard content-addressed quorum replication (spec §2.5): every OTHER
    // node in the cluster is a replica of this shard's objects, reached over the
    // same HTTP base URL it serves `/raft/*` on (the `/objects/*` routes are
    // mounted on the same router). One `HttpObjectPeer` per peer (excluding
    // self). A `write` returns once a quorum (`n/2+1` of local + peers) is
    // durable; missing replicas self-repair via anti-entropy on read.
    //
    // Phase 3 replica-set model: one replica set per shard spanning all nodes.
    // `ShardId(0)` is used as the peer's shard segment — it is accepted for
    // routing symmetry and the node-local store holds whatever shards it hosts;
    // the content address is shard-independent, so the segment does not affect
    // correctness here.
    let object_client = reqwest::Client::new();
    let object_peers: Vec<Arc<dyn ledge_cluster::ObjectPeer>> = peers
        .iter()
        .filter(|(id, _)| **id != node_id)
        .map(|(_, addr)| {
            Arc::new(HttpObjectPeer::with_client(
                addr.clone(),
                ShardId(0),
                object_client.clone(),
            )) as Arc<dyn ledge_cluster::ObjectPeer>
        })
        .collect();
    let replicated = Arc::new(ReplicatedObjectStore::new(objects_disk.clone(), object_peers));

    Ok(ClusterStack {
        refs: cluster_refs as Arc<dyn RefStore>,
        objects: replicated as Arc<dyn ObjectStore>,
        objects_disk,
        shards: Arc::new(raft_handles),
    })
}

pub fn build_app(state: AppState) -> Router {
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
                )),
        )
}
