use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use clap::Parser;
use ledge_core::HLC;
use ledge_object_store::DiskObjectStore;
use ledge_ref_store::RefStoreImpl;
use ledge_server::{build_app, config::LedgeConfig, AppState};
use tracing::info;

/// The selected storage seams: the `dyn ObjectStore` seam, the `dyn RefStore`
/// seam, the optional per-shard Raft handles, the optional concrete cluster ref
/// store (for `/cluster/ref-op`'s `apply_local_op`), and the optional shard map
/// (for placement). The trailing three are `Some` together only in cluster mode.
type StorageSeams = (
    Arc<dyn ledge_core::ObjectStore>,
    Arc<dyn ledge_core::RefStore>,
    Option<Arc<ledge_server::routes::ClusterHandles>>,
    Option<Arc<ledge_cluster::ClusterRefStore>>,
    Option<ledge_cluster::ShardMap>,
    // The atomic-commit seam the workspace manager promotes through: single-node
    // = `LocalAtomicCommit` (one ArcSwap swap); clustered = `TxnCoordinator`
    // (single-shard RefBatch fast path + multi-shard 2PC).
    Arc<dyn ledge_ref_store::AtomicCommit>,
);

#[derive(Parser, Debug)]
#[command(name = "ledge", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand, Debug)]
enum Commands {
    Start(StartArgs),
}

#[derive(clap::Args, Debug)]
struct StartArgs {
    #[arg(long)]
    addr: Option<String>,
    #[arg(long)]
    data_dir: Option<String>,
    #[arg(long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let Commands::Start(args) = cli.command;
    let mut cfg = LedgeConfig::load(args.config.as_ref())?;
    if let Some(addr) = args.addr {
        cfg.server.addr = addr;
    }
    if let Some(dir) = args.data_dir {
        cfg.server.data_dir = dir;
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .json()
        .init();
    info!(addr = %cfg.server.addr, "ledge starting");
    let data_dir = PathBuf::from(&cfg.server.data_dir);
    let hlc = Arc::new(HLC::new());
    let objects = Arc::new(DiskObjectStore::new(data_dir.clone())?);
    ledge_server::metrics::install_recorder()?;

    // ── Storage seam selection ───────────────────────────────────────────────
    // single-node (default): the dyn RefStore/ObjectStore seams are the same
    // concrete local stores, up-cast — byte-identical to Phase 1/2.
    // clustered (cfg.cluster.enabled): the dyn seams are the ClusterRefStore /
    // ReplicatedObjectStore over per-shard Raft, plus the per-shard handles for
    // the /raft + /cluster routes and the metrics poller.
    let (objects_dyn, refs_dyn, raft_shards, cluster_refs, shard_map, coordinator): StorageSeams =
        if cfg.cluster.enabled {
            // Build the authoritative shard map from config (identical on every
            // node). This SUPERSEDES the flat num_shards/peers fields; routing,
            // per-shard Raft membership, and ref-op forwarding all derive from it.
            let map = cfg
                .cluster
                .shard_map()
                .map_err(|e| anyhow::anyhow!("invalid [[cluster.shards]] map: {e}"))?;
            // openraft timer config: production-leaning defaults; election window
            // comfortably above the heartbeat so a stable leader holds the lease.
            let raft_config = Arc::new(
                openraft::Config {
                    heartbeat_interval: 250,
                    election_timeout_min: 1000,
                    election_timeout_max: 2000,
                    ..Default::default()
                }
                .validate()
                .map_err(|e| anyhow::anyhow!("invalid raft config: {e}"))?,
            );
            info!(
                node_id = cfg.cluster.node_id,
                num_shards = map.num_shards(),
                hosted = map.shards_hosted_by(cfg.cluster.node_id).len(),
                "cluster mode enabled: assembling per-shard Raft groups for hosted shards"
            );
            let stack = ledge_server::build_cluster_stack(
                data_dir.clone(),
                objects.clone(),
                hlc.clone(),
                cfg.cluster.node_id,
                map,
                raft_config,
            )
            .await?;

            // ── Per-shard Raft metrics poller (cluster only) ─────────────────────
            // One task per shard, watching that shard's metrics `watch::Receiver`
            // and projecting into the per-shard gauges. Single-node never starts
            // this, so those series are absent and /metrics is unchanged.
            for (shard, raft) in stack.shards.iter() {
                let shard_n = shard.0;
                let mut rx = raft.metrics();
                tokio::spawn(async move {
                    loop {
                        {
                            let m = rx.borrow().clone();
                            ledge_server::metrics::record_raft_metrics(
                                shard_n,
                                m.current_leader,
                                m.current_term,
                                m.last_applied.map(|l| l.index),
                                m.last_applied.map(|l| l.index),
                            );
                        }
                        if rx.changed().await.is_err() {
                            break; // Raft shut down: the metrics channel closed.
                        }
                    }
                });
            }

            // Clustered atomic-commit coordinator over the SAME ClusterRefStore:
            // cross-shard promotions go through 2PC, single-shard through one
            // RefBatch. Built here (in `ledge-server`) so `ledge-workspace` never
            // depends on `ledge-cluster` (which would close a crate cycle).
            let coordinator: Arc<dyn ledge_ref_store::AtomicCommit> =
                Arc::new(ledge_cluster::TxnCoordinator::new(stack.cluster_refs.clone()));
            (
                stack.objects,
                stack.refs,
                Some(stack.shards),
                Some(stack.cluster_refs),
                Some(stack.map),
                coordinator,
            )
        } else {
            let refs = Arc::new(RefStoreImpl::open(data_dir.clone(), hlc.clone())?);
            // Single-node atomic-commit seam over the concrete RefStoreImpl.
            let coordinator: Arc<dyn ledge_ref_store::AtomicCommit> =
                Arc::new(ledge_ref_store::LocalAtomicCommit::new(refs.clone()));
            (
                objects.clone() as Arc<dyn ledge_core::ObjectStore>,
                refs as Arc<dyn ledge_core::RefStore>,
                None,
                None,
                None,
                coordinator,
            )
        };

    let (workspaces, leases, gc) = ledge_server::build_workspace_stack_dyn(
        data_dir.clone(),
        objects.clone(),
        refs_dyn.clone(),
        hlc.clone(),
        coordinator,
    )?;

    // ── Lease WAL compaction: collapse the append log to a checkpoint when it
    //    crosses 64 MiB (matching the ref store's default threshold). ─────────────
    leases.spawn_compaction_task(64 * 1024 * 1024);

    // ── Expiry sweeper: every expiry_interval_secs, release each expired lease. ──
    {
        let workspaces = workspaces.clone();
        let leases = leases.clone();
        let interval_secs = cfg.workspace.expiry_interval_secs;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                match leases.expired(now_ms).await {
                    Ok(expired) => {
                        let n = expired.len() as u64;
                        for lease in expired {
                            if let Err(e) = workspaces.release(lease.id).await {
                                tracing::warn!(error = %e, id = %lease.id.to_hex(), "expiry release failed");
                            }
                        }
                        if n > 0 {
                            ledge_server::metrics::record_lease_expired(n);
                            if let Ok(live) = workspaces.list().await {
                                ledge_server::metrics::set_workspaces_active(live.len() as f64);
                            }
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "expiry sweep: lease scan failed"),
                }
            }
        });
    }

    // ── GC scheduler: every gc_interval_secs, run a mark-and-sweep pass. ──────────
    {
        let gc = gc.clone();
        let interval_secs = cfg.workspace.gc_interval_secs;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let start = std::time::Instant::now();
                match gc.run().await {
                    Ok(stats) => {
                        ledge_server::metrics::record_gc_run(&stats, start.elapsed());
                        tracing::info!(reclaimed = stats.reclaimed, bytes_freed = stats.bytes_freed, "scheduled gc pass");
                    }
                    Err(e) => tracing::warn!(error = %e, "scheduled gc pass failed"),
                }
            }
        });
    }

    let app = build_app(AppState {
        // The object/ref seams (`objects_dyn`/`refs_dyn`) were selected above:
        // concrete local stores up-cast in single-node mode (byte-identical to
        // Phase 2), or the ClusterRefStore/ReplicatedObjectStore in cluster mode.
        // `objects_disk` is always the node-local concrete store (git/RPC/GC).
        // `raft_shards` is None single-node ⇒ /raft + /cluster report 503.
        objects: objects_dyn,
        objects_disk: objects.clone(),
        refs: refs_dyn,
        workspaces,
        leases,
        gc,
        default_ttl_secs: cfg.workspace.default_ttl_secs,
        data_dir: data_dir.clone(),
        raft_shards,
        cluster_refs,
        shard_map,
    });
    let addr: SocketAddr = cfg
        .server
        .addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid addr {}: {}", cfg.server.addr, e))?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(bound_addr = %listener.local_addr()?, "ledge listening");
    axum::serve(listener, app).await?;
    Ok(())
}
