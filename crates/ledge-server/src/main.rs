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
    // The concrete `ReplicatedObjectStore` (cluster only) — held so the Phase 4g
    // reconfigure route can swap its replication peer set via `set_peers`.
    Option<Arc<ledge_cluster::ReplicatedObjectStore>>,
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
    /// Run the server (default launch path; byte-identical to pre-4d-1).
    Start(StartArgs),
    /// API-key provisioning (operates on the store at --data-dir; no server).
    Auth {
        #[command(subcommand)]
        cmd: ledge_server::cli::AuthCommand,
        /// Data dir holding the auth store (overrides config/CWD resolution).
        /// `global` so it may appear before OR after the subcommand
        /// (`auth --data-dir D list-keys` and `auth list-keys --data-dir D`).
        #[arg(long, global = true)]
        data_dir: Option<String>,
        #[arg(long, global = true)]
        config: Option<PathBuf>,
    },
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
    // Dispatch the provisioning subcommand BEFORE any server setup: it operates
    // directly on the AuthStore at the resolved data dir and never starts a
    // server, a listener, or the tracing subscriber. The `Start` arm falls
    // through to the byte-identical server launch path below.
    let args = match cli.command {
        Commands::Auth {
            cmd,
            data_dir,
            config,
        } => {
            let mut cfg = LedgeConfig::load(config.as_ref())?;
            if let Some(dir) = data_dir {
                cfg.server.data_dir = dir;
            }
            if let Some(line) =
                ledge_server::cli::run_auth(cmd, PathBuf::from(&cfg.server.data_dir)).await?
            {
                println!("{line}");
            }
            return Ok(());
        }
        Commands::Start(args) => args,
    };
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
    // ── S3 cold tier (default off). When `[s3].enabled`, build an
    //    AmazonS3/MinIO-backed tier from the operator-supplied credentials and
    //    install it on the disk store so `POST /admin/tier` can spill cold pack
    //    bodies off-machine. `from_parts` returns a `ledge_core::Result`; the
    //    `?` lifts its `thiserror`-backed error into main's `anyhow::Result`.
    //    Default-off path is byte-identical to before this block existed. ──────
    if cfg.s3.enabled {
        let tier = ledge_object_store::s3::S3Tier::from_parts(
            cfg.s3.endpoint.as_deref(),
            &cfg.s3.region,
            &cfg.s3.bucket,
            &cfg.s3.access_key_id,
            &cfg.s3.secret_access_key,
            &cfg.s3.prefix,
        )?;
        objects.set_cold(std::sync::Arc::new(tier));
        tracing::info!(bucket = %cfg.s3.bucket, "s3 cold tier enabled");
        // Recover-from-S3 on boot: reconcile the local pack dir with object
        // storage so a wiped/replaced node rebuilds its pack indexes from the
        // cold tier (bodies restore lazily on read). Non-fatal — a recover
        // failure logs a warning; the node still boots and serves hot data.
        match objects.recover_from_s3().await {
            Ok(n) if n > 0 => tracing::info!(packs = n, "recovered packs from S3 on boot"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "s3 recover-on-boot failed"),
        }
    }
    ledge_server::metrics::install_recorder()?;
    // Phase 4d-4: install the rustls crypto provider before ANY TLS config is
    // built (the cluster-stack client config below + the serve listeners need it).
    ledge_server::tls::install_crypto_provider();
    ledge_server::metrics::set_tls_posture(cfg.tls.enabled, cfg.tls.mtls);

    // ── Auth store (Phase 4d-1): opened in both single-node and cluster paths.
    //    Disabled (default) ⇒ an in-memory ctx (the middleware never reads it),
    //    byte-identical to the pre-4d-1 launch. Enabled ⇒ a real WAL-backed store
    //    plus a first-boot bootstrap: if the store is empty and the operator set
    //    `bootstrap_admin_token`, record that operator-supplied token as a root
    //    admin key so a fresh cluster is reachable. Idempotent — it only fires on
    //    an empty store, so a restart never re-bootstraps or duplicates the key. ──
    let auth_ctx = if cfg.auth.enabled {
        let auth_store =
            Arc::new(ledge_server::auth::AuthStore::open(data_dir.clone(), hlc.clone())?);
        match cfg.auth.bootstrap_admin_token.clone() {
            Some(token) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                // `bootstrap_admin_if_empty` only records on an empty store and
                // BLAKE3-hashes the operator's secret via `put_token`; the
                // plaintext token is NEVER logged (only the resulting key_id is).
                match ledge_server::cli::bootstrap_admin_if_empty(&auth_store, &token, now).await? {
                    Some(kid) => info!(key_id = %kid, "bootstrap admin key recorded from config"),
                    None => info!("auth store already provisioned; skipping bootstrap"),
                }
            }
            None if auth_store.list().is_empty() => {
                tracing::warn!(
                    "auth enabled but store empty and no bootstrap_admin_token; \
                     cluster unreachable until a key is minted via `ledge auth create-key`"
                );
            }
            None => {}
        }
        auth_store.spawn_compaction_task(64 * 1024 * 1024);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        ledge_server::metrics::set_auth_keys(auth_store.live_count(now) as f64);
        ledge_server::auth::AuthCtx::new(true, auth_store, cfg.auth.cluster_secret.clone())
    } else {
        ledge_server::auth::AuthCtx::disabled()
    };

    // ── Storage seam selection ───────────────────────────────────────────────
    // single-node (default): the dyn RefStore/ObjectStore seams are the same
    // concrete local stores, up-cast — byte-identical to Phase 1/2.
    // clustered (cfg.cluster.enabled): the dyn seams are the ClusterRefStore /
    // ReplicatedObjectStore over per-shard Raft, plus the per-shard handles for
    // the /raft + /cluster routes and the metrics poller.
    let (
        objects_dyn,
        refs_dyn,
        raft_shards,
        cluster_refs,
        cluster_objects,
        shard_map,
        coordinator,
    ): StorageSeams =
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
            // Phase 4d-4: outbound cluster clients verify peers against tls.ca_path and
            // (under mTLS) present this node's client identity. None ⇒ today's behavior.
            let tls_client_cfg: Option<rustls::ClientConfig> = if cfg.tls.enabled {
                if let Some(ca) = cfg.tls.ca_path.as_deref() {
                    let id = match (
                        cfg.tls.client_cert_path.as_deref(),
                        cfg.tls.client_key_path.as_deref(),
                    ) {
                        (Some(c), Some(k)) if cfg.tls.mtls => Some((c, k)),
                        _ => None,
                    };
                    Some(ledge_server::tls::client_config(ca, id)?)
                } else {
                    None
                }
            } else {
                None
            };
            let stack = ledge_server::build_cluster_stack(
                data_dir.clone(),
                objects.clone(),
                hlc.clone(),
                cfg.cluster.node_id,
                map,
                raft_config,
                if cfg.auth.enabled { cfg.auth.cluster_secret.clone() } else { None },
                tls_client_cfg,
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
                Some(stack.cluster_objects),
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
                None,
                coordinator,
            )
        };

    // Quota (Phase 4d-3): build the real context from `[quotas]` config. Its
    // `usage` Arc is shared into the manager (commit gate), the GC + cluster GC
    // (writers), and `AppState` (gauges) so one store, many parties, no cycle
    // (R Q1/Q4/Q15). Built BEFORE the workspace stack so every party holds the
    // SAME `Arc` + the SAME limits. `enabled=false` (default) ⇒ every gate is a
    // no-op (byte-identical to Phase 4d-2); measurement still runs (feeds gauges).
    let quota_limits = cfg.quotas.to_limits();
    let quota_usage = std::sync::Arc::new(ledge_workspace::UsageMap::default());
    let quota_rate = std::sync::Arc::new(ledge_server::quota::rate::TenantRateLimiter::new(
        cfg.quotas.max_requests_per_sec,
        cfg.quotas.burst,
    ));
    let quota = ledge_server::quota::QuotaCtx {
        limits: quota_limits,
        usage: quota_usage.clone(),
        rate: quota_rate,
    };

    let (workspaces, leases, gc) = ledge_server::build_workspace_stack_dyn(
        data_dir.clone(),
        objects.clone(),
        refs_dyn.clone(),
        hlc.clone(),
        coordinator,
        quota.limits,
        quota.usage.clone(),
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
                            // The sweeper runs cross-tenant; release on behalf of
                            // the lease's OWNING tenant so the ownership check
                            // (manager) matches (root passes "" ⇄ "root").
                            if let Err(e) = workspaces.release(lease.id, &lease.tenant_id).await {
                                tracing::warn!(error = %e, id = %lease.id.to_hex(), "expiry release failed");
                            }
                        }
                        if n > 0 {
                            ledge_server::metrics::record_lease_expired(n);
                            // Active gauge is a CROSS-TENANT total; count all live
                            // leases directly (the manager's list is tenant-scoped).
                            if let Ok(live) = leases.live(now_ms).await {
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
    // (Each pass ALSO refreshes the shared per-tenant UsageMap — the GC holds the
    // same Arc, R Q4/Q9. A startup pass bounds the "fails-open until first measure"
    // window to ≤ one interval — spec §3.4/§6.)
    {
        let gc = gc.clone();
        let quota_usage = quota_usage.clone();
        let interval_secs = cfg.workspace.gc_interval_secs;
        // Publish per-tenant usage gauges from the freshly-stored UsageMap (spec §5).
        let publish_gauges = |usage: &ledge_workspace::UsageMap| {
            for (tenant, u) in usage.load().iter() {
                ledge_server::metrics::set_quota_usage(tenant, u.bytes, u.objects);
            }
        };
        tokio::spawn(async move {
            // Startup measurement: refresh usage once before steady-state ticking
            // so the storage quota (commit gate) + gauges have data at boot.
            match gc.run().await {
                Ok(_) => publish_gauges(&quota_usage),
                Err(e) => tracing::warn!(error = %e, "startup gc/usage measurement failed"),
            }
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let start = std::time::Instant::now();
                match gc.run().await {
                    Ok(stats) => {
                        ledge_server::metrics::record_gc_run(&stats, start.elapsed());
                        publish_gauges(&quota_usage);
                        tracing::info!(reclaimed = stats.reclaimed, bytes_freed = stats.bytes_freed, "scheduled gc pass");
                    }
                    Err(e) => tracing::warn!(error = %e, "scheduled gc pass failed"),
                }
            }
        });
    }

    // ── Distributed-GC driver (cluster only): the node-local `ClusterGc` that
    //    `/admin/gc` runs in cluster mode and `/cluster/gc` fans out. It needs the
    //    concrete cluster ref store (hosted-shard roots + prepared 2PC locks), the
    //    node-local leases, and the node-local disk store as its sweep target.
    //    `leases` is built by `build_workspace_stack_dyn` (above), which is why
    //    this is assembled here rather than inside `build_cluster_stack`. Grace
    //    defaults to 1h (spec §4.4) to fence the object-resurrection race. `None`
    //    single-node ⇒ `/admin/gc` keeps the byte-identical single-node `Gc::run`.
    let cluster_gc = cluster_refs.as_ref().map(|refs| {
        Arc::new(ledge_cluster::gc::ClusterGc::new(
            refs.clone(),
            leases.clone(),
            objects.clone(),
            std::time::Duration::from_secs(3600),
            quota_usage.clone(),
        ))
    });

    // ── Cluster startup measurement (Phase 4d-3, R Q9). There is no cluster GC
    //    SCHEDULER (cluster GC is on-demand via `/admin/gc` + `/cluster/gc`), so
    //    run one node-local pass at boot to refresh the per-tenant UsageMap from
    //    this node's hosted-shard durable refs. Bounds the fails-open window to
    //    boot; subsequent refreshes ride the on-demand GC requests. Per-node
    //    (honest — spec §6).
    if let Some(cgc) = cluster_gc.clone() {
        let quota_usage = quota_usage.clone();
        tokio::spawn(async move {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            match cgc.run(now).await {
                Ok(_) => {
                    // Publish per-tenant usage gauges from this node's measurement (spec §5).
                    for (tenant, u) in quota_usage.load().iter() {
                        ledge_server::metrics::set_quota_usage(tenant, u.bytes, u.objects);
                    }
                }
                Err(e) => tracing::warn!(error = %e, "startup cluster gc/usage measurement failed"),
            }
        });
    }

    // Webhooks (default off): a WAL-backed registry + async signed dispatcher.
    let webhooks = if cfg.webhooks.enabled {
        let store = Arc::new(ledge_server::webhook::store::WebhookStore::open(
            data_dir.clone(),
            hlc.clone(),
        )?);
        store.spawn_compaction_task(8 * 1024 * 1024);
        ledge_server::metrics::set_webhooks_registered(store.count() as f64);
        Some(Arc::new(ledge_server::webhook::dispatch::WebhookDispatcher::new(store)))
    } else {
        None
    };

    // ── Git remote sync (Phase: git-sync) ────────────────────────────────────
    //    Built only when [sync].enabled. The engine clones an upstream bare mirror
    //    and ingests it into a fresh workspace, so it needs the CONCRETE node-local
    //    `Arc<DiskObjectStore>` (`objects`) for git-faithful object writes — NOT the
    //    `dyn ObjectStore` seam — plus the `dyn RefStore` seam (`refs_dyn`) and the
    //    workspace manager. `None` (default) ⇒ `POST /sync/import` reports 503.
    let sync = if cfg.sync.enabled {
        Some(std::sync::Arc::new(ledge_server::sync::SyncEngine::new(
            objects.clone(),
            refs_dyn.clone(),
            workspaces.clone(),
            cfg.sync.allowed_upstream_hosts.clone(),
        )))
    } else {
        None
    };

    // Boot-warm the upload-pack cache so a freshly-started or S3-recovered node
    // is clone-fast on the FIRST request, not after it. Uses the same sources the
    // serve path does — the `objects_dyn` seam for reads (replicated in cluster
    // mode), the concrete node-local disk store as the SHA-1 provider — so the
    // cached bytes are identical to a cold build. Best-effort; never fatal.
    {
        let warm_objects = objects_dyn.clone();
        let warm_refs = refs_dyn.clone();
        let warm_disk = objects.clone();
        match ledge_git::fetch::warm_all_segments(
            warm_objects,
            warm_refs,
            warm_disk.as_ref(),
            ledge_git::fetch::global_upload_cache(),
        )
        .await
        {
            Ok((segs, objs)) => {
                tracing::info!(segments = segs, objects = objs, "boot upload-pack warm complete")
            }
            Err(e) => tracing::warn!(error = %e, "boot upload-pack warm failed"),
        }
    }

    let app_state = AppState {
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
        cluster_objects,
        // Webhooks: built above ([webhooks].enabled ⇒ Some WAL-backed dispatcher,
        // else None ⇒ no events emitted + /webhooks routes report 503).
        webhooks,
        // Git remote sync engine (built above): Some only when [sync].enabled,
        // else None ⇒ /sync/import reports 503.
        sync,
        shard_map,
        cluster_gc,
        // Auth (Phase 4d-1): the ctx assembled above — a real WAL-backed store
        // (opened once, first-boot-bootstrapped, compaction task spawned) when
        // [auth] enabled, otherwise the infallible disabled (in-memory) context.
        auth: auth_ctx,
        // Quota (Phase 4d-3): the real context built above from `[quotas]`, SHARED
        // (limits + usage `Arc`) with the workspace manager + GC + cluster GC. The
        // GC refreshes the same `usage` map each pass (+ a startup pass), so the
        // commit gate + gauges read fresh measurements. `enabled=false` (default)
        // ⇒ every gate is a no-op (R Q15).
        quota,
    };

    // ── SSH transport (default off). When `[ssh].enabled`, spin up an embedded
    //    SSH server that serves `git-upload-pack` (clone/fetch) over the channel.
    //    Host key persists at `[ssh].host_key_path` (default <data_dir>/ssh_host
    //    _ed25519, generated on first boot). With no `authorized_keys_path`, ANY
    //    public key is accepted (dev) — gate it in prod. ──────────────────────
    if cfg.ssh.enabled {
        let host_key_path = cfg
            .ssh
            .host_key_path
            .clone()
            .map(PathBuf::from)
            .unwrap_or_else(|| data_dir.join("ssh_host_ed25519"));
        match ledge_server::ssh::load_or_create_host_key(&host_key_path) {
            Ok(host_key) => {
                let authorized = match cfg.ssh.authorized_keys_path.as_deref() {
                    Some(p) => match std::fs::read_to_string(p) {
                        Ok(txt) => ledge_server::ssh::parse_authorized_keys(&txt),
                        Err(e) => {
                            tracing::warn!(path = %p, error = %e, "ssh: authorized_keys unreadable; rejecting all keys");
                            // A configured-but-unreadable allowlist must fail closed.
                            vec![ledge_server::ssh::unreachable_key()]
                        }
                    },
                    None => {
                        tracing::warn!("ssh: no authorized_keys_path set — accepting ANY public key (dev only)");
                        Vec::new()
                    }
                };
                let ctx = ledge_server::ssh::SshCtx {
                    state: app_state.clone(),
                    authorized: std::sync::Arc::new(authorized),
                };
                let ssh_addr = cfg.ssh.addr.clone();
                info!(addr = %ssh_addr, "ledge ssh transport listening");
                tokio::spawn(async move {
                    if let Err(e) = ledge_server::ssh::serve(ctx, &ssh_addr, host_key).await {
                        tracing::error!(error = %e, "ssh server exited");
                    }
                });
            }
            Err(e) => tracing::error!(error = %e, "ssh: failed to load/create host key; SSH disabled"),
        }
    }

    let app = build_app(app_state);

    // Dedicated plain-HTTP metrics/health listener on [metrics].addr (default
    // :9090) so Prometheus + health probes have a TLS-agnostic scrape port even
    // when the client listener is TLS. /metrics + /healthz ALSO stay on the client
    // router (back-compat). Bind is awaited (fail-fast on a bad metrics.addr); the
    // serve runs for the process lifetime alongside the client/peer listeners.
    if cfg.metrics.enabled {
        let metrics_addr: SocketAddr = cfg
            .metrics
            .addr
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid metrics.addr {}: {}", cfg.metrics.addr, e))?;
        let metrics_listener = tokio::net::TcpListener::bind(metrics_addr).await?;
        info!(bound_addr = %metrics_listener.local_addr()?, "ledge metrics/health listener");
        tokio::spawn(async move {
            if let Err(e) =
                axum::serve(metrics_listener, ledge_server::build_metrics_app()).await
            {
                tracing::warn!(error = %e, "metrics listener exited");
            }
        });
    }

    let addr: SocketAddr = cfg
        .server
        .addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid addr {}: {}", cfg.server.addr, e))?;

    if !cfg.tls.enabled {
        // Plaintext path — byte-identical to pre-4d-4.
        let listener = tokio::net::TcpListener::bind(addr).await?;
        info!(bound_addr = %listener.local_addr()?, "ledge listening (http)");
        axum::serve(listener, app).await?;
        return Ok(());
    }

    // TLS on: client listener (server-cert only). cert/key presence is guaranteed
    // by config validation (Task 3), so the expects are unreachable on a validated config.
    let cert = cfg.tls.cert_path.as_deref().expect("validated: tls.cert_path");
    let key = cfg.tls.key_path.as_deref().expect("validated: tls.key_path");
    let client_tls = ledge_server::tls::server_config_tls_only(cert, key)?;
    let client_rustls = axum_server::tls_rustls::RustlsConfig::from_config(client_tls);

    if cfg.tls.mtls {
        let peer_addr: SocketAddr = cfg
            .tls
            .peer_addr
            .as_deref()
            .expect("validated: tls.peer_addr")
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid tls.peer_addr: {}", e))?;
        let ca = cfg.tls.ca_path.as_deref().expect("validated: tls.ca_path");
        let peer_tls = ledge_server::tls::server_config_mtls(cert, key, ca)?;
        let peer_rustls = axum_server::tls_rustls::RustlsConfig::from_config(peer_tls);
        let peer_app = app.clone();
        info!(client_addr = %addr, peer_addr = %peer_addr, "ledge listening (https client + mTLS peer listeners)");
        let client_fut = axum_server::bind_rustls(addr, client_rustls).serve(app.into_make_service());
        let peer_fut = axum_server::bind_rustls(peer_addr, peer_rustls).serve(peer_app.into_make_service());
        tokio::try_join!(
            async { client_fut.await.map_err(anyhow::Error::from) },
            async { peer_fut.await.map_err(anyhow::Error::from) },
        )?;
    } else {
        info!(bound_addr = %addr, "ledge listening (https client listener)");
        axum_server::bind_rustls(addr, client_rustls).serve(app.into_make_service()).await?;
    }
    Ok(())
}
