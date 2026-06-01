use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use clap::Parser;
use ledge_core::HLC;
use ledge_object_store::DiskObjectStore;
use ledge_ref_store::RefStoreImpl;
use ledge_server::{build_app, config::LedgeConfig, AppState};
use tracing::info;

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
    let refs = Arc::new(RefStoreImpl::open(data_dir.clone(), hlc.clone())?);
    ledge_server::metrics::install_recorder()?;

    let (workspaces, leases, gc) = ledge_server::build_workspace_stack(
        data_dir.clone(),
        objects.clone(),
        refs.clone(),
        hlc.clone(),
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
        objects,
        refs,
        workspaces,
        leases,
        gc,
        default_ttl_secs: cfg.workspace.default_ttl_secs,
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
