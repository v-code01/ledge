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
    let refs = Arc::new(RefStoreImpl::open(data_dir, hlc)?);
    ledge_server::metrics::install_recorder()?;
    let app = build_app(AppState { objects, refs });
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
