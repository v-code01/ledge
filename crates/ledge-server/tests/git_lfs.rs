//! End-to-end proof of Git LFS: a real `git lfs` client pushes a large file's
//! bytes through Ledge's Batch API + basic transfer, and a fresh clone pulls them
//! back byte-identical. Skips if the `git-lfs` binary isn't installed.

use std::{net::SocketAddr, path::Path, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::process::Command as TokioCommand;

use ledge_core::HLC;
use ledge_object_store::DiskObjectStore;
use ledge_ref_store::RefStoreImpl;
use ledge_server::{build_app, AppState};

async fn start() -> (String, TempDir) {
    let data_dir = TempDir::new().unwrap();
    let hlc = Arc::new(HLC::new());
    let objects = Arc::new(DiskObjectStore::new(data_dir.path().to_path_buf()).unwrap());
    let refs = Arc::new(RefStoreImpl::open(data_dir.path().to_path_buf(), hlc.clone()).unwrap());
    let (workspaces, leases, gc) = ledge_server::build_workspace_stack(
        data_dir.path().to_path_buf(),
        objects.clone(),
        refs.clone(),
        hlc,
        ledge_workspace::QuotaLimits::default(),
        Arc::new(ledge_workspace::UsageMap::default()),
    )
    .unwrap();
    let app = build_app(AppState {
        objects: objects.clone() as Arc<dyn ledge_core::ObjectStore>,
        objects_disk: objects.clone(),
        refs: refs.clone() as Arc<dyn ledge_core::RefStore>,
        workspaces,
        leases,
        gc,
        default_ttl_secs: 3600,
        data_dir: data_dir.path().to_path_buf(),
        raft_shards: None,
        cluster_refs: None,
        cluster_objects: None,
        webhooks: None,
        sync: None,
        shard_map: None,
        cluster_gc: None,
        auth: ledge_server::auth::AuthCtx::disabled(),
        quota: ledge_server::quota::QuotaCtx::disabled(),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    tokio::task::yield_now().await;
    (format!("http://127.0.0.1:{}", addr.port()), data_dir)
}

async fn run(cmd: &str, args: &[&str], cwd: &Path) -> std::process::Output {
    tokio::time::timeout(
        Duration::from_secs(60),
        TokioCommand::new(cmd)
            .args(args)
            .current_dir(cwd)
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output(),
    )
    .await
    .unwrap_or_else(|_| panic!("{cmd} {args:?} timed out"))
    .unwrap_or_else(|e| panic!("{cmd} {args:?} spawn failed: {e}"))
}

fn ok(o: &std::process::Output, ctx: &str) {
    assert!(
        o.status.success(),
        "{ctx} failed: {}",
        String::from_utf8_lossy(&o.stderr)
    );
}

#[tokio::test]
async fn lfs_push_and_clone_roundtrips_large_file() {
    // Skip cleanly if git-lfs isn't available (e.g. a minimal CI image).
    if TokioCommand::new("git")
        .args(["lfs", "version"])
        .output()
        .await
        .map(|o| !o.status.success())
        .unwrap_or(true)
    {
        eprintln!("skipping: git-lfs not installed");
        return;
    }
    let (base_url, _dd) = start().await;
    let remote = format!("{base_url}/lfsrepo");

    // A repo that tracks *.bin via LFS, with a 1 MiB deterministic file.
    let src = TempDir::new().unwrap();
    ok(
        &run("git", &["init", "--initial-branch=main", "."], src.path()).await,
        "init",
    );
    ok(
        &run("git", &["config", "user.email", "t@l"], src.path()).await,
        "email",
    );
    ok(
        &run("git", &["config", "user.name", "t"], src.path()).await,
        "name",
    );
    ok(
        &run("git", &["lfs", "install", "--local"], src.path()).await,
        "lfs install",
    );
    ok(
        &run("git", &["lfs", "track", "*.bin"], src.path()).await,
        "lfs track",
    );
    let big: Vec<u8> = (0..1024u32 * 1024)
        .map(|i| (i.wrapping_mul(2654435761) >> 24) as u8)
        .collect();
    std::fs::write(src.path().join("big.bin"), &big).unwrap();
    ok(
        &run("git", &["add", ".gitattributes", "big.bin"], src.path()).await,
        "add",
    );
    ok(
        &run("git", &["commit", "-m", "add lfs file"], src.path()).await,
        "commit",
    );

    // Push: the pointer commit over git, the bytes over the LFS Batch API.
    ok(
        &run(
            "git",
            &["push", &remote, "main:refs/heads/main"],
            src.path(),
        )
        .await,
        "push",
    );
    // The object is content-addressed on the server.
    let lfs_objs = std::fs::read_dir(_dd.path().join("lfs"))
        .map(|rd| {
            rd.flatten()
                .flat_map(|e| std::fs::read_dir(e.path()).into_iter().flatten().flatten())
                .flat_map(|e| std::fs::read_dir(e.path()).into_iter().flatten().flatten())
                .count()
        })
        .unwrap_or(0);
    assert!(
        lfs_objs >= 1,
        "the LFS object should be stored on the server"
    );

    // Clone: git pulls the pointer, then git-lfs fetches the bytes back.
    let clone = TempDir::new().unwrap();
    let clp = clone.path().join("c");
    ok(
        &run(
            "git",
            &["clone", &remote, clp.to_str().unwrap()],
            Path::new("/"),
        )
        .await,
        "clone",
    );
    let got = std::fs::read(clp.join("big.bin")).unwrap();
    assert_eq!(
        got, big,
        "the LFS file round-trips byte-identical through Ledge"
    );
}
