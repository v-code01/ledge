//! End-to-end proof that the delta-capable receive-pack ingests a *real*
//! `git push` of a deltified repo. A multi-commit repo with a sizeable file
//! changed across several commits, then `git repack -ad -f` forced, produces a
//! pack whose objects are OFS_DELTA/REF_DELTA encoded — exactly the shape the
//! pre-delta decoder rejected. We push that over HTTP into Ledge, clone it back,
//! and assert the pushed `main` SHA-1 round-trips byte-identically.

use std::{net::SocketAddr, path::Path, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::process::Command as TokioCommand;
use ledge_core::HLC;
use ledge_object_store::DiskObjectStore;
use ledge_ref_store::RefStoreImpl;
use ledge_server::{build_app, AppState};

async fn start_server() -> (String, TempDir) {
    let data_dir = TempDir::new().unwrap();
    let hlc     = Arc::new(HLC::new());
    let objects = Arc::new(DiskObjectStore::new(data_dir.path().to_path_buf()).unwrap());
    let refs    = Arc::new(RefStoreImpl::open(data_dir.path().to_path_buf(), hlc.clone()).unwrap());
    let (workspaces, leases, gc) = ledge_server::build_workspace_stack(
        data_dir.path().to_path_buf(), objects.clone(), refs.clone(), hlc,
        ledge_workspace::QuotaLimits::default(), std::sync::Arc::new(ledge_workspace::UsageMap::default()),
    ).unwrap();
    let app     = build_app(AppState { objects: objects.clone() as Arc<dyn ledge_core::ObjectStore>, objects_disk: objects.clone(), refs: refs.clone() as Arc<dyn ledge_core::RefStore>, workspaces, leases, gc, default_ttl_secs: 3600, data_dir: data_dir.path().to_path_buf(), raft_shards: None, cluster_refs: None, cluster_objects: None, webhooks: None, sync: None, shard_map: None, cluster_gc: None, auth: ledge_server::auth::AuthCtx::disabled(), quota: ledge_server::quota::QuotaCtx::disabled() });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.ok(); });
    tokio::task::yield_now().await;
    (format!("http://127.0.0.1:{}", addr.port()), data_dir)
}

/// Async git wrapper — uses tokio::process::Command so the server task keeps running.
async fn git(args: &[&str], cwd: &Path) -> std::process::Output {
    tokio::time::timeout(
        Duration::from_secs(60),
        TokioCommand::new("git")
            .args(args)
            .current_dir(cwd)
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output(),
    )
    .await
    .unwrap_or_else(|_| panic!("git {args:?} timed out after 60s"))
    .unwrap_or_else(|e| panic!("git {args:?} spawn failed: {e}"))
}

fn assert_git_ok(output: &std::process::Output, ctx: &str) {
    if !output.status.success() {
        panic!("{ctx} failed (exit {:?}):\nstdout: {}\nstderr: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr));
    }
}

/// Push a multi-commit, repacked-with-deltas repo into Ledge over HTTP, clone it
/// back, and assert the `main` SHA-1 round-trips. The pre-delta decoder would
/// have HTTP-500'd on the delta-compressed pack; this asserts the new decoder
/// resolves OFS_DELTA / REF_DELTA (incl. thin-pack store bases) correctly.
#[tokio::test]
async fn git_push_delta_pack_roundtrips() {
    let (base_url, _data_dir) = start_server().await;

    // Source repo: a 500-line file, mutated across six commits, then force-repacked
    // with a deep delta window so the resulting pack carries OFS/REF deltas.
    let src = TempDir::new().unwrap();
    assert_git_ok(&git(&["init", "--initial-branch=main", "."], src.path()).await, "init");
    assert_git_ok(&git(&["config", "user.email", "t@l"], src.path()).await, "email");
    assert_git_ok(&git(&["config", "user.name", "t"], src.path()).await, "name");
    let base: String = (0..500).map(|i| format!("line {i}\n")).collect();
    for v in 0..6 {
        std::fs::write(
            src.path().join("f.txt"),
            base.replace("line 5\n", &format!("CHANGED v{v}\n")),
        ).unwrap();
        assert_git_ok(&git(&["add", "."], src.path()).await, "add");
        assert_git_ok(&git(&["commit", "-m", &format!("c{v}")], src.path()).await, "commit");
    }
    // Force deltas: single pack, recompute deltas, deep window/depth.
    assert_git_ok(&git(&["repack", "-ad", "-f", "--window=50", "--depth=50"], src.path()).await, "repack");
    let want = String::from_utf8(git(&["rev-parse", "main"], src.path()).await.stdout)
        .unwrap().trim().to_string();

    // Sanity: confirm the repacked pack actually contains delta objects, otherwise
    // this test would silently degrade into a non-delta push and prove nothing.
    let pack_dir = String::from_utf8(
        git(&["rev-parse", "--git-path", "objects/pack"], src.path()).await.stdout,
    ).unwrap().trim().to_string();
    let pack_file = std::fs::read_dir(src.path().join(&pack_dir)).unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|pp| pp.extension().map(|x| x == "pack").unwrap_or(false))
        .expect("a .pack must exist after repack");
    // `git verify-pack -v` prints a per-object table then a summary; deltified
    // objects show a non-zero "chain length = N: M objects" line (and each delta
    // row carries a trailing depth + base-SHA). Assert at least one chain exists.
    let verify = git(&["verify-pack", "-v", pack_file.to_str().unwrap()], src.path()).await;
    let vtxt = String::from_utf8_lossy(&verify.stdout);
    let has_delta_chain = vtxt.lines().any(|l| {
        l.trim_start().starts_with("chain length = ")
            && !l.trim_start().starts_with("chain length = 0")
    });
    assert!(
        has_delta_chain,
        "repacked pack must carry deltas (else this test proves nothing):\n{vtxt}"
    );

    // Mirror git_push_and_reclone EXACTLY: durable default-repo URL, no workspace fork.
    let remote_url = format!("{base_url}/delta-push-repo");
    assert_git_ok(&git(&["remote", "add", "ledge", &remote_url], src.path()).await, "remote add");

    let push = git(&["push", &remote_url, "main:refs/heads/main"], src.path()).await;
    assert!(
        push.status.success(),
        "git push (delta pack) must succeed now: {}",
        String::from_utf8_lossy(&push.stderr)
    );

    // Re-clone from Ledge and assert the pushed SHA-1 round-trips.
    let out = TempDir::new().unwrap();
    let cl = git(&["clone", "--quiet", &remote_url, out.path().to_str().unwrap()], Path::new("/")).await;
    assert!(cl.status.success(), "clone back: {}", String::from_utf8_lossy(&cl.stderr));
    let got = String::from_utf8(git(&["rev-parse", "main"], out.path()).await.stdout)
        .unwrap().trim().to_string();
    assert_eq!(got, want, "pushed-then-cloned main SHA-1 matches (delta pack ingested correctly)");
}
