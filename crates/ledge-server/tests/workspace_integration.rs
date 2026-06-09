//! Workspace integration tests exercising the full agent round-trip over HTTP
//! with the real `git` binary.
//!
//! Each `tests/*.rs` is its own crate, so the harness helpers (`start_server`,
//! `git`, `assert_git_ok`) are duplicated here from `integration.rs` rather than
//! shared via a `mod common` — duplication keeps this file self-contained.
//!
//! Run single-threaded to avoid `git` config contention:
//!   cargo test -p ledge-server --test workspace_integration -- --test-threads=1

use ledge_core::HLC;
use ledge_object_store::DiskObjectStore;
use ledge_ref_store::RefStoreImpl;
use ledge_server::{build_app, AppState};
use std::{net::SocketAddr, path::Path, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::process::Command as TokioCommand;

/// Build a full workspace-capable server, bind to an ephemeral port, and spawn
/// it. Returns the base URL and the data-dir `TempDir` (kept alive by the caller).
async fn start_server() -> (String, TempDir) {
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
        std::sync::Arc::new(ledge_workspace::UsageMap::default()),
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

/// Async git wrapper — uses tokio::process::Command so the server task keeps running.
async fn git(args: &[&str], cwd: &Path) -> std::process::Output {
    tokio::time::timeout(
        Duration::from_secs(30),
        TokioCommand::new("git")
            .args(args)
            .current_dir(cwd)
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output(),
    )
    .await
    .unwrap_or_else(|_| panic!("git {args:?} timed out after 30s"))
    .unwrap_or_else(|e| panic!("git {args:?} spawn failed: {e}"))
}

fn assert_git_ok(output: &std::process::Output, ctx: &str) {
    if !output.status.success() {
        panic!(
            "{ctx} failed (exit {:?}):\nstdout: {}\nstderr: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

/// Headline flow: an agent forks a workspace from durable main, clones/pushes
/// against `/ws/<id>` in isolation, graduates its work to durable via
/// `POST /workspaces/<id>/commit`, and a third party observes the durable result.
#[tokio::test]
async fn fork_commit_clone_roundtrip() {
    let (base, _dd) = start_server().await;
    let client = reqwest::Client::new();

    // 1. Seed durable /myrepo with an initial commit on refs/heads/main.
    let src = TempDir::new().unwrap();
    let sp = src.path();
    assert_git_ok(&git(&["init", "--initial-branch=main", "."], sp).await, "init");
    assert_git_ok(&git(&["config", "user.email", "a@b.c"], sp).await, "email");
    assert_git_ok(&git(&["config", "user.name", "T"], sp).await, "name");
    std::fs::write(sp.join("base.txt"), b"durable base\n").unwrap();
    assert_git_ok(&git(&["add", "."], sp).await, "add");
    assert_git_ok(&git(&["commit", "-m", "base"], sp).await, "commit");
    let durable = format!("{base}/myrepo");
    assert_git_ok(&git(&["remote", "add", "ledge", &durable], sp).await, "remote");
    assert_git_ok(
        &git(&["push", "ledge", "main:refs/heads/main"], sp).await,
        "push durable",
    );

    // 2. Fork a workspace from refs/heads/main.
    let resp = client
        .post(format!("{base}/workspaces"))
        .json(&serde_json::json!({"source": "refs/heads/main", "ttl_seconds": 3600}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let id = body["id"].as_str().unwrap().to_string();

    // 3. Clone the workspace; it must see the durable main.
    let wsroot = TempDir::new().unwrap();
    let ws_url = format!("{base}/ws/{id}");
    assert_git_ok(
        &git(&["clone", &ws_url, "wsclone"], wsroot.path()).await,
        "clone ws",
    );
    let wsdir = wsroot.path().join("wsclone");
    assert!(
        wsdir.join("base.txt").exists(),
        "ws clone must contain durable base.txt"
    );

    // 4. Make a new commit in the workspace clone and push it back to /ws/<id>.
    assert_git_ok(
        &git(&["config", "user.email", "a@b.c"], &wsdir).await,
        "ws email",
    );
    assert_git_ok(&git(&["config", "user.name", "T"], &wsdir).await, "ws name");
    std::fs::write(wsdir.join("work.txt"), b"agent work\n").unwrap();
    assert_git_ok(&git(&["add", "."], &wsdir).await, "ws add");
    assert_git_ok(&git(&["commit", "-m", "agent work"], &wsdir).await, "ws commit");
    assert_git_ok(
        &git(&["push", "origin", "main:refs/heads/main"], &wsdir).await,
        "push ws",
    );

    // 5. Commit (graduate) the workspace main → durable main.
    let ws_ref = format!("refs/workspaces/{id}/heads/main");
    let resp = client
        .post(format!("{base}/workspaces/{id}/commit"))
        .json(&serde_json::json!({"mappings": { ws_ref: "refs/heads/main" }}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let outcomes: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        outcomes[0]["status"], "ok",
        "commit must graduate cleanly: {outcomes}"
    );

    // 6. Fresh durable clone must now contain the agent's work.
    let fresh = TempDir::new().unwrap();
    assert_git_ok(
        &git(&["clone", &durable, "freshclone"], fresh.path()).await,
        "fresh durable clone",
    );
    let fc = fresh.path().join("freshclone");
    assert!(
        fc.join("work.txt").exists(),
        "durable clone must contain graduated work.txt"
    );
    assert!(
        fc.join("base.txt").exists(),
        "durable clone must still contain base.txt"
    );
}

/// Release a workspace then GC: the workspace's never-graduated unique objects
/// are reclaimed, while durable objects survive a fresh clone.
#[tokio::test]
async fn release_then_gc_reclaims_discarded() {
    let (base, _dd) = start_server().await;
    let client = reqwest::Client::new();

    // Seed durable /myrepo.
    let src = TempDir::new().unwrap();
    let sp = src.path();
    assert_git_ok(&git(&["init", "--initial-branch=main", "."], sp).await, "init");
    assert_git_ok(&git(&["config", "user.email", "a@b.c"], sp).await, "email");
    assert_git_ok(&git(&["config", "user.name", "T"], sp).await, "name");
    std::fs::write(sp.join("keep.txt"), b"durable keep\n").unwrap();
    assert_git_ok(&git(&["add", "."], sp).await, "add");
    assert_git_ok(&git(&["commit", "-m", "keep"], sp).await, "commit");
    let durable = format!("{base}/myrepo");
    assert_git_ok(&git(&["remote", "add", "ledge", &durable], sp).await, "remote");
    assert_git_ok(
        &git(&["push", "ledge", "main:refs/heads/main"], sp).await,
        "push",
    );

    // Fork + push a UNIQUE blob into the workspace only (never graduated).
    let resp = client
        .post(format!("{base}/workspaces"))
        .json(&serde_json::json!({"source":"refs/heads/main","ttl_seconds":3600}))
        .send()
        .await
        .unwrap();
    let id = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let wsroot = TempDir::new().unwrap();
    assert_git_ok(
        &git(&["clone", &format!("{base}/ws/{id}"), "w"], wsroot.path()).await,
        "clone ws",
    );
    let wd = wsroot.path().join("w");
    assert_git_ok(&git(&["config", "user.email", "a@b.c"], &wd).await, "email");
    assert_git_ok(&git(&["config", "user.name", "T"], &wd).await, "name");
    std::fs::write(
        wd.join("discard.txt"),
        b"unique discarded payload 0xdeadbeef\n",
    )
    .unwrap();
    assert_git_ok(&git(&["add", "."], &wd).await, "add");
    assert_git_ok(&git(&["commit", "-m", "discard"], &wd).await, "commit");
    assert_git_ok(
        &git(&["push", "origin", "main:refs/heads/main"], &wd).await,
        "push ws",
    );

    // Release the workspace, then GC.
    let del = client
        .delete(format!("{base}/workspaces/{id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status().as_u16(), 200);
    let gc = client
        .post(format!("{base}/admin/gc"))
        .send()
        .await
        .unwrap();
    assert_eq!(gc.status().as_u16(), 200);
    let stats: serde_json::Value = gc.json().await.unwrap();
    assert!(
        stats["reclaimed"].as_u64().unwrap() > 0,
        "discarded objects must be reclaimed: {stats}"
    );

    // Durable repo still clones and contains keep.txt (committed objects survived).
    let fresh = TempDir::new().unwrap();
    assert_git_ok(
        &git(&["clone", &durable, "fc"], fresh.path()).await,
        "durable clone survives gc",
    );
    assert!(
        fresh.path().join("fc").join("keep.txt").exists(),
        "durable keep.txt must survive GC"
    );
}

/// Explicit release tombstones the workspace: a live GET is 200, but after
/// release GET is 404/410 and a second DELETE is still idempotently 200.
#[tokio::test]
async fn released_workspace_get_is_gone_or_not_found() {
    let (base, _dd) = start_server().await;
    let client = reqwest::Client::new();

    // Seed durable + fork.
    let src = TempDir::new().unwrap();
    let sp = src.path();
    assert_git_ok(&git(&["init", "--initial-branch=main", "."], sp).await, "init");
    assert_git_ok(&git(&["config", "user.email", "a@b.c"], sp).await, "email");
    assert_git_ok(&git(&["config", "user.name", "T"], sp).await, "name");
    std::fs::write(sp.join("f.txt"), b"x\n").unwrap();
    assert_git_ok(&git(&["add", "."], sp).await, "add");
    assert_git_ok(&git(&["commit", "-m", "c"], sp).await, "commit");
    assert_git_ok(
        &git(&["remote", "add", "ledge", &format!("{base}/myrepo")], sp).await,
        "remote",
    );
    assert_git_ok(
        &git(&["push", "ledge", "main:refs/heads/main"], sp).await,
        "push",
    );

    let resp = client
        .post(format!("{base}/workspaces"))
        .json(&serde_json::json!({"source":"refs/heads/main","ttl_seconds":3600}))
        .send()
        .await
        .unwrap();
    let id = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Live workspace GET → 200.
    let live = client
        .get(format!("{base}/workspaces/{id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(live.status().as_u16(), 200);

    // Release (this is exactly what the sweeper calls per expired lease).
    let del = client
        .delete(format!("{base}/workspaces/{id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status().as_u16(), 200);

    // GET after release → Gone (410) or Not Found (404), never 200.
    let after = client
        .get(format!("{base}/workspaces/{id}"))
        .send()
        .await
        .unwrap();
    assert!(
        matches!(after.status().as_u16(), 404 | 410),
        "released workspace must be 404/410, got {}",
        after.status()
    );

    // Idempotent release: a second DELETE is still 200.
    let del2 = client
        .delete(format!("{base}/workspaces/{id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        del2.status().as_u16(),
        200,
        "release must be idempotent"
    );
}
