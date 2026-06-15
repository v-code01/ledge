//! End-to-end proof that an incremental `git fetch` over Ledge transfers only the
//! NEW objects, not the whole closure — i.e. `have`-line negotiation works with a
//! real `git` client. A 25-commit repo is pushed and cloned; one commit is added
//! upstream and pushed; the clone then `git fetch`es. We assert (a) the fetch
//! updates `origin/main` to the new tip (the negotiation framing is accepted by
//! real git), and (b) the clone's object count grows by only a handful — the new
//! commit/tree/blob — rather than re-downloading the ~75-object history.

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
    tokio::spawn(async move { axum::serve(listener, app).await.ok(); });
    tokio::task::yield_now().await;
    (format!("http://127.0.0.1:{}", addr.port()), data_dir)
}

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
    .unwrap_or_else(|_| panic!("git {args:?} timed out"))
    .unwrap_or_else(|e| panic!("git {args:?} spawn failed: {e}"))
}

fn ok(o: &std::process::Output, ctx: &str) {
    assert!(
        o.status.success(),
        "{ctx} failed: {}",
        String::from_utf8_lossy(&o.stderr)
    );
}

/// Total object count in a repo (loose `count:` + `in-pack:`), via count-objects -v.
async fn obj_total(repo: &Path) -> u64 {
    let out = git(&["count-objects", "-v"], repo).await;
    let txt = String::from_utf8_lossy(&out.stdout);
    let mut count = 0u64;
    let mut in_pack = 0u64;
    for line in txt.lines() {
        if let Some(v) = line.strip_prefix("count: ") {
            count = v.trim().parse().unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("in-pack: ") {
            in_pack = v.trim().parse().unwrap_or(0);
        }
    }
    count + in_pack
}

#[tokio::test]
async fn partial_clone_blob_none_lazily_fetches() {
    let (base_url, _dd) = start_server().await;
    let remote = format!("{base_url}/partial-repo");

    let src = TempDir::new().unwrap();
    ok(&git(&["init", "--initial-branch=main", "."], src.path()).await, "init");
    ok(&git(&["config", "user.email", "t@l"], src.path()).await, "email");
    ok(&git(&["config", "user.name", "t"], src.path()).await, "name");
    for i in 0..3 {
        std::fs::write(src.path().join(format!("f{i}.txt")), format!("blob body number {i}\n")).unwrap();
        ok(&git(&["add", "."], src.path()).await, "add");
        ok(&git(&["commit", "-m", &format!("c{i}")], src.path()).await, "commit");
    }
    ok(&git(&["push", &remote, "main:refs/heads/main"], src.path()).await, "push");

    // --filter=blob:none: the initial pack omits blobs; the default checkout must
    // lazily fetch each blob by SHA (exercising allow-reachable-sha1-in-want and
    // the explicit-want-bypasses-filter rule).
    let cl = TempDir::new().unwrap();
    let clp = cl.path().join("c");
    let out = git(&["clone", "--filter=blob:none", "--quiet", &remote, clp.to_str().unwrap()], Path::new("/")).await;
    ok(&out, "partial clone");
    assert_eq!(
        String::from_utf8(git(&["config", "remote.origin.promisor"], &clp).await.stdout).unwrap().trim(),
        "true",
        "clone is a promisor (partial) repo"
    );
    // Checkout lazy-fetched the blobs → working tree is correct, nothing missing.
    assert_eq!(std::fs::read_to_string(clp.join("f2.txt")).unwrap(), "blob body number 2\n");
    let missing = String::from_utf8(
        git(&["rev-list", "--objects", "--all", "--missing=print"], &clp).await.stdout,
    ).unwrap().lines().filter(|l| l.starts_with('?')).count();
    assert_eq!(missing, 0, "all blobs lazily fetched after checkout");
}

#[tokio::test]
async fn shallow_clone_bounds_history() {
    let (base_url, _dd) = start_server().await;
    let remote = format!("{base_url}/shallow-repo");

    // 8-commit repo.
    let src = TempDir::new().unwrap();
    ok(&git(&["init", "--initial-branch=main", "."], src.path()).await, "init");
    ok(&git(&["config", "user.email", "t@l"], src.path()).await, "email");
    ok(&git(&["config", "user.name", "t"], src.path()).await, "name");
    for i in 0..8 {
        std::fs::write(src.path().join("f.txt"), format!("v{i}\n")).unwrap();
        ok(&git(&["add", "."], src.path()).await, "add");
        ok(&git(&["commit", "-m", &format!("c{i}")], src.path()).await, "commit");
    }
    ok(&git(&["push", &remote, "main:refs/heads/main"], src.path()).await, "push");
    let tip = String::from_utf8(git(&["rev-parse", "main"], src.path()).await.stdout).unwrap().trim().to_string();

    // --depth 1: exactly one commit, marked shallow, working tree intact.
    let c1 = TempDir::new().unwrap();
    let c1p = c1.path().join("c");
    ok(&git(&["clone", "--depth", "1", "--quiet", &remote, c1p.to_str().unwrap()], Path::new("/")).await, "depth-1 clone");
    let n1 = String::from_utf8(git(&["rev-list", "--count", "HEAD"], &c1p).await.stdout).unwrap().trim().to_string();
    assert_eq!(n1, "1", "depth-1 clone has exactly one commit");
    assert!(c1p.join(".git/shallow").exists(), "clone is marked shallow");
    assert_eq!(
        String::from_utf8(git(&["rev-parse", "HEAD"], &c1p).await.stdout).unwrap().trim(),
        tip,
        "shallow clone HEAD is the tip"
    );
    assert_eq!(std::fs::read_to_string(c1p.join("f.txt")).unwrap(), "v7\n", "working tree is correct");

    // --depth 3: exactly three commits.
    let c3 = TempDir::new().unwrap();
    let c3p = c3.path().join("c");
    ok(&git(&["clone", "--depth", "3", "--quiet", &remote, c3p.to_str().unwrap()], Path::new("/")).await, "depth-3 clone");
    let n3 = String::from_utf8(git(&["rev-list", "--count", "HEAD"], &c3p).await.stdout).unwrap().trim().to_string();
    assert_eq!(n3, "3", "depth-3 clone has exactly three commits");
}

#[tokio::test]
async fn incremental_fetch_transfers_only_new_objects() {
    let (base_url, _dd) = start_server().await;
    let remote = format!("{base_url}/fetch-inc-repo");

    // Source repo: 25 commits, each a new line in f.txt (≈75 objects total).
    let src = TempDir::new().unwrap();
    ok(&git(&["init", "--initial-branch=main", "."], src.path()).await, "init");
    ok(&git(&["config", "user.email", "t@l"], src.path()).await, "email");
    ok(&git(&["config", "user.name", "t"], src.path()).await, "name");
    for i in 0..25 {
        std::fs::write(src.path().join("f.txt"), format!("line {i}\n")).unwrap();
        ok(&git(&["add", "."], src.path()).await, "add");
        ok(&git(&["commit", "-m", &format!("c{i}")], src.path()).await, "commit");
    }
    ok(&git(&["push", &remote, "main:refs/heads/main"], src.path()).await, "push initial");

    // Clone it (full closure).
    let clone = TempDir::new().unwrap();
    let cl = git(&["clone", "--quiet", &remote, clone.path().to_str().unwrap()], Path::new("/")).await;
    ok(&cl, "clone");
    let total_after_clone = obj_total(clone.path()).await;
    assert!(
        total_after_clone >= 60,
        "the cloned repo should hold the full ~75-object history, got {total_after_clone}"
    );

    // Upstream advances by ONE commit.
    std::fs::write(src.path().join("f.txt"), "the new line\n").unwrap();
    ok(&git(&["add", "."], src.path()).await, "add2");
    ok(&git(&["commit", "-m", "c25-new"], src.path()).await, "commit2");
    let new_tip = String::from_utf8(git(&["rev-parse", "main"], src.path()).await.stdout)
        .unwrap().trim().to_string();
    ok(&git(&["push", &remote, "main:refs/heads/main"], src.path()).await, "push update");

    // Incremental fetch from the clone.
    let before = obj_total(clone.path()).await;
    let fetch = git(&["fetch", "origin"], clone.path()).await;
    ok(&fetch, "incremental fetch");
    let after = obj_total(clone.path()).await;

    // (a) the negotiation framing is accepted by real git: origin/main advanced.
    let got = String::from_utf8(git(&["rev-parse", "origin/main"], clone.path()).await.stdout)
        .unwrap().trim().to_string();
    assert_eq!(got, new_tip, "fetch must advance origin/main to the new tip");

    // (b) it was INCREMENTAL: only the new commit+tree+blob (a few objects), not
    // the whole closure. Allow a small upper bound for git bookkeeping objects.
    let transferred = after - before;
    assert!(
        transferred <= 8,
        "incremental fetch transferred {transferred} objects (expected ~3); have-line \
         negotiation is not excluding the shared history"
    );
    assert!(transferred >= 1, "fetch should have transferred the new objects");
}
