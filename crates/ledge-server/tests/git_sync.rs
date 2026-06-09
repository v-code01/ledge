//! End-to-end proof of the git-sync IMPORT loop over real HTTP with the real
//! `git` binary:
//!
//!   build a bare upstream → `POST /sync/import` → `git clone {base}/ws/{id}`
//!   → the cloned-from-Ledge commit SHA-1 byte-for-byte equals the upstream's.
//!
//! This exercises the FULL stack: SyncEngine clones+ingests the upstream with
//! canonical git SHA-1 fidelity, mirrors its refs into the workspace namespace,
//! and the workspace git smart-HTTP surface serves them back to a fresh clone.
//!
//! The disabled-503 case is covered by `tests/sync_disabled.rs`; this file owns
//! the round-trip-identity and upstream-failure-is-clean cases.
use std::{net::SocketAddr, path::Path, sync::Arc, time::Duration};

use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::process::Command as TokioCommand;

use ledge_core::HLC;
use ledge_object_store::DiskObjectStore;
use ledge_ref_store::RefStoreImpl;
use ledge_server::sync::SyncEngine;
use ledge_server::{build_app, AppState};

/// Boot a REAL `axum::serve` on `127.0.0.1:0` over a single-node, auth-disabled
/// `AppState` whose `sync` is `Some(SyncEngine)` — the only delta from
/// `integration.rs::start_server`, which sets `sync: None`. Returns the live
/// base URL plus the data dir (kept alive for the test's lifetime).
async fn start_server_with_sync() -> (String, TempDir) {
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

    // The engine takes the concrete object store + the dyn ref store, exactly
    // as wired in `main.rs` and the engine's own unit test. Empty allow-list ⇒
    // any upstream host is permitted (file:// upstreams have no host anyway).
    let refs_dyn: Arc<dyn ledge_core::RefStore> = refs.clone();
    let engine = Arc::new(SyncEngine::new(
        objects.clone(),
        refs_dyn.clone(),
        workspaces.clone(),
        Vec::new(),
    ));

    let app = build_app(AppState {
        objects: objects.clone() as Arc<dyn ledge_core::ObjectStore>,
        objects_disk: objects.clone(),
        refs: refs_dyn,
        workspaces,
        leases,
        gc,
        default_ttl_secs: 3600,
        data_dir: data_dir.path().to_path_buf(),
        raft_shards: None,
        cluster_refs: None,
        cluster_objects: None,
        webhooks: None,
        sync: Some(engine),
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

/// Async git wrapper — copied from `tests/integration.rs`. Uses
/// `tokio::process::Command` so the server task keeps running while git blocks,
/// and disables terminal prompts / system config for hermeticity.
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

/// The headline round-trip-identity proof: import an upstream over HTTP, then
/// `git clone` the resulting workspace from Ledge and assert the cloned commit
/// SHA-1 (and tag) are byte-identical to the upstream's. If this fails, the
/// import did NOT preserve canonical git SHA-1 — a real fidelity bug.
#[tokio::test]
async fn import_roundtrips_sha1_over_http() {
    let (base, _dir) = start_server_with_sync().await;

    // Build a local working repo: one commit on `main` + an annotated tag `v1`.
    let work = TempDir::new().unwrap();
    assert_git_ok(
        &git(&["init", "--initial-branch=main", "."], work.path()).await,
        "git init upstream",
    );
    assert_git_ok(
        &git(&["config", "user.email", "t@l"], work.path()).await,
        "config email",
    );
    assert_git_ok(
        &git(&["config", "user.name", "t"], work.path()).await,
        "config name",
    );
    std::fs::write(work.path().join("a.txt"), b"payload\n").unwrap();
    assert_git_ok(&git(&["add", "a.txt"], work.path()).await, "git add");
    assert_git_ok(&git(&["commit", "-m", "c1"], work.path()).await, "git commit");
    assert_git_ok(
        &git(&["tag", "-a", "v1", "-m", "v1"], work.path()).await,
        "git tag",
    );
    let up_sha = String::from_utf8(git(&["rev-parse", "main"], work.path()).await.stdout)
        .unwrap()
        .trim()
        .to_string();

    // Bare-mirror it so the upstream URL is a clean bare repo.
    let bare = TempDir::new().unwrap();
    assert_git_ok(
        &git(
            &[
                "clone",
                "--bare",
                work.path().to_str().unwrap(),
                bare.path().to_str().unwrap(),
            ],
            Path::new("/"),
        )
        .await,
        "git clone --bare upstream",
    );
    let upstream = format!("file://{}", bare.path().display());

    // Import via HTTP — a single POST. Capture its JSON to get the workspace id.
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/sync/import"))
        .json(&serde_json::json!({ "upstream_url": upstream }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status, 201, "import status; body={body:?}");
    let ws = body["workspace_id"]
        .as_str()
        .expect("workspace_id in import response")
        .to_string();
    assert_eq!(
        body["default_branch"].as_str(),
        Some("main"),
        "default_branch reported"
    );

    // Clone the workspace back out of Ledge with the real git binary.
    let out = TempDir::new().unwrap();
    let g = git(
        &[
            "clone",
            "--quiet",
            &format!("{base}/ws/{ws}"),
            out.path().to_str().unwrap(),
        ],
        Path::new("/"),
    )
    .await;
    assert!(
        g.status.success(),
        "git clone ws: {}",
        String::from_utf8_lossy(&g.stderr)
    );

    // File content survives the round trip byte-for-byte.
    assert_eq!(
        std::fs::read_to_string(out.path().join("a.txt")).unwrap(),
        "payload\n"
    );

    // THE proof: the commit SHA-1 of the clone-from-Ledge equals the upstream's.
    let got = String::from_utf8(git(&["rev-parse", "main"], out.path()).await.stdout)
        .unwrap()
        .trim()
        .to_string();
    assert_eq!(
        got, up_sha,
        "cloned-from-ledge main SHA-1 == upstream (round-trip identity)"
    );

    // The annotated tag came across too.
    let tags = String::from_utf8(git(&["tag"], out.path()).await.stdout).unwrap();
    assert!(tags.contains("v1"), "tag v1 present after clone: {tags:?}");
}

/// A bad upstream URL must fail closed with 502 and leave the server healthy —
/// the import error path must not poison the process or the workspace stack.
#[tokio::test]
async fn bad_upstream_is_clean_error() {
    let (base, _dir) = start_server_with_sync().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/sync/import"))
        .json(&serde_json::json!({ "upstream_url": "file:///no/such/repo.git" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 502, "bad upstream ⇒ 502");

    // Server is still serving after the failed import.
    let h = client
        .get(format!("{base}/healthz"))
        .send()
        .await
        .unwrap();
    assert!(h.status().is_success(), "server healthy after bad import");
}
