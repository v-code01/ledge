//! Regression: clone from a PURELY-PACKED store must succeed.
//!
//! Packing introduced a latent bug — `sha1_index` walked only loose dirs, so after
//! `repack` pruned the loose files a clone failed with "remote did not send all
//! necessary objects". This test pushes a repo, repacks it to a single pack (loose
//! count → 0), and clones it back, asserting the `main` SHA-1 round-trips. It fails
//! against the pre-fix (loose-only, uncached) `sha1_index`.
use std::path::Path;
use std::sync::Arc;

use ledge_server::{build_app, AppState};
use tempfile::TempDir;
use tokio::process::Command as TokioCommand;

/// Boot a single-node, auth-disabled server and ALSO hand back the concrete object
/// store so the test can drive `repack` directly.
async fn start_server_with_store() -> (String, Arc<ledge_object_store::DiskObjectStore>, TempDir) {
    let data_dir = TempDir::new().unwrap();
    let hlc = Arc::new(ledge_core::HLC::new());
    let objects = Arc::new(ledge_object_store::DiskObjectStore::new(data_dir.path().to_path_buf()).unwrap());
    let refs = Arc::new(ledge_ref_store::RefStoreImpl::open(data_dir.path().to_path_buf(), hlc.clone()).unwrap());
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
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://127.0.0.1:{}", addr.port()), objects, data_dir)
}

async fn git(args: &[&str], cwd: &Path) -> std::process::Output {
    TokioCommand::new("git")
        .args(args)
        .current_dir(cwd)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .await
        .unwrap()
}

fn ok(out: &std::process::Output, ctx: &str) {
    assert!(out.status.success(), "git {ctx}: {}", String::from_utf8_lossy(&out.stderr));
}

/// Count loose object files under `objects/`, excluding the `pack/` and `tmp/` dirs.
fn count_loose(data_dir: &Path) -> usize {
    let root = data_dir.join("objects");
    let mut n = 0;
    if let Ok(l1) = std::fs::read_dir(&root) {
        for d1 in l1.flatten() {
            let name = d1.file_name();
            if name == std::ffi::OsStr::new("tmp") || name == std::ffi::OsStr::new("pack") {
                continue;
            }
            if !d1.path().is_dir() {
                continue;
            }
            if let Ok(l2) = std::fs::read_dir(d1.path()) {
                for d2 in l2.flatten() {
                    if let Ok(l3) = std::fs::read_dir(d2.path()) {
                        n += l3.flatten().filter(|e| e.path().is_file()).count();
                    }
                }
            }
        }
    }
    n
}

#[tokio::test]
async fn clone_from_purely_packed_store_succeeds() {
    let (base_url, store, data_dir) = start_server_with_store().await;

    // Source repo: a sizeable file across several commits, then forced deltas.
    let src = TempDir::new().unwrap();
    ok(&git(&["init", "--initial-branch=main", "."], src.path()).await, "init");
    ok(&git(&["config", "user.email", "t@l"], src.path()).await, "email");
    ok(&git(&["config", "user.name", "t"], src.path()).await, "name");
    let base: String = (0..500).map(|i| format!("line {i}\n")).collect();
    for v in 0..6 {
        std::fs::write(src.path().join("f.txt"), base.replace("line 5\n", &format!("CHANGED v{v}\n"))).unwrap();
        ok(&git(&["add", "."], src.path()).await, "add");
        ok(&git(&["commit", "-m", &format!("c{v}")], src.path()).await, "commit");
    }
    ok(&git(&["repack", "-ad", "-f", "--window=50", "--depth=50"], src.path()).await, "repack");
    let want = String::from_utf8(git(&["rev-parse", "main"], src.path()).await.stdout).unwrap().trim().to_string();

    // Push into Ledge (durable default-repo).
    let remote_url = format!("{base_url}/pack-clone-repo");
    let push = git(&["push", &remote_url, "main:refs/heads/main"], src.path()).await;
    assert!(push.status.success(), "push: {}", String::from_utf8_lossy(&push.stderr));

    // REPACK the store: objects move loose → pack, loose pruned to 0.
    let loose_before = count_loose(data_dir.path());
    assert!(loose_before > 0, "objects should be loose after push");
    let stats = ledge_object_store::repack::repack(&store).await.unwrap();
    assert!(stats.objects_packed > 0);
    assert_eq!(count_loose(data_dir.path()), 0, "store is now PURELY packed (loose pruned)");

    // Clone from the purely-packed store — this is the bug fix.
    let out = TempDir::new().unwrap();
    let cl = git(&["clone", "--quiet", &remote_url, out.path().to_str().unwrap()], Path::new("/")).await;
    assert!(cl.status.success(), "clone from packed store: {}", String::from_utf8_lossy(&cl.stderr));
    let got = String::from_utf8(git(&["rev-parse", "main"], out.path()).await.stdout).unwrap().trim().to_string();
    assert_eq!(got, want, "cloned-from-packed main SHA-1 matches the pushed sha");
    assert_eq!(std::fs::read_to_string(out.path().join("f.txt")).unwrap(), base.replace("line 5\n", "CHANGED v5\n"));
}
