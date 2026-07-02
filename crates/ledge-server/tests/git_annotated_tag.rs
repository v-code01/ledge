//! Annotated tags are first-class: a commit referenced ONLY by an annotated tag
//! (not on any branch) must survive GC and clone correctly. Before the tag-walk
//! fix, `reachable_from`/`collect_pack_objects` treated tag objects as leaves, so
//! such a commit was neither marked reachable (GC could delete it — data loss) nor
//! packed on clone. This drives the real `git` client end-to-end through GC.

use std::path::Path;
use std::sync::Arc;

use ledge_server::{build_app, AppState};
use tempfile::TempDir;
use tokio::process::Command as TokioCommand;

/// Boot a single-node, auth-disabled server; return (base_url, gc-handle, data_dir).
async fn start_server() -> (String, Arc<ledge_workspace::Gc>, TempDir) {
    let data_dir = TempDir::new().unwrap();
    let hlc = Arc::new(ledge_core::HLC::new());
    let objects =
        Arc::new(ledge_object_store::DiskObjectStore::new(data_dir.path().to_path_buf()).unwrap());
    let refs = Arc::new(
        ledge_ref_store::RefStoreImpl::open(data_dir.path().to_path_buf(), hlc.clone()).unwrap(),
    );
    let (workspaces, leases, gc) = ledge_server::build_workspace_stack(
        data_dir.path().to_path_buf(),
        objects.clone(),
        refs.clone(),
        hlc,
        ledge_workspace::QuotaLimits::default(),
        Arc::new(ledge_workspace::UsageMap::default()),
    )
    .unwrap();
    let gc_handle = gc.clone();
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
    (
        format!("http://127.0.0.1:{}", addr.port()),
        gc_handle,
        data_dir,
    )
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
    assert!(
        out.status.success(),
        "git {ctx}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
async fn rev(args: &[&str], cwd: &Path) -> String {
    String::from_utf8(git(args, cwd).await.stdout)
        .unwrap()
        .trim()
        .to_string()
}

#[tokio::test]
async fn annotated_tag_commit_survives_gc_and_clones() {
    let (base_url, gc, _data_dir) = start_server().await;

    // Source repo: commit A (on main), commit B, tag B annotated, then rewind main
    // to A — so B is held ONLY by the annotated tag v1, not by any branch.
    let src = TempDir::new().unwrap();
    ok(
        &git(&["init", "--initial-branch=main", "."], src.path()).await,
        "init",
    );
    ok(
        &git(&["config", "user.email", "t@l"], src.path()).await,
        "email",
    );
    ok(
        &git(&["config", "user.name", "t"], src.path()).await,
        "name",
    );
    std::fs::write(src.path().join("f.txt"), "A\n").unwrap();
    ok(&git(&["add", "."], src.path()).await, "add A");
    ok(&git(&["commit", "-m", "A"], src.path()).await, "commit A");
    let a_sha = rev(&["rev-parse", "HEAD"], src.path()).await;
    std::fs::write(src.path().join("f.txt"), "B\n").unwrap();
    ok(&git(&["add", "."], src.path()).await, "add B");
    ok(&git(&["commit", "-m", "B"], src.path()).await, "commit B");
    let b_sha = rev(&["rev-parse", "HEAD"], src.path()).await;
    ok(
        &git(&["tag", "-a", "v1", "-m", "release B"], src.path()).await,
        "tag",
    );
    ok(
        &git(&["reset", "--hard", &a_sha], src.path()).await,
        "reset",
    );
    // Sanity: main is A, v1 peels to B, and B differs from A.
    assert_ne!(a_sha, b_sha);
    assert_eq!(rev(&["rev-parse", "v1^{commit}"], src.path()).await, b_sha);

    // Push main (→A) AND the annotated tag (→B) in one shot.
    let remote = format!("{base_url}/tag-repo");
    let push = git(
        &["push", &remote, "main:refs/heads/main", "v1:refs/tags/v1"],
        src.path(),
    )
    .await;
    ok(&push, "push");

    // GC: B is reachable ONLY via the annotated tag. With the tag-walk fix it is
    // marked reachable and kept; without it, GC would delete B (data loss).
    let stats = gc.run().await.unwrap();
    // B (+ its tree/blob) must NOT have been reclaimed.
    let _ = stats;

    // Clone fresh and prove B is present + the tag resolves to it.
    let out = TempDir::new().unwrap();
    let cl = git(
        &["clone", "--quiet", &remote, out.path().to_str().unwrap()],
        Path::new("/"),
    )
    .await;
    ok(&cl, "clone");
    assert_eq!(
        rev(&["rev-parse", "main"], out.path()).await,
        a_sha,
        "main is A"
    );
    assert_eq!(
        rev(&["rev-parse", "v1^{commit}"], out.path()).await,
        b_sha,
        "annotated tag v1 resolves to B in the clone"
    );
    // B's object is actually present (not a dangling ref): cat-file -e succeeds.
    ok(
        &git(&["cat-file", "-e", &b_sha], out.path()).await,
        "tagged commit B present after GC + clone",
    );
    // And B's tree content is intact.
    assert_eq!(
        rev(&["show", &format!("{b_sha}:f.txt")], out.path()).await,
        "B",
        "tagged commit's blob survived",
    );
}
