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
    ).unwrap();
    let app     = build_app(AppState { objects, refs, workspaces, leases, gc });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.ok(); });
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
        panic!("{ctx} failed (exit {:?}):\nstdout: {}\nstderr: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr));
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP-level tests (no git binary)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn healthz_endpoint_returns_200() {
    let (base_url, _data_dir) = start_server().await;
    let resp = reqwest::Client::new()
        .get(format!("{base_url}/healthz"))
        .timeout(Duration::from_secs(5))
        .send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn metrics_endpoint_returns_200() {
    let (base_url, _data_dir) = start_server().await;
    let resp = reqwest::Client::new()
        .get(format!("{base_url}/metrics"))
        .timeout(Duration::from_secs(5))
        .send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let ct = resp.headers().get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok()).unwrap_or("");
    assert!(ct.starts_with("text/plain"), "content-type: {ct}");
}

/// Verify the upload-pack discovery response via HTTP.
#[tokio::test]
async fn upload_pack_discovery_response_format() {
    let (base_url, _data_dir) = start_server().await;
    let resp = reqwest::Client::new()
        .get(format!("{base_url}/myrepo/info/refs?service=git-upload-pack"))
        .timeout(Duration::from_secs(5))
        .send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let ct = resp.headers().get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok()).unwrap_or("");
    assert!(ct.contains("git-upload-pack-advertisement"), "wrong content-type: {ct}");
    let body = resp.bytes().await.unwrap();
    // Service banner pkt-line: "# service=git-upload-pack\n" = 26 bytes + 4 prefix = 30 = 0x1e
    assert!(body.starts_with(b"001e# service=git-upload-pack\n"),
        "bad service banner: {:?}", &body[..body.len().min(40)]);
    // Followed by flush "0000"
    assert_eq!(&body[30..34], b"0000", "expected flush after service banner");
}

/// Verify the receive-pack discovery response via HTTP.
#[tokio::test]
async fn receive_pack_discovery_response_format() {
    let (base_url, _data_dir) = start_server().await;
    let resp = reqwest::Client::new()
        .get(format!("{base_url}/myrepo/info/refs?service=git-receive-pack"))
        .timeout(Duration::from_secs(5))
        .send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let body = resp.bytes().await.unwrap();
    // "# service=git-receive-pack\n" = 27 bytes + 4 prefix = 31 = 0x1f
    assert!(body.starts_with(b"001f# service=git-receive-pack\n"),
        "bad service banner: {:?}", &body[..body.len().min(40)]);
    assert_eq!(&body[31..35], b"0000", "expected flush after service banner");
}

// ─────────────────────────────────────────────────────────────────────────────
// git binary integration tests
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn git_clone_empty_repo() {
    let (base_url, _data_dir) = start_server().await;
    let clone_root = TempDir::new().unwrap();
    let output = git(&["clone", &format!("{base_url}/myrepo"), "cloned"], clone_root.path()).await;
    assert_git_ok(&output, "git clone empty repo");
    assert!(clone_root.path().join("cloned").is_dir());
}

#[tokio::test]
async fn git_push_and_reclone() {
    let (base_url, _data_dir) = start_server().await;
    let source = TempDir::new().unwrap();
    let sp = source.path();
    assert_git_ok(&git(&["init", "--initial-branch=main", "."], sp).await, "git init");
    assert_git_ok(&git(&["config", "user.email", "test@ledge.local"], sp).await, "config email");
    assert_git_ok(&git(&["config", "user.name", "Ledge Test"], sp).await, "config name");
    std::fs::write(sp.join("hello.txt"), b"ledge integration test payload\n").unwrap();
    assert_git_ok(&git(&["add", "hello.txt"], sp).await, "git add");
    assert_git_ok(&git(&["commit", "-m", "feat: initial commit"], sp).await, "git commit");
    let remote_url = format!("{base_url}/integration-test-repo");
    assert_git_ok(&git(&["remote", "add", "ledge", &remote_url], sp).await, "git remote add");
    assert_git_ok(&git(&["push", "ledge", "main:refs/heads/main"], sp).await, "git push");
    let clone_root = TempDir::new().unwrap();
    assert_git_ok(&git(&["clone", &remote_url, "fresh-clone"], clone_root.path()).await, "git clone after push");
    let cloned_file = clone_root.path().join("fresh-clone").join("hello.txt");
    assert!(cloned_file.exists(), "hello.txt must exist in fresh clone");
    assert_eq!(std::fs::read(&cloned_file).unwrap(), b"ledge integration test payload\n");
}
