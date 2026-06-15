//! End-to-end proof of the native SSH transport: a real `git` client clones,
//! fetches, AND pushes over `ssh://` against Ledge's embedded SSH server (russh),
//! with a real client key and the system `ssh` binary. We seed over HTTP, then
//! drive clone + incremental fetch + push entirely over SSH (the push is verified
//! durable by re-cloning over HTTP).

use std::{net::SocketAddr, path::Path, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::process::Command as TokioCommand;

use ledge_core::HLC;
use ledge_object_store::DiskObjectStore;
use ledge_ref_store::RefStoreImpl;
use ledge_server::ssh::{load_or_create_host_key, serve_on_socket, SshCtx};
use ledge_server::{build_app, AppState};

/// Bring up HTTP (to seed) + SSH (under test); return (http_url, ssh_port, keydir).
async fn start() -> (String, u16, TempDir) {
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
    let state = AppState {
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
    };

    // HTTP listener (seed the repo over it).
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr: SocketAddr = http.local_addr().unwrap();
    let app = build_app(state.clone());
    tokio::spawn(async move {
        axum::serve(http, app).await.ok();
    });

    // SSH listener (under test) on an ephemeral port.
    let ssh = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ssh_port = ssh.local_addr().unwrap().port();
    let host_key = load_or_create_host_key(&data_dir.path().join("hostkey")).unwrap();
    let ctx = SshCtx {
        state,
        authorized: Arc::new(Vec::new()), // accept any key (test)
    };
    // keep data_dir alive for the whole test by leaking it into the returned dir
    tokio::spawn(async move {
        serve_on_socket(ctx, ssh, host_key).await.ok();
    });
    tokio::task::yield_now().await;

    // We must keep data_dir alive; return it.
    (
        format!("http://127.0.0.1:{}", http_addr.port()),
        ssh_port,
        data_dir,
    )
}

async fn git(args: &[&str], cwd: &Path, ssh_cmd: Option<&str>) -> std::process::Output {
    let mut c = TokioCommand::new("git");
    c.args(args)
        .current_dir(cwd)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1");
    if let Some(s) = ssh_cmd {
        c.env("GIT_SSH_COMMAND", s);
    }
    tokio::time::timeout(Duration::from_secs(60), c.output())
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

#[tokio::test]
async fn clone_fetch_push_over_ssh() {
    let (http, ssh_port, _dd) = start().await;

    // Client SSH key.
    let keydir = TempDir::new().unwrap();
    let key = keydir.path().join("id_ed25519");
    let kg = TokioCommand::new("ssh-keygen")
        .args(["-q", "-t", "ed25519", "-N", "", "-f", key.to_str().unwrap()])
        .output()
        .await
        .expect("ssh-keygen");
    assert!(
        kg.status.success(),
        "ssh-keygen: {}",
        String::from_utf8_lossy(&kg.stderr)
    );
    let ssh_cmd = format!(
        "ssh -i {} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes",
        key.display()
    );

    // Seed a 3-commit repo over HTTP.
    let src = TempDir::new().unwrap();
    ok(
        &git(&["init", "--initial-branch=main", "."], src.path(), None).await,
        "init",
    );
    ok(
        &git(&["config", "user.email", "t@l"], src.path(), None).await,
        "email",
    );
    ok(
        &git(&["config", "user.name", "t"], src.path(), None).await,
        "name",
    );
    for i in 0..3 {
        std::fs::write(src.path().join("f.txt"), format!("line {i}\n")).unwrap();
        ok(&git(&["add", "."], src.path(), None).await, "add");
        ok(
            &git(&["commit", "-m", &format!("c{i}")], src.path(), None).await,
            "commit",
        );
    }
    let remote_http = format!("{http}/sshrepo");
    ok(
        &git(
            &["push", &remote_http, "main:refs/heads/main"],
            src.path(),
            None,
        )
        .await,
        "seed push",
    );
    let tip1 = String::from_utf8(git(&["rev-parse", "main"], src.path(), None).await.stdout)
        .unwrap()
        .trim()
        .to_string();

    // CLONE over SSH.
    let ssh_url = format!("ssh://git@127.0.0.1:{ssh_port}/sshrepo");
    let clone = TempDir::new().unwrap();
    let clone_path = clone.path().join("c");
    let cl = git(
        &["clone", "--quiet", &ssh_url, clone_path.to_str().unwrap()],
        Path::new("/"),
        Some(&ssh_cmd),
    )
    .await;
    ok(&cl, "ssh clone");
    let got = String::from_utf8(git(&["rev-parse", "main"], &clone_path, None).await.stdout)
        .unwrap()
        .trim()
        .to_string();
    assert_eq!(got, tip1, "ssh-cloned HEAD matches the source");

    // Advance upstream (HTTP push), then FETCH over SSH.
    std::fs::write(src.path().join("f.txt"), "newline\n").unwrap();
    ok(&git(&["add", "."], src.path(), None).await, "add2");
    ok(
        &git(&["commit", "-m", "c-new"], src.path(), None).await,
        "commit2",
    );
    let tip2 = String::from_utf8(git(&["rev-parse", "main"], src.path(), None).await.stdout)
        .unwrap()
        .trim()
        .to_string();
    ok(
        &git(
            &["push", &remote_http, "main:refs/heads/main"],
            src.path(),
            None,
        )
        .await,
        "update push",
    );

    let fe = git(&["fetch", "origin"], &clone_path, Some(&ssh_cmd)).await;
    ok(&fe, "ssh fetch");
    let after = String::from_utf8(
        git(&["rev-parse", "origin/main"], &clone_path, None)
            .await
            .stdout,
    )
    .unwrap()
    .trim()
    .to_string();
    assert_eq!(after, tip2, "ssh fetch advanced origin/main to the new tip");

    // PUSH over SSH: commit in the clone, push via ssh://, verify it landed by
    // cloning the repo back over HTTP and checking the tip.
    ok(
        &git(&["merge", "--ff-only", "origin/main"], &clone_path, None).await,
        "ff to origin",
    );
    ok(
        &git(&["config", "user.email", "c@l"], &clone_path, None).await,
        "clone email",
    );
    ok(
        &git(&["config", "user.name", "c"], &clone_path, None).await,
        "clone name",
    );
    std::fs::write(clone_path.join("pushed.txt"), "via ssh\n").unwrap();
    ok(&git(&["add", "."], &clone_path, None).await, "add push");
    ok(
        &git(&["commit", "-m", "pushed-over-ssh"], &clone_path, None).await,
        "commit push",
    );
    let pushed_tip = String::from_utf8(git(&["rev-parse", "HEAD"], &clone_path, None).await.stdout)
        .unwrap()
        .trim()
        .to_string();
    let pr = git(
        &["push", "origin", "HEAD:refs/heads/main"],
        &clone_path,
        Some(&ssh_cmd),
    )
    .await;
    ok(&pr, "ssh push");

    // Verify over HTTP that the server now has the SSH-pushed commit.
    let verify = TempDir::new().unwrap();
    let vpath = verify.path().join("v");
    ok(
        &git(
            &["clone", "--quiet", &remote_http, vpath.to_str().unwrap()],
            Path::new("/"),
            None,
        )
        .await,
        "verify clone",
    );
    let landed = String::from_utf8(git(&["rev-parse", "main"], &vpath, None).await.stdout)
        .unwrap()
        .trim()
        .to_string();
    assert_eq!(
        landed, pushed_tip,
        "the SSH-pushed commit is durable on the server"
    );
}
