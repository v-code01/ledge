//! End-to-end proof, over real HTTP against the real stores, that receive-pack
//! honors the client's `old-sha1` — git's core concurrency contract.
//!
//! `old new ref` means "apply this ONLY if the ref is still at `old`". It is the
//! only thing standing between two concurrent pushers and a lost update: whoever
//! lands second must be told to fetch first.
//!
//! A real `git` client never *sends* a stale `old-sha1` on its own — it always
//! uses the value the server just advertised, so its own client-side check fires
//! first and the server code never runs. The stale case arises exactly when two
//! pushers race: both read the same advertisement, both send `c1 -> cX`, and the
//! second one's `old` is stale by the time the server sees it. That race is what
//! this test drives deterministically, by issuing the losing request directly
//! rather than trying to win a timing window with two `git` processes.
//!
//! Unlike the handler-level unit tests in `ledge-git`, this runs the request
//! through the live Axum stack and the REAL `RefStoreImpl` (ART + WAL), so it
//! also proves the ref store's compare-and-swap actually rejects a mismatched
//! expectation rather than a mock standing in for it.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use ledge_core::HLC;
use ledge_git::fetch::encode_pack;
use ledge_git::pkt_line::{encode, encode_flush};
use ledge_object_store::DiskObjectStore;
use ledge_ref_store::RefStoreImpl;
use ledge_server::{build_app, AppState};
use tempfile::TempDir;
use tokio::net::TcpListener;

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

/// The canonical git SHA-1 of a blob, and a one-blob pack carrying it.
fn blob(content: &'static [u8]) -> ([u8; 20], Vec<u8>) {
    use sha1::{Digest, Sha1};
    let mut h = Sha1::new();
    h.update(format!("blob {}\0", content.len()).as_bytes());
    h.update(content);
    let sha: [u8; 20] = h.finalize().into();
    (sha, encode_pack(&[(3u8, Bytes::from_static(content))]))
}

/// One receive-pack request: `old -> new` on refs/heads/main, plus `pack`.
async fn push(base: &str, old: &[u8; 20], new: &[u8; 20], pack: &[u8]) -> String {
    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(&encode(
        format!(
            "{} {} refs/heads/main\0report-status\n",
            hex::encode(old),
            hex::encode(new)
        )
        .as_bytes(),
    ));
    body.extend_from_slice(&encode_flush());
    body.extend_from_slice(pack);

    let resp = reqwest::Client::new()
        .post(format!("{base}/repo/git-receive-pack"))
        .header("content-type", "application/x-git-receive-pack-request")
        .body(body)
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .expect("receive-pack request");
    assert!(
        resp.status().is_success(),
        "receive-pack returned {}",
        resp.status()
    );
    String::from_utf8_lossy(&resp.bytes().await.unwrap()).into_owned()
}

/// The SHA-1 that `refs/heads/main` currently advertises, or None if absent.
///
/// A receive-pack advertisement frames each ref as `<40-hex-sha1> <name>`, so the
/// 40 characters immediately before " refs/heads/main" are its current target.
async fn advertised_main(base: &str) -> Option<String> {
    let body = reqwest::get(format!("{base}/repo/info/refs?service=git-receive-pack"))
        .await
        .expect("info/refs")
        .text()
        .await
        .unwrap();
    let at = body.find(" refs/heads/main")?;
    Some(body[at - 40..at].to_string())
}

/// Two pushers race from the same advertisement. The first lands; the second,
/// whose `old-sha1` is now stale, MUST be rejected — and must not clobber the
/// commit that beat it.
#[tokio::test]
async fn a_stale_concurrent_push_is_rejected_over_http() {
    let (base, _data_dir) = start_server().await;
    let null = [0u8; 20];
    let (sha_a, pack_a) = blob(b"commit A: the shared starting point");
    let (sha_b, pack_b) = blob(b"commit B: pusher one wins the race");
    let (sha_c, pack_c) = blob(b"commit C: pusher two is stale");

    // Both pushers clone/fetch and see main = A.
    let r = push(&base, &null, &sha_a, &pack_a).await;
    assert!(r.contains("ok refs/heads/main"), "create main = A: {r}");
    assert_eq!(
        advertised_main(&base).await.as_deref(),
        Some(hex::encode(sha_a).as_str())
    );

    // Pusher one lands A -> B.
    let r = push(&base, &sha_a, &sha_b, &pack_b).await;
    assert!(r.contains("ok refs/heads/main"), "pusher one A -> B: {r}");

    // Pusher two still believes main is at A (it read the same advertisement) and
    // sends A -> C. The server must refuse: its view is stale.
    let r = push(&base, &sha_a, &sha_c, &pack_c).await;
    assert!(
        r.contains("ng refs/heads/main"),
        "a stale concurrent push must be REJECTED, got: {r}"
    );

    // And B must still be there. This is the assertion that matters: a silent
    // clobber here means a pushed commit was acknowledged and then lost.
    assert_eq!(
        advertised_main(&base).await.as_deref(),
        Some(hex::encode(sha_b).as_str()),
        "main must still point at B: C must not have clobbered the commit that won"
    );
}

/// A push whose `old-sha1` names an object the server has never seen is stale
/// info, not a licence to overwrite: it must be refused, never silently applied
/// against whatever the ref currently holds.
#[tokio::test]
async fn a_push_from_an_unknown_old_sha1_is_rejected_over_http() {
    let (base, _data_dir) = start_server().await;
    let null = [0u8; 20];
    let (sha_a, pack_a) = blob(b"the only object this server knows");
    let (sha_c, pack_c) = blob(b"the object the stale pusher wants to set");
    let unknown = [0x5au8; 20]; // never pushed, never stored

    let r = push(&base, &null, &sha_a, &pack_a).await;
    assert!(r.contains("ok refs/heads/main"), "create main = A: {r}");

    let r = push(&base, &unknown, &sha_c, &pack_c).await;
    assert!(
        r.contains("ng refs/heads/main"),
        "an unknown old-sha1 must be rejected as stale info, got: {r}"
    );
    assert_eq!(
        advertised_main(&base).await.as_deref(),
        Some(hex::encode(sha_a).as_str()),
        "main must be untouched"
    );
}
