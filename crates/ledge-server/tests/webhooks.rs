//! Webhooks end-to-end: a REAL local axum sink receives a signed `ref.committed`
//! delivery driven by a durable workspace commit through the in-process app
//! router. The dispatcher delivers over `reqwest`, so the target MUST be a
//! separately-`tokio::spawn`ed listening server (a `tower::oneshot` router is not
//! reachable over the wire). The app-under-test is still driven via
//! `ServiceExt::oneshot` on `build_app(state)`; its commit handler then fires the
//! dispatcher's reqwest at the spawned sink.
//!
//! Coverage:
//! - `signed_delivery` — happy path: exactly one delivery, payload + headers +
//!   blake3 signature all verified.
//! - `tenant_isolation` — globex's commit produces no delivery (only acme
//!   registered a webhook).
//! - `delete_stops_delivery` — DELETE then commit yields no new delivery.
//! - `dead_url_does_not_break_commit` — an unreachable target never fails commit.
//!
//! The disabled→503 case lives in `tests/webhooks_disabled.rs` (not duplicated).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, HeaderMap, Request, StatusCode};
use tempfile::TempDir;
use tower::ServiceExt;

use ledge_core::{ObjectId, RefName, RefStore, HLC};
use ledge_ref_store::RefStoreImpl;
use ledge_server::auth::principal::{PrincipalKind, Scopes};
use ledge_server::auth::store::AuthStore;
use ledge_server::auth::AuthCtx;
use ledge_server::quota::QuotaCtx;
use ledge_server::webhook::dispatch::WebhookDispatcher;
use ledge_server::webhook::store::WebhookStore;
use ledge_server::{build_app, AppState};

/// Recorded deliveries: (headers, raw body) tuples appended by the sink handler.
type Recorded = Arc<Mutex<Vec<(HeaderMap, axum::body::Bytes)>>>;

/// Boot a real listening axum sink on an ephemeral port that records every POST
/// to `/hook`. Returns the bound port. The server runs on a detached task for
/// the lifetime of the test process; `yield_now` lets the accept loop start.
async fn boot_sink(recorded: Recorded) -> u16 {
    let app = axum::Router::new().route(
        "/hook",
        axum::routing::post(move |headers: HeaderMap, body: axum::body::Bytes| {
            let recorded = recorded.clone();
            async move {
                recorded.lock().unwrap().push((headers, body));
                StatusCode::OK
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    tokio::task::yield_now().await;
    port
}

/// Build an AppState with auth ENABLED, two tenants (acme, globex), webhooks
/// ENABLED (in-memory store), and quotas disabled. Mirrors
/// `quota_matrix::app_with_quota` but hands back the SHARED `Arc<RefStoreImpl>`
/// so the test can set a workspace ref directly to drive a real commit.
/// Returns `(router, shared_refs, acme_token, globex_token)`.
async fn app_with_webhooks(dir: &TempDir) -> (axum::Router, Arc<RefStoreImpl>, String, String) {
    let p = dir.path().to_path_buf();
    let hlc = Arc::new(HLC::new());
    let objects = Arc::new(ledge_object_store::DiskObjectStore::new(p.clone()).unwrap());
    let refs = Arc::new(RefStoreImpl::open(p.clone(), hlc.clone()).unwrap());
    let (workspaces, leases, gc) = ledge_server::build_workspace_stack(
        p.clone(),
        objects.clone(),
        refs.clone(),
        hlc.clone(),
        ledge_workspace::QuotaLimits::default(),
        Arc::new(ledge_workspace::UsageMap::default()),
    )
    .unwrap();
    let store = Arc::new(AuthStore::open(p.clone(), hlc).unwrap());
    let acme = store
        .mint("acme", PrincipalKind::User, Scopes::ALL, None, 0)
        .await
        .unwrap();
    let globex = store
        .mint("globex", PrincipalKind::User, Scopes::ALL, None, 0)
        .await
        .unwrap();
    let auth = AuthCtx {
        enabled: true,
        store,
        cluster_secret: None,
    };
    // Tests deliver to a local 127.0.0.1 sink, so opt into private targets.
    let webhooks = Some(Arc::new(
        WebhookDispatcher::new(Arc::new(WebhookStore::in_memory())).allow_private_targets(true),
    ));
    let state = AppState {
        objects: objects.clone() as Arc<dyn ledge_core::ObjectStore>,
        objects_disk: objects.clone(),
        refs: refs.clone() as Arc<dyn ledge_core::RefStore>,
        workspaces,
        leases,
        gc,
        default_ttl_secs: 3600,
        data_dir: p,
        raft_shards: None,
        cluster_refs: None,
        cluster_objects: None,
        webhooks,
        sync: None,
        shard_map: None,
        cluster_gc: None,
        auth,
        quota: QuotaCtx::disabled(),
    };
    (build_app(state), refs, acme, globex)
}

/// Issue an authed JSON request through the app, returning `(status, body bytes)`.
async fn call(
    app: &axum::Router,
    method: &str,
    uri: &str,
    token: &str,
    body: &str,
) -> (StatusCode, axum::body::Bytes) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes)
}

/// Register a webhook for `token`'s tenant pointing at the sink; returns
/// `(id_hex, secret_bytes)` parsed from the 201 JSON.
async fn register_webhook(app: &axum::Router, token: &str, sink_port: u16) -> (String, [u8; 32]) {
    let body = format!(r#"{{"url":"http://127.0.0.1:{sink_port}/hook"}}"#);
    let (status, bytes) = call(app, "POST", "/webhooks", token, &body).await;
    assert_eq!(status, StatusCode::CREATED, "register must 201");
    let j: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let id = j["id"].as_str().unwrap().to_string();
    let secret_hex = j["secret"].as_str().unwrap();
    assert_eq!(secret_hex.len(), 64, "secret is 32 bytes hex");
    let mut secret = [0u8; 32];
    for (i, slot) in secret.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&secret_hex[i * 2..i * 2 + 2], 16).unwrap();
    }
    (id, secret)
}

/// Drive a durable commit as `token`: fork a workspace, set its `heads/main`
/// workspace ref directly on the shared store, then commit-promote it to
/// `refs/heads/main`. Returns the workspace id hex. Asserts the commit 200s
/// (a successful `CommitOutcome::Ok`, which is what fires the webhook).
async fn drive_commit(app: &axum::Router, refs: &Arc<RefStoreImpl>, token: &str) -> String {
    // Fork an empty workspace.
    let (status, bytes) = call(
        app,
        "POST",
        "/workspaces",
        token,
        r#"{"source":[],"ttl_seconds":3600}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "fork must 200");
    let w = serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Set the workspace ref directly (CAS-create, expected None). The commit only
    // CAS-promotes the ref pointer; the object itself need not exist.
    let ws_ref = RefName::new(&format!("refs/workspaces/{w}/heads/main")).unwrap();
    let oid = ObjectId::from_bytes([1u8; 32]);
    refs.update(&ws_ref, oid, None).await.unwrap();

    // Commit: map the workspace ref to the durable `refs/heads/main`.
    let body = format!(r#"{{"mappings":{{"refs/workspaces/{w}/heads/main":"refs/heads/main"}}}}"#);
    let (status, _bytes) = call(
        app,
        "POST",
        &format!("/workspaces/{w}/commit"),
        token,
        &body,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "commit must 200 (CommitOutcome::Ok)"
    );
    w
}

/// Poll `recorded` until it reaches `want` entries or the deadline elapses.
/// Returns the final length. Bounded (~5s) so a genuine non-delivery still fails
/// the assertion rather than hanging.
async fn wait_for(recorded: &Recorded, want: usize) -> usize {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let len = recorded.lock().unwrap().len();
        if len >= want || std::time::Instant::now() >= deadline {
            return len;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Headline: a commit fires exactly one signed `ref.committed` delivery whose
/// payload, `x-ledge-event` header, and blake3 signature all verify.
#[tokio::test]
async fn signed_delivery() {
    let dir = TempDir::new().unwrap();
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let port = boot_sink(recorded.clone()).await;
    let (app, refs, acme, _globex) = app_with_webhooks(&dir).await;

    let (_id, secret) = register_webhook(&app, &acme, port).await;
    drive_commit(&app, &refs, &acme).await;

    // Poll (not a fixed sleep) for the delivery to land.
    let len = wait_for(&recorded, 1).await;
    let deliveries = recorded.lock().unwrap();
    assert_eq!(len, 1, "exactly one delivery expected, got {len}");
    let (headers, body) = &deliveries[0];

    // Payload contract.
    let payload: serde_json::Value = serde_json::from_slice(body).unwrap();
    assert_eq!(payload["event"], "ref.committed");
    assert_eq!(payload["tenant"], "acme");
    assert_eq!(payload["ref"], "refs/heads/main");
    assert!(
        payload["new_target"].is_string(),
        "new_target must be present"
    );

    // Event header.
    assert_eq!(
        headers.get("x-ledge-event").unwrap().to_str().unwrap(),
        "ref.committed"
    );

    // Signature: recompute blake3 keyed hash over the EXACT raw body.
    let got_sig = headers.get("x-ledge-signature").unwrap().to_str().unwrap();
    let expect_sig = ledge_server::webhook::sign(&secret, body);
    assert_eq!(
        got_sig, expect_sig,
        "signature must verify against raw body"
    );
}

/// Only acme registered a webhook; globex's commit must produce no delivery.
#[tokio::test]
async fn tenant_isolation() {
    let dir = TempDir::new().unwrap();
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let port = boot_sink(recorded.clone()).await;
    let (app, refs, acme, globex) = app_with_webhooks(&dir).await;

    // acme registers + commits → 1 delivery.
    register_webhook(&app, &acme, port).await;
    drive_commit(&app, &refs, &acme).await;
    assert_eq!(wait_for(&recorded, 1).await, 1, "acme delivery must land");

    // globex commits with NO webhook of its own → no new delivery.
    drive_commit(&app, &refs, &globex).await;
    // Give any (erroneous) globex delivery a bounded window to appear; want=2 so
    // wait_for returns at the deadline if isolation holds.
    let len = wait_for(&recorded, 2).await;
    assert_eq!(
        len, 1,
        "globex commit must not produce a delivery, got {len}"
    );
}

/// Deleting the webhook stops further deliveries.
#[tokio::test]
async fn delete_stops_delivery() {
    let dir = TempDir::new().unwrap();
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let port = boot_sink(recorded.clone()).await;
    let (app, refs, acme, _globex) = app_with_webhooks(&dir).await;

    let (id, _secret) = register_webhook(&app, &acme, port).await;
    drive_commit(&app, &refs, &acme).await;
    assert_eq!(wait_for(&recorded, 1).await, 1, "first delivery must land");

    // DELETE the webhook → 204.
    let (status, _b) = call(&app, "DELETE", &format!("/webhooks/{id}"), &acme, "").await;
    assert_eq!(status, StatusCode::NO_CONTENT, "delete must 204");

    // Commit again → no NEW delivery.
    drive_commit(&app, &refs, &acme).await;
    let len = wait_for(&recorded, 2).await;
    assert_eq!(len, 1, "no new delivery after delete, got {len}");
}

/// A dead webhook target must NOT break the commit (delivery is best-effort and
/// isolated in a spawned task). 127.0.0.1:1 refuses connections.
#[tokio::test]
async fn dead_url_does_not_break_commit() {
    let dir = TempDir::new().unwrap();
    let (app, refs, acme, _globex) = app_with_webhooks(&dir).await;

    let (status, bytes) = call(
        &app,
        "POST",
        "/webhooks",
        &acme,
        r#"{"url":"http://127.0.0.1:1/hook"}"#,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "register dead url must still 201"
    );
    let _: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    // The commit itself must succeed despite the unreachable target.
    drive_commit(&app, &refs, &acme).await;
}
