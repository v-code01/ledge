//! Sync-disabled invariant: with `AppState.sync == None` the `POST /sync/import`
//! route is mounted but fails closed with 503. Mirrors the auth-disabled,
//! webhooks-disabled single-node `AppState` literal used by `tests/integration.rs`
//! and `tests/webhooks_disabled.rs`.
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tempfile::TempDir;
use tower::ServiceExt;

use ledge_core::HLC;
use ledge_object_store::DiskObjectStore;
use ledge_ref_store::RefStoreImpl;
use ledge_server::{build_app, AppState};

/// Build a single-node, auth-disabled, webhooks-disabled, sync-disabled app router.
fn disabled_app() -> (axum::Router, TempDir) {
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
        shard_map: None,
        cluster_gc: None,
        auth: ledge_server::auth::AuthCtx::disabled(),
        quota: ledge_server::quota::QuotaCtx::disabled(),
        sync: None,
    };
    (build_app(state), data_dir)
}

#[tokio::test]
async fn sync_disabled_returns_503() {
    let (app, _dir) = disabled_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sync/import")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"upstream_url":"file:///x"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}
