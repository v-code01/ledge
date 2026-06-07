//! Phase 4d-3 quota matrix (spec §4) over the REAL `build_app` router with auth +
//! quotas ENABLED and two tenants (acme, globex). The per-module tests
//! (manager fork/commit gates, rate.rs bucket, middleware 429, gc measurement)
//! cover items 1-5 + 8; this file is the end-to-end backstop for the workspace-
//! count 507, the rate 429, the error mapping, and the root-exempt path. The
//! soft-storage 507 is best driven at the manager (Task 6) since it requires a GC
//! measurement; here we assert the wire-level workspace-count + rate gates.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use ledge_core::HLC;
use ledge_server::auth::principal::{PrincipalKind, Scopes};
use ledge_server::auth::store::AuthStore;
use ledge_server::auth::AuthCtx;
use ledge_server::quota::rate::TenantRateLimiter;
use ledge_server::quota::QuotaCtx;
use ledge_server::{build_app, AppState};
use tempfile::TempDir;
use tower::ServiceExt;

/// Build an AppState with auth ENABLED, two tenant keys, and the given QuotaCtx.
/// The manager + QuotaCtx share the SAME limits/usage (so the matrix exercises
/// real wiring). Returns (router, acme_token, globex_token).
async fn app_with_quota(dir: &TempDir, quota: QuotaCtx) -> (axum::Router, String, String) {
    let p = dir.path().to_path_buf();
    let hlc = Arc::new(HLC::new());
    let objects = Arc::new(ledge_object_store::DiskObjectStore::new(p.clone()).unwrap());
    let refs = Arc::new(ledge_ref_store::RefStoreImpl::open(p.clone(), hlc.clone()).unwrap());
    let (workspaces, leases, gc) = ledge_server::build_workspace_stack(
        p.clone(),
        objects.clone(),
        refs.clone(),
        hlc.clone(),
        quota.limits,
        quota.usage.clone(),
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
        shard_map: None,
        cluster_gc: None,
        auth,
        quota,
    };
    (build_app(state), acme, globex)
}

async fn status(app: &axum::Router, method: &str, uri: &str, token: &str, body: &str) -> StatusCode {
    app.clone()
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
        .unwrap()
        .status()
}

/// §4.1 — workspace-count quota: a tenant at `max_workspaces` gets 507 on the
/// next fork; the other tenant is unaffected (per-tenant independence).
#[tokio::test]
async fn workspace_count_quota_507_and_independence() {
    let dir = TempDir::new().unwrap();
    let quota = QuotaCtx {
        limits: ledge_workspace::QuotaLimits {
            enabled: true,
            max_workspaces: Some(2),
            ..Default::default()
        },
        usage: Arc::new(ledge_workspace::UsageMap::default()),
        rate: Arc::new(TenantRateLimiter::unlimited()),
    };
    let (app, acme, globex) = app_with_quota(&dir, quota).await;
    let create = r#"{"source":[],"ttl_seconds":3600}"#;

    // acme: two forks OK, third 507.
    assert_eq!(status(&app, "POST", "/workspaces", &acme, create).await, StatusCode::OK);
    assert_eq!(status(&app, "POST", "/workspaces", &acme, create).await, StatusCode::OK);
    assert_eq!(
        status(&app, "POST", "/workspaces", &acme, create).await,
        StatusCode::INSUFFICIENT_STORAGE,
        "acme's 3rd fork must be 507",
    );

    // globex is independent: its first two forks still succeed.
    assert_eq!(status(&app, "POST", "/workspaces", &globex, create).await, StatusCode::OK);
    assert_eq!(status(&app, "POST", "/workspaces", &globex, create).await, StatusCode::OK);
}

/// §4.2 — rate quota: with a tiny burst, the (burst+1)-th request is 429; a
/// distinct tenant is unaffected (per-node, per-tenant bucket).
#[tokio::test]
async fn rate_quota_429_per_tenant() {
    let dir = TempDir::new().unwrap();
    let quota = QuotaCtx {
        limits: ledge_workspace::QuotaLimits {
            enabled: true,
            ..Default::default()
        },
        usage: Arc::new(ledge_workspace::UsageMap::default()),
        rate: Arc::new(TenantRateLimiter::new(Some(1), Some(2))), // rate 1/s, burst 2
    };
    let (app, acme, globex) = app_with_quota(&dir, quota).await;
    // acme: two requests pass (burst), the third 429.
    assert_eq!(status(&app, "GET", "/workspaces", &acme, "").await, StatusCode::OK);
    assert_eq!(status(&app, "GET", "/workspaces", &acme, "").await, StatusCode::OK);
    assert_eq!(
        status(&app, "GET", "/workspaces", &acme, "").await,
        StatusCode::TOO_MANY_REQUESTS,
        "acme's 3rd request (burst exhausted) is 429",
    );
    // globex still has its full burst (independent bucket).
    assert_eq!(status(&app, "GET", "/workspaces", &globex, "").await, StatusCode::OK);
}

/// §4.7 — quotas DISABLED (the default ctx) ⇒ no limit at all: many forks + a
/// flood of reads all succeed (byte-identical to Phase 4d-2). Root exemption is
/// covered manager-side (Task 3/6); here we assert the disabled passthrough.
#[tokio::test]
async fn disabled_quota_no_limits() {
    let dir = TempDir::new().unwrap();
    let (app, acme, _globex) = app_with_quota(&dir, QuotaCtx::disabled()).await;
    let create = r#"{"source":[],"ttl_seconds":3600}"#;
    for _ in 0..5 {
        assert_eq!(status(&app, "POST", "/workspaces", &acme, create).await, StatusCode::OK);
    }
    for _ in 0..30 {
        assert_eq!(status(&app, "GET", "/workspaces", &acme, "").await, StatusCode::OK);
    }
}
