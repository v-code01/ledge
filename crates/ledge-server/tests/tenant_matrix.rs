//! Phase 4d-2 isolation matrix (spec §6) over the REAL `build_app` router with
//! auth ENABLED and two tenants (acme, globex). The per-module tests
//! (workspace_routes::tenant_rest_tests, routes::tenant_git_tests,
//! rpc_routes::rpc_get_workspace_is_tenant_isolated) cover items 1-8; this file
//! is the end-to-end backstop and the object-reachability check (§6.10). The
//! auth-DISABLED equivalence (§6.9 / item 9) is the rest of the suite, which runs
//! as root with an empty prefix — byte-identical to pre-4d-2.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use ledge_core::{ObjectId, RefName, RefStore, HLC};
use ledge_server::auth::principal::{PrincipalKind, Scopes};
use ledge_server::auth::store::AuthStore;
use ledge_server::auth::AuthCtx;
use ledge_server::{build_app, AppState};
use tempfile::TempDir;
use tower::ServiceExt;

async fn two_tenant(
    dir: &TempDir,
) -> (
    axum::Router,
    String,
    String,
    Arc<ledge_ref_store::RefStoreImpl>,
    Arc<ledge_object_store::DiskObjectStore>,
) {
    let p = dir.path().to_path_buf();
    let hlc = Arc::new(HLC::new());
    let objects = Arc::new(ledge_object_store::DiskObjectStore::new(p.clone()).unwrap());
    let refs = Arc::new(ledge_ref_store::RefStoreImpl::open(p.clone(), hlc.clone()).unwrap());
    let (workspaces, leases, gc) = ledge_server::build_workspace_stack(
        p.clone(),
        objects.clone(),
        refs.clone(),
        hlc.clone(),
        ledge_workspace::QuotaLimits::default(),
        std::sync::Arc::new(ledge_workspace::UsageMap::default()),
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
        quota: ledge_server::quota::QuotaCtx::disabled(),
    };
    (build_app(state), acme, globex, refs, objects)
}

/// Write a real git blob so wire discovery can resolve its SHA-1; return the
/// BLAKE3 [`ObjectId`] to point a ref at. (Synthetic ids would 500 the advertise
/// when the wire layer fails to resolve a non-existent object's SHA-1.)
async fn seed_blob(objects: &ledge_object_store::DiskObjectStore, content: &'static [u8]) -> ObjectId {
    objects
        .write_git_object(3, axum::body::Bytes::from_static(content))
        .await
        .unwrap()
}

/// §6.5/§6.6 — durable refs are physically partitioned; each tenant's discovery
/// lists only its own branches, and the physical store shows refs/tenants/<t>/.
#[tokio::test]
async fn durable_refs_physically_partitioned() {
    let dir = TempDir::new().unwrap();
    let (app, acme, globex, refs, objects) = two_tenant(&dir).await;
    // Point each tenant's PHYSICAL durable ref at a real git object so the wire
    // discovery can resolve its SHA-1.
    let acme_oid = seed_blob(&objects, b"acme blob").await;
    let globex_oid = seed_blob(&objects, b"globex blob").await;
    refs.update(
        &RefName::new("refs/tenants/acme/heads/main").unwrap(),
        acme_oid,
        None,
    )
    .await
    .unwrap();
    refs.update(
        &RefName::new("refs/tenants/globex/heads/main").unwrap(),
        globex_oid,
        None,
    )
    .await
    .unwrap();

    let adv = |token: &str| {
        let app = app.clone();
        let token = token.to_string();
        async move {
            let r = app
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri("/repo/info/refs?service=git-upload-pack")
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(r.status(), StatusCode::OK);
            String::from_utf8_lossy(&to_bytes(r.into_body(), usize::MAX).await.unwrap())
                .into_owned()
        }
    };
    let a = adv(&acme).await;
    let g = adv(&globex).await;
    // Each tenant sees ONLY its own branch as a plain refs/heads/main (present_ref
    // strips the tenant segment), and NEVER the physical tenants/ prefix.
    assert!(a.contains("refs/heads/main") && !a.contains("tenants/"));
    assert!(g.contains("refs/heads/main") && !g.contains("tenants/"));
    // Physically distinct namespaces co-exist (proves no collision).
    assert_eq!(
        refs.get(&RefName::new("refs/tenants/acme/heads/main").unwrap())
            .await
            .unwrap()
            .unwrap()
            .target,
        acme_oid
    );
    assert_eq!(
        refs.get(&RefName::new("refs/tenants/globex/heads/main").unwrap())
            .await
            .unwrap()
            .unwrap()
            .target,
        globex_oid
    );
}

/// §6.1 (end-to-end backstop) — a foreign workspace id is a uniform 404 across
/// GET, renew, commit, delete, AND /ws git.
#[tokio::test]
async fn foreign_workspace_is_404_everywhere() {
    let dir = TempDir::new().unwrap();
    let (app, acme, globex, _refs, _objects) = two_tenant(&dir).await;
    // acme creates a workspace.
    let r = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workspaces")
                .header("content-type", "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {acme}"))
                .body(Body::from(r#"{"source":[],"ttl_seconds":3600}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let id = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(r.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let cases = [
        ("GET", format!("/workspaces/{id}"), ""),
        ("POST", format!("/workspaces/{id}/renew"), r#"{"ttl_seconds":60}"#),
        ("POST", format!("/workspaces/{id}/commit"), r#"{"mappings":{}}"#),
        ("DELETE", format!("/workspaces/{id}"), ""),
        ("GET", format!("/ws/{id}/info/refs?service=git-upload-pack"), ""),
    ];
    for (method, uri, body) in cases {
        let r = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(&uri)
                    .header("content-type", "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {globex}"))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            r.status(),
            StatusCode::NOT_FOUND,
            "globex {method} {uri} must be 404"
        );
    }
}

/// Like [`two_tenant`] but with quotas ENABLED (`max_workspaces`) so the manager's
/// `fork` count gate fires. The manager's limits + the `QuotaCtx`'s limits share
/// ONE value, and the usage `Arc` is shared (R Q1/Q15) — built once, threaded into
/// `build_workspace_stack` and `AppState.quota`.
async fn two_tenant_quota(dir: &TempDir, max_workspaces: u64) -> (axum::Router, String) {
    let p = dir.path().to_path_buf();
    let hlc = Arc::new(HLC::new());
    let objects = Arc::new(ledge_object_store::DiskObjectStore::new(p.clone()).unwrap());
    let refs = Arc::new(ledge_ref_store::RefStoreImpl::open(p.clone(), hlc.clone()).unwrap());
    // The enabled quota context: the manager + the AppState.quota share these
    // exact limits (Copy) and the SAME usage Arc.
    let limits = ledge_workspace::QuotaLimits {
        enabled: true,
        max_workspaces: Some(max_workspaces),
        ..Default::default()
    };
    let usage = std::sync::Arc::new(ledge_workspace::UsageMap::default());
    let quota = ledge_server::quota::QuotaCtx {
        limits,
        usage: usage.clone(),
        rate: std::sync::Arc::new(ledge_server::quota::rate::TenantRateLimiter::unlimited()),
    };
    let (workspaces, leases, gc) = ledge_server::build_workspace_stack(
        p.clone(),
        objects.clone(),
        refs.clone(),
        hlc.clone(),
        limits,
        usage,
    )
    .unwrap();
    let store = Arc::new(AuthStore::open(p.clone(), hlc).unwrap());
    let acme = store
        .mint("acme", PrincipalKind::User, Scopes::ALL, None, 0)
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
    (build_app(state), acme)
}

/// Like [`two_tenant_quota`] but takes full [`QuotaLimits`] and also returns the
/// SHARED usage `Arc` so an end-to-end test can simulate a GC measurement and then
/// drive the manager's SOFT `commit` storage gate through the REST handler.
async fn two_tenant_quota_limits(
    dir: &TempDir,
    limits: ledge_workspace::QuotaLimits,
) -> (axum::Router, String, Arc<ledge_workspace::UsageMap>) {
    let p = dir.path().to_path_buf();
    let hlc = Arc::new(HLC::new());
    let objects = Arc::new(ledge_object_store::DiskObjectStore::new(p.clone()).unwrap());
    let refs = Arc::new(ledge_ref_store::RefStoreImpl::open(p.clone(), hlc.clone()).unwrap());
    let usage = std::sync::Arc::new(ledge_workspace::UsageMap::default());
    let quota = ledge_server::quota::QuotaCtx {
        limits,
        usage: usage.clone(),
        rate: std::sync::Arc::new(ledge_server::quota::rate::TenantRateLimiter::unlimited()),
    };
    let (workspaces, leases, gc) = ledge_server::build_workspace_stack(
        p.clone(),
        objects.clone(),
        refs.clone(),
        hlc.clone(),
        limits,
        usage.clone(),
    )
    .unwrap();
    let store = Arc::new(AuthStore::open(p.clone(), hlc).unwrap());
    let acme = store
        .mint("acme", PrincipalKind::User, Scopes::ALL, None, 0)
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
    (build_app(state), acme, usage)
}

/// Phase 4d-3 Task 6 (end-to-end) — with quotas enabled and a tiny
/// `max_durable_bytes`, a tenant whose LAST GC measurement is at/over the limit has
/// its `POST /workspaces/<id>/commit` rejected by the manager's SOFT storage gate
/// and surfaces as **507 Insufficient Storage** through the REST handler
/// (`QuotaExceeded("durable_bytes: …")` → `map_lookup_err`). The gate fires before
/// the mapping/promotion logic, so even an empty-mapping commit trips it.
#[tokio::test]
async fn over_quota_commit_is_507_through_rest() {
    let dir = TempDir::new().unwrap();
    let limits = ledge_workspace::QuotaLimits {
        enabled: true,
        max_durable_bytes: Some(500),
        ..Default::default()
    };
    let (app, acme, usage) = two_tenant_quota_limits(&dir, limits).await;

    // Fork a workspace for acme so the commit ownership check passes.
    let fork = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workspaces")
                .header("content-type", "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {acme}"))
                .body(Body::from(r#"{"source":[],"ttl_seconds":3600}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(fork.status().is_success(), "fork must succeed: {}", fork.status());
    let body = to_bytes(fork.into_body(), 1 << 20).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let id = v["id"].as_str().unwrap().to_string();

    // Simulate a GC pass measuring acme AT/OVER the byte limit (1000 >= 500).
    let mut m = std::collections::HashMap::new();
    m.insert(
        "acme".to_string(),
        ledge_workspace::TenantUsage { bytes: 1000, objects: 1 },
    );
    usage.store(Arc::new(m));

    // The commit gate fires before any mapping work ⇒ 507 even with empty mappings.
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/workspaces/{id}/commit"))
                .header("content-type", "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {acme}"))
                .body(Body::from(r#"{"mappings":{}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::INSUFFICIENT_STORAGE,
        "over-quota commit must surface as 507"
    );
}

/// Phase 4d-3 (end-to-end) — with quotas enabled and `max_workspaces=1`, acme's
/// SECOND `POST /workspaces` is rejected by the manager's `fork` count gate and
/// surfaces as **507 Insufficient Storage** through the REST handler (the
/// `QuotaExceeded("workspaces: …")` → `map_lookup_err` non-`requests:` mapping).
#[tokio::test]
async fn over_quota_fork_is_507_through_rest() {
    let dir = TempDir::new().unwrap();
    let (app, acme) = two_tenant_quota(&dir, 1).await;

    let create = || {
        let app = app.clone();
        let acme = acme.clone();
        async move {
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workspaces")
                    .header("content-type", "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {acme}"))
                    .body(Body::from(r#"{"source":[],"ttl_seconds":3600}"#))
                    .unwrap(),
            )
            .await
            .unwrap()
        }
    };

    // First fork: under the limit ⇒ created (201/200).
    let first = create().await;
    assert!(
        first.status().is_success(),
        "first fork must succeed, got {}",
        first.status()
    );

    // Second fork: at the limit ⇒ QuotaExceeded("workspaces: 1 limit reached")
    // ⇒ 507 Insufficient Storage.
    let second = create().await;
    assert_eq!(
        second.status(),
        StatusCode::INSUFFICIENT_STORAGE,
        "over-quota fork must surface as 507"
    );
}
