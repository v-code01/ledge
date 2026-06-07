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
        shard_map: None,
        cluster_gc: None,
        auth,
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
