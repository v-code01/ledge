use std::{sync::Arc, time::Instant};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use tracing::{info, warn};
use crate::metrics;

/// Per-shard Raft handles for the cluster control plane. `None` in single-node
/// mode (the `/raft/*` and `/cluster/*` handlers see `None` → 503), `Some` only
/// when `cluster.enabled`. Carried in [`AppState`] so the cluster route handlers
/// can reach the local node's per-shard Raft handle without touching any
/// single-node path. Defined here (not `cluster_routes`) so `AppState` does not
/// depend on a module that is itself stateful.
pub type ClusterHandles =
    std::collections::BTreeMap<ledge_cluster::ShardId, openraft::Raft<ledge_raft::TypeConfig>>;

#[derive(Clone)]
pub struct AppState {
    /// Object-store **trait seam** (`dyn ObjectStore`): the git/workspace read
    /// paths route through this. Single-node injects `Arc<DiskObjectStore>`;
    /// cluster injects `Arc<ReplicatedObjectStore>` (both up-cast). The concrete
    /// disk store is still available as `objects_disk` for the paths that need
    /// `DiskObjectStore`-only methods.
    pub objects: Arc<dyn ledge_core::ObjectStore>,
    /// Concrete on-disk object store, retained for the paths that need methods
    /// NOT on the [`ledge_core::ObjectStore`] trait: the git wire `Sha1Provider`
    /// (`sha1_of`/`git_type_of`/`write_git_object`), the capnp RPC `writeObject`,
    /// and per-node-local GC (`list_all_ids`/`delete`). In cluster mode this is
    /// the node-local disk store underneath `ReplicatedObjectStore` — the git
    /// protocol + object wire path operate on the local node's object store,
    /// while ref replication goes through the Raft `dyn RefStore` (Phase 3 scope).
    pub objects_disk: Arc<ledge_object_store::DiskObjectStore>,
    /// Ref-store **trait seam** (`dyn RefStore`): every ref mutation/read for git
    /// and the workspace control plane routes through this. Single-node injects
    /// `Arc<RefStoreImpl>`; cluster injects `Arc<ClusterRefStore>` (both up-cast).
    pub refs: Arc<dyn ledge_core::RefStore>,
    pub workspaces: Arc<ledge_workspace::WorkspaceManager>,
    pub leases: Arc<ledge_workspace::LeaseStore>,
    pub gc: Arc<ledge_workspace::Gc>,
    /// Fallback TTL (seconds) for `POST /workspaces` when the request omits
    /// `ttl_seconds`. Sourced from `workspace.default_ttl_secs` config.
    pub default_ttl_secs: u64,
    /// On-disk root of this repo's data (objects + refs + leases). Source of
    /// the CoW snapshot at `POST /admin/snapshot` (Phase 2d).
    pub data_dir: std::path::PathBuf,
    /// Per-shard Raft handles when `cluster.enabled`, else `None`. Read only by
    /// the `/raft/*` and `/cluster/*` handlers; single-node leaves it `None` so
    /// those handlers report not-clustered (503) and nothing else is affected.
    pub raft_shards: Option<Arc<ClusterHandles>>,
    /// The concrete clustered ref store, when `cluster.enabled`. Used ONLY by the
    /// `POST /cluster/ref-op` handler to apply a shard-targeted op to a LOCAL
    /// shard handle (the `dyn RefStore` seam in [`refs`](Self::refs) is too narrow
    /// for that — it re-routes by name). `None` single-node ⇒ the handler 503s,
    /// consistent with the other cluster routes. It is the SAME `Arc` underlying
    /// `refs` in cluster mode (one store, two views).
    pub cluster_refs: Option<Arc<ledge_cluster::ClusterRefStore>>,
    /// The concrete `ReplicatedObjectStore` in cluster mode — held so the Phase 4g
    /// reconfigure route can swap its replication peer set. `None` single-node.
    pub cluster_objects: Option<std::sync::Arc<ledge_cluster::ReplicatedObjectStore>>,
    /// Outbound webhook dispatcher (Some only when [webhooks].enabled). None ⇒
    /// no events emitted + the /webhooks routes report 503.
    pub webhooks: Option<std::sync::Arc<crate::webhook::dispatch::WebhookDispatcher>>,
    /// Git remote sync engine (Some only when [sync].enabled). None ⇒ /sync routes 503.
    pub sync: Option<std::sync::Arc<crate::sync::SyncEngine>>,
    /// The node-local distributed-GC driver, when `cluster.enabled`. `POST
    /// /admin/gc` runs THIS via `ClusterGc::run` in cluster mode; `POST
    /// /cluster/gc` fans out and aggregates. `None` single-node ⇒ `/admin/gc`
    /// falls back to the single-node `gc`, and `/cluster/gc` 503s.
    pub cluster_gc: Option<Arc<ledge_cluster::gc::ClusterGc>>,
    /// The authoritative shard map, when `cluster.enabled`. Lets `/cluster/ref-op`
    /// answer "you misrouted — shard S lives on these members" and lets
    /// `/cluster/status` report declared placement (members) for EVERY shard, not
    /// just the locally-hosted ones. `None` single-node.
    pub shard_map: Option<ledge_cluster::ShardMap>,
    /// Authentication context (Phase 4d-1): enabled flag, the API-key store, and
    /// the node-to-node cluster secret. `AuthCtx::disabled()` in single-node dev
    /// and all tests; the real ctx in `main.rs` when `[auth] enabled=true`.
    pub auth: crate::auth::AuthCtx,
    /// Per-tenant quota context (Phase 4d-3): the durable limits, the shared
    /// usage snapshot, and the request-rate limiter. `QuotaCtx::disabled()` in
    /// single-node dev + all tests; the real ctx in `main.rs` when
    /// `[quotas] enabled=true`. With it disabled, every gate is a no-op (R Q15).
    pub quota: crate::quota::QuotaCtx,
}

#[derive(Deserialize)]
pub struct InfoRefsQuery {
    service: String,
}

/// Verify that workspace `id` is owned by `tenant`; `Ok(())` if owned, `Err(404)`
/// otherwise (a foreign or unknown workspace is indistinguishable — no existence
/// leak, spec §5). The check is one lease read; workspace refs themselves stay
/// physically tenant-agnostic (`refs/workspaces/<id>/…`, R2).
async fn ws_tenant_ok(state: &AppState, id: &str, tenant: &str) -> Result<(), StatusCode> {
    let wid = ledge_workspace::WorkspaceId::from_hex(id).map_err(|_| StatusCode::NOT_FOUND)?;
    match state.leases.get(wid).await {
        Ok(Some(l)) => {
            let norm = |t: &str| if t.is_empty() { "root" } else { t }.to_string();
            if norm(&l.tenant_id) == norm(tenant) {
                Ok(())
            } else {
                metrics::record_tenant_denied();
                Err(StatusCode::NOT_FOUND)
            }
        }
        // Absent lease: a never-existed workspace → 404 (same as unknown id).
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn healthz() -> impl IntoResponse {
    axum::Json(serde_json::json!({"status": "ok"}))
}

pub async fn metrics_handler() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        metrics::render(),
    )
}

pub async fn info_refs(
    Path(repo): Path<String>,
    Query(q): Query<InfoRefsQuery>,
    State(state): State<AppState>,
    principal: crate::auth::Principal,
) -> Response {
    // Default-repo durable refs are physically namespaced per tenant: root →
    // "" (legacy global, byte-identical), a named tenant → "tenants/<t>/", so the
    // client's `refs/heads/main` is stored/listed as `refs/tenants/<t>/heads/main`
    // and presented stripped (spec §3.1).
    let segment = ledge_core::tenant_prefix(&principal.tenant_id);
    let start = Instant::now();
    info!(repo = %repo, service = %q.service, "git info/refs");
    match q.service.as_str() {
        "git-upload-pack" => {
            metrics::record_git_request("upload-pack");
            let result = ledge_git::fetch::handle_upload_pack_discovery(
                state.objects.clone(),
                state.refs.clone(),
                state.objects_disk.as_ref(),
                &segment,
            )
            .await;
            metrics::record_git_request_duration("upload-pack", start.elapsed());
            match result {
                Ok(b) => git_response("application/x-git-upload-pack-advertisement", b),
                Err(e) => {
                    warn!(error = %e, "upload-pack discovery failed");
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            }
        }
        "git-receive-pack" => {
            metrics::record_git_request("receive-pack");
            let result = ledge_git::push::handle_receive_pack_discovery(
                state.refs.clone(),
                state.objects_disk.as_ref(),
                &segment,
            )
            .await;
            metrics::record_git_request_duration("receive-pack", start.elapsed());
            match result {
                Ok(b) => git_response("application/x-git-receive-pack-advertisement", b),
                Err(e) => {
                    warn!(error = %e, "receive-pack discovery failed");
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            }
        }
        unknown => {
            warn!(service = %unknown, "unknown service");
            StatusCode::BAD_REQUEST.into_response()
        }
    }
}

pub async fn upload_pack(
    Path(repo): Path<String>,
    State(state): State<AppState>,
    principal: crate::auth::Principal,
    body: Bytes,
) -> Response {
    let segment = ledge_core::tenant_prefix(&principal.tenant_id);
    let start = Instant::now();
    info!(repo = %repo, "git-upload-pack");
    metrics::record_git_request("upload-pack");
    let result = ledge_git::fetch::handle_upload_pack(
        body,
        state.objects.clone(),
        state.refs.clone(),
        state.objects_disk.as_ref(),
        &segment,
        Some(ledge_git::fetch::global_upload_cache()),
    )
    .await;
    metrics::record_git_request_duration("upload-pack", start.elapsed());
    match result {
        Ok(p) => git_response("application/x-git-upload-pack-result", p),
        Err(e) => {
            warn!(error = %e, "upload-pack failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn receive_pack(
    Path(repo): Path<String>,
    State(state): State<AppState>,
    principal: crate::auth::Principal,
    body: Bytes,
) -> Response {
    let segment = ledge_core::tenant_prefix(&principal.tenant_id);
    let start = Instant::now();
    info!(repo = %repo, "git-receive-pack");
    metrics::record_git_request("receive-pack");
    let result = ledge_git::push::handle_receive_pack(
        body,
        state.refs.clone(),
        state.objects_disk.as_ref(),
        &segment,
    )
    .await;
    metrics::record_git_request_duration("receive-pack", start.elapsed());
    match result {
        Ok(r) => git_response("application/x-git-receive-pack-result", r),
        Err(e) => {
            warn!(error = %e, "receive-pack failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// GET /ws/{id}/info/refs — workspace-scoped discovery.
pub async fn ws_info_refs(
    Path(id): Path<String>,
    Query(q): Query<InfoRefsQuery>,
    State(state): State<AppState>,
    principal: crate::auth::Principal,
) -> Response {
    // Cross-tenant access to a foreign/unknown workspace is 404 (no existence
    // leak). Workspace refs stay physically tenant-agnostic (R2); isolation is
    // this ownership check + the unguessable id.
    if let Err(code) = ws_tenant_ok(&state, &id, &principal.tenant_id).await {
        return code.into_response();
    }
    let segment = format!("workspaces/{id}/");
    let start = Instant::now();
    info!(ws = %id, service = %q.service, "ws git info/refs");
    match q.service.as_str() {
        "git-upload-pack" => {
            metrics::record_git_request("upload-pack");
            let r = ledge_git::fetch::handle_upload_pack_discovery(
                state.objects.clone(),
                state.refs.clone(),
                state.objects_disk.as_ref(),
                &segment,
            )
            .await;
            metrics::record_git_request_duration("upload-pack", start.elapsed());
            match r {
                Ok(b) => git_response("application/x-git-upload-pack-advertisement", b),
                Err(e) => {
                    warn!(error = %e, "ws upload-pack discovery failed");
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            }
        }
        "git-receive-pack" => {
            metrics::record_git_request("receive-pack");
            let r = ledge_git::push::handle_receive_pack_discovery(
                state.refs.clone(),
                state.objects_disk.as_ref(),
                &segment,
            )
            .await;
            metrics::record_git_request_duration("receive-pack", start.elapsed());
            match r {
                Ok(b) => git_response("application/x-git-receive-pack-advertisement", b),
                Err(e) => {
                    warn!(error = %e, "ws receive-pack discovery failed");
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            }
        }
        unknown => {
            warn!(service = %unknown, "unknown service");
            StatusCode::BAD_REQUEST.into_response()
        }
    }
}

/// POST /ws/{id}/git-upload-pack
pub async fn ws_upload_pack(
    Path(id): Path<String>,
    State(state): State<AppState>,
    principal: crate::auth::Principal,
    body: Bytes,
) -> Response {
    if let Err(code) = ws_tenant_ok(&state, &id, &principal.tenant_id).await {
        return code.into_response();
    }
    let segment = format!("workspaces/{id}/");
    let start = Instant::now();
    metrics::record_git_request("upload-pack");
    let r = ledge_git::fetch::handle_upload_pack(
        body,
        state.objects.clone(),
        state.refs.clone(),
        state.objects_disk.as_ref(),
        &segment,
        Some(ledge_git::fetch::global_upload_cache()),
    )
    .await;
    metrics::record_git_request_duration("upload-pack", start.elapsed());
    match r {
        Ok(p) => git_response("application/x-git-upload-pack-result", p),
        Err(e) => {
            warn!(error = %e, "ws upload-pack failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// POST /ws/{id}/git-receive-pack
pub async fn ws_receive_pack(
    Path(id): Path<String>,
    State(state): State<AppState>,
    principal: crate::auth::Principal,
    body: Bytes,
) -> Response {
    if let Err(code) = ws_tenant_ok(&state, &id, &principal.tenant_id).await {
        return code.into_response();
    }
    let segment = format!("workspaces/{id}/");
    let start = Instant::now();
    metrics::record_git_request("receive-pack");
    let r = ledge_git::push::handle_receive_pack(
        body,
        state.refs.clone(),
        state.objects_disk.as_ref(),
        &segment,
    )
    .await;
    metrics::record_git_request_duration("receive-pack", start.elapsed());
    match r {
        Ok(r) => git_response("application/x-git-receive-pack-result", r),
        Err(e) => {
            warn!(error = %e, "ws receive-pack failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

fn git_response(ct: &'static str, body: Vec<u8>) -> Response {
    (StatusCode::OK, [(header::CONTENT_TYPE, ct)], body).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    async fn healthz_returns_ok() {
        let r = healthz().await.into_response();
        assert_eq!(r.status(), StatusCode::OK);
        let b = to_bytes(r.into_body(), usize::MAX).await.unwrap();
        let j: serde_json::Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(j["status"], "ok");
    }
}

#[cfg(test)]
mod tenant_git_tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::{header, Request};
    use ledge_core::{ObjectId, RefName, RefStore, HLC};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tower::ServiceExt;

    /// Two-tenant app PLUS handles to the shared ref + object stores, so a test
    /// can seed a tenant's PHYSICAL durable ref (pointing at a REAL git object, so
    /// the wire discovery can resolve its SHA-1) and assert isolation at the wire.
    type GitApp = (
        axum::Router,
        String,
        String,
        Arc<ledge_ref_store::RefStoreImpl>,
        Arc<ledge_object_store::DiskObjectStore>,
    );

    async fn git_app(dir: &TempDir) -> GitApp {
        use crate::auth::principal::{PrincipalKind, Scopes};
        use crate::auth::store::AuthStore;
        use crate::auth::AuthCtx;
        let p = dir.path().to_path_buf();
        let hlc = Arc::new(HLC::new());
        let objects = Arc::new(ledge_object_store::DiskObjectStore::new(p.clone()).unwrap());
        let refs = Arc::new(ledge_ref_store::RefStoreImpl::open(p.clone(), hlc.clone()).unwrap());
        let (workspaces, leases, gc) =
            crate::build_workspace_stack(
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
        let auth = AuthCtx { enabled: true, store, cluster_secret: None };
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
            webhooks: None,
            sync: None,
            shard_map: None,
            cluster_gc: None,
            auth,
            quota: crate::quota::QuotaCtx::disabled(),
        };
        (crate::build_app(state), acme, globex, refs, objects)
    }

    /// Write a real git blob so the wire discovery can resolve its SHA-1, and
    /// return the BLAKE3 [`ObjectId`] to point a ref at.
    async fn seed_blob(
        objects: &ledge_object_store::DiskObjectStore,
        content: &'static [u8],
    ) -> ObjectId {
        objects
            .write_git_object(3, axum::body::Bytes::from_static(content))
            .await
            .unwrap()
    }

    async fn discovery(app: &axum::Router, token: &str) -> String {
        let r = app
            .clone()
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
        let b = to_bytes(r.into_body(), usize::MAX).await.unwrap();
        String::from_utf8_lossy(&b).into_owned()
    }

    /// §6.5/§6.6 — each tenant's default-repo discovery lists ONLY its own
    /// branches (physically under refs/tenants/<t>/), presented as refs/heads/main.
    #[tokio::test]
    async fn default_repo_discovery_is_tenant_isolated() {
        let dir = TempDir::new().unwrap();
        let (app, acme, globex, refs, objects) = git_app(&dir).await;
        // Seed each tenant's PHYSICAL durable ref directly, pointing at a real
        // git object so the wire discovery can resolve its SHA-1.
        let acme_oid = seed_blob(&objects, b"acme blob").await;
        let globex_oid = seed_blob(&objects, b"globex blob").await;
        refs.update(&RefName::new("refs/tenants/acme/heads/main").unwrap(), acme_oid, None)
            .await
            .unwrap();
        refs.update(
            &RefName::new("refs/tenants/globex/heads/feature").unwrap(),
            globex_oid,
            None,
        )
        .await
        .unwrap();

        let acme_adv = discovery(&app, &acme).await;
        // acme sees its own branch, presented client-facing (prefix stripped).
        assert!(acme_adv.contains("refs/heads/main"), "acme must see its main: {acme_adv}");
        assert!(!acme_adv.contains("refs/heads/feature"), "acme must NOT see globex's feature");
        assert!(!acme_adv.contains("tenants/"), "client never sees the physical prefix");

        let globex_adv = discovery(&app, &globex).await;
        assert!(globex_adv.contains("refs/heads/feature"));
        assert!(!globex_adv.contains("refs/heads/main"), "globex must NOT see acme's main");
    }

    /// §6.4 — globex GET /ws/<acme-id>/info/refs → 404; acme on its own → 200.
    #[tokio::test]
    async fn workspace_git_ownership_enforced() {
        let dir = TempDir::new().unwrap();
        let (app, acme, globex, _refs, _objects) = git_app(&dir).await;
        // acme creates a workspace via REST.
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
        let b = to_bytes(r.into_body(), usize::MAX).await.unwrap();
        let id = serde_json::from_slice::<serde_json::Value>(&b).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();

        let ws_get = |token: &str| {
            let app = app.clone();
            let uri = format!("/ws/{id}/info/refs?service=git-upload-pack");
            let token = token.to_string();
            async move {
                app.oneshot(
                    Request::builder()
                        .method("GET")
                        .uri(uri)
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
                .status()
            }
        };
        assert_eq!(ws_get(&globex).await, StatusCode::NOT_FOUND, "globex denied acme's ws git");
        assert_eq!(ws_get(&acme).await, StatusCode::OK, "acme serves its own ws git");
    }
}
