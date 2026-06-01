pub mod config;
pub mod metrics;
pub mod routes;
pub mod workspace_routes;

pub use routes::AppState;

use std::sync::Arc;
use std::time::Duration;
use axum::Router;
use tower_http::{timeout::TimeoutLayer, trace::TraceLayer};

use ledge_core::HLC;
use ledge_object_store::DiskObjectStore;
use ledge_ref_store::RefStoreImpl;
use ledge_workspace::{Gc, LeaseStore, WorkspaceManager};

/// Open the lease store and assemble the workspace control-plane trio
/// (manager, lease store, GC) from already-open object/ref stores.
pub fn build_workspace_stack(
    data_dir: std::path::PathBuf,
    objects: Arc<DiskObjectStore>,
    refs: Arc<RefStoreImpl>,
    hlc: Arc<HLC>,
) -> ledge_core::Result<(Arc<WorkspaceManager>, Arc<LeaseStore>, Arc<Gc>)> {
    let leases = Arc::new(LeaseStore::open(data_dir, hlc.clone())?);
    let manager = Arc::new(WorkspaceManager::new(refs.clone(), leases.clone(), hlc));
    let gc = Arc::new(Gc::new(refs, leases.clone(), objects));
    Ok((manager, leases, gc))
}

pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", axum::routing::get(routes::healthz))
        .route("/metrics", axum::routing::get(routes::metrics_handler))
        // ── Workspace control plane (spec §7) ──────────────────────────────
        .route(
            "/workspaces",
            axum::routing::post(workspace_routes::create_workspace)
                .get(workspace_routes::list_workspaces),
        )
        .route(
            "/workspaces/{id}",
            axum::routing::get(workspace_routes::get_workspace)
                .delete(workspace_routes::delete_workspace),
        )
        .route(
            "/workspaces/{id}/renew",
            axum::routing::post(workspace_routes::renew_workspace),
        )
        .route(
            "/workspaces/{id}/commit",
            axum::routing::post(workspace_routes::commit_workspace),
        )
        .route("/admin/gc", axum::routing::post(workspace_routes::admin_gc))
        // ── Workspace-scoped git (segment = workspaces/{id}/) ──────────────
        .route("/ws/{id}/info/refs", axum::routing::get(routes::ws_info_refs))
        .route(
            "/ws/{id}/git-upload-pack",
            axum::routing::post(routes::ws_upload_pack),
        )
        .route(
            "/ws/{id}/git-receive-pack",
            axum::routing::post(routes::ws_receive_pack),
        )
        // ── Default repo git (segment = "") ────────────────────────────────
        .route("/{repo}/info/refs", axum::routing::get(routes::info_refs))
        .route(
            "/{repo}/git-upload-pack",
            axum::routing::post(routes::upload_pack),
        )
        .route(
            "/{repo}/git-receive-pack",
            axum::routing::post(routes::receive_pack),
        )
        .with_state(state)
        .layer(
            tower::ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(TimeoutLayer::with_status_code(
                    axum::http::StatusCode::REQUEST_TIMEOUT,
                    Duration::from_secs(60),
                )),
        )
}
