pub mod config;
pub mod metrics;
pub mod routes;

pub use routes::AppState;

use std::time::Duration;
use axum::Router;
use tower_http::{timeout::TimeoutLayer, trace::TraceLayer};

pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", axum::routing::get(routes::healthz))
        .route("/metrics", axum::routing::get(routes::metrics_handler))
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
