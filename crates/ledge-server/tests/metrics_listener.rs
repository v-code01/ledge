//! The dedicated metrics/health router (`build_metrics_app`) serves /metrics and
//! /healthz with no state and no auth — the TLS-agnostic scrape/probe port.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use ledge_server::build_metrics_app;
use tower::ServiceExt;

async fn code(path: &str) -> StatusCode {
    build_metrics_app()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn metrics_router_serves_metrics_and_healthz() {
    assert_eq!(code("/metrics").await, StatusCode::OK);
    assert_eq!(code("/healthz").await, StatusCode::OK);
}

#[tokio::test]
async fn metrics_router_has_no_other_routes() {
    // No auth, no app routes — an unknown path is a plain 404 (not 401).
    assert_eq!(code("/workspaces").await, StatusCode::NOT_FOUND);
}
