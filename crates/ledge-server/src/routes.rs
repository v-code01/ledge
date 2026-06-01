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

#[derive(Clone)]
pub struct AppState {
    pub objects: Arc<ledge_object_store::DiskObjectStore>,
    pub refs: Arc<ledge_ref_store::RefStoreImpl>,
}

#[derive(Deserialize)]
pub struct InfoRefsQuery {
    service: String,
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
) -> Response {
    let start = Instant::now();
    info!(repo = %repo, service = %q.service, "git info/refs");
    match q.service.as_str() {
        "git-upload-pack" => {
            metrics::record_git_request("upload-pack");
            let result = ledge_git::fetch::handle_upload_pack_discovery(
                state.objects.clone() as Arc<dyn ledge_core::ObjectStore>,
                state.refs.clone() as Arc<dyn ledge_core::RefStore>,
                state.objects.as_ref(),
                "",
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
                state.refs.clone() as Arc<dyn ledge_core::RefStore>,
                state.objects.as_ref(),
                "",
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
    body: Bytes,
) -> Response {
    let start = Instant::now();
    info!(repo = %repo, "git-upload-pack");
    metrics::record_git_request("upload-pack");
    let result = ledge_git::fetch::handle_upload_pack(
        body,
        state.objects.clone() as Arc<dyn ledge_core::ObjectStore>,
        state.refs.clone() as Arc<dyn ledge_core::RefStore>,
        state.objects.as_ref(),
        "",
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
    body: Bytes,
) -> Response {
    let start = Instant::now();
    info!(repo = %repo, "git-receive-pack");
    metrics::record_git_request("receive-pack");
    let result = ledge_git::push::handle_receive_pack(
        body,
        state.refs.clone() as Arc<dyn ledge_core::RefStore>,
        state.objects.as_ref(),
        "",
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
