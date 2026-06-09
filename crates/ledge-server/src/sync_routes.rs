//! Git remote sync routes. `POST /sync/import` — clone an upstream repo into a
//! new workspace for the caller's tenant. 503 when `[sync]` disabled.
//!
//! Outcome metrics (`ledge_sync_*`) are recorded INSIDE [`crate::sync::SyncEngine::import`]
//! itself (the engine owns the import/export instrumentation), so this handler
//! deliberately does NOT re-record them — doing so would double-count every
//! import in `ledge_sync_total` and the duration histogram.
use axum::{extract::State, http::StatusCode, response::IntoResponse, response::Response, Json};

use crate::routes::AppState;

/// `POST /sync/import` body. Only `upstream_url` is required; `upstream_auth`
/// carries an optional credential (PAT/token) for private upstreams, and
/// `ttl_seconds` overrides the server's default workspace TTL.
#[derive(serde::Deserialize)]
pub struct ImportRequest {
    pub upstream_url: String,
    #[serde(default)]
    pub upstream_auth: Option<String>,
    #[serde(default)]
    pub ttl_seconds: Option<u64>,
}

/// Clone + ingest an upstream repo into a fresh workspace for `principal.tenant_id`.
///
/// - 503 when `[sync]` is disabled (engine `None`), matching the other
///   feature-gated routes' fail-closed posture.
/// - 400 when `upstream_url` is not an http(s)/file URL (cheap scheme guard
///   before the engine's allow-list host check).
/// - 201 + `{workspace_id, default_branch, refs[]}` on success.
/// - 502 when the upstream clone/ingest fails (the error string is surfaced to
///   the caller; the upstream URL/credential never appear in metrics labels).
pub async fn import(
    State(state): State<AppState>,
    principal: crate::auth::Principal,
    Json(req): Json<ImportRequest>,
) -> Response {
    let Some(engine) = &state.sync else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let u = &req.upstream_url;
    if !(u.starts_with("https://") || u.starts_with("http://") || u.starts_with("file://")) {
        return (StatusCode::BAD_REQUEST, "upstream_url must be http(s) or file").into_response();
    }
    let ttl = req.ttl_seconds.unwrap_or(state.default_ttl_secs);
    match engine
        .import(
            &principal.tenant_id,
            &req.upstream_url,
            req.upstream_auth.as_deref(),
            ttl,
        )
        .await
    {
        Ok(r) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "workspace_id": r.workspace_id,
                "default_branch": r.default_branch,
                "refs": r.refs.iter().map(|x| serde_json::json!({
                    "name": x.name, "target_sha1": x.target_sha1
                })).collect::<Vec<_>>(),
            })),
        )
            .into_response(),
        Err(e) => {
            tracing::warn!(error = %e, "sync import failed");
            (StatusCode::BAD_GATEWAY, e.to_string()).into_response()
        }
    }
}

/// `POST /workspaces/{id}/sync/push` body. `upstream_url` is required;
/// `upstream_auth` carries an optional credential (PAT/token) for private
/// upstreams; `refs` narrows the push to a subset of the workspace heads (all
/// heads when absent); `force` permits a non-fast-forward overwrite upstream.
#[derive(serde::Deserialize)]
pub struct PushRequest {
    pub upstream_url: String,
    #[serde(default)]
    pub upstream_auth: Option<String>,
    #[serde(default)]
    pub refs: Option<Vec<String>>,
    #[serde(default)]
    pub force: bool,
}

/// Push the workspace's heads to an upstream — the EXPORT half of the bridge.
///
/// Unlike [`import`], the export op-level metric is NOT recorded inside the
/// engine (it only records `record_sync_objects("export", n)`), so THIS handler
/// owns `record_sync("export", …)` — exactly once per call, never double-counted.
///
/// - 503 when `[sync]` is disabled (engine `None`) — fail-closed, like `import`.
/// - 400 when `upstream_url` is not an http(s)/file URL (cheap scheme guard).
/// - 404 when the workspace is foreign to the caller's tenant (the engine's
///   ownership check surfaces `LedgeError::NotFound`); never recorded as a sync
///   failure since no upstream I/O occurred.
/// - 200 + `{pushed:[{ref,sha1}], rejected:[{ref,reason}]}` on a reached
///   upstream. A non-fast-forward ref lands in `rejected` (HTTP still 200)
///   unless `force` was set; `result` label is `rejected` iff any ref bounced.
/// - 502 when the upstream push itself fails (network/auth/transport); the
///   error string is surfaced but the URL/credential never enter metric labels.
pub async fn push(
    State(state): State<AppState>,
    principal: crate::auth::Principal,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(req): Json<PushRequest>,
) -> Response {
    let Some(engine) = &state.sync else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let u = &req.upstream_url;
    if !(u.starts_with("https://") || u.starts_with("http://") || u.starts_with("file://")) {
        return (StatusCode::BAD_REQUEST, "upstream_url must be http(s) or file").into_response();
    }
    match engine
        .export(
            &principal.tenant_id,
            &id,
            &req.upstream_url,
            req.upstream_auth.as_deref(),
            req.refs.clone(),
            req.force,
        )
        .await
    {
        Ok(r) => {
            crate::metrics::record_sync(
                "export",
                if r.rejected.is_empty() {
                    "ok"
                } else {
                    "rejected"
                },
            );
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "pushed": r.pushed.iter().map(|x| serde_json::json!({
                        "ref": x.reference, "sha1": x.sha1
                    })).collect::<Vec<_>>(),
                    "rejected": r.rejected.iter().map(|x| serde_json::json!({
                        "ref": x.reference, "reason": x.reason
                    })).collect::<Vec<_>>(),
                })),
            )
                .into_response()
        }
        Err(e) => {
            // Foreign-workspace ownership failure ⇒ 404, NOT a sync I/O failure:
            // no upstream was contacted, so it must not pollute the failure metric.
            if matches!(e, ledge_core::LedgeError::NotFound(_)) {
                return StatusCode::NOT_FOUND.into_response();
            }
            crate::metrics::record_sync("export", "failed");
            tracing::warn!(error = %e, "sync push failed");
            (StatusCode::BAD_GATEWAY, e.to_string()).into_response()
        }
    }
}
