//! Workspace control-plane REST handlers + JSON DTOs (spec §7).
//!
//! These handlers surface the [`ledge_workspace::WorkspaceManager`] lifecycle
//! (fork / list / get / renew / commit / release) and the GC admin endpoint
//! over Axum. Error mapping (spec §7): unknown id → 404; expired/tombstoned
//! `get` → 410; commit conflict → 200 with per-ref `conflict`; malformed body
//! → 400 (automatic via the `Json` extractor rejection); store error → 500.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::warn;

use ledge_core::RefName;
use ledge_workspace::{CommitOutcome, WorkspaceId, WorkspaceView};

use crate::{metrics, routes::AppState};

/// `source` accepts either a single ref string or an array of ref strings.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum StringOrVec {
    One(String),
    Many(Vec<String>),
}

impl StringOrVec {
    pub fn into_vec(self) -> Vec<String> {
        match self {
            StringOrVec::One(s) => vec![s],
            StringOrVec::Many(v) => v,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ForkRequest {
    pub source: StringOrVec,
    /// Optional. When omitted, the server falls back to `default_ttl_secs`
    /// (config `workspace.default_ttl_secs`).
    #[serde(default)]
    pub ttl_seconds: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RefDto {
    pub name: String,
    pub target_hex: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ForkResponse {
    pub id: String,
    pub expires_at_ms: u64,
    pub refs: Vec<RefDto>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkspaceSummary {
    pub id: String,
    pub expires_at_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct RenewRequest {
    pub ttl_seconds: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LeaseDto {
    pub id: String,
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
    pub generation: u64,
}

#[derive(Debug, Deserialize)]
pub struct CommitRequest {
    /// `{ "<ws-ref>": "<durable-ref>" }`. BTreeMap for deterministic ordering.
    pub mappings: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommitOutcomeDto {
    pub target: String,
    pub status: String, // "ok" | "conflict"
    pub target_hex: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkspaceViewDto {
    pub id: String,
    pub lease: LeaseDto,
    pub refs: Vec<RefDto>,
}

/// Wall-clock ms since the Unix epoch (matches Lease semantics, spec §3.3).
fn wall_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn view_to_dto(v: &WorkspaceView) -> WorkspaceViewDto {
    WorkspaceViewDto {
        id: v.id.to_hex(),
        lease: LeaseDto {
            id: v.lease.id.to_hex(),
            created_at_ms: v.lease.created_at_ms,
            expires_at_ms: v.lease.expires_at_ms,
            generation: v.lease.generation,
        },
        refs: v
            .refs
            .iter()
            .map(|(name, e)| RefDto {
                name: name.clone(),
                target_hex: e.target.to_hex(),
            })
            .collect(),
    }
}

/// Parse a workspace id from a path segment. An unparseable id is `None`,
/// which callers map to 404 (an id that could never name a workspace).
fn parse_id(id: &str) -> Option<WorkspaceId> {
    WorkspaceId::from_hex(id).ok()
}

/// Map a manager lookup error to a status: a transient cluster fault is
/// retryable → 503; tombstoned|expired → 410; unknown → 404; everything else
/// (genuine corruption / I/O) is a non-retryable server fault → 500.
fn map_lookup_err(e: ledge_core::LedgeError) -> Response {
    // A retryable cluster-availability fault must surface as 503, NOT 500, so
    // clients know to back off and retry rather than treat it as terminal.
    if matches!(e, ledge_core::LedgeError::Unavailable(_)) {
        warn!(error = %e, "workspace op unavailable (retryable)");
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let msg = e.to_string();
    if msg.contains("tombstoned") || msg.contains("expired") {
        StatusCode::GONE.into_response()
    } else if msg.contains("not found") || msg.contains("unknown") {
        StatusCode::NOT_FOUND.into_response()
    } else {
        warn!(error = %e, "workspace op failed");
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    }
}

/// Live (unexpired, non-tombstoned) workspace count, for the active gauge.
async fn live_count(state: &AppState) -> f64 {
    state
        .workspaces
        .list()
        .await
        .map(|v| v.len() as f64)
        .unwrap_or(0.0)
}

/// POST /workspaces
pub async fn create_workspace(
    State(state): State<AppState>,
    Json(req): Json<ForkRequest>,
) -> Response {
    let mut sources = Vec::new();
    for s in req.source.into_vec() {
        match RefName::new(&s) {
            Ok(r) => sources.push(r),
            Err(e) => {
                return (StatusCode::BAD_REQUEST, format!("bad source ref {s}: {e}"))
                    .into_response();
            }
        }
    }
    let ttl_secs = req.ttl_seconds.unwrap_or(state.default_ttl_secs);
    match state
        .workspaces
        .fork(&sources, Duration::from_secs(ttl_secs))
        .await
    {
        Ok(view) => {
            metrics::record_workspace_fork();
            metrics::set_workspaces_active(live_count(&state).await);
            let body = ForkResponse {
                id: view.id.to_hex(),
                expires_at_ms: view.lease.expires_at_ms,
                refs: view
                    .refs
                    .iter()
                    .map(|(name, e)| RefDto {
                        name: name.clone(),
                        target_hex: e.target.to_hex(),
                    })
                    .collect(),
            };
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(e) => {
            warn!(error = %e, "fork failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// GET /workspaces
pub async fn list_workspaces(State(state): State<AppState>) -> Response {
    match state.workspaces.list().await {
        Ok(views) => {
            let out: Vec<WorkspaceSummary> = views
                .iter()
                .map(|v| WorkspaceSummary {
                    id: v.id.to_hex(),
                    expires_at_ms: v.lease.expires_at_ms,
                })
                .collect();
            (StatusCode::OK, Json(out)).into_response()
        }
        Err(e) => {
            warn!(error = %e, "list failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// GET /workspaces/:id  — 200 view / 404 unknown / 410 expired|tombstoned
pub async fn get_workspace(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let wid = match parse_id(&id) {
        Some(w) => w,
        None => return StatusCode::NOT_FOUND.into_response(),
    };
    match state.workspaces.get(wid).await {
        Ok(Some(view)) => {
            // A returned view whose lease has expired is Gone, not Found.
            if view.lease.expires_at_ms <= wall_now_ms() {
                return StatusCode::GONE.into_response();
            }
            (StatusCode::OK, Json(view_to_dto(&view))).into_response()
        }
        // Distinguish tombstoned (lease exists, dead) from never-existed via the
        // lease store: if a tombstone is present → 410, else 404.
        Ok(None) => match state.leases.get(wid).await {
            Ok(Some(_)) => StatusCode::GONE.into_response(), // tombstoned
            _ => StatusCode::NOT_FOUND.into_response(),
        },
        Err(e) => {
            warn!(error = %e, "get failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// POST /workspaces/:id/renew
pub async fn renew_workspace(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<RenewRequest>,
) -> Response {
    let wid = match parse_id(&id) {
        Some(w) => w,
        None => return StatusCode::NOT_FOUND.into_response(),
    };
    match state
        .workspaces
        .renew(wid, Duration::from_secs(req.ttl_seconds))
        .await
    {
        Ok(lease) => (
            StatusCode::OK,
            Json(LeaseDto {
                id: lease.id.to_hex(),
                created_at_ms: lease.created_at_ms,
                expires_at_ms: lease.expires_at_ms,
                generation: lease.generation,
            }),
        )
            .into_response(),
        Err(e) => map_lookup_err(e),
    }
}

/// POST /workspaces/:id/commit
pub async fn commit_workspace(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<CommitRequest>,
) -> Response {
    let wid = match parse_id(&id) {
        Some(w) => w,
        None => return StatusCode::NOT_FOUND.into_response(),
    };
    let mut mappings = Vec::new();
    for (ws, durable) in &req.mappings {
        let ws_ref = match RefName::new(ws) {
            Ok(r) => r,
            Err(e) => {
                return (StatusCode::BAD_REQUEST, format!("bad ws ref {ws}: {e}")).into_response();
            }
        };
        let d_ref = match RefName::new(durable) {
            Ok(r) => r,
            Err(e) => {
                return (StatusCode::BAD_REQUEST, format!("bad durable ref {durable}: {e}"))
                    .into_response();
            }
        };
        mappings.push((ws_ref, d_ref));
    }
    match state.workspaces.commit(wid, &mappings).await {
        Ok(outcomes) => {
            metrics::record_workspace_commit(outcomes.len() as u64);
            let out: Vec<CommitOutcomeDto> = outcomes
                .iter()
                .map(|o| match o {
                    CommitOutcome::Ok { target, entry } => CommitOutcomeDto {
                        target: target.clone(),
                        status: "ok".into(),
                        target_hex: Some(entry.target.to_hex()),
                    },
                    CommitOutcome::Conflict { target, current } => CommitOutcomeDto {
                        target: target.clone(),
                        status: "conflict".into(),
                        target_hex: Some(current.target.to_hex()),
                    },
                })
                .collect();
            (StatusCode::OK, Json(out)).into_response()
        }
        Err(e) => map_lookup_err(e),
    }
}

/// DELETE /workspaces/:id  — idempotent release
pub async fn delete_workspace(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let wid = match parse_id(&id) {
        Some(w) => w,
        None => return StatusCode::NOT_FOUND.into_response(),
    };
    match state.workspaces.release(wid).await {
        Ok(()) => {
            metrics::record_workspace_release();
            metrics::set_workspaces_active(live_count(&state).await);
            StatusCode::OK.into_response()
        }
        Err(e) => {
            warn!(error = %e, "release failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// POST /admin/gc
pub async fn admin_gc(State(state): State<AppState>) -> Response {
    let start = std::time::Instant::now();
    match state.gc.run().await {
        Ok(stats) => {
            metrics::record_gc_run(&stats, start.elapsed());
            (StatusCode::OK, Json(stats)).into_response()
        }
        Err(e) => {
            warn!(error = %e, "gc failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_parses_single_string() {
        let v: ForkRequest =
            serde_json::from_str(r#"{"source":"refs/heads/main","ttl_seconds":3600}"#).unwrap();
        assert_eq!(v.source.into_vec(), vec!["refs/heads/main".to_string()]);
        assert_eq!(v.ttl_seconds, Some(3600));
    }

    #[test]
    fn source_parses_array() {
        let v: ForkRequest = serde_json::from_str(
            r#"{"source":["refs/heads/main","refs/tags/v1"],"ttl_seconds":60}"#,
        )
        .unwrap();
        assert_eq!(
            v.source.into_vec(),
            vec!["refs/heads/main".to_string(), "refs/tags/v1".to_string()],
        );
    }

    #[test]
    fn fork_response_roundtrips() {
        let r = ForkResponse {
            id: "abc123".into(),
            expires_at_ms: 42,
            refs: vec![RefDto {
                name: "refs/heads/main".into(),
                target_hex: "00ff".into(),
            }],
        };
        let s = serde_json::to_string(&r).unwrap();
        let back: ForkResponse = serde_json::from_str(&s).unwrap();
        assert_eq!(back.id, "abc123");
        assert_eq!(back.refs[0].name, "refs/heads/main");
    }

    #[test]
    fn commit_request_parses_mappings() {
        let v: CommitRequest = serde_json::from_str(
            r#"{"mappings":{"refs/workspaces/abc/heads/main":"refs/heads/main"}}"#,
        )
        .unwrap();
        assert_eq!(
            v.mappings
                .get("refs/workspaces/abc/heads/main")
                .map(String::as_str),
            Some("refs/heads/main"),
        );
    }

    #[test]
    fn renew_request_parses_ttl() {
        let v: RenewRequest = serde_json::from_str(r#"{"ttl_seconds":120}"#).unwrap();
        assert_eq!(v.ttl_seconds, 120);
    }

    #[test]
    fn unavailable_maps_to_503_retryable() {
        // A transient cluster fault must be retryable (503), never confused with
        // a terminal 500 or a 404/410.
        let resp = map_lookup_err(ledge_core::LedgeError::Unavailable(
            "shard Shard(0): no leader elected".into(),
        ));
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn lookup_err_status_classification() {
        use ledge_core::{LedgeError, ObjectId};
        // tombstoned|expired → 410 Gone
        assert_eq!(
            map_lookup_err(LedgeError::Corruption("ws tombstoned".into())).status(),
            StatusCode::GONE,
        );
        // unknown|not found → 404
        assert_eq!(
            map_lookup_err(LedgeError::NotFound(ObjectId::from_bytes([0u8; 32]))).status(),
            StatusCode::NOT_FOUND,
        );
        // anything else (genuine corruption) → 500, non-retryable
        assert_eq!(
            map_lookup_err(LedgeError::Corruption("bad crc".into())).status(),
            StatusCode::INTERNAL_SERVER_ERROR,
        );
    }
}

#[cfg(test)]
mod route_tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use ledge_core::HLC;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tower::ServiceExt; // oneshot

    fn test_state(dir: &TempDir) -> AppState {
        let p = dir.path().to_path_buf();
        let hlc = Arc::new(HLC::new());
        let objects = Arc::new(ledge_object_store::DiskObjectStore::new(p.clone()).unwrap());
        let refs = Arc::new(ledge_ref_store::RefStoreImpl::open(p.clone(), hlc.clone()).unwrap());
        let (workspaces, leases, gc) =
            crate::build_workspace_stack(p.clone(), objects.clone(), refs.clone(), hlc).unwrap();
        AppState {
            objects: objects.clone() as std::sync::Arc<dyn ledge_core::ObjectStore>,
            objects_disk: objects.clone(),
            refs: refs.clone() as std::sync::Arc<dyn ledge_core::RefStore>,
            workspaces,
            leases,
            gc,
            default_ttl_secs: 3600,
            data_dir: p,
            raft_shards: None,
        }
    }

    #[tokio::test]
    async fn create_then_get_workspace() {
        let dir = TempDir::new().unwrap();
        let app = crate::build_app(test_state(&dir));

        // Fork of an empty source list still yields a workspace with zero refs.
        let body = r#"{"source":[],"ttl_seconds":3600}"#;
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workspaces")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let b = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let j: serde_json::Value = serde_json::from_slice(&b).unwrap();
        let id = j["id"].as_str().unwrap().to_string();

        let resp2 = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/workspaces/{id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp2.status(), StatusCode::OK);
        let b2 = to_bytes(resp2.into_body(), usize::MAX).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&b2).unwrap();
        assert_eq!(v["id"], id);
    }

    /// POST /workspaces with no `ttl_seconds` is accepted and the lease expiry
    /// reflects the configured default (here 3600s → ~3.6e6 ms in the future).
    #[tokio::test]
    async fn create_without_ttl_uses_default() {
        let dir = TempDir::new().unwrap();
        let state = test_state(&dir);
        let default_ttl_ms = state.default_ttl_secs * 1000;
        let app = crate::build_app(state);

        let before_ms = wall_now_ms();
        let body = r#"{"source":[]}"#; // no ttl_seconds
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workspaces")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let b = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let j: serde_json::Value = serde_json::from_slice(&b).unwrap();
        let id = j["id"].as_str().unwrap().to_string();
        let expires = j["expires_at_ms"].as_u64().unwrap();
        // Expiry must be in the future, and consistent with the default TTL.
        assert!(expires > before_ms, "expiry {expires} not after {before_ms}");
        // Lower-bound: at least the default TTL out from when we started.
        assert!(
            expires >= before_ms + default_ttl_ms - 1000,
            "expiry {expires} short of default TTL window (base {before_ms}, ttl_ms {default_ttl_ms})"
        );

        // GET shows the same future expiry (200, not 410).
        let resp2 = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/workspaces/{id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp2.status(), StatusCode::OK);
        let b2 = to_bytes(resp2.into_body(), usize::MAX).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&b2).unwrap();
        assert!(v["lease"]["expires_at_ms"].as_u64().unwrap() > wall_now_ms());
    }

    /// An explicit `ttl_seconds` is honored over the default. A tiny TTL yields
    /// an expiry well below the (much larger) default window.
    #[tokio::test]
    async fn create_with_explicit_ttl_is_honored() {
        let dir = TempDir::new().unwrap();
        let app = crate::build_app(test_state(&dir));

        let before_ms = wall_now_ms();
        let body = r#"{"source":[],"ttl_seconds":60}"#;
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workspaces")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let b = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let j: serde_json::Value = serde_json::from_slice(&b).unwrap();
        let expires = j["expires_at_ms"].as_u64().unwrap();
        // 60s TTL → expiry within [before, before + ~120s], far below the 3600s default.
        assert!(expires > before_ms, "expiry {expires} not after {before_ms}");
        assert!(
            expires < before_ms + 120_000,
            "explicit 60s TTL not honored: expiry {expires} too far from base {before_ms}"
        );
    }

    #[tokio::test]
    async fn get_unknown_workspace_404() {
        let dir = TempDir::new().unwrap();
        let app = crate::build_app(test_state(&dir));
        // A syntactically valid but nonexistent id (correct hex length).
        let fake = "0".repeat(32);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/workspaces/{fake}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
