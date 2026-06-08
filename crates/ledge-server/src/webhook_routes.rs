//! Per-tenant webhook CRUD (tenant-scoped via Principal). Disabled ⇒ 503.
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use crate::routes::AppState;
use crate::webhook::{EventKind, WebhookId};

#[derive(serde::Deserialize)]
pub struct RegisterRequest {
    pub url: String,
    #[serde(default)]
    pub events: Vec<EventKind>,
}

fn wall_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push_str(&format!("{x:02x}"));
    }
    s
}

/// POST /webhooks — register a webhook for the caller's tenant. Returns the
/// secret ONCE (hex). 503 when webhooks are disabled.
pub async fn register(
    State(state): State<AppState>,
    principal: crate::auth::Principal,
    Json(req): Json<RegisterRequest>,
) -> Response {
    let Some(d) = &state.webhooks else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    if !(req.url.starts_with("http://") || req.url.starts_with("https://")) {
        return (StatusCode::BAD_REQUEST, "url must be http(s)").into_response();
    }
    match d
        .store()
        .register(&principal.tenant_id, req.url, req.events, wall_now_ms())
    {
        Ok(wh) => {
            crate::metrics::set_webhooks_registered(d.store().count() as f64);
            (
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "id": wh.id.to_hex(),
                    "secret": hex(&wh.secret),
                    "url": wh.url,
                    "events": wh.events,
                    "created_at_ms": wh.created_at_ms,
                })),
            )
                .into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// GET /webhooks — list the caller tenant's webhooks (NO secret).
pub async fn list(State(state): State<AppState>, principal: crate::auth::Principal) -> Response {
    let Some(d) = &state.webhooks else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let out: Vec<_> = d
        .store()
        .list(&principal.tenant_id)
        .into_iter()
        .map(|w| {
            serde_json::json!({
                "id": w.id.to_hex(), "url": w.url, "events": w.events,
                "created_at_ms": w.created_at_ms, "active": w.active,
            })
        })
        .collect();
    Json(out).into_response()
}

/// DELETE /webhooks/{id} — delete one of the caller tenant's webhooks. 404 if
/// unknown or foreign (no existence leak).
pub async fn delete(
    State(state): State<AppState>,
    principal: crate::auth::Principal,
    Path(id): Path<String>,
) -> Response {
    let Some(d) = &state.webhooks else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let Some(wid) = WebhookId::from_hex(&id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match d.store().delete(&principal.tenant_id, wid) {
        Ok(true) => {
            crate::metrics::set_webhooks_registered(d.store().count() as f64);
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
