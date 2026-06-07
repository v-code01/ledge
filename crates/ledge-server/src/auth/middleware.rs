//! The single auth chokepoint (Phase 4d-1 spec §4.3): an
//! `axum::middleware::from_fn_with_state` layer inserted in `build_app` above
//! Trace/Timeout. It classifies each request PUBLIC / INTERNAL / CLIENT, verifies
//! the credential, gates `/admin/*` on the admin scope, and injects the resolved
//! `Principal` into request extensions. With auth disabled it injects a synthetic
//! root principal so every handler still extracts one (byte-identical behavior).

use axum::extract::{Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::Engine;

use crate::auth::principal::Principal;
use crate::metrics;
use crate::routes::AppState;

/// Path classification (spec §4.3, plan Reconciliation R6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Class {
    Public,
    Internal,
    Client,
}

fn classify(path: &str) -> Class {
    if path == "/healthz" || path == "/metrics" {
        Class::Public
    } else if path.starts_with("/raft/")
        || path.starts_with("/cluster/")
        || path.starts_with("/objects/")
    {
        Class::Internal
    } else {
        Class::Client
    }
}

/// Wall-clock ms (for expiry checks). The store takes `now_ms` as a parameter so
/// it stays deterministic; the middleware supplies the real clock.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Extract a Bearer token from `Authorization: Bearer <t>`.
fn bearer(headers: &header::HeaderMap) -> Option<String> {
    let v = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    v.strip_prefix("Bearer ").map(|s| s.trim().to_string())
}

/// Extract + canonicalize a token from `Authorization: Basic <b64(user:pass)>`
/// per spec §3.2 (plan Reconciliation R5): if `user` already starts with
/// `ledge_`, the token is `user` (password ignored); else the token is
/// `ledge_<user>_<pass>`.
fn basic_token(headers: &header::HeaderMap) -> Option<String> {
    let v = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let b64 = v.strip_prefix("Basic ")?.trim();
    let raw = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    let decoded = String::from_utf8(raw).ok()?;
    let (user, pass) = decoded.split_once(':').unwrap_or((decoded.as_str(), ""));
    if user.starts_with("ledge_") {
        Some(user.to_string())
    } else if !user.is_empty() {
        Some(format!("ledge_{user}_{pass}"))
    } else {
        None
    }
}

/// Any presented client token (Bearer preferred, then Basic).
fn client_token(headers: &header::HeaderMap) -> Option<String> {
    bearer(headers).or_else(|| basic_token(headers))
}

/// Constant-time string compare for the cluster secret. A length difference is
/// observable (the secret is a fixed shared service value, not per-user input),
/// but the byte comparison itself is constant-time to avoid a content-timing
/// side channel.
fn secret_matches(presented: &str, configured: &Option<String>) -> bool {
    match configured {
        Some(s) => {
            use subtle::ConstantTimeEq;
            presented.len() == s.len()
                && bool::from(presented.as_bytes().ct_eq(s.as_bytes()))
        }
        None => false,
    }
}

/// The middleware function passed to `from_fn_with_state`.
pub async fn auth_layer(State(state): State<AppState>, mut req: Request, next: Next) -> Response {
    let path = req.uri().path().to_string();
    let class = classify(&path);

    // ── Disabled mode: synthetic root, no credential checks (spec §4.3). ──────
    if !state.auth.enabled {
        if class != Class::Public {
            req.extensions_mut().insert(Principal::root());
        }
        // Admin gate is vacuous for root (is_admin == true). Pass through.
        metrics::record_auth_request("ok");
        return next.run(req).await;
    }

    // ── Enabled mode. ─────────────────────────────────────────────────────────
    let principal = match class {
        Class::Public => {
            metrics::record_auth_request("ok");
            return next.run(req).await;
        }
        Class::Internal => {
            // Require the cluster secret as a bearer token.
            match bearer(req.headers()) {
                Some(tok) if secret_matches(&tok, &state.auth.cluster_secret) => {
                    Principal::service("peer")
                }
                _ => {
                    metrics::record_auth_request("unauthenticated");
                    tracing::warn!(path = %path, reason = "internal-secret-mismatch", "auth 401");
                    return StatusCode::UNAUTHORIZED.into_response();
                }
            }
        }
        Class::Client => {
            let Some(tok) = client_token(req.headers()) else {
                metrics::record_auth_request("unauthenticated");
                tracing::warn!(path = %path, reason = "no-credential", "auth 401");
                return StatusCode::UNAUTHORIZED.into_response();
            };
            let now = now_ms();
            match state.auth.store.verify(&tok, now) {
                Some(p) => {
                    // Keep the live-key gauge fresh off the verify path (cheap
                    // read-locked count; no secret touched).
                    metrics::set_auth_keys(state.auth.store.live_count(now) as f64);
                    p
                }
                None => {
                    metrics::record_auth_request("unauthenticated");
                    tracing::warn!(path = %path, reason = "verify-failed", "auth 401");
                    return StatusCode::UNAUTHORIZED.into_response();
                }
            }
        }
    };

    // ── Admin gate (CLIENT `/admin/*` only). ──────────────────────────────────
    if class == Class::Client && path.starts_with("/admin/") && !principal.scopes.is_admin() {
        metrics::record_auth_request("forbidden");
        tracing::warn!(path = %path, principal = %principal.principal_id, "auth 403 (non-admin)");
        return StatusCode::FORBIDDEN.into_response();
    }

    metrics::record_auth_request("ok");
    req.extensions_mut().insert(principal);
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::principal::{PrincipalKind, Scopes};
    use crate::auth::store::AuthStore;
    use crate::auth::AuthCtx;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use axum::routing::get;
    use axum::Router;
    use std::sync::Arc;
    use tower::ServiceExt; // oneshot

    fn admin_scopes() -> Scopes {
        Scopes::ALL
    }
    fn ro_scopes() -> Scopes {
        Scopes {
            read: true,
            write: false,
            admin: false,
        }
    }

    /// A minimal router that mounts the middleware and echoes the extracted
    /// principal id (proves injection), reusing the workspace test AppState
    /// builder so the middleware sees a real `AppState` shape.
    async fn app_with(ctx: AuthCtx) -> Router {
        async fn whoami(p: Principal) -> String {
            p.principal_id
        }
        async fn admin_only(p: Principal) -> String {
            p.principal_id
        }
        async fn public_ok() -> &'static str {
            "ok"
        }

        let dir = tempfile::tempdir().unwrap();
        let mut state = crate::workspace_routes::test_state_for_auth(&dir);
        state.auth = ctx;
        Router::new()
            .route("/workspaces", get(whoami))
            .route("/admin/gc", axum::routing::post(admin_only))
            .route("/healthz", get(public_ok))
            .route("/cluster/gc", axum::routing::post(whoami))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_layer,
            ))
            .with_state(state)
    }

    async fn status_of(
        app: Router,
        method: &str,
        uri: &str,
        auth: Option<(&str, &str)>,
    ) -> StatusCode {
        let mut b = HttpRequest::builder().method(method).uri(uri);
        if let Some((scheme, val)) = auth {
            b = b.header(header::AUTHORIZATION, format!("{scheme} {val}"));
        }
        app.oneshot(b.body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn disabled_lets_anonymous_client_through() {
        let app = app_with(AuthCtx::disabled()).await;
        assert_eq!(
            status_of(app, "GET", "/workspaces", None).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn enabled_client_no_cred_401() {
        let store = Arc::new(AuthStore::in_memory(Arc::new(ledge_core::HLC::new())));
        let ctx = AuthCtx {
            enabled: true,
            store,
            cluster_secret: None,
        };
        let app = app_with(ctx).await;
        assert_eq!(
            status_of(app, "GET", "/workspaces", None).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn enabled_valid_bearer_200() {
        let store = Arc::new(AuthStore::in_memory(Arc::new(ledge_core::HLC::new())));
        let token = store
            .mint("acme", PrincipalKind::User, ro_scopes(), None, 0)
            .await
            .unwrap();
        let ctx = AuthCtx {
            enabled: true,
            store,
            cluster_secret: None,
        };
        let app = app_with(ctx).await;
        assert_eq!(
            status_of(app, "GET", "/workspaces", Some(("Bearer", &token))).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn enabled_valid_basic_full_token_username_200() {
        let store = Arc::new(AuthStore::in_memory(Arc::new(ledge_core::HLC::new())));
        let token = store
            .mint("acme", PrincipalKind::User, ro_scopes(), None, 0)
            .await
            .unwrap();
        // git form: username = full token, empty password.
        let b64 = base64::engine::general_purpose::STANDARD.encode(format!("{token}:"));
        let ctx = AuthCtx {
            enabled: true,
            store,
            cluster_secret: None,
        };
        let app = app_with(ctx).await;
        assert_eq!(
            status_of(app, "GET", "/workspaces", Some(("Basic", &b64))).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn enabled_valid_basic_keyid_password_split_200() {
        let store = Arc::new(AuthStore::in_memory(Arc::new(ledge_core::HLC::new())));
        let token = store
            .mint("acme", PrincipalKind::User, ro_scopes(), None, 0)
            .await
            .unwrap();
        let rest = token.strip_prefix("ledge_").unwrap();
        let (key_id, secret) = rest.split_once('_').unwrap();
        // git form: username = key_id, password = secret.
        let b64 = base64::engine::general_purpose::STANDARD.encode(format!("{key_id}:{secret}"));
        let ctx = AuthCtx {
            enabled: true,
            store,
            cluster_secret: None,
        };
        let app = app_with(ctx).await;
        assert_eq!(
            status_of(app, "GET", "/workspaces", Some(("Basic", &b64))).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn enabled_wrong_secret_unknown_key_malformed_401() {
        let store = Arc::new(AuthStore::in_memory(Arc::new(ledge_core::HLC::new())));
        let token = store
            .mint("acme", PrincipalKind::User, ro_scopes(), None, 0)
            .await
            .unwrap();
        // Same key_id, wrong secret tail.
        let (key_id, _) = token
            .strip_prefix("ledge_")
            .unwrap()
            .split_once('_')
            .unwrap();
        let wrong = format!("ledge_{key_id}_{}", "A".repeat(43));
        let ctx = AuthCtx {
            enabled: true,
            store,
            cluster_secret: None,
        };
        let app = app_with(ctx).await;
        assert_eq!(
            status_of(app.clone(), "GET", "/workspaces", Some(("Bearer", &wrong))).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status_of(
                app.clone(),
                "GET",
                "/workspaces",
                Some(("Bearer", "ledge_deadbeefdeadbeef_AAAA"))
            )
            .await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status_of(app, "GET", "/workspaces", Some(("Bearer", "not-a-token"))).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn enabled_nonadmin_on_admin_403_admin_ok() {
        let store = Arc::new(AuthStore::in_memory(Arc::new(ledge_core::HLC::new())));
        let ro = store
            .mint("acme", PrincipalKind::User, ro_scopes(), None, 0)
            .await
            .unwrap();
        let admin = store
            .mint("acme", PrincipalKind::User, admin_scopes(), None, 0)
            .await
            .unwrap();
        let ctx = AuthCtx {
            enabled: true,
            store,
            cluster_secret: None,
        };
        let app = app_with(ctx).await;
        assert_eq!(
            status_of(app.clone(), "POST", "/admin/gc", Some(("Bearer", &ro))).await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            status_of(app, "POST", "/admin/gc", Some(("Bearer", &admin))).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn enabled_internal_needs_cluster_secret() {
        let store = Arc::new(AuthStore::in_memory(Arc::new(ledge_core::HLC::new())));
        // A real client key must NOT satisfy INTERNAL (only the service secret).
        let client = store
            .mint("acme", PrincipalKind::User, admin_scopes(), None, 0)
            .await
            .unwrap();
        let ctx = AuthCtx {
            enabled: true,
            store,
            cluster_secret: Some("svc".into()),
        };
        let app = app_with(ctx).await;
        // No secret → 401.
        assert_eq!(
            status_of(app.clone(), "POST", "/cluster/gc", None).await,
            StatusCode::UNAUTHORIZED
        );
        // A client key (not the service secret) → 401.
        assert_eq!(
            status_of(app.clone(), "POST", "/cluster/gc", Some(("Bearer", &client))).await,
            StatusCode::UNAUTHORIZED
        );
        // Correct secret → 200 (the test router echoes the service principal).
        assert_eq!(
            status_of(app, "POST", "/cluster/gc", Some(("Bearer", "svc"))).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn public_always_open_even_enabled() {
        let store = Arc::new(AuthStore::in_memory(Arc::new(ledge_core::HLC::new())));
        let ctx = AuthCtx {
            enabled: true,
            store,
            cluster_secret: None,
        };
        let app = app_with(ctx).await;
        assert_eq!(
            status_of(app, "GET", "/healthz", None).await,
            StatusCode::OK
        );
    }
}
