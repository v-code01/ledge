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

/// Does this CLIENT request mutate state (→ needs the `write` scope)?
///
/// The default is "a non-GET method writes", with the exceptions that are
/// non-GET yet READ-ONLY and therefore must stay open to a read-only key:
/// - `git-upload-pack` — a git *fetch* is a POST but reads nothing-but-serves.
/// - the LFS `batch` negotiation — a POST that only returns transfer URLs; the
///   actual object upload (a separate `PUT`) is where the write is gated.
/// - `/rpc` — one endpoint multiplexes read and write methods inside the binary
///   body, so it is classified per-method in the RPC handler (which gates
///   `writeObject`/`fork`/`commit`/… on `can_write`), not here.
///
/// `info/refs` (ref advertisement, the fetch/push handshake) is a GET, so it is
/// already a read by the default. A misclassification here fails safe only if it
/// errs toward "write" (denies a read-only key) — erring toward "read" would let
/// a read-only key mutate — so unknown non-GET methods default to write.
fn is_client_write(method: &axum::http::Method, path: &str) -> bool {
    if path.ends_with("/git-upload-pack")
        || path.ends_with("/info/lfs/objects/batch")
        || path == "/rpc"
    {
        return false;
    }
    !matches!(
        *method,
        axum::http::Method::GET | axum::http::Method::HEAD | axum::http::Method::OPTIONS
    )
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
            presented.len() == s.len() && bool::from(presented.as_bytes().ct_eq(s.as_bytes()))
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

    // ── Write gate (CLIENT mutating routes). ───────────────────────────────────
    // A `read`-only key may fetch/list/read but must NOT push, fork, commit,
    // register a webhook, or otherwise mutate. `admin` implies `write`, so an
    // admin key passes. `/rpc` is exempt here and gated per-method in its handler
    // (the read/write split is inside the binary body). Disabled mode and the
    // Service principal (cluster secret) both hold ALL scopes, so neither is
    // affected.
    if class == Class::Client
        && is_client_write(req.method(), &path)
        && !principal.scopes.can_write()
    {
        metrics::record_auth_request("forbidden");
        tracing::warn!(path = %path, principal = %principal.principal_id, "auth 403 (write scope required)");
        return StatusCode::FORBIDDEN.into_response();
    }

    // ── Rate quota (Phase 4d-3): CLIENT requests only, post-principal (tenant
    //    known), pre-next.run. Enforced only when quotas enabled AND tenant != root
    //    (R Q7); root/disabled bypass via `enforced_for`. A deny is a 429. The
    //    clock is injected inside `check_now` (Instant::now) so the limiter stays
    //    deterministic-testable in isolation (Task 4.1). ───────────────────────────
    if class == Class::Client
        && state.quota.limits.enforced_for(&principal.tenant_id)
        && !state.quota.rate.check_now(&principal.tenant_id)
    {
        metrics::record_quota_denied("requests");
        tracing::warn!(path = %path, tenant = %principal.tenant_id, "quota 429 (rate)");
        return StatusCode::TOO_MANY_REQUESTS.into_response();
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
    fn rw_scopes() -> Scopes {
        Scopes {
            read: true,
            write: true,
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
            .route("/workspaces", get(whoami).post(whoami))
            .route("/admin/gc", axum::routing::post(admin_only))
            .route("/healthz", get(public_ok))
            .route("/cluster/gc", axum::routing::post(whoami))
            // Git write (push) and read (fetch) routes for the write-gate tests.
            .route("/repo/git-receive-pack", axum::routing::post(whoami))
            .route("/repo/git-upload-pack", axum::routing::post(whoami))
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

    /// Build an enabled-auth app plus a token minted with `scopes`.
    async fn app_and_token(scopes: Scopes) -> (Router, String) {
        let store = Arc::new(AuthStore::in_memory(Arc::new(ledge_core::HLC::new())));
        let token = store
            .mint("acme", PrincipalKind::User, scopes, None, 0)
            .await
            .unwrap();
        let ctx = AuthCtx {
            enabled: true,
            store,
            cluster_secret: None,
        };
        (app_with(ctx).await, token)
    }

    /// A read-only key must be able to READ but must be denied every WRITE. This
    /// is the whole point of a `read` scope: before this gate, `write` was minted
    /// into keys but never checked, so a read-only key could push, fork, commit,
    /// register webhooks — anything. `admin` implies `write`, so it is unaffected.
    #[tokio::test]
    async fn read_only_key_is_denied_writes_but_allowed_reads() {
        let (app, tok) = app_and_token(ro_scopes()).await;
        let auth = Some(("Bearer", tok.as_str()));

        // Reads pass.
        assert_eq!(
            status_of(app.clone(), "GET", "/workspaces", auth).await,
            StatusCode::OK,
            "read-only key may list workspaces (GET)"
        );
        // A git fetch is a POST but a READ — must stay open.
        assert_eq!(
            status_of(app.clone(), "POST", "/repo/git-upload-pack", auth).await,
            StatusCode::OK,
            "read-only key may fetch (git-upload-pack)"
        );

        // Writes are denied.
        assert_eq!(
            status_of(app.clone(), "POST", "/workspaces", auth).await,
            StatusCode::FORBIDDEN,
            "read-only key must NOT create a workspace (POST)"
        );
        assert_eq!(
            status_of(app.clone(), "DELETE", "/workspaces", auth).await,
            StatusCode::FORBIDDEN,
            "read-only key must NOT delete (DELETE)"
        );
        assert_eq!(
            status_of(app, "POST", "/repo/git-receive-pack", auth).await,
            StatusCode::FORBIDDEN,
            "read-only key must NOT push (git-receive-pack)"
        );
    }

    /// A read+write (non-admin) key passes the write gate on the same routes —
    /// the gate rejects the missing scope, not the method.
    #[tokio::test]
    async fn write_key_passes_the_write_gate() {
        let (app, tok) = app_and_token(rw_scopes()).await;
        let auth = Some(("Bearer", tok.as_str()));
        assert_eq!(
            status_of(app.clone(), "POST", "/workspaces", auth).await,
            StatusCode::OK,
            "a write-scoped key may create a workspace"
        );
        assert_eq!(
            status_of(app, "POST", "/repo/git-receive-pack", auth).await,
            StatusCode::OK,
            "a write-scoped key may push"
        );
    }

    /// An admin key implies write, so it passes the write gate too (and is the
    /// only key that also passes the `/admin/*` gate).
    #[tokio::test]
    async fn admin_key_passes_the_write_gate() {
        let (app, tok) = app_and_token(admin_scopes()).await;
        let auth = Some(("Bearer", tok.as_str()));
        assert_eq!(
            status_of(app, "POST", "/workspaces", auth).await,
            StatusCode::OK,
            "admin implies write"
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
            status_of(
                app.clone(),
                "POST",
                "/cluster/gc",
                Some(("Bearer", &client))
            )
            .await,
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

    /// A router like `app_with` but with an ENABLED rate quota (rate/burst) and a
    /// minted tenant key, so we can drive the middleware's 429 path. `/healthz`
    /// is mounted to assert PUBLIC routes are never rate-limited.
    async fn app_with_rate(rate: u32, burst: u32) -> (Router, String) {
        async fn whoami(p: Principal) -> String {
            p.principal_id
        }
        async fn public_ok() -> &'static str {
            "ok"
        }
        let dir = tempfile::tempdir().unwrap();
        let mut state = crate::workspace_routes::test_state_for_auth(&dir);
        let store = Arc::new(AuthStore::in_memory(Arc::new(ledge_core::HLC::new())));
        let token = store
            .mint("acme", PrincipalKind::User, Scopes::ALL, None, 0)
            .await
            .unwrap();
        state.auth = AuthCtx {
            enabled: true,
            store,
            cluster_secret: None,
        };
        state.quota = crate::quota::QuotaCtx {
            limits: ledge_workspace::QuotaLimits {
                enabled: true,
                ..Default::default()
            },
            usage: std::sync::Arc::new(ledge_workspace::UsageMap::default()),
            rate: std::sync::Arc::new(crate::quota::rate::TenantRateLimiter::new(
                Some(rate),
                Some(burst),
            )),
        };
        let app = Router::new()
            .route("/workspaces", get(whoami))
            .route("/healthz", get(public_ok))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_layer,
            ))
            .with_state(state);
        (app, token)
    }

    #[tokio::test]
    async fn enabled_rate_quota_429_after_burst() {
        // burst=2 ⇒ first two CLIENT requests pass, the third is 429.
        let (app, token) = app_with_rate(1, 2).await;
        assert_eq!(
            status_of(app.clone(), "GET", "/workspaces", Some(("Bearer", &token))).await,
            StatusCode::OK
        );
        assert_eq!(
            status_of(app.clone(), "GET", "/workspaces", Some(("Bearer", &token))).await,
            StatusCode::OK
        );
        assert_eq!(
            status_of(app, "GET", "/workspaces", Some(("Bearer", &token))).await,
            StatusCode::TOO_MANY_REQUESTS,
            "third request (burst exhausted) must be 429",
        );
    }

    #[tokio::test]
    async fn public_route_never_rate_limited() {
        // /healthz is PUBLIC: even with the bucket exhausted, it must stay 200.
        let (app, token) = app_with_rate(1, 1).await;
        // Exhaust the CLIENT bucket.
        assert_eq!(
            status_of(app.clone(), "GET", "/workspaces", Some(("Bearer", &token))).await,
            StatusCode::OK
        );
        assert_eq!(
            status_of(app.clone(), "GET", "/workspaces", Some(("Bearer", &token))).await,
            StatusCode::TOO_MANY_REQUESTS
        );
        // PUBLIC route is unaffected by the rate limiter, repeatedly.
        for _ in 0..20 {
            assert_eq!(
                status_of(app.clone(), "GET", "/healthz", None).await,
                StatusCode::OK,
                "PUBLIC route must never be rate-limited",
            );
        }
    }

    #[tokio::test]
    async fn disabled_quota_never_rate_limits() {
        // Auth ENABLED but the rate limiter unlimited (default QuotaCtx) ⇒ no 429
        // ever, even under a flood. (The default disabled() limits ⇒ enforced_for
        // is false ⇒ the rate check is bypassed entirely.)
        let store = Arc::new(AuthStore::in_memory(Arc::new(ledge_core::HLC::new())));
        let token = store
            .mint("acme", PrincipalKind::User, Scopes::ALL, None, 0)
            .await
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let mut state = crate::workspace_routes::test_state_for_auth(&dir);
        state.auth = AuthCtx {
            enabled: true,
            store,
            cluster_secret: None,
        };
        // state.quota stays QuotaCtx::disabled() (from test_state_for_auth).
        async fn whoami(p: Principal) -> String {
            p.principal_id
        }
        let app = Router::new()
            .route("/workspaces", get(whoami))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_layer,
            ))
            .with_state(state);
        for _ in 0..50 {
            assert_eq!(
                status_of(app.clone(), "GET", "/workspaces", Some(("Bearer", &token))).await,
                StatusCode::OK,
                "disabled quota must never 429",
            );
        }
    }
}
