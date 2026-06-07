//! Phase 4d-1 security matrix (spec §7) over the REAL `build_app` router with
//! auth ENABLED. This drives the production router + handlers end-to-end via
//! `tower::ServiceExt::oneshot`, complementing the middleware UNIT tests in
//! `auth::middleware::tests` (which exercise the layer over a minimal router).
//!
//! Coverage map (spec §7 items):
//!   1  no credential on a CLIENT route        → 401   (matrix_1)
//!   2  malformed / unknown key                → 401   (matrix_2)
//!   3  valid Bearer                           → 200   (matrix_3)
//!   4  valid Basic, both git forms            → 200   (matrix_4)
//!   5  wrong secret (same key_id)             → 401   (matrix_5)
//!   6  revoked key                            → 401   (matrix_6_revoked)
//!   6  expired key                            → 401   (matrix_6_expired)
//!   7  admin gate on /admin/gc AND /admin/snapshot (non-admin 403, admin allowed)
//!   8  internal /cluster/gc: no secret / wrong secret → 401; correct → clears auth
//!   9  internal /cluster/gc with a CLIENT api key     → 401
//!  12  backward-compat: auth DISABLED ⇒ CLIENT no-header → 200 (synthetic root)
//!  14  public /healthz + /metrics open even when auth ENABLED
//!
//! Items 10-11 (store durability/compaction) are `auth::store::tests` (Task 2);
//! item 13 (CLI mint/revoke/list) is `cli::tests` (Task 7). The auth-DISABLED
//! equivalence (item 12, full surface) is the rest of the suite — every other
//! test uses `AuthCtx::disabled()` and stays byte-identical green.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use base64::Engine;
use ledge_core::HLC;
use ledge_server::auth::principal::{PrincipalKind, Scopes};
use ledge_server::auth::store::AuthStore;
use ledge_server::auth::AuthCtx;
use ledge_server::{build_app, AppState};
use tempfile::TempDir;
use tower::ServiceExt; // oneshot

/// Read-only scopes (no admin) for the non-admin principal.
fn ro_scopes() -> Scopes {
    Scopes {
        read: true,
        write: false,
        admin: false,
    }
}

/// Build a real `AppState` over `dir`, with `auth` set from `ctx_builder`.
/// `ctx_builder` receives the populated store + the cluster secret so a test can
/// flip `enabled` or drop the secret while reusing the same minted keys.
async fn app_with_store(
    dir: &TempDir,
    store: Arc<AuthStore>,
    cluster_secret: Option<String>,
    enabled: bool,
) -> axum::Router {
    let p = dir.path().to_path_buf();
    let hlc = Arc::new(HLC::new());
    let objects = Arc::new(ledge_object_store::DiskObjectStore::new(p.clone()).unwrap());
    let refs = Arc::new(ledge_ref_store::RefStoreImpl::open(p.clone(), hlc.clone()).unwrap());
    let (workspaces, leases, gc) = ledge_server::build_workspace_stack(
        p.clone(),
        objects.clone(),
        refs.clone(),
        hlc.clone(),
        ledge_workspace::QuotaLimits::default(),
        std::sync::Arc::new(ledge_workspace::UsageMap::default()),
    )
    .unwrap();
    let auth = AuthCtx::new(enabled, store, cluster_secret);
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
        shard_map: None,
        cluster_gc: None,
        auth,
        quota: ledge_server::quota::QuotaCtx::disabled(),
    };
    build_app(state)
}

/// Build a real AppState with auth ENABLED over a populated store.
/// Returns (app, store, admin_token, ro_token). The cluster secret is
/// `"svc-secret"`. Tokens are minted with `now_ms = 0` and never expire.
async fn enabled_app(dir: &TempDir) -> (axum::Router, Arc<AuthStore>, String, String) {
    let store = Arc::new(AuthStore::open(dir.path().to_path_buf(), Arc::new(HLC::new())).unwrap());
    let admin = store
        .mint("root", PrincipalKind::User, Scopes::ALL, None, 0)
        .await
        .unwrap();
    let ro = store
        .mint("acme", PrincipalKind::User, ro_scopes(), None, 0)
        .await
        .unwrap();
    let app = app_with_store(dir, store.clone(), Some("svc-secret".into()), true).await;
    (app, store, admin, ro)
}

/// Drive one request and return its status. `hdr` is an optional
/// `(scheme, value)` that becomes `Authorization: <scheme> <value>`.
async fn status(
    app: axum::Router,
    method: &str,
    uri: &str,
    hdr: Option<(&str, String)>,
    body: &str,
) -> StatusCode {
    let mut b = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some((scheme, val)) = hdr {
        b = b.header(header::AUTHORIZATION, format!("{scheme} {val}"));
    }
    app.oneshot(b.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap()
        .status()
}

// ── Item 1: no credential on a CLIENT route → 401. ──────────────────────────
#[tokio::test]
async fn matrix_1_no_cred_client_401() {
    let dir = TempDir::new().unwrap();
    let (app, _store, _admin, _ro) = enabled_app(&dir).await;
    assert_eq!(
        status(app, "GET", "/workspaces", None, "").await,
        StatusCode::UNAUTHORIZED
    );
}

// ── Item 2: malformed / unknown key → 401. ──────────────────────────────────
#[tokio::test]
async fn matrix_2_malformed_and_unknown_401() {
    let dir = TempDir::new().unwrap();
    let (app, _store, _admin, _ro) = enabled_app(&dir).await;
    // Not a `ledge_` token at all.
    assert_eq!(
        status(
            app.clone(),
            "GET",
            "/workspaces",
            Some(("Bearer", "not-a-token".into())),
            ""
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
    // Well-formed shape but an unknown key_id.
    assert_eq!(
        status(
            app,
            "GET",
            "/workspaces",
            Some(("Bearer", "ledge_deadbeefdeadbeef_AAAA".into())),
            ""
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
}

// ── Item 3: valid Bearer → 200. ─────────────────────────────────────────────
#[tokio::test]
async fn matrix_3_valid_bearer_200() {
    let dir = TempDir::new().unwrap();
    let (app, _store, _admin, ro) = enabled_app(&dir).await;
    assert_eq!(
        status(app, "GET", "/workspaces", Some(("Bearer", ro)), "").await,
        StatusCode::OK
    );
}

// ── Item 4: valid Basic, BOTH git forms → 200. ──────────────────────────────
#[tokio::test]
async fn matrix_4_valid_basic_both_git_forms_200() {
    let dir = TempDir::new().unwrap();
    let (app, _store, _admin, ro) = enabled_app(&dir).await;
    // Form A: username = full token, empty password.
    let full = base64::engine::general_purpose::STANDARD.encode(format!("{ro}:"));
    assert_eq!(
        status(app.clone(), "GET", "/workspaces", Some(("Basic", full)), "").await,
        StatusCode::OK
    );
    // Form B: username = key_id, password = secret.
    let rest = ro.strip_prefix("ledge_").unwrap();
    let (key_id, secret) = rest.split_once('_').unwrap();
    let split = base64::engine::general_purpose::STANDARD.encode(format!("{key_id}:{secret}"));
    assert_eq!(
        status(app, "GET", "/workspaces", Some(("Basic", split)), "").await,
        StatusCode::OK
    );
}

// ── Item 5: wrong secret (same key_id) → 401. ───────────────────────────────
#[tokio::test]
async fn matrix_5_wrong_secret_401() {
    let dir = TempDir::new().unwrap();
    let (app, _store, _admin, ro) = enabled_app(&dir).await;
    let (kid, _) = ro.strip_prefix("ledge_").unwrap().split_once('_').unwrap();
    let wrong = format!("ledge_{kid}_{}", "A".repeat(43));
    assert_eq!(
        status(app, "GET", "/workspaces", Some(("Bearer", wrong)), "").await,
        StatusCode::UNAUTHORIZED
    );
}

// ── Item 6: revoked key → 401 (end-to-end through the real router). ──────────
#[tokio::test]
async fn matrix_6_revoked_key_401() {
    let dir = TempDir::new().unwrap();
    let (app, store, _admin, ro) = enabled_app(&dir).await;
    // The key works before revocation.
    assert_eq!(
        status(app.clone(), "GET", "/workspaces", Some(("Bearer", ro.clone())), "").await,
        StatusCode::OK
    );
    // Revoke it, then the SAME app (shared Arc<AuthStore>) must reject it.
    let (kid, _) = ro.strip_prefix("ledge_").unwrap().split_once('_').unwrap();
    assert!(store.revoke(kid).await.unwrap(), "revoke present key");
    assert_eq!(
        status(app, "GET", "/workspaces", Some(("Bearer", ro)), "").await,
        StatusCode::UNAUTHORIZED
    );
}

// ── Item 6: expired key → 401 (end-to-end). The token's TTL has already passed
//    by wall-clock time, so the middleware's real-clock expiry check rejects it. ─
#[tokio::test]
async fn matrix_6_expired_key_401() {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(AuthStore::open(dir.path().to_path_buf(), Arc::new(HLC::new())).unwrap());
    // Mint with now_ms anchored at epoch and a 1ms TTL ⇒ expires_at_ms = 1,
    // which is far in the past relative to the middleware's wall clock.
    let expired = store
        .mint(
            "acme",
            PrincipalKind::User,
            ro_scopes(),
            Some(Duration::from_millis(1)),
            0,
        )
        .await
        .unwrap();
    let app = app_with_store(&dir, store, Some("svc-secret".into()), true).await;
    assert_eq!(
        status(app, "GET", "/workspaces", Some(("Bearer", expired)), "").await,
        StatusCode::UNAUTHORIZED
    );
}

// ── Item 7: admin gate on /admin/gc AND /admin/snapshot. ────────────────────
#[tokio::test]
async fn matrix_7_admin_gate_gc_and_snapshot() {
    let dir = TempDir::new().unwrap();
    let (app, _store, admin, ro) = enabled_app(&dir).await;
    // The admin gate fires in the MIDDLEWARE, before the handler's body
    // extractor runs, so a non-admin is rejected 403 regardless of body shape.
    let snap_body = serde_json::json!({
        "dest": dir.path().join("snap-dest").to_string_lossy()
    })
    .to_string();
    // Non-admin → 403 on BOTH admin routes.
    assert_eq!(
        status(app.clone(), "POST", "/admin/gc", Some(("Bearer", ro.clone())), "").await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        status(
            app.clone(),
            "POST",
            "/admin/snapshot",
            Some(("Bearer", ro)),
            &snap_body
        )
        .await,
        StatusCode::FORBIDDEN
    );
    // Admin → cleared by the gate on BOTH; gc runs (200) and snapshot CoW-clones
    // the data dir to a fresh dest (200). Neither may be a 401/403.
    assert_eq!(
        status(app.clone(), "POST", "/admin/gc", Some(("Bearer", admin.clone())), "").await,
        StatusCode::OK
    );
    let snap = status(
        app,
        "POST",
        "/admin/snapshot",
        Some(("Bearer", admin)),
        &snap_body,
    )
    .await;
    assert_ne!(snap, StatusCode::UNAUTHORIZED, "admin clears auth on snapshot");
    assert_ne!(snap, StatusCode::FORBIDDEN, "admin clears the admin gate");
}

// ── Items 8-9: internal /cluster/gc requires the cluster secret only. ───────
#[tokio::test]
async fn matrix_8_9_internal_route_secret_only() {
    let dir = TempDir::new().unwrap();
    let (app, _store, admin, _ro) = enabled_app(&dir).await;
    // No secret → 401.
    assert_eq!(
        status(app.clone(), "POST", "/cluster/gc", None, "").await,
        StatusCode::UNAUTHORIZED
    );
    // Wrong secret → 401.
    assert_eq!(
        status(
            app.clone(),
            "POST",
            "/cluster/gc",
            Some(("Bearer", "wrong-secret".into())),
            ""
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
    // A CLIENT admin key (not the service secret) → 401 (item 9).
    assert_eq!(
        status(app.clone(), "POST", "/cluster/gc", Some(("Bearer", admin)), "").await,
        StatusCode::UNAUTHORIZED
    );
    // Correct secret → clears the middleware (handler then 503s single-node,
    // which is NOT a 401/403 — the assertion is the auth decision, not the
    // single-node cluster outcome).
    let s = status(
        app,
        "POST",
        "/cluster/gc",
        Some(("Bearer", "svc-secret".into())),
        "",
    )
    .await;
    assert_ne!(s, StatusCode::UNAUTHORIZED, "correct secret must clear auth");
    assert_ne!(s, StatusCode::FORBIDDEN);
}

// ── Item 12: backward-compat — auth DISABLED ⇒ CLIENT no-header → 200, with the
//    REAL build_app router (synthetic root injected). ─────────────────────────
#[tokio::test]
async fn matrix_12_disabled_client_no_header_200() {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(AuthStore::in_memory(Arc::new(HLC::new())));
    let app = app_with_store(&dir, store, None, false).await;
    assert_eq!(
        status(app, "GET", "/workspaces", None, "").await,
        StatusCode::OK
    );
}

// ── Item 14: public /healthz + /metrics open even when auth ENABLED. ────────
#[tokio::test]
async fn matrix_14_public_open_when_enabled() {
    let dir = TempDir::new().unwrap();
    let (app, _store, _admin, _ro) = enabled_app(&dir).await;
    assert_eq!(
        status(app.clone(), "GET", "/healthz", None, "").await,
        StatusCode::OK
    );
    assert_eq!(
        status(app, "GET", "/metrics", None, "").await,
        StatusCode::OK
    );
}

// ── Metrics finalization (spec §8): assert `ledge_auth_requests_total{result}`
//    advances on the ok / unauthenticated / forbidden paths exactly once each
//    per request, driving the REAL middleware. The recorder is a thread-local
//    `DebuggingRecorder` installed on a `current_thread` runtime so every
//    `oneshot` future emits onto the captured thread (mirrors the
//    `ledge-cluster` txn_metrics pattern). ──────────────────────────────────────
#[tokio::test(flavor = "current_thread")]
async fn auth_metrics_counter_advances_per_result() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use metrics_util::{CompositeKey, MetricKind};

    /// Sum of the `ledge_auth_requests_total` counter for one `result` label.
    fn result_count(snap: &[(CompositeKey, DebugValue)], result: &str) -> u64 {
        snap.iter()
            .filter_map(|(ck, v)| {
                if ck.kind() != MetricKind::Counter
                    || ck.key().name() != "ledge_auth_requests_total"
                {
                    return None;
                }
                let has = ck
                    .key()
                    .labels()
                    .any(|l| l.key() == "result" && l.value() == result);
                match (has, v) {
                    (true, DebugValue::Counter(c)) => Some(*c),
                    _ => None,
                }
            })
            .sum()
    }

    let dir = TempDir::new().unwrap();
    let (app, _store, _admin, ro) = enabled_app(&dir).await;

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);

    // 1 `ok`: a valid read-only Bearer on a CLIENT route.
    assert_eq!(
        status(app.clone(), "GET", "/workspaces", Some(("Bearer", ro.clone())), "").await,
        StatusCode::OK
    );
    // 1 `unauthenticated`: no credential on a CLIENT route → 401.
    assert_eq!(
        status(app.clone(), "GET", "/workspaces", None, "").await,
        StatusCode::UNAUTHORIZED
    );
    // 1 `forbidden`: the non-admin on an /admin route → 403.
    assert_eq!(
        status(app, "POST", "/admin/gc", Some(("Bearer", ro)), "").await,
        StatusCode::FORBIDDEN
    );

    let snap: Vec<(CompositeKey, DebugValue)> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .map(|(ck, _u, _d, v)| (ck, v))
        .collect();

    assert_eq!(result_count(&snap, "ok"), 1, "one ok request");
    assert_eq!(
        result_count(&snap, "unauthenticated"),
        1,
        "one unauthenticated request"
    );
    assert_eq!(result_count(&snap, "forbidden"), 1, "one forbidden request");
}
