//! Authentication subsystem (Phase 4d-1): opaque API keys, a WAL-backed key
//! store, the request-classifying middleware, and the typed `Principal` every
//! handler can extract. `AuthCtx` (added in the store task) is the handle
//! `AppState` carries.

pub mod principal;
pub mod store;

pub use principal::{Principal, PrincipalKind, Scopes};
pub use store::{ApiKeyRecord, AuthStore};

pub mod middleware; // created in Task 4; declared here so AppState can reference it later

use std::sync::Arc;

/// The auth handle carried by `AppState` (spec §4.4; one field, not loose
/// fields — plan Reconciliation R1). Cheap to clone (Arc + bool + Option).
#[derive(Clone)]
pub struct AuthCtx {
    /// Whether credentials are required. False ⇒ synthetic-root pass-through.
    pub enabled: bool,
    /// The API-key store (verify/mint/list). In-memory when disabled.
    pub store: Arc<store::AuthStore>,
    /// Shared node-to-node bearer secret for INTERNAL routes; `None` ⇒ INTERNAL
    /// allowed unconditionally (disabled mode) or no peers configured.
    pub cluster_secret: Option<String>,
}

impl AuthCtx {
    /// The disabled context: an in-memory store, no cluster secret. Used by all
    /// existing tests and single-node dev (back-compat — plan Reconciliation R8).
    pub fn disabled() -> Self {
        AuthCtx {
            enabled: false,
            store: Arc::new(store::AuthStore::in_memory(Arc::new(ledge_core::HLC::new()))),
            cluster_secret: None,
        }
    }

    /// The enabled context: a real opened store + optional node-to-node secret.
    /// Built by `main.rs` when `[auth] enabled=true`.
    pub fn new(enabled: bool, store: Arc<store::AuthStore>, cluster_secret: Option<String>) -> Self {
        AuthCtx { enabled, store, cluster_secret }
    }
}
