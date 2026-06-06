//! Authentication subsystem (Phase 4d-1): opaque API keys, a WAL-backed key
//! store, the request-classifying middleware, and the typed `Principal` every
//! handler can extract. `AuthCtx` (added in the store task) is the handle
//! `AppState` carries.

pub mod principal;
pub mod store;

pub use principal::{Principal, PrincipalKind, Scopes};
pub use store::{ApiKeyRecord, AuthStore};
