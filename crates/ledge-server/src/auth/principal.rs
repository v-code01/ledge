//! `Principal`, `Scopes`, `PrincipalKind`, and the Axum `FromRequestParts`
//! extractor (Phase 4d-1 spec §4.1).
//!
//! A `Principal` is the verified identity attached to a request by the auth
//! middleware (`super::middleware`) into the request's extensions. Handlers read
//! it through the [`Principal`] extractor. Scopes are a fixed three-capability
//! set (not free-form strings) so a permission check can never silently typo a
//! scope name.

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::StatusCode;

/// What kind of identity a credential resolves to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PrincipalKind {
    /// A human/agent API key (tenant-scoped).
    User,
    /// A node-to-node service credential (the cluster secret), or the synthetic
    /// root injected when auth is disabled.
    Service,
}

/// A fixed capability set. `admin` implies `read`+`write` (a superuser), so the
/// accessors check `admin` too — there is no way to hold `admin` without read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Scopes {
    pub read: bool,
    pub write: bool,
    pub admin: bool,
}

impl Scopes {
    /// All capabilities (admin superuser). Used by the synthetic root principal
    /// (auth disabled) and the Service principal (verified cluster secret).
    pub const ALL: Scopes = Scopes {
        read: true,
        write: true,
        admin: true,
    };
    /// True iff this principal may read (admin counts).
    pub fn can_read(&self) -> bool {
        self.read || self.admin
    }
    /// True iff this principal may write (admin counts).
    pub fn can_write(&self) -> bool {
        self.write || self.admin
    }
    /// True iff this principal holds the admin scope.
    pub fn is_admin(&self) -> bool {
        self.admin
    }
}

/// A verified request identity. Cheap to clone (small strings + Copy fields);
/// the middleware clones one into request extensions and the extractor clones it
/// back out.
#[derive(Clone, Debug)]
pub struct Principal {
    /// Tenant the credential belongs to ("acme", "root", "system"). Consumed by
    /// 4d-2 for resource scoping; 4d-1 only records it.
    pub tenant_id: String,
    /// The credential's id: the `key_id`, `"service:<node>"`, or `"root"`.
    pub principal_id: String,
    /// User (API key) or Service (cluster secret / synthetic root).
    pub kind: PrincipalKind,
    /// Capability set.
    pub scopes: Scopes,
}

impl Principal {
    /// The synthetic root principal injected when auth is disabled (spec §4.3)
    /// and the Service principal for a verified cluster secret. Full scopes,
    /// tenant `root`/`system`, kind Service.
    pub fn root() -> Self {
        Principal {
            tenant_id: "root".into(),
            principal_id: "root".into(),
            kind: PrincipalKind::Service,
            scopes: Scopes::ALL,
        }
    }

    /// The Service principal for a verified node-to-node cluster secret.
    pub fn service(node: &str) -> Self {
        Principal {
            tenant_id: "system".into(),
            principal_id: format!("service:{node}"),
            kind: PrincipalKind::Service,
            scopes: Scopes::ALL,
        }
    }
}

/// Extractor: read the `Principal` the middleware injected into request
/// extensions. Absence is a bug-guard (the middleware injects one for every
/// CLIENT/INTERNAL route) → 401, never a panic.
impl<S> FromRequestParts<S> for Principal
where
    S: Send + Sync,
{
    type Rejection = StatusCode;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<Principal>()
            .cloned()
            .ok_or(StatusCode::UNAUTHORIZED)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;

    #[test]
    fn scopes_admin_implies_read_write() {
        let admin = Scopes {
            read: false,
            write: false,
            admin: true,
        };
        assert!(admin.can_read(), "admin implies read");
        assert!(admin.can_write(), "admin implies write");
        assert!(admin.is_admin());
    }

    #[test]
    fn scopes_read_only_cannot_write_or_admin() {
        let ro = Scopes {
            read: true,
            write: false,
            admin: false,
        };
        assert!(ro.can_read());
        assert!(!ro.can_write());
        assert!(!ro.is_admin());
    }

    #[test]
    fn scopes_all_is_superuser() {
        assert!(Scopes::ALL.can_read() && Scopes::ALL.can_write() && Scopes::ALL.is_admin());
    }

    #[tokio::test]
    async fn extractor_returns_principal_from_extensions() {
        let mut req = Request::builder().body(()).unwrap();
        req.extensions_mut().insert(Principal::root());
        let (mut parts, _) = req.into_parts();
        let p = Principal::from_request_parts(&mut parts, &())
            .await
            .unwrap();
        assert_eq!(p.principal_id, "root");
        assert!(p.scopes.is_admin());
    }

    #[tokio::test]
    async fn extractor_401_when_absent() {
        let req = Request::builder().body(()).unwrap();
        let (mut parts, _) = req.into_parts();
        let err = Principal::from_request_parts(&mut parts, &())
            .await
            .unwrap_err();
        assert_eq!(err, StatusCode::UNAUTHORIZED);
    }
}
