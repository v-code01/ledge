use std::sync::Arc;

use crate::{LedgeError, Result};

/// A validated, interned ref path.
///
/// # Invariants
/// - Always starts with `refs/`.
/// - Never contains `..` (prevents path-traversal attacks).
/// - Never contains `//` (prevents ambiguous paths).
/// - Never contains an ASCII control byte (`< 0x20` or `0x7F`). Git's own
///   ref-format rules forbid these, and one of them — the NUL byte `0x00` — is
///   reserved by the ref store's radix tree as the key-exhausted sentinel, so
///   an embedded NUL would alias two distinct refs onto the same tree slot and
///   silently lose one. Rejecting control bytes here enforces that invariant at
///   the boundary.
///
/// # Cloning
/// Backed by `Arc<str>`, so `clone()` is an O(1) atomic reference-count
/// increment — no heap allocation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RefName(Arc<str>);

impl RefName {
    /// Construct a `RefName`, validating all invariants.
    ///
    /// # Errors
    /// Returns [`LedgeError::InvalidRefName`] if any invariant is violated.
    pub fn new(s: &str) -> Result<Self> {
        if !s.starts_with("refs/") {
            return Err(LedgeError::InvalidRefName(format!(
                "ref must start with 'refs/': {s:?}"
            )));
        }
        if s.contains("..") {
            return Err(LedgeError::InvalidRefName(format!(
                "ref must not contain '..': {s:?}"
            )));
        }
        if s.contains("//") {
            return Err(LedgeError::InvalidRefName(format!(
                "ref must not contain '//': {s:?}"
            )));
        }
        // Reject ASCII control bytes. The NUL byte in particular is the radix
        // tree's key-exhausted sentinel; an embedded NUL would collide two refs
        // onto one slot and lose one. The rest (other C0 controls, DEL) are
        // forbidden by git's ref-format rules and have no place in a ref path.
        if let Some(bad) = s.bytes().find(|&b| b < 0x20 || b == 0x7f) {
            return Err(LedgeError::InvalidRefName(format!(
                "ref must not contain control byte {bad:#04x}: {s:?}"
            )));
        }
        Ok(Self(Arc::from(s)))
    }

    /// Return the validated ref string as a `&str`.
    ///
    /// The returned pointer is stable for the lifetime of `self` (and any
    /// clone sharing the same `Arc`).
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RefName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Infallible — consumes the `RefName` without re-allocating.
impl From<RefName> for String {
    fn from(r: RefName) -> String {
        r.0.to_string()
    }
}

/// Used by `serde` deserialization path — validates the string.
impl TryFrom<String> for RefName {
    type Error = LedgeError;

    fn try_from(s: String) -> Result<Self> {
        Self::new(&s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_ref() {
        let r = RefName::new("refs/heads/main").expect("valid ref");
        assert_eq!(r.as_str(), "refs/heads/main");
    }

    #[test]
    fn valid_ref_agents() {
        RefName::new("refs/agents/worker-1/state").expect("valid");
    }

    #[test]
    fn valid_ref_tags() {
        RefName::new("refs/tags/v1.0.0").expect("valid");
    }

    #[test]
    fn must_start_with_refs_slash() {
        assert!(RefName::new("heads/main").is_err());
        assert!(RefName::new("refs").is_err());
        assert!(RefName::new("").is_err());
    }

    #[test]
    fn rejects_double_dot() {
        assert!(RefName::new("refs/heads/../main").is_err());
        assert!(RefName::new("refs/..").is_err());
    }

    #[test]
    fn rejects_double_slash() {
        assert!(RefName::new("refs/heads//main").is_err());
        assert!(RefName::new("refs//heads").is_err());
    }

    #[test]
    fn rejects_nul_and_control_bytes() {
        // A NUL would collide with the radix tree's key-exhausted sentinel and
        // silently alias two refs — must be rejected at construction.
        assert!(RefName::new("refs/heads/a\0b").is_err());
        assert!(RefName::new("refs/heads/\0").is_err());
        // Other C0 controls and DEL are git-forbidden too.
        assert!(RefName::new("refs/heads/a\tb").is_err());
        assert!(RefName::new("refs/heads/a\nb").is_err());
        assert!(RefName::new("refs/heads/a\x7fb").is_err());
        assert!(RefName::new("refs/heads/a\x1bb").is_err());
        // Ordinary printable refs still pass.
        assert!(RefName::new("refs/heads/feature-x.y_z").is_ok());
    }

    #[test]
    fn display() {
        let r = RefName::new("refs/heads/feature-x").unwrap();
        assert_eq!(format!("{r}"), "refs/heads/feature-x");
    }

    #[test]
    fn clone_shares_arc() {
        let r1 = RefName::new("refs/heads/main").unwrap();
        let r2 = r1.clone();
        assert_eq!(r1, r2);
        assert!(std::ptr::eq(r1.as_str().as_ptr(), r2.as_str().as_ptr()));
    }

    #[test]
    fn serde_roundtrip() {
        let r = RefName::new("refs/heads/main").unwrap();
        let json = serde_json::to_string(&r).unwrap();
        let r2: RefName = serde_json::from_str(&json).unwrap();
        assert_eq!(r, r2);
    }

    #[test]
    fn hash_equality_consistent() {
        use std::collections::HashMap;
        let r = RefName::new("refs/heads/main").unwrap();
        let mut map = HashMap::new();
        map.insert(r.clone(), 42u32);
        assert_eq!(map[&r], 42);
    }
}
