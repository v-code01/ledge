use crate::ObjectId;

/// A single versioned ref state atomically committed by the ref store.
///
/// # Invariants
/// - `version` starts at 1 on creation and increments by 1 on every
///   successful CAS update.  A zero version is never valid at rest.
/// - `hlc` is a Hybrid Logical Clock timestamp (wall-time component in the
///   upper 48 bits, logical counter in the lower 16 bits).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RefEntry {
    /// The object this ref currently points to.
    pub target: ObjectId,
    /// HLC timestamp of the last write, used for causal ordering.
    pub hlc: u64,
    /// Monotone write version; used for optimistic concurrency control.
    pub version: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ObjectId;

    #[test]
    fn fields() {
        let e = RefEntry {
            target: ObjectId::from_bytes([0xffu8; 32]),
            hlc: 12345,
            version: 1,
        };
        assert_eq!(e.hlc, 12345);
        assert_eq!(e.version, 1);
        assert_eq!(e.target.as_bytes(), &[0xffu8; 32]);
    }

    #[test]
    fn clone_eq() {
        let e = RefEntry {
            target: ObjectId::from_bytes([0x01u8; 32]),
            hlc: 9999,
            version: 3,
        };
        assert_eq!(e.clone(), e);
    }

    #[test]
    fn serde_roundtrip() {
        let e = RefEntry {
            target: ObjectId::from_bytes([0xabu8; 32]),
            hlc: 100,
            version: 2,
        };
        let json = serde_json::to_string(&e).unwrap();
        let back: RefEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn version_distinctness() {
        let base = RefEntry {
            target: ObjectId::from_bytes([0u8; 32]),
            hlc: 0,
            version: 1,
        };
        let next = RefEntry {
            version: 2,
            ..base.clone()
        };
        assert_ne!(base, next);
    }
}
