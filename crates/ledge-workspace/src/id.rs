use std::sync::atomic::{AtomicU64, Ordering};

use ledge_core::{LedgeError, Result};

/// Monotonic per-process counter feeding the low 64 bits of a `WorkspaceId`.
/// `Relaxed` is sufficient: we only need uniqueness, not cross-thread ordering
/// of the counter relative to other memory — `fetch_add` is atomic, so no two
/// calls observe the same value.
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// 128-bit workspace identifier, rendered as 32-char lowercase hex.
///
/// Generated monotonically from an HLC tick (high 64 bits, big-endian) plus a
/// per-process counter (low 64 bits, big-endian) — collision-free without a
/// CSPRNG dependency. Big-endian layout makes hex lexicographic order match
/// numeric order.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, serde::Serialize, serde::Deserialize)]
pub struct WorkspaceId([u8; 16]);

impl WorkspaceId {
    /// Generate a fresh, process-unique id. High 8 bytes = `hlc.tick()`,
    /// low 8 bytes = a monotonic counter. Two calls in the same millisecond
    /// (same wall component) still differ: `tick()` is strictly increasing and
    /// the counter is independent.
    ///
    /// Complexity: O(1) (one HLC CAS loop iteration + one atomic fetch_add).
    pub fn generate(hlc: &ledge_core::HLC) -> Self {
        let high = hlc.tick().to_be_bytes();
        let low = COUNTER.fetch_add(1, Ordering::Relaxed).to_be_bytes();
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&high);
        bytes[8..].copy_from_slice(&low);
        WorkspaceId(bytes)
    }

    /// Lowercase 32-char hex rendering of the 16 raw bytes.
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(32);
        for b in &self.0 {
            // {:02x} = zero-padded lowercase hex, exactly 2 chars per byte.
            use std::fmt::Write;
            let _ = write!(s, "{b:02x}");
        }
        s
    }

    /// Parse a 32-char lowercase-or-uppercase hex string back into a `WorkspaceId`.
    ///
    /// # Errors
    /// `LedgeError::Corruption` if the length is not 32 or any pair is not valid hex.
    pub fn from_hex(s: &str) -> Result<Self> {
        if s.len() != 32 {
            return Err(LedgeError::Corruption(format!(
                "WorkspaceId hex must be 32 chars, got {}",
                s.len()
            )));
        }
        let mut bytes = [0u8; 16];
        for (i, b) in bytes.iter_mut().enumerate() {
            let hi = s.as_bytes()[i * 2];
            let lo = s.as_bytes()[i * 2 + 1];
            *b = hex_pair(hi, lo)
                .ok_or_else(|| LedgeError::Corruption(format!("invalid WorkspaceId hex '{s}'")))?;
        }
        Ok(WorkspaceId(bytes))
    }

    /// Borrow the raw 16-byte representation.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Construct directly from raw bytes (stable wire/replication form).
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        WorkspaceId(bytes)
    }
}

/// Decode two ASCII hex digits into one byte, or `None` if either is non-hex.
#[inline]
fn hex_pair(hi: u8, lo: u8) -> Option<u8> {
    fn nib(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    }
    Some((nib(hi)? << 4) | nib(lo)?)
}

impl std::fmt::Display for WorkspaceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledge_core::HLC;

    #[test]
    fn generate_produces_distinct_ids() {
        let hlc = HLC::new();
        let a = WorkspaceId::generate(&hlc);
        let b = WorkspaceId::generate(&hlc);
        let c = WorkspaceId::generate(&hlc);
        assert_ne!(a, b);
        assert_ne!(b, c);
        assert_ne!(a, c);
    }

    #[test]
    fn hex_is_32_lowercase_chars() {
        let hlc = HLC::new();
        let id = WorkspaceId::generate(&hlc);
        let h = id.to_hex();
        assert_eq!(h.len(), 32);
        assert!(h
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn hex_round_trip() {
        let hlc = HLC::new();
        let id = WorkspaceId::generate(&hlc);
        let back = WorkspaceId::from_hex(&id.to_hex()).unwrap();
        assert_eq!(id, back);
        assert_eq!(id.as_bytes(), back.as_bytes());
    }

    #[test]
    fn display_equals_to_hex() {
        let hlc = HLC::new();
        let id = WorkspaceId::generate(&hlc);
        assert_eq!(format!("{id}"), id.to_hex());
    }

    #[test]
    fn from_hex_rejects_bad_input() {
        assert!(WorkspaceId::from_hex("xyz").is_err()); // too short + non-hex
        assert!(WorkspaceId::from_hex(&"0".repeat(31)).is_err()); // wrong length (31)
        assert!(WorkspaceId::from_hex(&"0".repeat(33)).is_err()); // wrong length (33)
        assert!(WorkspaceId::from_hex(&"g".repeat(32)).is_err()); // right length, non-hex
    }

    #[test]
    fn same_hlc_ids_differ_via_counter() {
        // Even if two ids share the same HLC tick value in the high bits,
        // the monotonic counter in the low bits keeps them distinct.
        let hlc = HLC::new();
        let mut ids = std::collections::HashSet::new();
        for _ in 0..1000 {
            assert!(ids.insert(WorkspaceId::generate(&hlc)), "duplicate id");
        }
        assert_eq!(ids.len(), 1000);
    }

    proptest::proptest! {
        /// WorkspaceId hex round-trips for any generated id. `seed` only varies
        /// the iteration count; each generate() yields a fresh HLC+counter id,
        /// exercising many distinct bit patterns across the proptest run.
        #[test]
        fn prop_workspace_id_hex_roundtrip(seed in 0u64..100_000) {
            let hlc = ledge_core::HLC::new();
            let id = WorkspaceId::generate(&hlc);
            let _ = seed; // seed varies the proptest iterations
            let hex = id.to_hex();
            let back = WorkspaceId::from_hex(&hex).unwrap();
            proptest::prop_assert_eq!(id, back);
        }
    }
}
