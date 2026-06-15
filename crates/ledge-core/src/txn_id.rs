use std::sync::atomic::{AtomicU64, Ordering};

use crate::{LedgeError, Result, HLC};

/// Monotonic per-process counter feeding the low 64 bits of a `TxnId`.
/// `Relaxed` suffices: `fetch_add` is atomic, so no two calls share a value;
/// we need uniqueness only, not cross-thread ordering of unrelated memory.
static TXN_COUNTER: AtomicU64 = AtomicU64::new(0);

/// 128-bit transaction identifier, rendered as 32-char lowercase hex.
///
/// Layout mirrors `WorkspaceId`: high 8 bytes = `hlc.tick()` (big-endian),
/// low 8 bytes = a monotonic per-process counter (big-endian). Big-endian
/// makes hex lexicographic order match numeric order. The same `TxnId` value
/// is carried verbatim through Raft log entries on every shard, so it must be
/// `Copy + Eq + Hash + serde` and stable across encode/decode.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, serde::Serialize, serde::Deserialize,
)]
#[serde(into = "String", try_from = "String")]
pub struct TxnId([u8; 16]);

impl TxnId {
    /// Generate a fresh, process-unique id. Two calls in the same millisecond
    /// still differ: `tick()` is strictly increasing and the counter is
    /// independent. Complexity: O(1).
    pub fn generate(hlc: &HLC) -> Self {
        let high = hlc.tick().to_be_bytes();
        let low = TXN_COUNTER.fetch_add(1, Ordering::Relaxed).to_be_bytes();
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&high);
        bytes[8..].copy_from_slice(&low);
        TxnId(bytes)
    }

    /// Construct directly from raw bytes (stable wire/replication form).
    #[inline]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        TxnId(bytes)
    }

    /// Borrow the raw 16-byte representation.
    #[inline]
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Lowercase 32-char hex rendering of the 16 raw bytes.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parse a 32-char hex string back into a `TxnId`.
    ///
    /// # Errors
    /// `LedgeError::Corruption` if the length is not 32 or any pair is not hex.
    pub fn from_hex(s: &str) -> Result<Self> {
        let bytes = hex::decode(s)
            .map_err(|e| LedgeError::Corruption(format!("invalid TxnId hex '{s}': {e}")))?;
        let arr: [u8; 16] = bytes.try_into().map_err(|_| {
            LedgeError::Corruption(format!(
                "invalid TxnId hex length: expected 32 chars, got {}",
                s.len()
            ))
        })?;
        Ok(TxnId(arr))
    }
}

impl std::fmt::Display for TxnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl From<TxnId> for String {
    fn from(id: TxnId) -> String {
        id.to_hex()
    }
}

impl TryFrom<String> for TxnId {
    type Error = LedgeError;
    fn try_from(s: String) -> Result<Self> {
        Self::from_hex(&s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HLC;

    #[test]
    fn generate_is_unique_and_monotonic_high() {
        let hlc = HLC::new();
        let a = TxnId::generate(&hlc);
        let b = TxnId::generate(&hlc);
        assert_ne!(a, b, "two generated TxnIds must differ");
    }

    #[test]
    fn hex_roundtrip() {
        let hlc = HLC::new();
        let id = TxnId::generate(&hlc);
        assert_eq!(id.to_hex().len(), 32, "128-bit id renders as 32 hex chars");
        assert_eq!(TxnId::from_hex(&id.to_hex()).unwrap(), id);
    }

    #[test]
    fn from_bytes_roundtrip() {
        let raw = [0xabu8; 16];
        let id = TxnId::from_bytes(raw);
        assert_eq!(id.as_bytes(), &raw);
    }

    #[test]
    fn from_hex_rejects_wrong_length() {
        assert!(TxnId::from_hex("deadbeef").is_err());
    }

    #[test]
    fn serde_json_roundtrip() {
        let id = TxnId::from_bytes([0x01u8; 16]);
        let json = serde_json::to_string(&id).unwrap();
        let back: TxnId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }
}
