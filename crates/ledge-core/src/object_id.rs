use crate::{LedgeError, Result};

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, serde::Serialize, serde::Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct ObjectId([u8; 32]);

impl ObjectId {
    #[inline]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    #[inline]
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn from_hex(s: &str) -> Result<Self> {
        let bytes = hex::decode(s)
            .map_err(|e| LedgeError::Corruption(format!("invalid ObjectId hex '{s}': {e}")))?;
        let arr: [u8; 32] = bytes.try_into().map_err(|_| {
            LedgeError::Corruption(format!(
                "invalid ObjectId hex length: expected 64 chars, got {}",
                s.len()
            ))
        })?;
        Ok(Self(arr))
    }
}

impl std::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl From<ObjectId> for String {
    fn from(id: ObjectId) -> String {
        id.to_hex()
    }
}

impl TryFrom<String> for ObjectId {
    type Error = LedgeError;
    fn try_from(s: String) -> Result<Self> {
        Self::from_hex(&s)
    }
}

impl From<blake3::Hash> for ObjectId {
    fn from(h: blake3::Hash) -> Self {
        Self(*h.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_zeros() -> ObjectId {
        ObjectId::from_bytes([0u8; 32])
    }

    fn sequential() -> ObjectId {
        let mut bytes = [0u8; 32];
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = i as u8;
        }
        ObjectId::from_bytes(bytes)
    }

    #[test]
    fn from_bytes_roundtrip() {
        let raw = [0xabu8; 32];
        let id = ObjectId::from_bytes(raw);
        assert_eq!(id.as_bytes(), &raw);
    }

    #[test]
    fn to_hex_length() {
        assert_eq!(all_zeros().to_hex().len(), 64);
    }

    #[test]
    fn to_hex_known_value() {
        assert_eq!(all_zeros().to_hex(), "0".repeat(64));
    }

    #[test]
    fn from_hex_roundtrip() {
        let id = sequential();
        assert_eq!(ObjectId::from_hex(&id.to_hex()).unwrap(), id);
    }

    #[test]
    fn from_hex_rejects_wrong_length() {
        assert!(ObjectId::from_hex("deadbeef").is_err());
    }

    #[test]
    fn from_hex_rejects_non_hex() {
        assert!(ObjectId::from_hex(&"z".repeat(64)).is_err());
    }

    #[test]
    fn display_matches_to_hex() {
        let id = sequential();
        assert_eq!(format!("{id}"), id.to_hex());
    }

    #[test]
    fn copy_semantics() {
        let id1 = all_zeros();
        let id2 = id1;
        assert_eq!(id1, id2);
    }

    #[test]
    fn eq_and_hash_consistent() {
        use std::collections::HashSet;
        let id = sequential();
        let mut set = HashSet::new();
        set.insert(id);
        set.insert(id);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn serde_json_roundtrip() {
        let id = sequential();
        let json = serde_json::to_string(&id).unwrap();
        let id2: ObjectId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, id2);
    }
}
