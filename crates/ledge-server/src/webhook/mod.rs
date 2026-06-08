//! Webhooks / event surface: types + signing. Store, dispatcher, and routes live
//! in sibling modules (added by later tasks).

pub mod dispatch;
pub mod store;

use serde::{Deserialize, Serialize};

/// 128-bit webhook id (hex in the API).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WebhookId(pub [u8; 16]);

impl WebhookId {
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(32);
        for b in self.0 {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }
    pub fn from_hex(s: &str) -> Option<Self> {
        if s.len() != 32 {
            return None;
        }
        let mut b = [0u8; 16];
        for (i, slot) in b.iter_mut().enumerate() {
            *slot = u8::from_str_radix(s.get(i * 2..i * 2 + 2)?, 16).ok()?;
        }
        Some(WebhookId(b))
    }
}

/// The event kinds a webhook can subscribe to. Extensible; serialized snake_case
/// in config but the WIRE name (in the payload + `X-Ledge-Event`) is dotted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    RefCommitted,
}

impl EventKind {
    pub fn wire(&self) -> &'static str {
        match self {
            EventKind::RefCommitted => "ref.committed",
        }
    }
}

/// A registered webhook (durable record).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    pub id: WebhookId,
    pub tenant_id: String,
    pub url: String,
    pub secret: [u8; 32],
    /// Empty ⇒ all event kinds.
    pub events: Vec<EventKind>,
    pub created_at_ms: u64,
    pub active: bool,
}

impl WebhookConfig {
    /// Whether this (active) webhook should receive an event of `kind`.
    pub fn handles(&self, kind: EventKind) -> bool {
        self.active && (self.events.is_empty() || self.events.contains(&kind))
    }
}

/// `blake3=<hex>` keyed-hash signature of `body` under the 32-byte `secret`.
/// Receivers recompute `blake3::keyed_hash(secret, body)` to verify authenticity.
pub fn sign(secret: &[u8; 32], body: &[u8]) -> String {
    format!("blake3={}", blake3::keyed_hash(secret, body).to_hex())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sign_matches_recompute() {
        let secret = [7u8; 32];
        let body = b"{\"event\":\"ref.committed\"}";
        let sig = sign(&secret, body);
        assert!(sig.starts_with("blake3="));
        assert_eq!(sig, sign(&secret, body));
        assert_ne!(sig, sign(&[8u8; 32], body));
    }
    #[test]
    fn webhook_id_hex_roundtrips() {
        let id = WebhookId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        assert_eq!(WebhookId::from_hex(&id.to_hex()), Some(id));
        assert_eq!(id.to_hex().len(), 32);
        assert!(WebhookId::from_hex("nothex").is_none());
    }
    #[test]
    fn event_kind_filter() {
        let mut w = WebhookConfig {
            id: WebhookId([0; 16]),
            tenant_id: "t".into(),
            url: "http://x".into(),
            secret: [0; 32],
            events: vec![],
            created_at_ms: 0,
            active: true,
        };
        assert!(w.handles(EventKind::RefCommitted)); // empty events ⇒ all
        w.events = vec![EventKind::RefCommitted];
        assert!(w.handles(EventKind::RefCommitted));
        w.active = false;
        assert!(!w.handles(EventKind::RefCommitted)); // inactive ⇒ none
    }
}
