//! `WebhookStore` — durable, WAL-backed per-tenant webhook registry.
//!
//! A near-copy of [`crate::auth::store::AuthStore`]: the SAME CRC32 + bincode
//! framed WAL (`len: u32 LE | crc32: u32 LE | bincode(entry)`), torn-tail
//! truncation on replay, and checkpoint compaction. It differs only in (a) the
//! entry enum ([`Record`]) and (b) the index key ([`WebhookId`], not a key_id
//! `String`). Unlike `AuthStore`, the registry API is synchronous (the file
//! handle sits behind a `std::sync::Mutex`), since the dispatcher and routes
//! call `register`/`list`/`delete` from non-async contexts.
//!
//! The WAL lives at `<data_dir>/webhooks/wal`.

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock, Weak};

use ledge_core::{LedgeError, Result, HLC};
use rand::RngCore;

use crate::webhook::{EventKind, WebhookConfig, WebhookId};

/// Byte size of the fixed frame header (length u32 + crc32 u32) — identical to
/// the auth/lease/ref WAL.
const HEADER_LEN: usize = 8;

/// One WAL record: a webhook upsert, a delete, or a compaction checkpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum Record {
    /// Create or update a webhook (last-writer-wins by replay order).
    Upsert(WebhookConfig),
    /// Remove a webhook by id; on replay the entry is dropped from the index.
    Delete(WebhookId),
    /// Full snapshot written by `compact()`. On replay, clears the index then
    /// inserts every webhook.
    Checkpoint(Vec<WebhookConfig>),
}

/// Encode a [`Record`] into a complete on-disk frame (identical layout to the
/// auth/lease/ref WAL).
fn encode_frame(entry: &Record) -> Result<Vec<u8>> {
    let payload = bincode::serde::encode_to_vec(entry, bincode::config::standard())
        .map_err(|e| LedgeError::Corruption(format!("webhook WAL encode: {e}")))?;
    let length = payload.len() as u32;
    let crc = crc32fast::hash(&payload);
    let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
    frame.extend_from_slice(&length.to_le_bytes());
    frame.extend_from_slice(&crc.to_le_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// Decode one frame at `pos`; `None` on truncation / CRC mismatch / decode error
/// (caller truncates the file at the last valid boundary).
fn decode_frame(data: &[u8], pos: usize) -> Option<(Record, usize)> {
    if pos + HEADER_LEN > data.len() {
        return None;
    }
    let length = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
    let crc_stored = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap());
    let payload_end = pos + HEADER_LEN + length;
    if payload_end > data.len() {
        return None;
    }
    let payload = &data[pos + HEADER_LEN..payload_end];
    if crc32fast::hash(payload) != crc_stored {
        return None;
    }
    let (entry, _): (Record, _) =
        bincode::serde::decode_from_slice(payload, bincode::config::standard()).ok()?;
    Some((entry, payload_end))
}

/// Durable, WAL-backed per-tenant webhook registry with an in-memory index.
/// `file` is `None` in [`in_memory`](Self::in_memory) mode (disabled/test):
/// appends are no-ops.
pub struct WebhookStore {
    /// WAL file at EOF for appends; `None` = in-memory (no persistence).
    file: Mutex<Option<std::fs::File>>,
    /// Path to the WAL (`<data_dir>/webhooks/wal`); empty for in-memory.
    path: PathBuf,
    /// Live index keyed by [`WebhookId`].
    index: RwLock<HashMap<WebhookId, WebhookConfig>>,
    /// Shared clock (kept for parity with `AuthStore`; future HLC-stamped ops).
    #[allow(dead_code)]
    hlc: Arc<HLC>,
}

impl WebhookStore {
    /// Open (or create) `<data_dir>/webhooks/wal`, replay it, rebuild the index.
    /// A torn tail frame is truncated, exactly like the auth WAL.
    pub fn open(data_dir: PathBuf, hlc: Arc<HLC>) -> Result<Self> {
        let dir = data_dir.join("webhooks");
        std::fs::create_dir_all(&dir).map_err(LedgeError::Io)?;
        let path = dir.join("wal");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(LedgeError::Io)?;

        let mut data = Vec::new();
        file.read_to_end(&mut data).map_err(LedgeError::Io)?;

        let mut all: Vec<Record> = Vec::new();
        let mut pos = 0usize;
        let mut last_valid = 0usize;
        while pos < data.len() {
            match decode_frame(&data, pos) {
                Some((entry, new_pos)) => {
                    all.push(entry);
                    last_valid = new_pos;
                    pos = new_pos;
                }
                None => break,
            }
        }
        if last_valid < data.len() {
            file.set_len(last_valid as u64).map_err(LedgeError::Io)?;
        }
        file.seek(SeekFrom::End(0)).map_err(LedgeError::Io)?;

        let index = Self::rebuild_index(&all);
        Ok(WebhookStore {
            file: Mutex::new(Some(file)),
            path,
            index: RwLock::new(index),
            hlc,
        })
    }

    /// An in-memory store (no persistence): used by tests and disabled mode.
    /// `register`/`delete`/`compact` are no-ops on disk; the index still works.
    pub fn in_memory() -> Self {
        WebhookStore {
            file: Mutex::new(None),
            path: PathBuf::new(),
            index: RwLock::new(HashMap::new()),
            hlc: Arc::new(HLC::new()),
        }
    }

    /// Rebuild the index from replay entries in order (Checkpoint clears).
    fn rebuild_index(all: &[Record]) -> HashMap<WebhookId, WebhookConfig> {
        let mut index: HashMap<WebhookId, WebhookConfig> = HashMap::new();
        for entry in all {
            match entry {
                Record::Upsert(w) => {
                    index.insert(w.id, w.clone());
                }
                Record::Delete(id) => {
                    index.remove(id);
                }
                Record::Checkpoint(webhooks) => {
                    index.clear();
                    for w in webhooks {
                        index.insert(w.id, w.clone());
                    }
                }
            }
        }
        index
    }

    /// Append a frame to the WAL if persistent; no-op if in-memory.
    fn append(&self, entry: &Record) -> Result<()> {
        let frame = encode_frame(entry)?;
        let mut guard = self.file.lock().unwrap();
        if let Some(file) = guard.as_mut() {
            file.write_all(&frame).map_err(LedgeError::Io)?;
        }
        Ok(())
    }

    /// Mint a webhook for `tenant`: a random 16-byte id + 32-byte CSPRNG secret.
    /// Persists an `Upsert` frame, indexes the record, and returns it. The new
    /// webhook is `active`. `now_ms` is caller-supplied so the store stays
    /// clock-free and deterministic in tests.
    pub fn register(
        &self,
        tenant: &str,
        url: String,
        events: Vec<EventKind>,
        now_ms: u64,
    ) -> Result<WebhookConfig> {
        // 16 random bytes → webhook id; 32 CSPRNG bytes → signing secret.
        let mut id_bytes = [0u8; 16];
        rand::rngs::OsRng.fill_bytes(&mut id_bytes);
        let mut secret = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut secret);

        let cfg = WebhookConfig {
            id: WebhookId(id_bytes),
            tenant_id: tenant.to_string(),
            url,
            secret,
            events,
            created_at_ms: now_ms,
            active: true,
        };
        self.append(&Record::Upsert(cfg.clone()))?;
        self.index.write().unwrap().insert(cfg.id, cfg.clone());
        Ok(cfg)
    }

    /// All webhooks belonging to `tenant`, unsorted.
    pub fn list(&self, tenant: &str) -> Vec<WebhookConfig> {
        self.index
            .read()
            .unwrap()
            .values()
            .filter(|w| w.tenant_id == tenant)
            .cloned()
            .collect()
    }

    /// Delete a webhook only if it exists AND belongs to `tenant`. Persists a
    /// `Delete` frame and removes it from the index. Returns whether a webhook
    /// was removed (a foreign or absent id ⇒ `false`, no mutation).
    pub fn delete(&self, tenant: &str, id: WebhookId) -> Result<bool> {
        {
            // Ownership check under the read lock before any persistence.
            let idx = self.index.read().unwrap();
            match idx.get(&id) {
                Some(w) if w.tenant_id == tenant => {}
                _ => return Ok(false),
            }
        }
        self.append(&Record::Delete(id))?;
        Ok(self.index.write().unwrap().remove(&id).is_some())
    }

    /// Webhooks of `tenant` that should receive an event of `kind`
    /// (i.e. [`WebhookConfig::handles`] is true).
    pub fn for_event(&self, tenant: &str, kind: EventKind) -> Vec<WebhookConfig> {
        self.list(tenant)
            .into_iter()
            .filter(|w| w.handles(kind))
            .collect()
    }

    /// Total number of registered webhooks across all tenants.
    pub fn count(&self) -> usize {
        self.index.read().unwrap().len()
    }

    /// Path to the backing WAL (empty for in-memory).
    pub fn wal_path(&self) -> &std::path::Path {
        &self.path
    }

    /// Compact the WAL to a single `Checkpoint` holding the live index, then
    /// truncate. No-op if in-memory. Mirrors `AuthStore::compact`.
    pub fn compact(&self) -> Result<()> {
        let webhooks: Vec<WebhookConfig> = {
            let idx = self.index.read().unwrap();
            idx.values().cloned().collect()
        };
        let frame = encode_frame(&Record::Checkpoint(webhooks))?;
        let mut guard = self.file.lock().unwrap();
        if let Some(file) = guard.as_mut() {
            file.seek(SeekFrom::Start(0)).map_err(LedgeError::Io)?;
            file.write_all(&frame).map_err(LedgeError::Io)?;
            file.set_len(frame.len() as u64).map_err(LedgeError::Io)?;
            file.seek(SeekFrom::End(0)).map_err(LedgeError::Io)?;
            file.flush().map_err(LedgeError::Io)?;
        }
        Ok(())
    }

    /// Background compaction task (mirror of `AuthStore::spawn_compaction_task`).
    /// No-op effectively for in-memory stores (size stat fails → 0).
    pub fn spawn_compaction_task(self: &Arc<Self>, threshold_bytes: u64) {
        let weak: Weak<Self> = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let Some(store) = weak.upgrade() else { break };
                let size = std::fs::metadata(store.wal_path())
                    .map(|m| m.len())
                    .unwrap_or(0);
                if size > threshold_bytes {
                    if let Err(e) = store.compact() {
                        tracing::warn!(error = %e, "webhook WAL compaction failed");
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::webhook::EventKind;

    #[test]
    fn register_list_delete_in_memory() {
        let s = WebhookStore::in_memory();
        let w = s
            .register("acme", "http://sink".into(), vec![], 100)
            .unwrap();
        assert_eq!(s.list("acme").len(), 1);
        assert_eq!(w.secret.len(), 32);
        assert_eq!(w.tenant_id, "acme");
        assert!(w.active);
        // foreign delete ⇒ false, no removal
        assert!(!s.delete("globex", w.id).unwrap());
        assert_eq!(s.list("acme").len(), 1);
        // own delete ⇒ true
        assert!(s.delete("acme", w.id).unwrap());
        assert!(s.list("acme").is_empty());
    }

    #[test]
    fn tenant_isolation_and_for_event() {
        let s = WebhookStore::in_memory();
        let _a = s.register("acme", "http://a".into(), vec![], 1).unwrap();
        let _g = s.register("globex", "http://g".into(), vec![], 1).unwrap();
        assert_eq!(s.list("acme").len(), 1);
        assert_eq!(s.for_event("acme", EventKind::RefCommitted).len(), 1);
        assert_eq!(s.for_event("globex", EventKind::RefCommitted).len(), 1);
        assert_eq!(s.count(), 2);
    }

    #[test]
    fn wal_roundtrip_reopen() {
        let dir = tempfile::TempDir::new().unwrap();
        let hlc = std::sync::Arc::new(ledge_core::HLC::new());
        {
            let s = WebhookStore::open(dir.path().to_path_buf(), hlc.clone()).unwrap();
            s.register("acme", "http://sink".into(), vec![], 5).unwrap();
        }
        let s2 = WebhookStore::open(dir.path().to_path_buf(), hlc).unwrap();
        assert_eq!(s2.list("acme").len(), 1);
    }
}
