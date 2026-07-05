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
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock, Weak};

use ledge_core::{LedgeError, Result, HLC};
use rand::RngCore;

use crate::webhook::{EventKind, WebhookConfig, WebhookId};

/// Map a shared-WAL error into this crate's error. Corruption keeps the WAL's
/// offset/record-count detail and is tagged with the store name.
fn map_wal(e: ledge_wal::WalError) -> LedgeError {
    match e {
        ledge_wal::WalError::Io(io) => LedgeError::Io(io),
        ledge_wal::WalError::Encode(s) => LedgeError::Corruption(format!("webhook WAL: {s}")),
        ledge_wal::WalError::Corruption(s) => LedgeError::Corruption(format!(
            "webhook WAL: {s}. Refusing to truncate — restore from a backup."
        )),
    }
}

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
        // Shared primitive: opens (dir-fsync on create), replays frames, recovers
        // a torn tail, and fails loud on in-place corruption.
        let (file, all) = ledge_wal::open_replay::<Record>(&path).map_err(map_wal)?;

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

    /// Append a frame to the WAL if persistent; no-op if in-memory. Durable
    /// (write + fsync) before returning.
    fn append(&self, entry: &Record) -> Result<()> {
        let mut guard = self.file.lock().unwrap();
        if let Some(file) = guard.as_mut() {
            ledge_wal::append_record(file, entry).map_err(map_wal)?;
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
    ///
    /// Crash-atomic: temp file + fsync + atomic rename + dir fsync + handle
    /// reopen, so a crash mid-rewrite leaves either the intact old log or the new
    /// checkpoint — never a torn frame 0, which open() now rejects (losing every
    /// webhook). The pre-lock snapshot race is deferred to the shared ledge-wal
    /// extraction, as in AuthStore::compact.
    pub fn compact(&self) -> Result<()> {
        let webhooks: Vec<WebhookConfig> = {
            let idx = self.index.read().unwrap();
            idx.values().cloned().collect()
        };
        let mut guard = self.file.lock().unwrap();
        if guard.is_some() {
            // Shared primitive: crash-atomic temp + fsync + rename + dir fsync +
            // reopen; returns the fresh handle at EOF.
            let new_file = ledge_wal::write_checkpoint(&self.path, &Record::Checkpoint(webhooks))
                .map_err(map_wal)?;
            *guard = Some(new_file);
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

    #[test]
    fn mid_stream_corruption_fails_loud_not_silent() {
        // Bit-rot in an early frame must fail open, not silently drop the webhook
        // records after it.
        let dir = tempfile::TempDir::new().unwrap();
        let hlc = std::sync::Arc::new(ledge_core::HLC::new());
        let path = dir.path().join("webhooks").join("wal");
        {
            let s = WebhookStore::open(dir.path().to_path_buf(), hlc.clone()).unwrap();
            for i in 0..5 {
                s.register("acme", format!("http://sink{i}"), vec![], 100)
                    .unwrap();
            }
        }
        {
            // Byte 9 is inside the first frame's payload (header is bytes 0..8).
            let mut data = std::fs::read(&path).unwrap();
            data[9] ^= 0xFF;
            std::fs::write(&path, &data).unwrap();
        }
        assert!(
            WebhookStore::open(dir.path().to_path_buf(), hlc).is_err(),
            "mid-stream corruption must fail open, not silently drop webhooks"
        );
    }
}
