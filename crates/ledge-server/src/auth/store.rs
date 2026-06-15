//! `AuthStore` — durable, WAL-backed API-key store (Phase 4d-1 spec §4.2).
//!
//! A near-copy of [`ledge_workspace::lease::LeaseStore`]: the SAME CRC32 +
//! bincode framed WAL (`len: u32 LE | crc32: u32 LE | bincode(entry)`),
//! torn-tail truncation on replay, and checkpoint compaction. It differs only in
//! (a) the entry enum ([`AuthWalEntry`]) and (b) the index key (`String` key_id,
//! not `WorkspaceId`). Only `BLAKE3(secret)` is persisted — the plaintext secret
//! is never stored or logged.
//!
//! # Token format (spec §3.1)
//! `ledge_<key_id>_<secret>` where `key_id` is 16-char lowercase hex (8 random
//! bytes) and `secret` is base64url-no-pad of 32 CSPRNG bytes. `mint` returns the
//! token once; the store keeps only `BLAKE3(secret_bytes)`.

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, RwLock, Weak};

use base64::Engine;
use ledge_core::{LedgeError, Result, HLC};
use rand::RngCore;
use subtle::ConstantTimeEq;
use tokio::sync::Mutex;

use super::principal::{Principal, PrincipalKind, Scopes};

/// Byte size of the fixed frame header (length u32 + crc32 u32) — identical to
/// the lease/ref WAL.
const HEADER_LEN: usize = 8;

/// A persisted API key. Only the BLAKE3 hash of the secret is stored.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ApiKeyRecord {
    /// 16-char lowercase hex public lookup handle.
    pub key_id: String,
    /// `BLAKE3(secret_bytes)` — never the plaintext secret.
    pub secret_hash: [u8; 32],
    /// Tenant this key belongs to.
    pub tenant_id: String,
    /// User or Service.
    pub kind: PrincipalKind,
    /// Capability bits (mirrored into [`Scopes`] on verify).
    pub read: bool,
    pub write: bool,
    pub admin: bool,
    /// Creation time (ms since epoch), informational.
    pub created_at_ms: u64,
    /// Optional expiry (ms since epoch); `None` = never expires.
    pub expires_at_ms: Option<u64>,
    /// True once revoked (kept in the WAL until compaction drops it).
    pub revoked: bool,
}

impl ApiKeyRecord {
    /// The [`Scopes`] this record grants.
    fn scopes(&self) -> Scopes {
        Scopes {
            read: self.read,
            write: self.write,
            admin: self.admin,
        }
    }
    /// The [`Principal`] a successful verify resolves to.
    fn principal(&self) -> Principal {
        Principal {
            tenant_id: self.tenant_id.clone(),
            principal_id: self.key_id.clone(),
            kind: self.kind,
            scopes: self.scopes(),
        }
    }
    /// Live iff not revoked and not past expiry at `now_ms`.
    fn is_live(&self, now_ms: u64) -> bool {
        !self.revoked && self.expires_at_ms.map(|e| e > now_ms).unwrap_or(true)
    }
}

/// One WAL record: a key upsert, a revoke, or a compaction checkpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum AuthWalEntry {
    /// Create or update a key (last-writer-wins by replay order).
    Put(ApiKeyRecord),
    /// Mark a key revoked; `hlc` records when. Sets `revoked=true` in the index.
    Revoke { key_id: String, hlc: u64 },
    /// Full live snapshot written by `compact()`. On replay, clears the index
    /// then inserts every (non-revoked) key.
    Checkpoint { keys: Vec<ApiKeyRecord> },
}

/// Encode an `AuthWalEntry` into a complete on-disk frame (identical layout to
/// the lease/ref WAL).
fn encode_frame(entry: &AuthWalEntry) -> Result<Vec<u8>> {
    let payload = bincode::serde::encode_to_vec(entry, bincode::config::standard())
        .map_err(|e| LedgeError::Corruption(format!("auth WAL encode: {e}")))?;
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
fn decode_frame(data: &[u8], pos: usize) -> Option<(AuthWalEntry, usize)> {
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
    let (entry, _): (AuthWalEntry, _) =
        bincode::serde::decode_from_slice(payload, bincode::config::standard()).ok()?;
    Some((entry, payload_end))
}

/// Durable, WAL-backed API-key store with an in-memory index. `file` is `None`
/// in [`in_memory`](Self::in_memory) mode (disabled/test): appends are no-ops.
pub struct AuthStore {
    /// WAL file at EOF for appends; `None` = in-memory (no persistence).
    file: Mutex<Option<std::fs::File>>,
    /// Path to the WAL (`<data_dir>/auth/wal`); empty for in-memory.
    path: PathBuf,
    /// Live index keyed by `key_id`. Revoked keys stay (with `revoked=true`)
    /// until compaction drops them, so a revoked-key verify can fail fast.
    index: RwLock<HashMap<String, ApiKeyRecord>>,
    /// Shared clock to HLC-stamp revokes.
    hlc: Arc<HLC>,
}

impl AuthStore {
    /// Open (or create) `<data_dir>/auth/wal`, replay it, rebuild the index.
    /// A torn tail frame is truncated, exactly like the lease WAL.
    pub fn open(data_dir: PathBuf, hlc: Arc<HLC>) -> Result<Self> {
        let dir = data_dir.join("auth");
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

        let mut all: Vec<AuthWalEntry> = Vec::new();
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
        Ok(AuthStore {
            file: Mutex::new(Some(file)),
            path,
            index: RwLock::new(index),
            hlc,
        })
    }

    /// An in-memory store (no persistence): used by `AuthCtx::disabled()` and
    /// tests. `put`/`revoke`/`compact` are no-ops on disk; the index still works.
    pub fn in_memory(hlc: Arc<HLC>) -> Self {
        AuthStore {
            file: Mutex::new(None),
            path: PathBuf::new(),
            index: RwLock::new(HashMap::new()),
            hlc,
        }
    }

    /// Rebuild the index from replay entries in order (Checkpoint clears).
    fn rebuild_index(all: &[AuthWalEntry]) -> HashMap<String, ApiKeyRecord> {
        let mut index: HashMap<String, ApiKeyRecord> = HashMap::new();
        for entry in all {
            match entry {
                AuthWalEntry::Put(r) => {
                    index.insert(r.key_id.clone(), r.clone());
                }
                AuthWalEntry::Revoke { key_id, .. } => {
                    if let Some(r) = index.get_mut(key_id) {
                        r.revoked = true;
                    }
                }
                AuthWalEntry::Checkpoint { keys } => {
                    index.clear();
                    for r in keys {
                        index.insert(r.key_id.clone(), r.clone());
                    }
                }
            }
        }
        index
    }

    /// Append a frame to the WAL if persistent; no-op if in-memory.
    async fn append(&self, entry: &AuthWalEntry) -> Result<()> {
        let frame = encode_frame(entry)?;
        let mut guard = self.file.lock().await;
        if let Some(file) = guard.as_mut() {
            file.write_all(&frame).map_err(LedgeError::Io)?;
        }
        Ok(())
    }

    /// Append a `Put` frame and upsert the index. Create-or-update.
    pub async fn put(&self, rec: ApiKeyRecord) -> Result<()> {
        self.append(&AuthWalEntry::Put(rec.clone())).await?;
        self.index.write().unwrap().insert(rec.key_id.clone(), rec);
        Ok(())
    }

    /// Append a `Revoke` frame and set `revoked=true` in the index. Returns
    /// `true` if a key was present to revoke, `false` otherwise (idempotent).
    pub async fn revoke(&self, key_id: &str) -> Result<bool> {
        let hlc = self.hlc.tick();
        self.append(&AuthWalEntry::Revoke {
            key_id: key_id.to_string(),
            hlc,
        })
        .await?;
        let mut idx = self.index.write().unwrap();
        match idx.get_mut(key_id) {
            Some(r) => {
                r.revoked = true;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Mint a fresh key for `tenant`/`kind`/`scopes` with optional TTL. Persists
    /// `BLAKE3(secret)` and returns the full `ledge_<key_id>_<secret>` token
    /// ONCE (never stored). `created_at_ms`/`now_ms` are caller-supplied so the
    /// store stays clock-free and deterministic in tests.
    pub async fn mint(
        &self,
        tenant_id: &str,
        kind: PrincipalKind,
        scopes: Scopes,
        ttl: Option<std::time::Duration>,
        now_ms: u64,
    ) -> Result<String> {
        // 8 random bytes → 16-char lowercase hex key_id.
        let mut id_bytes = [0u8; 8];
        rand::rngs::OsRng.fill_bytes(&mut id_bytes);
        let key_id = hex_lower(&id_bytes);
        // 32 CSPRNG bytes → base64url-no-pad secret.
        let mut secret_bytes = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut secret_bytes);
        let secret = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret_bytes);
        let secret_hash = *blake3::hash(&secret_bytes).as_bytes();

        let rec = ApiKeyRecord {
            key_id: key_id.clone(),
            secret_hash,
            tenant_id: tenant_id.to_string(),
            kind,
            read: scopes.read,
            write: scopes.write,
            admin: scopes.admin,
            created_at_ms: now_ms,
            expires_at_ms: ttl.map(|d| now_ms + d.as_millis() as u64),
            revoked: false,
        };
        self.put(rec).await?;
        Ok(format!("ledge_{key_id}_{secret}"))
    }

    /// Record an operator-supplied full token (`ledge_<id>_<secret>`) as a key
    /// for `tenant`/`kind`/`scopes`. Used by first-boot bootstrap (spec §4.4) so
    /// a fresh cluster has a reachable admin without an interactive mint. Returns
    /// the `key_id` recorded. Errors if the token is malformed.
    ///
    /// The operator generated the token out-of-band (so they already hold the
    /// secret); only `BLAKE3(secret)` is persisted — the plaintext is never
    /// stored or logged, exactly as for [`mint`](Self::mint).
    pub async fn put_token(
        &self,
        token: &str,
        tenant_id: &str,
        kind: PrincipalKind,
        scopes: Scopes,
        now_ms: u64,
    ) -> Result<String> {
        let (key_id, secret_b64) = parse_token(token)
            .ok_or_else(|| LedgeError::Corruption("malformed bootstrap token".into()))?;
        let secret_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(secret_b64.as_bytes())
            .map_err(|e| LedgeError::Corruption(format!("bootstrap token secret: {e}")))?;
        let secret_hash = *blake3::hash(&secret_bytes).as_bytes();
        let rec = ApiKeyRecord {
            key_id: key_id.clone(),
            secret_hash,
            tenant_id: tenant_id.to_string(),
            kind,
            read: scopes.read,
            write: scopes.write,
            admin: scopes.admin,
            created_at_ms: now_ms,
            expires_at_ms: None,
            revoked: false,
        };
        self.put(rec).await?;
        Ok(key_id)
    }

    /// Verify a presented token at `now_ms`. Parses `ledge_<key_id>_<secret>`,
    /// looks up the record, and on (present ∧ live ∧ constant-time hash match)
    /// returns the resolved [`Principal`]; else `None`. The secret is
    /// base64url-decoded back to bytes before hashing so it matches `mint`.
    pub fn verify(&self, token: &str, now_ms: u64) -> Option<Principal> {
        let (key_id, secret_b64) = parse_token(token)?;
        let secret_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(secret_b64.as_bytes())
            .ok()?;
        let presented_hash = *blake3::hash(&secret_bytes).as_bytes();

        let idx = self.index.read().unwrap();
        let rec = idx.get(&key_id)?;
        if !rec.is_live(now_ms) {
            return None;
        }
        // Constant-time digest comparison (no early-exit timing leak).
        let ok: bool = presented_hash.ct_eq(&rec.secret_hash).into();
        if ok {
            Some(rec.principal())
        } else {
            None
        }
    }

    /// All records (metadata only — no secrets are ever stored), unsorted.
    pub fn list(&self) -> Vec<ApiKeyRecord> {
        self.index.read().unwrap().values().cloned().collect()
    }

    /// Count of live (non-revoked, non-expired at `now_ms`) keys — for the
    /// `ledge_auth_keys` gauge.
    pub fn live_count(&self, now_ms: u64) -> usize {
        self.index
            .read()
            .unwrap()
            .values()
            .filter(|r| r.is_live(now_ms))
            .count()
    }

    /// Compact the WAL to a single `Checkpoint` holding only non-revoked keys,
    /// then truncate. No-op if in-memory. Mirrors `LeaseStore::compact`.
    pub async fn compact(&self) -> Result<()> {
        let keys: Vec<ApiKeyRecord> = {
            let idx = self.index.read().unwrap();
            idx.values().filter(|r| !r.revoked).cloned().collect()
        };
        let frame = encode_frame(&AuthWalEntry::Checkpoint { keys })?;
        {
            let mut guard = self.file.lock().await;
            if let Some(file) = guard.as_mut() {
                file.seek(SeekFrom::Start(0)).map_err(LedgeError::Io)?;
                file.write_all(&frame).map_err(LedgeError::Io)?;
                file.set_len(frame.len() as u64).map_err(LedgeError::Io)?;
                file.seek(SeekFrom::End(0)).map_err(LedgeError::Io)?;
                file.flush().map_err(LedgeError::Io)?;
            }
        }
        // Drop revoked keys from the index post-compaction (they are gone on disk).
        let mut idx = self.index.write().unwrap();
        idx.retain(|_, r| !r.revoked);
        Ok(())
    }

    /// Path to the backing WAL (empty for in-memory). Diagnostics + size-based
    /// compaction triggers (mirrors `LeaseStore::wal_path`).
    pub fn wal_path(&self) -> &std::path::Path {
        &self.path
    }

    /// Background compaction task (mirror of `LeaseStore::spawn_compaction_task`).
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
                    if let Err(e) = store.compact().await {
                        tracing::warn!(error = %e, "auth WAL compaction failed");
                    }
                }
            }
        });
    }
}

/// Lowercase hex (no `hex` dep in `ledge-server`; mirror the cluster crate's
/// local helper). 8 bytes → 16 chars.
fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Parse `ledge_<key_id>_<secret>` → `(key_id, secret_b64)`. The secret may
/// itself contain `_` (URL_SAFE base64 uses `-`/`_`), so split on the FIRST two
/// underscores only: prefix `ledge`, then `key_id`, then the rest is the secret.
/// Returns `None` on any shape mismatch.
fn parse_token(token: &str) -> Option<(String, String)> {
    // Expect exactly: "ledge" "_" <key_id> "_" <secret...>
    let rest = token.strip_prefix("ledge_")?;
    // key_id is 16 hex chars; the secret follows the next `_`.
    let (key_id, secret) = rest.split_once('_')?;
    if key_id.is_empty() || secret.is_empty() {
        return None;
    }
    Some((key_id.to_string(), secret.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn hlc() -> Arc<HLC> {
        Arc::new(HLC::new())
    }

    fn rw_scopes() -> Scopes {
        Scopes {
            read: true,
            write: true,
            admin: false,
        }
    }

    #[tokio::test]
    async fn mint_then_verify_roundtrips() {
        let dir = tempdir().unwrap();
        let store = AuthStore::open(dir.path().to_path_buf(), hlc()).unwrap();
        let token = store
            .mint("acme", PrincipalKind::User, rw_scopes(), None, 1_000)
            .await
            .unwrap();
        assert!(
            token.starts_with("ledge_"),
            "token has the ledge_ prefix: {token}"
        );
        let p = store.verify(&token, 2_000).expect("valid token verifies");
        assert_eq!(p.tenant_id, "acme");
        assert!(p.scopes.can_write() && p.scopes.can_read() && !p.scopes.is_admin());
        assert_eq!(p.kind, PrincipalKind::User);
    }

    #[tokio::test]
    async fn verify_rejects_unknown_and_malformed() {
        let store = AuthStore::in_memory(hlc());
        assert!(
            store.verify("ledge_deadbeefdeadbeef_AAAA", 0).is_none(),
            "unknown key_id"
        );
        assert!(store.verify("not-a-token", 0).is_none(), "malformed");
        assert!(store.verify("ledge__", 0).is_none(), "empty parts");
    }

    #[tokio::test]
    async fn verify_rejects_wrong_secret() {
        let dir = tempdir().unwrap();
        let store = AuthStore::open(dir.path().to_path_buf(), hlc()).unwrap();
        let token = store
            .mint("acme", PrincipalKind::User, rw_scopes(), None, 0)
            .await
            .unwrap();
        // Swap the secret tail for a valid-base64 wrong value with the SAME key_id.
        let (key_id, _secret) = token
            .strip_prefix("ledge_")
            .unwrap()
            .split_once('_')
            .unwrap();
        let wrong = format!("ledge_{key_id}_{}", "A".repeat(43));
        assert!(
            store.verify(&wrong, 0).is_none(),
            "wrong secret must fail (hash mismatch)"
        );
    }

    #[tokio::test]
    async fn verify_rejects_revoked_and_expired() {
        let dir = tempdir().unwrap();
        let store = AuthStore::open(dir.path().to_path_buf(), hlc()).unwrap();
        // Expiring key: ttl 10ms from now_ms=0 ⇒ expires_at_ms=10.
        let token = store
            .mint(
                "acme",
                PrincipalKind::User,
                rw_scopes(),
                Some(std::time::Duration::from_millis(10)),
                0,
            )
            .await
            .unwrap();
        assert!(store.verify(&token, 5).is_some(), "live before expiry");
        assert!(
            store.verify(&token, 10).is_none(),
            "expired at boundary (expires>now is false)"
        );
        assert!(store.verify(&token, 50).is_none(), "expired past boundary");

        // Revocation: a never-expiring key, revoked, must fail.
        let tok2 = store
            .mint("acme", PrincipalKind::User, rw_scopes(), None, 0)
            .await
            .unwrap();
        let (kid, _) = tok2
            .strip_prefix("ledge_")
            .unwrap()
            .split_once('_')
            .unwrap();
        assert!(
            store.revoke(kid).await.unwrap(),
            "revoke returns true for a present key"
        );
        assert!(store.verify(&tok2, 0).is_none(), "revoked key fails verify");
        assert!(
            !store.revoke("nope").await.unwrap(),
            "revoke of absent key returns false"
        );
    }

    #[tokio::test]
    async fn reopen_replays_keys_and_revokes() {
        let dir = tempdir().unwrap();
        let (good_token, revoked_kid) = {
            let store = AuthStore::open(dir.path().to_path_buf(), hlc()).unwrap();
            let good = store
                .mint("t", PrincipalKind::User, rw_scopes(), None, 0)
                .await
                .unwrap();
            let bad = store
                .mint("t", PrincipalKind::User, rw_scopes(), None, 0)
                .await
                .unwrap();
            let (kid, _) = bad.strip_prefix("ledge_").unwrap().split_once('_').unwrap();
            store.revoke(kid).await.unwrap();
            (good, kid.to_string())
        }; // dropped → file closed
        let store = AuthStore::open(dir.path().to_path_buf(), hlc()).unwrap();
        assert!(
            store.verify(&good_token, 0).is_some(),
            "good key survives reopen"
        );
        let revoked = store
            .list()
            .into_iter()
            .find(|r| r.key_id == revoked_kid)
            .unwrap();
        assert!(revoked.revoked, "revoke survives reopen");
    }

    #[tokio::test]
    async fn truncated_tail_recovery() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auth").join("wal");
        let first_token = {
            let store = AuthStore::open(dir.path().to_path_buf(), hlc()).unwrap();
            let t = store
                .mint("t", PrincipalKind::User, rw_scopes(), None, 0)
                .await
                .unwrap();
            for _ in 0..4 {
                store
                    .mint("t", PrincipalKind::User, rw_scopes(), None, 0)
                    .await
                    .unwrap();
            }
            t
        };
        // Corrupt the tail: drop the last 3 bytes (partial final frame).
        {
            let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            let len = f.metadata().unwrap().len();
            f.set_len(len - 3).unwrap();
        }
        let store = AuthStore::open(dir.path().to_path_buf(), hlc()).unwrap();
        // The first key (well before the torn tail) must survive.
        assert!(
            store.verify(&first_token, 0).is_some(),
            "early key survives torn-tail truncation"
        );
        // At least 4 of 5 keys survive (the 5th frame's tail was cut).
        assert!(
            store.list().len() >= 4,
            "expected >= 4 survivors, got {}",
            store.list().len()
        );
    }

    #[tokio::test]
    async fn compaction_drops_revoked_keeps_live() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auth").join("wal");
        let live_token = {
            let store = AuthStore::open(dir.path().to_path_buf(), hlc()).unwrap();
            // Grow the WAL with many puts so compaction visibly shrinks it.
            for _ in 0..50 {
                let t = store
                    .mint("t", PrincipalKind::User, rw_scopes(), None, 0)
                    .await
                    .unwrap();
                let (kid, _) = t.strip_prefix("ledge_").unwrap().split_once('_').unwrap();
                store.revoke(kid).await.unwrap(); // all revoked
            }
            let live = store
                .mint("t", PrincipalKind::User, rw_scopes(), None, 0)
                .await
                .unwrap();
            let pre = std::fs::metadata(&path).unwrap().len();
            store.compact().await.unwrap();
            let post = std::fs::metadata(&path).unwrap().len();
            assert!(
                post < pre,
                "compaction shrinks the WAL: post {post} !< pre {pre}"
            );
            live
        };
        let store = AuthStore::open(dir.path().to_path_buf(), hlc()).unwrap();
        assert!(
            store.verify(&live_token, 0).is_some(),
            "live key survives compaction"
        );
        assert!(
            store.list().iter().all(|r| !r.revoked),
            "no revoked keys remain after compaction"
        );
    }

    #[tokio::test]
    async fn put_token_records_verifiable_operator_token() {
        let dir = tempdir().unwrap();
        let store = AuthStore::open(dir.path().to_path_buf(), hlc()).unwrap();
        // Mint elsewhere to get a real well-formed token, then record it as bootstrap.
        let scratch = AuthStore::in_memory(hlc());
        let token = scratch
            .mint("x", PrincipalKind::User, Scopes::ALL, None, 0)
            .await
            .unwrap();
        store
            .put_token(&token, "root", PrincipalKind::User, Scopes::ALL, 0)
            .await
            .unwrap();
        assert!(
            store.verify(&token, 0).is_some(),
            "bootstrapped token verifies"
        );
    }

    #[tokio::test]
    async fn put_token_rejects_malformed() {
        let store = AuthStore::in_memory(hlc());
        assert!(
            store
                .put_token("not-a-token", "root", PrincipalKind::User, Scopes::ALL, 0)
                .await
                .is_err(),
            "malformed token is rejected"
        );
    }

    #[tokio::test]
    async fn in_memory_store_works_without_persistence() {
        let store = AuthStore::in_memory(hlc());
        let t = store
            .mint("t", PrincipalKind::User, rw_scopes(), None, 0)
            .await
            .unwrap();
        assert!(store.verify(&t, 0).is_some(), "in-memory mint+verify works");
        assert!(
            store.wal_path().as_os_str().is_empty(),
            "in-memory has no path"
        );
    }
}
