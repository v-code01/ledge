use crate::pkt_line::{decode_line, encode, encode_flush, PktLine};
use async_trait::async_trait;
use bytes::Bytes;
use ledge_core::{LedgeError, ObjectId, ObjectStore, RefStore};
use ledge_object_store::graph::{commit_parent_sha1s, commit_tree_sha1, tree_child_sha1s};
use ledge_object_store::DiskObjectStore;
use std::sync::Arc;

/// Git pack object type tags (3-bit field in pack varint header).
#[derive(Clone, Copy, Debug)]
pub enum GitObjectKind {
    Commit = 1,
    Tree = 2,
    Blob = 3,
    Tag = 4,
}

/// Object-store capability needed by the git fetch/push handlers.
///
/// This decouples the handlers from the concrete `DiskObjectStore` so that
/// tests can supply an in-memory implementation.  It exposes the canonical
/// git SHA-1, the git object type, and a typed write so that pushed objects
/// are persisted with their true type and served back with correct typed
/// SHA-1s and pack entries.
#[async_trait]
pub trait Sha1Provider: Send + Sync {
    /// Return the Git-compatible SHA-1 (typed header hash) for `id`, or `None`
    /// if the object is not present in the store.
    async fn sha1_of(&self, id: ObjectId) -> Option<[u8; 20]>;

    /// Git object type byte (1=commit, 2=tree, 3=blob, 4=tag), or `None` if
    /// unknown / object missing.
    async fn git_type_of(&self, id: ObjectId) -> Option<u8>;

    /// Persist `content` tagged with its git object `git_type`, returning the
    /// BLAKE3-addressed [`ObjectId`].
    async fn write_git_object(
        &self,
        git_type: u8,
        content: bytes::Bytes,
    ) -> ledge_core::Result<ObjectId>;

    /// Build a `git-SHA-1 → ObjectId` index over all stored objects.
    ///
    /// Used by the fetch path to resolve child SHA-1s found while walking a
    /// commit's reachable object graph (commit → tree → blob).
    ///
    /// Returns an `Arc` so the disk implementation can hand back its memoized
    /// index by pointer rather than rebuilding (and re-scanning the whole store)
    /// on every clone request.
    async fn sha1_index(&self) -> std::sync::Arc<std::collections::HashMap<[u8; 20], ObjectId>>;

    /// Fetch a stored object's (git_type, content) by its git SHA-1 — for resolving
    /// thin-pack REF_DELTA bases. None if absent.
    async fn read_git_object_by_sha1(&self, sha1: &[u8; 20]) -> Option<(u8, Vec<u8>)>;
}

/// Bridge `DiskObjectStore` to the `Sha1Provider` trait.
///
/// `DiskObjectStore::sha1_of` / `git_type_of` read the typed header stored in
/// the object file and return `Result`.  We map `Err` to `None` so the
/// handlers can treat a missing value the same as a missing object.
#[async_trait]
impl Sha1Provider for DiskObjectStore {
    async fn sha1_of(&self, id: ObjectId) -> Option<[u8; 20]> {
        self.sha1_of(id).await.ok()
    }
    async fn git_type_of(&self, id: ObjectId) -> Option<u8> {
        self.git_type_of(id).await.ok()
    }
    async fn write_git_object(
        &self,
        git_type: u8,
        content: bytes::Bytes,
    ) -> ledge_core::Result<ObjectId> {
        self.write_git_object(git_type, content).await
    }
    async fn sha1_index(&self) -> std::sync::Arc<std::collections::HashMap<[u8; 20], ObjectId>> {
        self.sha1_index()
            .await
            .unwrap_or_else(|_| std::sync::Arc::new(std::collections::HashMap::new()))
    }
    async fn read_git_object_by_sha1(&self, sha1: &[u8; 20]) -> Option<(u8, Vec<u8>)> {
        let id = *Sha1Provider::sha1_index(self).await.get(sha1)?;
        let ty = Sha1Provider::git_type_of(self, id).await?;
        let content = ledge_core::ObjectStore::read(self, id).await.ok()?;
        Some((ty, content.to_vec()))
    }
}

/// Map a git object type byte to its pack [`GitObjectKind`].
///
/// Unknown / out-of-range bytes default to [`GitObjectKind::Blob`] so a
/// malformed type never aborts pack encoding (it degrades to raw bytes).
fn kind_from_type_byte(type_byte: u8) -> GitObjectKind {
    match type_byte {
        1 => GitObjectKind::Commit,
        2 => GitObjectKind::Tree,
        4 => GitObjectKind::Tag,
        _ => GitObjectKind::Blob,
    }
}

/// Encode the git pack object type-and-size varint.
///
/// Format (from the git pack-format spec):
/// ```text
/// byte 0: MSB=more | type[2:0] in bits 6-4 | size[3:0] in bits 3-0
/// subsequent bytes (if MSB was set): MSB=more | size[6:0]
/// ```
///
/// This is the variable-length integer encoding used in git packfiles for each
/// object's header.  The type occupies 3 bits (values 1–7), and the size
/// encodes the decompressed object length.
fn encode_type_size_varint(kind: GitObjectKind, size: usize) -> Vec<u8> {
    let type_bits = (kind as u8) & 0x07;
    let mut result = Vec::with_capacity(4);
    // Low 4 bits of size go into the first byte alongside the type.
    let size_low = (size & 0x0F) as u8;
    let remaining = size >> 4;
    if remaining == 0 {
        // Single byte: no continuation bit, type in bits 6-4, size[3:0] in 3-0.
        result.push((type_bits << 4) | size_low);
    } else {
        // First byte has continuation bit set.
        result.push(0x80 | (type_bits << 4) | size_low);
        let mut rest = remaining;
        loop {
            if rest < 0x80 {
                result.push(rest as u8);
                break;
            } else {
                // Continuation byte: MSB=1, low 7 bits of remaining size.
                result.push(0x80 | (rest as u8 & 0x7F));
                rest >>= 7;
            }
        }
    }
    result
}

/// Zlib-deflate `data` using the default compression level.
///
/// Git packfiles store each object's content as zlib-compressed data
/// immediately after the object header varint.
fn zlib_deflate(data: &[u8]) -> Vec<u8> {
    use flate2::write::ZlibEncoder;
    use std::io::Write;
    let mut enc = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(data).expect("deflate write");
    enc.finish().expect("deflate finish")
}

/// Build a non-delta packfile from `objects`.
///
/// Each object carries its true git type (1=commit, 2=tree, 3=blob, 4=tag) so
/// that a real `git` client can validate the object graph after clone — a
/// commit advertised on `refs/heads/main` must arrive as a commit, not a blob.
/// The packfile layout is:
///
/// ```text
/// magic:   "PACK"                       4 bytes
/// version: 2 (big-endian u32)           4 bytes
/// count:   number of objects (BE u32)   4 bytes
/// [for each object]
///   type-size varint                    1+ bytes
///   zlib-deflated content               variable
/// SHA-1 checksum of all preceding bytes 20 bytes
/// ```
///
/// # Arguments
/// * `objects` — slice of `(git_type: u8, content: Bytes)` pairs, where
///   `git_type` is the git object type byte.
pub fn encode_pack(objects: &[(u8, Bytes)]) -> Vec<u8> {
    use sha1::{Digest, Sha1};
    let mut pack = Vec::new();
    // Pack header
    pack.extend_from_slice(b"PACK");
    pack.extend_from_slice(&2u32.to_be_bytes()); // version 2
    pack.extend_from_slice(&(objects.len() as u32).to_be_bytes());
    // Object entries
    for (git_type, content) in objects {
        let kind = kind_from_type_byte(*git_type);
        pack.extend_from_slice(&encode_type_size_varint(kind, content.len()));
        pack.extend_from_slice(&zlib_deflate(content));
    }
    // SHA-1 checksum of the whole pack (excluding the trailing 20 bytes).
    let checksum: [u8; 20] = Sha1::digest(&pack).into();
    pack.extend_from_slice(&checksum);
    pack
}

/// Map a stored ref name to the client-facing name by removing the workspace
/// segment. `segment == ""` is the identity (Phase 1 default-repo behavior).
///
/// `refs/workspaces/<id>/heads/main` with segment `workspaces/<id>/`
/// → `refs/heads/main`. A stored name that does not begin with the segment is
/// returned unchanged (defensive; never panics).
pub(crate) fn present_ref(stored: &str, segment: &str) -> String {
    if segment.is_empty() {
        return stored.to_string();
    }
    match stored.strip_prefix(&format!("refs/{segment}")) {
        Some(rest) => format!("refs/{rest}"),
        None => stored.to_string(),
    }
}

/// Map a client-facing ref name to the stored name by inserting the workspace
/// segment immediately after `refs/`. `segment == ""` is the identity.
///
/// `refs/heads/main` with segment `workspaces/<id>/`
/// → `refs/workspaces/<id>/heads/main`. A client name that does not begin with
/// `refs/` is returned unchanged.
pub(crate) fn store_ref(client: &str, segment: &str) -> String {
    if segment.is_empty() {
        return client.to_string();
    }
    match client.strip_prefix("refs/") {
        Some(rest) => format!("refs/{segment}{rest}"),
        None => client.to_string(),
    }
}

/// Handle `GET /:repo/info/refs?service=git-upload-pack`.
///
/// Returns the git smart-HTTP discovery response: a flush-terminated pkt-line
/// stream with the service announcement followed by the list of refs and their
/// SHA-1s (converted from ledge's BLAKE3 object IDs via `sha1_store`).
///
/// If the repository has no refs, we emit the conventional zero-id
/// capabilities advertisement so the client knows what extensions are
/// supported.
///
/// # Errors
/// Returns `LedgeError::Corruption` if a ref's BLAKE3 object ID has no
/// corresponding SHA-1 entry (which would indicate the object store is
/// internally inconsistent).
pub async fn handle_upload_pack_discovery(
    _objects: Arc<dyn ObjectStore>,
    refs: Arc<dyn RefStore>,
    sha1_store: &dyn Sha1Provider,
    segment: &str,
) -> ledge_core::Result<Vec<u8>> {
    let mut out = Vec::new();
    // Service line + flush (required by git smart HTTP spec §3).
    out.extend_from_slice(&encode(b"# service=git-upload-pack\n"));
    out.extend_from_slice(&encode_flush());

    // List only the segment's namespace. segment=="" ⇒ "refs/" (Phase 1).
    let list_prefix = format!("refs/{segment}");
    let mut all_refs = refs.list(&list_prefix).await?;
    // Present client-facing names; sort on the PRESENTED name so HEAD selection
    // and advertisement order match what the client will see.
    let mut presented: Vec<(String, ledge_core::RefEntry)> = all_refs
        .drain(..)
        .map(|(n, e)| (present_ref(n.as_str(), segment), e))
        .collect();
    presented.sort_by(|a, b| a.0.cmp(&b.0));

    if presented.is_empty() {
        // No refs yet — emit the null-id capabilities advertisement.
        out.extend_from_slice(&encode(
            b"0000000000000000000000000000000000000000 capabilities^{}\0\n",
        ));
    } else {
        // Determine the branch HEAD points at. Convention: prefer
        // refs/heads/main, then refs/heads/master, else the first ref.
        // The advertisement leads with HEAD (same SHA-1 as the default
        // branch) plus a `symref=HEAD:<branch>` capability so the client
        // knows which branch to check out after clone. Selection keys off the
        // PRESENTED (client-facing) name.
        let default_ref = presented
            .iter()
            .find(|(n, _)| n == "refs/heads/main")
            .or_else(|| presented.iter().find(|(n, _)| n == "refs/heads/master"))
            .unwrap_or(&presented[0]);
        let head_sha1 = sha1_store.sha1_of(default_ref.1.target).await.ok_or_else(|| {
            LedgeError::Corruption(format!(
                "no SHA-1 for HEAD target object {}",
                default_ref.1.target.to_hex()
            ))
        })?;
        let head_sha1_hex = hex::encode(head_sha1);
        let default_name = default_ref.0.clone();

        // HEAD line carries the capabilities, including the symref hint.
        out.extend_from_slice(&encode(
            format!(
                "{} HEAD\0symref=HEAD:{}\n",
                head_sha1_hex, default_name
            )
            .as_bytes(),
        ));

        for (ref_name, entry) in &presented {
            let sha1 = sha1_store.sha1_of(entry.target).await.ok_or_else(|| {
                LedgeError::Corruption(format!(
                    "no SHA-1 for object {} (ref {})",
                    entry.target.to_hex(),
                    ref_name
                ))
            })?;
            out.extend_from_slice(
                &encode(format!("{} {}\n", hex::encode(sha1), ref_name).as_bytes()),
            );
        }
    }
    out.extend_from_slice(&encode_flush());
    Ok(out)
}

/// Handle `POST /:repo/git-upload-pack`.
///
/// Parses the client's pkt-line request body to collect `want` lines,
/// resolves each wanted SHA-1 back to a BLAKE3 `ObjectId` via the ref store,
/// reads the object content, encodes a packfile, and returns:
///
/// ```text
/// "NAK\n"  <pack-data>
/// ```
///
/// The `NAK` prefix is the git server's signal that it has no ancestors in
/// common with the client (we don't implement shallow clones or have-line
/// negotiation) and is immediately followed by the raw packfile.
///
/// # Errors
/// Propagates `LedgeError` from the ref store or object store.
pub async fn handle_upload_pack(
    body: Bytes,
    objects: Arc<dyn ObjectStore>,
    refs: Arc<dyn RefStore>,
    sha1_store: &dyn Sha1Provider,
    segment: &str,
    cache: Option<&UploadPackCache>,
) -> ledge_core::Result<Vec<u8>> {
    // Fetch is resolved by SHA-1 through `sha1_index`, never by ref name, so the
    // workspace segment does not change object selection. The parameter exists
    // for call-site uniformity with discovery/receive.
    let _ = segment;
    // ── Parse `want` lines from the request body ──────────────────────────────
    let mut cursor: &[u8] = &body;
    let mut wanted_sha1s: Vec<[u8; 20]> = Vec::new();
    loop {
        if cursor.is_empty() {
            break;
        }
        let (line, rem) = decode_line(cursor)?;
        cursor = rem;
        match line {
            // Flush terminates the want list.
            PktLine::Flush => break,
            PktLine::Data(d) => {
                let s = String::from_utf8_lossy(&d);
                if let Some(rest) = s.strip_prefix("want ") {
                    // The want line may carry capability flags after the SHA-1.
                    let sha1_hex = rest.split_whitespace().next().unwrap_or("").trim_end_matches('\n');
                    if sha1_hex.len() == 40 {
                        if let Ok(bytes) = hex::decode(sha1_hex) {
                            let mut arr = [0u8; 20];
                            arr.copy_from_slice(&bytes);
                            wanted_sha1s.push(arr);
                        }
                    }
                }
            }
            PktLine::Delimiter => {}
        }
    }

    // ── Cache lookup: a wanted tip sha uniquely determines its object closure ──
    // so a hit on (segment, sorted wants) serves identical bytes without re-walking
    // the graph or re-encoding the pack. A changed repo yields different wants → miss.
    let cache_key = upload_pack_cache_key(segment, &wanted_sha1s);
    if let Some(c) = cache {
        if let Some(bytes) = c.get(&cache_key) {
            return Ok((*bytes).clone());
        }
    }

    // ── Build the global SHA-1 → ObjectId index ──────────────────────────────
    // A clone must receive the full object closure reachable from each wanted
    // commit (commit → tree(s) → blob(s)), not just the wanted objects
    // themselves.  Children are referenced by git SHA-1, so we need a reverse
    // map from SHA-1 to the BLAKE3-addressed ObjectId for every stored object.
    let _ = refs;
    let sha1_to_obj = sha1_store.sha1_index().await;

    // ── Walk the reachable object closure (BFS) ───────────────────────────────
    // Starting from the wanted SHA-1s, traverse commit/tree references,
    // collecting each object exactly once.  `seen` guards against cycles and
    // shared sub-trees; `queue` drives the breadth-first traversal.
    let mut pack_objects: Vec<(u8, Bytes)> = Vec::new();
    let mut seen: std::collections::HashSet<[u8; 20]> = std::collections::HashSet::new();
    let mut queue: std::collections::VecDeque<[u8; 20]> = wanted_sha1s.iter().copied().collect();

    while let Some(sha1) = queue.pop_front() {
        if !seen.insert(sha1) {
            continue;
        }
        let Some(obj_id) = sha1_to_obj.get(&sha1).copied() else {
            // Unknown SHA-1: skip rather than abort (client may `want` an id we
            // do not have; git will report the missing object).
            continue;
        };
        let Ok(content) = objects.read(obj_id).await else {
            continue;
        };
        // Default to blob (3) if the type byte is somehow unavailable.
        let git_type = sha1_store.git_type_of(obj_id).await.unwrap_or(3);

        // Enqueue children so the entire reachable graph is packed.
        match git_type {
            1 => {
                if let Some(tree) = commit_tree_sha1(&content) {
                    queue.push_back(tree);
                }
                for parent in commit_parent_sha1s(&content) {
                    queue.push_back(parent);
                }
            }
            2 => {
                for child in tree_child_sha1s(&content) {
                    queue.push_back(child);
                }
            }
            _ => {}
        }

        pack_objects.push((git_type, content));
    }

    // ── Encode and return ─────────────────────────────────────────────────────
    let pack = encode_pack(&pack_objects);

    // Smart-HTTP upload-pack response: the NAK acknowledgement is pkt-line
    // framed ("0008NAK\n"); the pack data follows directly as raw bytes (no
    // side-band negotiated in Phase 1).
    let nak = encode(b"NAK\n");
    let mut response = Vec::with_capacity(nak.len() + pack.len());
    response.extend_from_slice(&nak);
    response.extend_from_slice(&pack);

    if let Some(c) = cache {
        c.put(cache_key, std::sync::Arc::new(response.clone()));
    }
    Ok(response)
}

/// Bounded LRU cache of encoded upload-pack responses, keyed by a hash of the
/// (segment, sorted wanted SHA-1s). A wanted tip sha uniquely determines its object
/// closure, so a cached response is never stale: a changed repo yields different
/// wants (a miss). Bounded by entry count AND total bytes; evicts least-recently-used.
pub struct UploadPackCache {
    inner: std::sync::Mutex<CacheInner>,
    max_entries: usize,
    max_bytes: usize,
}
struct CacheInner {
    map: std::collections::HashMap<[u8; 32], std::sync::Arc<Vec<u8>>>,
    order: std::collections::VecDeque<[u8; 32]>,
    bytes: usize,
    hits: u64,
    misses: u64,
}
impl UploadPackCache {
    pub fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            inner: std::sync::Mutex::new(CacheInner {
                map: std::collections::HashMap::new(),
                order: std::collections::VecDeque::new(),
                bytes: 0,
                hits: 0,
                misses: 0,
            }),
            max_entries,
            max_bytes,
        }
    }
    pub fn hits(&self) -> u64 {
        self.inner.lock().unwrap().hits
    }
    pub fn misses(&self) -> u64 {
        self.inner.lock().unwrap().misses
    }
    pub fn get(&self, key: &[u8; 32]) -> Option<std::sync::Arc<Vec<u8>>> {
        let mut g = self.inner.lock().unwrap();
        if let Some(v) = g.map.get(key).cloned() {
            g.hits += 1;
            g.order.retain(|k| k != key);
            g.order.push_front(*key);
            Some(v)
        } else {
            g.misses += 1;
            None
        }
    }
    pub fn put(&self, key: [u8; 32], val: std::sync::Arc<Vec<u8>>) {
        let mut g = self.inner.lock().unwrap();
        if g.map.contains_key(&key) {
            return;
        }
        g.bytes += val.len();
        g.map.insert(key, val);
        g.order.push_front(key);
        while g.order.len() > self.max_entries || g.bytes > self.max_bytes {
            match g.order.pop_back() {
                Some(old) => {
                    if let Some(v) = g.map.remove(&old) {
                        g.bytes -= v.len();
                    }
                }
                None => break,
            }
        }
    }
}

/// Cache key for a clone request: hash of segment + the sorted wanted SHA-1s.
pub fn upload_pack_cache_key(segment: &str, wanted_sha1s: &[[u8; 20]]) -> [u8; 32] {
    let mut sorted = wanted_sha1s.to_vec();
    sorted.sort_unstable();
    let mut h = blake3::Hasher::new();
    h.update(segment.as_bytes());
    h.update(&[0u8]);
    for w in &sorted {
        h.update(w);
    }
    *h.finalize().as_bytes()
}

/// Process-global cache the server routes share (a pure content-keyed memoization).
pub fn global_upload_cache() -> &'static UploadPackCache {
    static C: std::sync::OnceLock<UploadPackCache> = std::sync::OnceLock::new();
    C.get_or_init(|| UploadPackCache::new(32, 256 * 1024 * 1024))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use ledge_core::{LedgeError, ObjectId, RefEntry, RefName, RefSnapshot};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    struct MemObjectStore {
        objects: Mutex<HashMap<[u8; 32], Bytes>>,
        sha1s: Mutex<HashMap<[u8; 32], [u8; 20]>>,
        types: Mutex<HashMap<[u8; 32], u8>>,
    }
    impl MemObjectStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                objects: Mutex::new(HashMap::new()),
                sha1s: Mutex::new(HashMap::new()),
                types: Mutex::new(HashMap::new()),
            })
        }
        fn seed(&self, content: Bytes, sha1: [u8; 20]) -> ObjectId {
            let hash = *blake3::hash(&content).as_bytes();
            let id = ObjectId::from_bytes(hash);
            self.objects.lock().unwrap().insert(hash, content);
            self.sha1s.lock().unwrap().insert(hash, sha1);
            self.types.lock().unwrap().insert(hash, 3); // blob
            id
        }
    }
    #[async_trait]
    impl Sha1Provider for MemObjectStore {
        async fn sha1_of(&self, id: ObjectId) -> Option<[u8; 20]> {
            self.sha1s.lock().unwrap().get(id.as_bytes()).copied()
        }
        async fn git_type_of(&self, id: ObjectId) -> Option<u8> {
            self.types.lock().unwrap().get(id.as_bytes()).copied()
        }
        async fn write_git_object(
            &self,
            git_type: u8,
            content: bytes::Bytes,
        ) -> ledge_core::Result<ObjectId> {
            let hash = *blake3::hash(&content).as_bytes();
            let id = ObjectId::from_bytes(hash);
            self.objects.lock().unwrap().insert(hash, content);
            self.types.lock().unwrap().insert(hash, git_type);
            Ok(id)
        }
        async fn sha1_index(&self) -> std::sync::Arc<HashMap<[u8; 20], ObjectId>> {
            std::sync::Arc::new(
                self.sha1s
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(blake, sha1)| (*sha1, ObjectId::from_bytes(*blake)))
                    .collect(),
            )
        }
        async fn read_git_object_by_sha1(&self, sha1: &[u8; 20]) -> Option<(u8, Vec<u8>)> {
            let blake = *self
                .sha1s
                .lock()
                .unwrap()
                .iter()
                .find(|(_, s)| *s == sha1)
                .map(|(b, _)| b)?;
            let content = self.objects.lock().unwrap().get(&blake).cloned()?;
            let ty = self.types.lock().unwrap().get(&blake).copied()?;
            Some((ty, content.to_vec()))
        }
    }
    #[async_trait]
    impl ledge_core::ObjectStore for MemObjectStore {
        async fn write(&self, content: Bytes) -> ledge_core::Result<ObjectId> {
            let hash = *blake3::hash(&content).as_bytes();
            let id = ObjectId::from_bytes(hash);
            self.objects.lock().unwrap().insert(hash, content);
            Ok(id)
        }
        async fn write_batch(&self, cs: Vec<Bytes>) -> ledge_core::Result<Vec<ObjectId>> {
            let mut ids = vec![];
            for c in cs {
                ids.push(self.write(c).await?);
            }
            Ok(ids)
        }
        async fn read(&self, id: ObjectId) -> ledge_core::Result<Bytes> {
            self.objects
                .lock()
                .unwrap()
                .get(id.as_bytes())
                .cloned()
                .ok_or(LedgeError::NotFound(id))
        }
        async fn exists(&self, id: ObjectId) -> ledge_core::Result<bool> {
            Ok(self.objects.lock().unwrap().contains_key(id.as_bytes()))
        }
    }

    struct MemRefStore {
        refs: Mutex<HashMap<String, RefEntry>>,
    }
    impl MemRefStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                refs: Mutex::new(HashMap::new()),
            })
        }
        fn insert(&self, name: &str, target: ObjectId) {
            self.refs.lock().unwrap().insert(
                name.to_string(),
                RefEntry {
                    target,
                    hlc: 1,
                    version: 1,
                },
            );
        }
    }
    struct MemRefSnapshot(HashMap<String, RefEntry>);
    impl RefSnapshot for MemRefSnapshot {
        fn get(&self, name: &RefName) -> Option<RefEntry> {
            self.0.get(name.as_str()).cloned()
        }
        fn list(&self, prefix: &str) -> Vec<(RefName, RefEntry)> {
            self.0
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| (RefName::new(k).unwrap(), v.clone()))
                .collect()
        }
    }
    #[async_trait]
    impl ledge_core::RefStore for MemRefStore {
        async fn get(&self, name: &RefName) -> ledge_core::Result<Option<RefEntry>> {
            Ok(self.refs.lock().unwrap().get(name.as_str()).cloned())
        }
        async fn update(
            &self,
            name: &RefName,
            new: ObjectId,
            _: Option<ObjectId>,
        ) -> ledge_core::Result<RefEntry> {
            let e = RefEntry {
                target: new,
                hlc: 2,
                version: 2,
            };
            self.refs
                .lock()
                .unwrap()
                .insert(name.as_str().to_string(), e.clone());
            Ok(e)
        }
        async fn delete(&self, name: &RefName, _: ObjectId) -> ledge_core::Result<()> {
            self.refs.lock().unwrap().remove(name.as_str());
            Ok(())
        }
        async fn list(&self, prefix: &str) -> ledge_core::Result<Vec<(RefName, RefEntry)>> {
            let map = self.refs.lock().unwrap();
            let mut r: Vec<_> = map
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| (RefName::new(k).unwrap(), v.clone()))
                .collect();
            r.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
            Ok(r)
        }
        fn snapshot(&self) -> Arc<dyn RefSnapshot> {
            Arc::new(MemRefSnapshot(
                self.refs
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            ))
        }
    }

    fn make_sha1(seed: u8) -> [u8; 20] {
        let mut s = [0u8; 20];
        s[0] = seed;
        s[1] = 0xAB;
        s[2] = 0xCD;
        s
    }

    #[tokio::test]
    async fn discovery_returns_service_header() {
        let objects = MemObjectStore::new();
        let refs = MemRefStore::new();
        let response = handle_upload_pack_discovery(
            objects.clone() as Arc<dyn ledge_core::ObjectStore>,
            refs.clone() as Arc<dyn ledge_core::RefStore>,
            objects.as_ref(),
            "",
        )
        .await
        .unwrap();
        let (first, rest) = crate::pkt_line::decode_line(&response).unwrap();
        assert!(
            matches!(first, crate::pkt_line::PktLine::Data(d) if d == b"# service=git-upload-pack\n")
        );
        let (second, _) = crate::pkt_line::decode_line(rest).unwrap();
        assert!(matches!(second, crate::pkt_line::PktLine::Flush));
    }

    #[tokio::test]
    async fn discovery_with_refs_contains_sha1() {
        let objects = MemObjectStore::new();
        let refs = MemRefStore::new();
        let sha1 = make_sha1(0x01);
        let id = objects.seed(Bytes::from_static(b"blob content"), sha1);
        refs.insert("refs/heads/main", id);
        let response = handle_upload_pack_discovery(
            objects.clone() as Arc<dyn ledge_core::ObjectStore>,
            refs.clone() as Arc<dyn ledge_core::RefStore>,
            objects.as_ref(),
            "",
        )
        .await
        .unwrap();
        let sha1_hex = hex::encode(sha1);
        let mut found = false;
        let mut cursor: &[u8] = &response;
        // The discovery response contains two flush packets:
        //   1. After the "# service=git-upload-pack\n" line
        //   2. After the ref advertisement
        // We scan the entire buffer so that we don't stop at the first flush
        // before reaching the ref lines.
        while !cursor.is_empty() {
            let (line, rem) = crate::pkt_line::decode_line(cursor).unwrap();
            cursor = rem;
            if let crate::pkt_line::PktLine::Data(d) = line {
                let s = String::from_utf8_lossy(&d);
                if s.contains(&sha1_hex) && s.contains("refs/heads/main") {
                    found = true;
                }
            }
        }
        assert!(found, "discovery must contain SHA-1 and ref name");
    }

    #[tokio::test]
    async fn discovery_workspace_segment_presents_stripped_names() {
        let objects = MemObjectStore::new();
        let refs = MemRefStore::new();
        let sha1 = make_sha1(0x07);
        let id = objects.seed(Bytes::from_static(b"ws blob"), sha1);
        // Stored under the workspace namespace.
        refs.insert("refs/workspaces/abc/heads/main", id);
        // A durable ref that must NOT appear in the workspace advertisement.
        let other = objects.seed(Bytes::from_static(b"durable blob"), make_sha1(0x08));
        refs.insert("refs/heads/main", other);

        let response = handle_upload_pack_discovery(
            objects.clone() as Arc<dyn ledge_core::ObjectStore>,
            refs.clone() as Arc<dyn ledge_core::RefStore>,
            objects.as_ref(),
            "workspaces/abc/",
        )
        .await
        .unwrap();

        let sha1_hex = hex::encode(sha1);
        let mut saw_presented = false;
        let mut saw_stored = false;
        let mut saw_head_symref = false;
        let mut cursor: &[u8] = &response;
        while !cursor.is_empty() {
            let (line, rem) = crate::pkt_line::decode_line(cursor).unwrap();
            cursor = rem;
            if let crate::pkt_line::PktLine::Data(d) = line {
                let s = String::from_utf8_lossy(&d);
                if s.contains(&sha1_hex) && s.contains("refs/heads/main") { saw_presented = true; }
                if s.contains("refs/workspaces/abc/") { saw_stored = true; }
                if s.contains("symref=HEAD:refs/heads/main") { saw_head_symref = true; }
            }
        }
        assert!(saw_presented, "must advertise the stripped client name refs/heads/main");
        assert!(!saw_stored, "must NOT leak the stored workspace-prefixed name");
        assert!(saw_head_symref, "HEAD symref must point at the stripped name");
    }

    #[tokio::test]
    async fn upload_pack_starts_with_nak_then_pack() {
        let objects = MemObjectStore::new();
        let refs = MemRefStore::new();
        let sha1 = make_sha1(0x02);
        let id = objects.seed(Bytes::from(b"some blob".to_vec()), sha1);
        refs.insert("refs/heads/main", id);
        let sha1_hex = hex::encode(sha1);
        let mut req = Vec::new();
        req.extend_from_slice(&crate::pkt_line::encode(
            format!("want {}\n", sha1_hex).as_bytes(),
        ));
        req.extend_from_slice(&crate::pkt_line::encode_flush());
        req.extend_from_slice(&crate::pkt_line::encode(b"done\n"));
        let pack = handle_upload_pack(
            Bytes::from(req),
            objects.clone() as Arc<dyn ledge_core::ObjectStore>,
            refs.clone() as Arc<dyn ledge_core::RefStore>,
            objects.as_ref(),
            "",
            None,
        )
        .await
        .unwrap();
        // NAK is pkt-line framed: "0008NAK\n" (8 bytes), then pack data.
        assert!(pack.starts_with(b"0008NAK\n"));
        assert!(pack[8..].starts_with(b"PACK"));
    }

    #[tokio::test]
    async fn upload_pack_segment_is_want_resolved_not_ref_scoped() {
        let objects = MemObjectStore::new();
        let refs = MemRefStore::new();
        let sha1 = make_sha1(0x09);
        let id = objects.seed(Bytes::from(b"ws fetch blob".to_vec()), sha1);
        refs.insert("refs/workspaces/abc/heads/main", id);
        let mut req = Vec::new();
        req.extend_from_slice(&crate::pkt_line::encode(
            format!("want {}\n", hex::encode(sha1)).as_bytes(),
        ));
        req.extend_from_slice(&crate::pkt_line::encode_flush());
        req.extend_from_slice(&crate::pkt_line::encode(b"done\n"));
        let pack = handle_upload_pack(
            Bytes::from(req),
            objects.clone() as Arc<dyn ledge_core::ObjectStore>,
            refs.clone() as Arc<dyn ledge_core::RefStore>,
            objects.as_ref(),
            "workspaces/abc/",
            None,
        )
        .await
        .unwrap();
        assert!(pack.starts_with(b"0008NAK\n"));
        assert!(pack[8..].starts_with(b"PACK"));
    }

    #[tokio::test]
    async fn upload_pack_correct_object_count() {
        let objects = MemObjectStore::new();
        let refs = MemRefStore::new();
        let sha1_a = make_sha1(0x0A);
        let sha1_b = make_sha1(0x0B);
        let id_a = objects.seed(Bytes::from(b"object A".to_vec()), sha1_a);
        let id_b = objects.seed(Bytes::from(b"object B".to_vec()), sha1_b);
        refs.insert("refs/heads/main", id_a);
        refs.insert("refs/heads/dev", id_b);
        let mut req = Vec::new();
        req.extend_from_slice(&crate::pkt_line::encode(
            format!("want {}\n", hex::encode(sha1_a)).as_bytes(),
        ));
        req.extend_from_slice(&crate::pkt_line::encode(
            format!("want {}\n", hex::encode(sha1_b)).as_bytes(),
        ));
        req.extend_from_slice(&crate::pkt_line::encode_flush());
        req.extend_from_slice(&crate::pkt_line::encode(b"done\n"));
        let pack = handle_upload_pack(
            Bytes::from(req),
            objects.clone() as Arc<dyn ledge_core::ObjectStore>,
            refs.clone() as Arc<dyn ledge_core::RefStore>,
            objects.as_ref(),
            "",
            None,
        )
        .await
        .unwrap();
        assert!(pack.starts_with(b"0008NAK\n"));
        let pd = &pack[8..];
        assert_eq!(&pd[..4], b"PACK");
        assert_eq!(u32::from_be_bytes(pd[4..8].try_into().unwrap()), 2u32);
        assert_eq!(u32::from_be_bytes(pd[8..12].try_into().unwrap()), 2u32);
    }

    #[tokio::test]
    async fn upload_pack_cache_hit_serves_identical_bytes() {
        let objects = MemObjectStore::new();
        let refs = MemRefStore::new();
        let sha1 = make_sha1(0x42);
        let id = objects.seed(Bytes::from(b"cache me".to_vec()), sha1);
        refs.insert("refs/heads/main", id);
        let mut req = Vec::new();
        req.extend_from_slice(&crate::pkt_line::encode(
            format!("want {}\n", hex::encode(sha1)).as_bytes(),
        ));
        req.extend_from_slice(&crate::pkt_line::encode_flush());
        req.extend_from_slice(&crate::pkt_line::encode(b"done\n"));
        let body = Bytes::from(req);
        let store_arc = objects.clone() as Arc<dyn ledge_core::ObjectStore>;
        let refs_arc = refs.clone() as Arc<dyn ledge_core::RefStore>;
        let cache = UploadPackCache::new(8, 64 * 1024 * 1024);
        let first = handle_upload_pack(
            body.clone(),
            store_arc.clone(),
            refs_arc.clone(),
            objects.as_ref(),
            "",
            Some(&cache),
        )
        .await
        .unwrap();
        assert_eq!(cache.misses(), 1);
        let second = handle_upload_pack(
            body.clone(),
            store_arc.clone(),
            refs_arc.clone(),
            objects.as_ref(),
            "",
            Some(&cache),
        )
        .await
        .unwrap();
        assert_eq!(cache.hits(), 1, "second identical request is a cache hit");
        assert_eq!(first, second, "cached bytes identical to fresh build");
    }

    #[test]
    fn cache_evicts_lru_and_keys_by_wantset() {
        let c = UploadPackCache::new(2, 1 << 30);
        let k = |n: u8| upload_pack_cache_key("", &[[n; 20]]);
        c.put(k(1), std::sync::Arc::new(vec![1]));
        c.put(k(2), std::sync::Arc::new(vec![2]));
        assert!(c.get(&k(1)).is_some()); // touch 1 (now MRU)
        c.put(k(3), std::sync::Arc::new(vec![3])); // evicts LRU = 2
        assert!(c.get(&k(2)).is_none(), "LRU evicted");
        assert!(c.get(&k(1)).is_some());
        assert!(c.get(&k(3)).is_some());
        assert_ne!(
            upload_pack_cache_key("a", &[[1; 20]]),
            upload_pack_cache_key("b", &[[1; 20]])
        );
    }

    #[test]
    fn present_ref_empty_segment_is_identity() {
        assert_eq!(present_ref("refs/heads/main", ""), "refs/heads/main");
        assert_eq!(present_ref("refs/tags/v1", ""), "refs/tags/v1");
    }

    #[test]
    fn store_ref_empty_segment_is_identity() {
        assert_eq!(store_ref("refs/heads/main", ""), "refs/heads/main");
        assert_eq!(store_ref("refs/tags/v1", ""), "refs/tags/v1");
    }

    #[test]
    fn present_ref_strips_workspace_segment() {
        assert_eq!(
            present_ref("refs/workspaces/abc/heads/main", "workspaces/abc/"),
            "refs/heads/main"
        );
        assert_eq!(
            present_ref("refs/workspaces/abc/tags/v1", "workspaces/abc/"),
            "refs/tags/v1"
        );
    }

    #[test]
    fn store_ref_inserts_workspace_segment() {
        assert_eq!(
            store_ref("refs/heads/main", "workspaces/abc/"),
            "refs/workspaces/abc/heads/main"
        );
        assert_eq!(
            store_ref("refs/tags/v1", "workspaces/abc/"),
            "refs/workspaces/abc/tags/v1"
        );
    }

    #[test]
    fn present_store_roundtrip_both_segments() {
        // Phase 4d-2: a multi-segment tenant prefix ("tenants/<t>/") composes
        // identically to the workspace segment — the machinery is a pure string
        // insert/strip after `refs/`, transparent to segment depth.
        for seg in ["", "workspaces/abc/", "tenants/acme/"] {
            for client in ["refs/heads/main", "refs/tags/v1", "refs/heads/feature/x"] {
                assert_eq!(present_ref(&store_ref(client, seg), seg), client);
            }
        }
    }

    #[test]
    fn present_store_roundtrip_tenant_segment() {
        // refs/heads/main ↔ refs/tenants/acme/heads/main (durable default-repo
        // ref for a named tenant — spec §3.1).
        assert_eq!(
            store_ref("refs/heads/main", "tenants/acme/"),
            "refs/tenants/acme/heads/main"
        );
        assert_eq!(
            present_ref("refs/tenants/acme/heads/main", "tenants/acme/"),
            "refs/heads/main"
        );
        // A different tenant's stored ref does NOT match acme's segment ⇒ filtered
        // out of acme's view (discovery lists only `refs/tenants/acme/`, but prove
        // present_ref is defensive on a non-matching name too).
        assert_eq!(
            present_ref("refs/tenants/globex/heads/main", "tenants/acme/"),
            "refs/tenants/globex/heads/main"
        );
    }

    #[test]
    fn present_ref_passes_through_non_matching_segment() {
        // A stored ref that does not begin with the segment is returned unchanged
        // (defensive: a list() prefix guarantees a match, but never panic).
        assert_eq!(present_ref("refs/heads/main", "workspaces/abc/"), "refs/heads/main");
    }

    #[test]
    fn encode_pack_empty() {
        let p = encode_pack(&[]);
        assert_eq!(&p[..4], b"PACK");
        assert_eq!(u32::from_be_bytes(p[4..8].try_into().unwrap()), 2u32);
        assert_eq!(u32::from_be_bytes(p[8..12].try_into().unwrap()), 0u32);
        assert_eq!(p.len(), 32); // 12 header + 20 SHA-1 footer
    }

    #[test]
    fn encode_pack_one_object() {
        let p = encode_pack(&[(3u8, Bytes::from(b"hello world".to_vec()))]);
        assert_eq!(&p[..4], b"PACK");
        assert_eq!(u32::from_be_bytes(p[8..12].try_into().unwrap()), 1u32);
        assert!(p.len() > 32);
    }
}
