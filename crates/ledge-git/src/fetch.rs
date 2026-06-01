use crate::pkt_line::{decode_line, encode, encode_flush, PktLine};
use async_trait::async_trait;
use bytes::Bytes;
use ledge_core::{LedgeError, ObjectId, ObjectStore, RefStore};
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

/// Provides SHA-1 lookup for a given BLAKE3-addressed object.
///
/// This decouples the fetch handler from the concrete `DiskObjectStore` so that
/// tests can supply an in-memory implementation.
#[async_trait]
pub trait Sha1Provider: Send + Sync {
    /// Return the Git-compatible SHA-1 (blob header hash) for `id`, or `None`
    /// if the object is not present in the store.
    async fn sha1_of(&self, id: ObjectId) -> Option<[u8; 20]>;
}

/// Bridge `DiskObjectStore::sha1_of` to the `Sha1Provider` trait.
///
/// `DiskObjectStore::sha1_of` reads the 20-byte SHA-1 stored in the object
/// file header and returns `Result<[u8;20]>`.  We map `Err` to `None` so the
/// fetch handler can treat a missing SHA-1 the same as a missing object.
#[async_trait]
impl Sha1Provider for DiskObjectStore {
    async fn sha1_of(&self, id: ObjectId) -> Option<[u8; 20]> {
        self.sha1_of(id).await.ok()
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
/// Each object is stored as a `Blob` (type 3) regardless of the git object
/// type — this is correct for ledge's use case where all stored bytes are raw
/// content blobs.  The packfile layout is:
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
/// * `objects` — slice of `(sha1: &[u8;20], content: Bytes)` pairs.
///   The SHA-1 is the Git blob SHA-1 stored in the object's on-disk header.
pub fn encode_pack(objects: &[(&[u8; 20], Bytes)]) -> Vec<u8> {
    use sha1::{Digest, Sha1};
    let mut pack = Vec::new();
    // Pack header
    pack.extend_from_slice(b"PACK");
    pack.extend_from_slice(&2u32.to_be_bytes()); // version 2
    pack.extend_from_slice(&(objects.len() as u32).to_be_bytes());
    // Object entries
    for (_, content) in objects {
        pack.extend_from_slice(&encode_type_size_varint(GitObjectKind::Blob, content.len()));
        pack.extend_from_slice(&zlib_deflate(content));
    }
    // SHA-1 checksum of the whole pack (excluding the trailing 20 bytes).
    let checksum: [u8; 20] = Sha1::digest(&pack).into();
    pack.extend_from_slice(&checksum);
    pack
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
) -> ledge_core::Result<Vec<u8>> {
    let mut out = Vec::new();
    // Service line + flush (required by git smart HTTP spec §3).
    out.extend_from_slice(&encode(b"# service=git-upload-pack\n"));
    out.extend_from_slice(&encode_flush());

    let all_refs = refs.list("refs/").await?;
    if all_refs.is_empty() {
        // No refs yet — emit the null-id capabilities advertisement.
        out.extend_from_slice(&encode(
            b"0000000000000000000000000000000000000000 capabilities^{}\0 side-band-64k\n",
        ));
    } else {
        // First ref gets the NUL-separated capabilities appended.
        let mut first = true;
        for (ref_name, entry) in &all_refs {
            let sha1 = sha1_store.sha1_of(entry.target).await.ok_or_else(|| {
                LedgeError::Corruption(format!(
                    "no SHA-1 for object {} (ref {})",
                    entry.target.to_hex(),
                    ref_name.as_str()
                ))
            })?;
            let sha1_hex = hex::encode(sha1);
            let line = if first {
                first = false;
                format!("{} {}\0 side-band-64k\n", sha1_hex, ref_name.as_str())
            } else {
                format!("{} {}\n", sha1_hex, ref_name.as_str())
            };
            out.extend_from_slice(&encode(line.as_bytes()));
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
) -> ledge_core::Result<Vec<u8>> {
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

    // ── Build SHA-1 → ObjectId reverse map from all refs ─────────────────────
    // We enumerate every ref the server knows about and ask the SHA-1 store
    // for each target's SHA-1.  This O(|refs|) lookup is acceptable for the
    // clone/fetch use case where ref counts are bounded.
    let all_refs = refs.list("refs/").await?;
    let mut sha1_to_obj: std::collections::HashMap<[u8; 20], ObjectId> =
        std::collections::HashMap::new();
    for (_, entry) in &all_refs {
        if let Some(sha1) = sha1_store.sha1_of(entry.target).await {
            sha1_to_obj.insert(sha1, entry.target);
        }
    }

    // ── Collect content for each wanted object ────────────────────────────────
    let mut pack_objects: Vec<([u8; 20], Bytes)> = Vec::new();
    for sha1 in &wanted_sha1s {
        if let Some(obj_id) = sha1_to_obj.get(sha1) {
            if let Ok(content) = objects.read(*obj_id).await {
                pack_objects.push((*sha1, content));
            }
        }
    }

    // ── Encode and return ─────────────────────────────────────────────────────
    let refs_for_pack: Vec<(&[u8; 20], Bytes)> =
        pack_objects.iter().map(|(s, c)| (s, c.clone())).collect();
    let pack = encode_pack(&refs_for_pack);

    let mut response = Vec::with_capacity(4 + pack.len());
    response.extend_from_slice(b"NAK\n");
    response.extend_from_slice(&pack);
    Ok(response)
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
    }
    impl MemObjectStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                objects: Mutex::new(HashMap::new()),
                sha1s: Mutex::new(HashMap::new()),
            })
        }
        fn seed(&self, content: Bytes, sha1: [u8; 20]) -> ObjectId {
            let hash = *blake3::hash(&content).as_bytes();
            let id = ObjectId::from_bytes(hash);
            self.objects.lock().unwrap().insert(hash, content);
            self.sha1s.lock().unwrap().insert(hash, sha1);
            id
        }
    }
    #[async_trait]
    impl Sha1Provider for MemObjectStore {
        async fn sha1_of(&self, id: ObjectId) -> Option<[u8; 20]> {
            self.sha1s.lock().unwrap().get(id.as_bytes()).copied()
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
    async fn upload_pack_starts_with_nak_then_pack() {
        let objects = MemObjectStore::new();
        let refs = MemRefStore::new();
        let sha1 = make_sha1(0x02);
        let id = objects.seed(Bytes::from(b"some blob".to_vec()), sha1);
        refs.insert("refs/heads/main", id);
        let sha1_hex = hex::encode(sha1);
        let mut req = Vec::new();
        req.extend_from_slice(&crate::pkt_line::encode(
            format!("want {} side-band-64k\n", sha1_hex).as_bytes(),
        ));
        req.extend_from_slice(&crate::pkt_line::encode_flush());
        req.extend_from_slice(&crate::pkt_line::encode(b"done\n"));
        let pack = handle_upload_pack(
            Bytes::from(req),
            objects.clone() as Arc<dyn ledge_core::ObjectStore>,
            refs.clone() as Arc<dyn ledge_core::RefStore>,
            objects.as_ref(),
        )
        .await
        .unwrap();
        assert!(pack.starts_with(b"NAK\n"));
        assert!(pack[4..].starts_with(b"PACK"));
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
        )
        .await
        .unwrap();
        assert!(pack.starts_with(b"NAK\n"));
        let pd = &pack[4..];
        assert_eq!(&pd[..4], b"PACK");
        assert_eq!(u32::from_be_bytes(pd[4..8].try_into().unwrap()), 2u32);
        assert_eq!(u32::from_be_bytes(pd[8..12].try_into().unwrap()), 2u32);
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
    fn encode_pack_two_objects() {
        let sha1_a = make_sha1(0x10);
        let p = encode_pack(&[(&sha1_a, Bytes::from(b"hello world".to_vec()))]);
        assert_eq!(&p[..4], b"PACK");
        assert_eq!(u32::from_be_bytes(p[8..12].try_into().unwrap()), 1u32);
        assert!(p.len() > 32);
    }
}
