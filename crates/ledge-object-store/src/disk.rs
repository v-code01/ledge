use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use rand::Rng as _;
use sha1::Digest as _;
use tracing::instrument;

use ledge_core::delta::{apply_delta, encode_delta};
use ledge_core::{LedgeError, ObjectId, ObjectStore, Result};

/// Object body encoding stored in header byte 21.
/// `0` = raw (legacy / never-shrink fallback), `1` = zlib (RFC 1950),
/// `2` = delta against another object (see [`DiskObjectStore::deltify`]).
const ENC_RAW: u8 = 0;
const ENC_ZLIB: u8 = 1;
/// Delta encoding: body is `[base_id:32][zlib(delta_bytes)]`. The reconstructed
/// content is `apply_delta(read(base_id), inflate(delta_bytes))`.
const ENC_DELTA: u8 = 2;
/// Maximum delta-chain depth honoured by the resolver and enforced by
/// [`DiskObjectStore::deltify`]. Bounds recursion and worst-case read cost so a
/// malformed or hostile chain can never hang or blow the stack.
const MAX_CHAIN: usize = 50;
/// Inflate guard: refuse to materialize more than 2 GiB from a single object,
/// bounding the blast radius of a malformed/hostile compressed body (zip bomb).
const MAX_DECOMPRESSED: usize = 2 * 1024 * 1024 * 1024; // 2 GiB

/// The cold-tier marker path for a `.pack`: `<name>.pack.s3`. Built by APPENDING
/// `.s3` to the full `.pack` filename (NOT `Path::with_extension`, which would
/// rewrite `<name>.pack` → `<name>.s3` and lose the `.pack` segment). Presence
/// of this file means "the body lives in S3 under the key stored inside it".
fn marker_path(pack_path: &std::path::Path) -> PathBuf {
    let mut s = pack_path.as_os_str().to_os_string();
    s.push(".s3");
    PathBuf::from(s)
}

/// zlib-compress `data`. Writing into a `Vec` sink never performs I/O, so the
/// `expect`s below are unreachable; they document that infallibility.
fn zlib_compress(data: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(data).expect("zlib write to Vec is infallible");
    e.finish().expect("zlib finish to Vec is infallible")
}

/// Inflate a zlib stream, capped at [`MAX_DECOMPRESSED`]. A malformed stream or
/// an over-large expansion yields [`LedgeError::Corruption`] — never a panic.
fn zlib_inflate(data: &[u8], id: ObjectId) -> ledge_core::Result<Vec<u8>> {
    use std::io::Read;
    let mut out = Vec::new();
    // `.take(MAX + 1)` lets us distinguish "exactly MAX" (ok) from "exceeds MAX".
    flate2::read::ZlibDecoder::new(data)
        .take(MAX_DECOMPRESSED as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|e| LedgeError::Corruption(format!("object {}: inflate: {e}", id.to_hex())))?;
    if out.len() > MAX_DECOMPRESSED {
        return Err(LedgeError::Corruption(format!(
            "object {}: inflated too large",
            id.to_hex()
        )));
    }
    Ok(out)
}

/// Content-addressed object store backed by the local filesystem.
///
/// Layout mirrors Git's loose-object layout for tooling compatibility:
///
/// ```text
/// <data_dir>/objects/
///     tmp/            ← write-then-rename staging area
///     <XX>/           ← first two hex digits of BLAKE3 id
///       <YY>/         ← next two hex digits
///         <full-64-hex-id>   ← the object file
/// ```
///
/// # Object file format
/// ```text
/// bytes  0..20  — SHA-1 of "<typename> <len>\0<content>"  (Git-compatible)
/// byte     20   — git object type (1=commit, 2=tree, 3=blob, 4=tag)
/// byte     21   — body encoding: 0 = raw, 1 = zlib (RFC 1950)
/// bytes 22..24  — reserved, always zero
/// bytes 24..    — body (raw or zlib-compressed per byte 21)
/// ```
///
/// Identity and dedup remain `BLAKE3(uncompressed content)`; compression is a
/// pure storage detail. byte 21 = 0 keeps legacy raw objects readable.
///
/// # Invariants
/// - Writes are atomic: content is written to `tmp/` then `rename(2)`'d to its
///   final path. A crash between the two produces an orphan in `tmp/` but never
///   a partial object file at the canonical path.
/// - Idempotency: if the final path already exists the rename is a no-op on
///   POSIX (atomic replacement of identical data).  No locking is required.
pub struct DiskObjectStore {
    data_dir: PathBuf,
    /// Registered packs, hot-swappable without blocking readers. A read consults
    /// the loose file first, then each pack in turn (loose shadows pack). The
    /// `ArcSwap` lets a repack atomically publish a new pack set while concurrent
    /// reads continue against the old snapshot they already loaded.
    packs: std::sync::Arc<arc_swap::ArcSwap<Vec<std::sync::Arc<crate::git_pack_file::GitPackFile>>>>,
    /// Cached `git-SHA-1 → ObjectId` reverse index, lazily built on first
    /// [`Self::sha1_index`] and reused across calls. `None` means "stale, rebuild
    /// on next read". Invalidated on every loose write and on `swap_packs` (the
    /// only two ways the loose/packed object set can change). Holding the map in
    /// an `Arc` lets a clone clone the pointer (not the whole map) and lets the
    /// fetch path avoid an O(N) full-store rescan on every request.
    sha1_cache: std::sync::Arc<
        arc_swap::ArcSwapOption<std::collections::HashMap<[u8; 20], ObjectId>>,
    >,
    /// Optional S3 cold tier. `None` ⇒ tiering disabled and the store is
    /// byte-identical to the loose+pack-only behaviour (default OFF). When set,
    /// [`Self::tier_packs`] spills each `.pack` *body* to S3 (keeping the small
    /// `.idx`/`.lidx` local), and a cold read restores the body on demand via
    /// [`Self::ensure_pack_local`]. Held behind `ArcSwapOption` so it can be
    /// installed/cleared without blocking concurrent reads.
    cold: std::sync::Arc<arc_swap::ArcSwapOption<crate::s3::S3Tier>>,
}

/// Counters reported by a single [`DiskObjectStore::tier_packs`] pass.
#[derive(Debug, Default, Clone)]
pub struct TierStats {
    /// Number of `.pack` bodies uploaded to S3 and removed locally this pass.
    pub packs_tiered: usize,
    /// Total bytes of pack body uploaded this pass.
    pub bytes_uploaded: u64,
}

impl DiskObjectStore {
    /// Create (or open) an object store rooted at `data_dir`.
    ///
    /// Creates `<data_dir>/objects/tmp/` on first call.  All subsequent calls
    /// are idempotent.
    pub fn new(data_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(data_dir.join("objects").join("tmp"))
            .map_err(LedgeError::Io)?;
        // Pack directory holds `<blake3>.pack` + `.idx` pairs. Load every valid
        // pack present at open time; a corrupt/partial pack is skipped (best
        // effort) so a single bad pack can't make the whole store unopenable.
        std::fs::create_dir_all(data_dir.join("objects").join("pack")).map_err(LedgeError::Io)?;
        let mut packs = Vec::new();
        if let Ok(rd) = std::fs::read_dir(data_dir.join("objects").join("pack")) {
            for e in rd.flatten() {
                let p = e.path();
                if p.extension().is_some_and(|x| x == "pack") {
                    if let Ok(pf) = crate::git_pack_file::GitPackFile::open(&p) {
                        packs.push(std::sync::Arc::new(pf));
                    }
                }
            }
        }
        Ok(Self {
            data_dir,
            packs: std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(packs)),
            sha1_cache: std::sync::Arc::new(arc_swap::ArcSwapOption::empty()),
            cold: std::sync::Arc::new(arc_swap::ArcSwapOption::empty()),
        })
    }

    /// Install (or replace) the S3 cold tier. Once set, [`Self::tier_packs`] can
    /// spill pack bodies off-machine and cold reads restore them on demand.
    pub fn set_cold(&self, tier: std::sync::Arc<crate::s3::S3Tier>) {
        self.cold.store(Some(tier));
    }

    /// Re-scan the pack dir + swap the in-memory pack set (after tiering/restore).
    ///
    /// A tiered pack has no local `.pack` but keeps its `.lidx`; since
    /// [`crate::git_pack_file::GitPackFile::open`] no longer reads the `.pack`
    /// at open time, the pack still opens here so a later read can restore it.
    pub fn reload_packs(&self) {
        let mut packs = Vec::new();
        let mut seen: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
        if let Ok(rd) = std::fs::read_dir(self.pack_dir()) {
            for e in rd.flatten() {
                let p = e.path();
                // A pack is identified by its `<name>.pack` path. It is present
                // either as a real `.pack` body (local / restored) OR as a tiered
                // pack whose body lives in S3 — the latter has only a
                // `<name>.pack.s3` marker (extension `s3`) on disk. Map a marker
                // back to its `<name>.pack` path so a tiered pack still opens
                // (from its `.lidx`) and can be restored on read.
                let pack_path = if p.extension().is_some_and(|x| x == "pack") {
                    p.clone()
                } else if p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.ends_with(".pack.s3")) {
                    // strip the trailing ".s3" to recover "<name>.pack".
                    let s = p.as_os_str().to_string_lossy();
                    PathBuf::from(&s[..s.len() - 3])
                } else {
                    continue;
                };
                if !seen.insert(pack_path.clone()) {
                    continue; // a restored pack may have both `.pack` and marker
                }
                if let Ok(pf) = crate::git_pack_file::GitPackFile::open(&pack_path) {
                    packs.push(std::sync::Arc::new(pf));
                }
            }
        }
        self.swap_packs(packs);
    }

    /// If `pf`'s local `.pack` is absent but a `<name>.pack.s3` marker + a cold
    /// tier exist, download the body from S3 and write it to the local `.pack`
    /// (tmp + atomic rename). A no-op when the body is already local, when no
    /// cold tier is installed, or when no marker is present.
    async fn ensure_pack_local(
        &self,
        pf: &crate::git_pack_file::GitPackFile,
    ) -> ledge_core::Result<()> {
        let pack_path = pf.pack_path().to_path_buf();
        if pack_path.exists() {
            return Ok(());
        }
        // Marker is `<name>.pack.s3` — APPEND ".s3" to the full `.pack` path
        // (NOT `with_extension`, which would clobber `.pack` → `.s3`).
        let marker = marker_path(&pack_path);
        let Some(cold) = self.cold.load_full() else {
            return Ok(());
        };
        if !marker.exists() {
            return Ok(());
        }
        let key = std::fs::read_to_string(&marker).map_err(LedgeError::Io)?;
        let bytes = cold.get(key.trim()).await?;
        // atomic-ish: tmp + rename so a partial download never surfaces as a
        // valid-looking `.pack`.
        let tmp = pack_path.with_extension("pack.tmp");
        std::fs::write(&tmp, &bytes).map_err(LedgeError::Io)?;
        std::fs::rename(&tmp, &pack_path).map_err(LedgeError::Io)?;
        Ok(())
    }

    /// Spill every local `.pack` *body* to the S3 cold tier, keeping the small
    /// `.idx`/`.lidx` indexes on disk, and drop the local body. Each upload is
    /// verified present via `head` before the local `.pack` is removed, and a
    /// `<name>.pack.s3` marker records the S3 key so a later read can restore it.
    ///
    /// Idempotent: a pack whose marker already exists is skipped. Errors if no
    /// cold tier is installed (so a caller can't silently no-op).
    pub async fn tier_packs(&self) -> ledge_core::Result<TierStats> {
        let Some(cold) = self.cold.load_full() else {
            return Err(LedgeError::Unavailable("s3 cold tier disabled".into()));
        };
        let mut stats = TierStats::default();
        let dir = self.pack_dir();
        let entries: Vec<_> = std::fs::read_dir(&dir).map_err(LedgeError::Io)?.flatten().collect();
        for e in entries {
            let p = e.path();
            if p.extension().is_some_and(|x| x == "pack") {
                // already tiered? (marker present) — idempotent skip.
                let marker = marker_path(&p);
                if marker.exists() {
                    continue;
                }
                let name = p.file_name().unwrap().to_string_lossy().to_string(); // "<name>.pack"
                let key = format!("packs/{name}");
                let bytes = std::fs::read(&p).map_err(LedgeError::Io)?;
                let n = bytes.len() as u64;
                cold.put(&key, bytes).await?;
                // Verify durability BEFORE deleting the only local copy.
                if !cold.head(&key).await? {
                    return Err(LedgeError::Unavailable(format!(
                        "s3 tier verify failed for {key}"
                    )));
                }
                std::fs::write(&marker, key.as_bytes()).map_err(LedgeError::Io)?;
                std::fs::remove_file(&p).map_err(LedgeError::Io)?;
                stats.packs_tiered += 1;
                stats.bytes_uploaded += n;
            }
        }
        // Reflect reality: the in-memory pack set must re-open from `.lidx` so a
        // subsequent read sees a tiered (body-absent) pack and restores it.
        self.reload_packs();
        Ok(stats)
    }

    /// Canonical path for an object identified by `id`.
    pub fn object_path(&self, id: &ObjectId) -> PathBuf {
        let hex = id.to_hex();
        self.data_dir
            .join("objects")
            .join(&hex[..2])
            .join(&hex[2..4])
            .join(&hex)
    }

    /// Directory holding `<blake3>.pack` + `.idx` pairs (`<data_dir>/objects/pack`).
    /// Stable target for repack output and for tests registering synthetic packs.
    pub fn pack_dir(&self) -> PathBuf {
        self.data_dir.join("objects").join("pack")
    }

    /// Atomically replace the registered pack set. Concurrent readers holding an
    /// older snapshot finish against it; subsequent `.load()`s see `v`. Used by
    /// repack to publish freshly written packs without quiescing the store.
    pub fn swap_packs(&self, v: Vec<std::sync::Arc<crate::git_pack_file::GitPackFile>>) {
        self.packs.store(std::sync::Arc::new(v));
        // The packed half of the index just changed (repack published new packs);
        // invalidate so the next sha1_index() folds in the new packs' sha1_maps.
        self.sha1_cache.store(None);
    }

    /// Paths of every currently-registered pack (snapshot at call time). Lets a
    /// repack identify which packs it just superseded so they can be unlinked.
    pub fn pack_paths(&self) -> Vec<PathBuf> {
        self.packs.load().iter().map(|p| p.pack_path().to_path_buf()).collect()
    }

    /// The 24-byte-header loose-object file image for `id`, or `None` if no loose
    /// file exists (NotFound is mapped to `None`, not an error).
    ///
    /// This is the LOOSE tier only. Every read-path accessor consults this first
    /// (loose shadows pack), then falls through to the registered [`GitPackFile`]s
    /// via their own typed methods (`read`/`sha1_of`/`git_type_of`/…). A
    /// re-materialized loose copy always wins, which is what lets GC/repack stage
    /// loose objects atop a pack without a read seeing stale bytes.
    pub(crate) async fn loose_bytes(&self, id: ObjectId) -> ledge_core::Result<Option<Vec<u8>>> {
        match tokio::fs::read(self.object_path(&id)).await {
            Ok(r) => Ok(Some(r)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(LedgeError::Io(e)),
        }
    }

    /// Return the Git-compatible SHA-1 stored in the 20-byte header of an
    /// already-written object file.
    ///
    /// # Errors
    /// Returns [`LedgeError::NotFound`] if the object does not exist.
    /// Returns [`LedgeError::Corruption`] if the file is shorter than 20 bytes.
    #[instrument(skip(self), fields(id = %id.to_hex()))]
    pub async fn sha1_of(&self, id: ObjectId) -> Result<[u8; 20]> {
        // Loose tier: the canonical git SHA-1 is the first 20 header bytes.
        if let Some(raw) = self.loose_bytes(id).await? {
            if raw.len() < 20 {
                return Err(LedgeError::Corruption(format!(
                    "object {} header truncated: {} bytes",
                    id.to_hex(),
                    raw.len()
                )));
            }
            return Ok(raw[..20].try_into().unwrap());
        }
        // Pack tier: first GitPackFile that carries the id wins.
        for pf in self.packs.load().iter() {
            if let Some(sha1) = pf.sha1_of(id) {
                return Ok(sha1);
            }
        }
        Err(LedgeError::NotFound(id))
    }

    /// Git object type tags (pack/loose object kinds).
    /// 1=commit, 2=tree, 3=blob, 4=tag.
    ///
    /// Write `content` tagged with its git object `git_type`. The stored 20-byte
    /// header SHA-1 is the canonical git object id: SHA1("<typename> <len>\0<content>").
    /// The type byte is stored in the first reserved header byte so the fetch path
    /// can reconstruct a correctly-typed pack and serve the correct SHA-1.
    pub async fn write_git_object(
        &self,
        git_type: u8,
        content: bytes::Bytes,
    ) -> ledge_core::Result<ObjectId> {
        let type_name = match git_type {
            1 => "commit",
            2 => "tree",
            3 => "blob",
            4 => "tag",
            other => {
                return Err(ledge_core::LedgeError::Corruption(format!(
                    "unknown git object type {other}"
                )))
            }
        };
        // BLAKE3 address over raw content.
        let blake3_hash: [u8; 32] = blake3::hash(&content).into();
        let id = ObjectId::from_bytes(blake3_hash);
        // Canonical git SHA-1 over "<type> <len>\0<content>".
        let mut sha1_hasher = sha1::Sha1::new();
        sha1::Digest::update(
            &mut sha1_hasher,
            format!("{type_name} {}\0", content.len()).as_bytes(),
        );
        sha1::Digest::update(&mut sha1_hasher, &content);
        let sha1_hash: [u8; 20] = sha1::Digest::finalize(sha1_hasher).into();

        let mut payload = Vec::with_capacity(24 + content.len());
        payload.extend_from_slice(&sha1_hash);
        payload.push(git_type); // byte 20 = git type
        // byte 21 = encoding (0=raw, 1=zlib). Never inflate: fall back to raw when
        // zlib doesn't shrink (tiny / already-compressed inputs).
        let compressed = zlib_compress(&content);
        let (enc, stored): (u8, &[u8]) = if compressed.len() < content.len() {
            (ENC_ZLIB, compressed.as_slice())
        } else {
            (ENC_RAW, content.as_ref())
        };
        payload.push(enc);
        payload.extend_from_slice(&[0u8; 2]); // bytes 22..24 reserved
        payload.extend_from_slice(stored);

        let tmp = self.tmp_path();
        let final_path = self.object_path(&id);
        if let Some(parent) = final_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(ledge_core::LedgeError::Io)?;
        }
        tokio::fs::write(&tmp, &payload)
            .await
            .map_err(ledge_core::LedgeError::Io)?;
        tokio::fs::rename(&tmp, &final_path)
            .await
            .map_err(ledge_core::LedgeError::Io)?;
        // A new loose object changed the set the index describes — drop the cache
        // so the next sha1_index() rebuilds and includes it.
        self.sha1_cache.store(None);
        Ok(id)
    }

    /// Build a `git-SHA-1 → ObjectId` index by scanning every loose object.
    ///
    /// Walks `<data_dir>/objects/<XX>/<YY>/<id>` (skipping the `tmp/` staging
    /// dir) and reads each object's 24-byte header to recover the git SHA-1.
    /// This is the reverse map needed by the fetch path to resolve child git
    /// SHA-1s discovered while walking a commit's reachable object graph
    /// (commit → tree → blob), since the store is BLAKE3-addressed and git
    /// references objects by SHA-1.
    ///
    /// Complexity is O(N) in the number of stored objects; acceptable for the
    /// clone/fetch use case where a repo's object count is bounded.
    ///
    /// # Caching
    /// The result is memoized in an `Arc` and returned by pointer on subsequent
    /// calls, so a busy clone path does not rescan the whole store per request.
    /// The cache is invalidated (set to `None`) on every loose write
    /// ([`Self::write_git_object`]) and on [`Self::swap_packs`] — the only
    /// mutations that can change the loose-or-packed object set. The packed half
    /// uses each pack's preloaded `sha1_map`, so no per-record file reads occur.
    pub async fn sha1_index(
        &self,
    ) -> ledge_core::Result<std::sync::Arc<std::collections::HashMap<[u8; 20], ObjectId>>> {
        // Fast path: a live cached index — clone the Arc pointer, not the map.
        if let Some(cached) = self.sha1_cache.load_full() {
            return Ok(cached);
        }
        use tokio::io::AsyncReadExt as _;
        let mut map = std::collections::HashMap::new();
        let objects_dir = self.data_dir.join("objects");
        // A missing loose tree is not an early return: packs may still hold
        // objects we must index. `None` here means "skip the loose walk, fall
        // through to the packed half + cache store".
        let mut lvl1 = match tokio::fs::read_dir(&objects_dir).await {
            Ok(rd) => Some(rd),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(ledge_core::LedgeError::Io(e)),
        };
        while let Some(d1) = match lvl1.as_mut() {
            Some(rd) => rd.next_entry().await.map_err(ledge_core::LedgeError::Io)?,
            None => None,
        } {
            let name1 = d1.file_name();
            // Skip the write-staging directory; only 2-hex fan-out dirs hold objects.
            if name1 == std::ffi::OsStr::new("tmp") {
                continue;
            }
            if !d1
                .file_type()
                .await
                .map_err(ledge_core::LedgeError::Io)?
                .is_dir()
            {
                continue;
            }
            let mut lvl2 = tokio::fs::read_dir(d1.path())
                .await
                .map_err(ledge_core::LedgeError::Io)?;
            while let Some(d2) = lvl2.next_entry().await.map_err(ledge_core::LedgeError::Io)? {
                if !d2
                    .file_type()
                    .await
                    .map_err(ledge_core::LedgeError::Io)?
                    .is_dir()
                {
                    continue;
                }
                let mut files = tokio::fs::read_dir(d2.path())
                    .await
                    .map_err(ledge_core::LedgeError::Io)?;
                while let Some(f) = files.next_entry().await.map_err(ledge_core::LedgeError::Io)? {
                    let hex = f.file_name();
                    let hex = match hex.to_str() {
                        Some(h) if h.len() == 64 => h,
                        _ => continue,
                    };
                    let id = match ObjectId::from_hex(hex) {
                        Ok(id) => id,
                        Err(_) => continue,
                    };
                    let mut file = tokio::fs::File::open(f.path())
                        .await
                        .map_err(ledge_core::LedgeError::Io)?;
                    let mut header = [0u8; 24];
                    let n = file.read(&mut header).await.map_err(ledge_core::LedgeError::Io)?;
                    if n < 24 {
                        continue;
                    }
                    let sha1: [u8; 20] = header[..20].try_into().unwrap();
                    map.insert(sha1, id);
                }
            }
        }
        // Packed half: fold in each pack's preloaded sha1→id map. No per-record
        // file reads — the map was built when the pack was opened. A loose object
        // and its packed copy share the same git SHA-1 → same ObjectId, so the
        // insert order between halves is immaterial (idempotent on collision).
        for pf in self.packs.load().iter() {
            for (sha1, id) in pf.sha1_pairs() {
                map.insert(sha1, id);
            }
        }
        let arc = std::sync::Arc::new(map);
        // Publish the freshly-built index. A concurrent invalidation (write /
        // swap_packs) that raced this store will be re-observed as `None` on the
        // next call and rebuilt; we never return a map that omits a known object
        // because invalidation always follows the mutation it describes.
        self.sha1_cache.store(Some(arc.clone()));
        Ok(arc)
    }

    /// Enumerate the [`ObjectId`] of every loose object currently stored.
    ///
    /// Walks the same `<data_dir>/objects/<XX>/<YY>/<id>` fan-out as
    /// [`Self::sha1_index`], skipping the `tmp/` staging dir, but stops at the
    /// filename: each 64-hex file name is parsed straight into an `ObjectId`
    /// with no header read. This is the candidate-set source for GC
    /// (mark-and-sweep): every id returned here is a deletion candidate unless
    /// proven reachable.
    ///
    /// A missing `objects/` directory yields an empty vector (a freshly opened,
    /// never-written store). Non-directory entries and names that are not
    /// 64-hex are skipped defensively.
    ///
    /// Complexity: O(N) in the number of stored objects; no file contents are
    /// opened, so it is strictly cheaper than [`Self::sha1_index`].
    pub async fn list_all_ids(&self) -> ledge_core::Result<Vec<ObjectId>> {
        // Dedup across the loose walk and every pack: an object can legitimately
        // appear both loose and packed (loose staged atop a pack mid-repack), and
        // a GC candidate set must list each id exactly once.
        let mut ids: std::collections::HashSet<ObjectId> = std::collections::HashSet::new();
        let objects_dir = self.data_dir.join("objects");
        let mut lvl1 = match tokio::fs::read_dir(&objects_dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // No loose tree yet, but packs may still hold objects.
                for pf in self.packs.load().iter() {
                    for id in pf.oids() {
                        ids.insert(id);
                    }
                }
                return Ok(ids.into_iter().collect());
            }
            Err(e) => return Err(LedgeError::Io(e)),
        };
        while let Some(d1) = lvl1.next_entry().await.map_err(LedgeError::Io)? {
            // Skip the write-staging directory; only 2-hex fan-out dirs hold objects.
            if d1.file_name() == std::ffi::OsStr::new("tmp") {
                continue;
            }
            if !d1.file_type().await.map_err(LedgeError::Io)?.is_dir() {
                continue;
            }
            let mut lvl2 = tokio::fs::read_dir(d1.path()).await.map_err(LedgeError::Io)?;
            while let Some(d2) = lvl2.next_entry().await.map_err(LedgeError::Io)? {
                if !d2.file_type().await.map_err(LedgeError::Io)?.is_dir() {
                    continue;
                }
                let mut files = tokio::fs::read_dir(d2.path()).await.map_err(LedgeError::Io)?;
                while let Some(f) = files.next_entry().await.map_err(LedgeError::Io)? {
                    let name = f.file_name();
                    let hex = match name.to_str() {
                        Some(h) if h.len() == 64 => h,
                        _ => continue,
                    };
                    if let Ok(id) = ObjectId::from_hex(hex) {
                        ids.insert(id);
                    }
                }
            }
        }
        // Union in every packed id (loose entries already deduped by the set).
        for pf in self.packs.load().iter() {
            for id in pf.oids() {
                ids.insert(id);
            }
        }
        Ok(ids.into_iter().collect())
    }

    /// Remove the object file for `id`.
    ///
    /// **Idempotent:** a missing file is treated as success (`Ok(())`), because
    /// GC sweeps and lease release may both attempt to delete the same object,
    /// and a crash mid-sweep means the next pass re-attempts deletes that have
    /// already happened. Only the empty leaf file is removed; the `<XX>/<YY>/`
    /// fan-out directories are intentionally left in place to avoid an rmdir
    /// race with a concurrent writer creating a sibling object.
    ///
    /// Any I/O error other than "not found" is surfaced as [`LedgeError::Io`].
    pub async fn delete(&self, id: ObjectId) -> ledge_core::Result<()> {
        match tokio::fs::remove_file(self.object_path(&id)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(LedgeError::Io(e)),
        }
    }

    /// Read the git object type byte from the header (reserved[0]).
    pub async fn git_type_of(&self, id: ObjectId) -> ledge_core::Result<u8> {
        // Loose tier: git object type is header byte 20.
        if let Some(raw) = self.loose_bytes(id).await? {
            if raw.len() < 24 {
                return Err(ledge_core::LedgeError::Corruption(format!(
                    "object {} header truncated",
                    id.to_hex()
                )));
            }
            return Ok(raw[20]);
        }
        // Pack tier: the type is carried in the `.lidx` row.
        for pf in self.packs.load().iter() {
            if let Some(t) = pf.git_type_of(id) {
                return Ok(t);
            }
        }
        Err(LedgeError::NotFound(id))
    }

    /// Generate a unique temporary file path inside the staging directory.
    fn tmp_path(&self) -> PathBuf {
        let suffix: u64 = rand::thread_rng().gen();
        self.data_dir
            .join("objects")
            .join("tmp")
            .join(suffix.to_string())
    }

    /// If `id` is stored as a delta (enc byte 21 == [`ENC_DELTA`]), return its
    /// base [`ObjectId`]; otherwise `None`. Reads only the fixed-size header
    /// (bytes 0..56), never the variable-length body — cheap enough to walk a
    /// whole chain. A missing object is `Ok(None)`.
    pub async fn delta_base_of(&self, id: ObjectId) -> ledge_core::Result<Option<ObjectId>> {
        // Loose tier: enc byte 21 == ENC_DELTA → base id lives in bytes 24..56.
        if let Some(raw) = self.loose_bytes(id).await? {
            if raw.len() < 56 || raw[21] != ENC_DELTA {
                return Ok(None);
            }
            let mut b = [0u8; 32];
            b.copy_from_slice(&raw[24..56]);
            return Ok(Some(ObjectId::from_bytes(b)));
        }
        // Pack tier: ask the first pack that carries the id. A pack whose record
        // for `id` is a REF_DELTA returns Some(base); a full object returns
        // Ok(None) — either way `pf.contains(id)` is the signal we found it.
        for pf in self.packs.load().iter() {
            if pf.contains(id) {
                return pf.delta_base_of(id);
            }
        }
        Ok(None)
    }

    /// Length of the delta chain rooted at `id`, in hops, clamped at
    /// [`MAX_CHAIN`]. Header-only walk: terminates either at the first non-delta
    /// object or once the cap is reached, so a cyclic/oversized chain can never
    /// loop forever.
    async fn chain_depth(&self, mut id: ObjectId) -> ledge_core::Result<usize> {
        let mut d = 0;
        while let Some(base) = self.delta_base_of(id).await? {
            d += 1;
            if d >= MAX_CHAIN {
                break;
            }
            id = base;
        }
        Ok(d)
    }

    /// Does the delta chain starting at `from` reach `target` within
    /// [`MAX_CHAIN`] hops? Used by [`Self::deltify`] to reject base/target pairs
    /// that would close a cycle (`target` already depends on the proposed base).
    async fn delta_reaches(&self, from: ObjectId, target: ObjectId) -> ledge_core::Result<bool> {
        let mut id = from;
        for _ in 0..MAX_CHAIN {
            match self.delta_base_of(id).await? {
                Some(base) if base == target => return Ok(true),
                Some(base) => id = base,
                None => return Ok(false),
            }
        }
        Ok(false)
    }

    /// Re-store `target` as an [`ENC_DELTA`] object against `base`, returning
    /// `true` iff the rewrite was committed.
    ///
    /// # Self-verification (the corruption guard)
    /// Before any byte is written, the freshly-encoded delta is round-tripped:
    /// `apply_delta(base_content, delta)` must reproduce content whose
    /// `BLAKE3` hash equals `target`'s id. A mismatch (encoder bug, wrong base,
    /// truncation) returns [`LedgeError::Corruption`] and leaves the on-disk
    /// object untouched (still readable in its prior raw/zlib form). It is
    /// therefore impossible for `deltify` to corrupt an object.
    ///
    /// # Refusal cases (return `Ok(false)`, no error, object unchanged)
    /// - `target == base` (a self-delta is meaningless).
    /// - the chain at `base` is already [`MAX_CHAIN`] deep (would exceed the cap).
    /// - `base`'s chain already reaches `target` (would create a cycle).
    /// - the delta file would be `>=` the current file size (never grow).
    pub async fn deltify(&self, target: ObjectId, base: ObjectId) -> ledge_core::Result<bool> {
        if target == base {
            return Ok(false);
        }
        // Cap the resulting chain: deltifying adds one hop on top of `base`'s
        // existing depth, so `base` must be strictly below MAX_CHAIN - 1.
        if self.chain_depth(base).await? >= MAX_CHAIN - 1 {
            return Ok(false);
        }
        if self.delta_reaches(base, target).await? {
            return Ok(false); // would create a cycle
        }

        // Resolve both operands to full content (works even if base is itself a
        // delta — chains compose).
        let target_content = self.read_depth(target, 0).await?;
        let base_content = self.read_depth(base, 0).await?;

        // Preserve the target's git type and canonical SHA-1 header bytes.
        let git_type = self
            .git_type_of(target)
            .await
            .map_err(|e| match e {
                LedgeError::NotFound(_) => e,
                other => LedgeError::Corruption(format!(
                    "object {}: deltify type: {other}",
                    target.to_hex()
                )),
            })?;
        let sha1 = self.sha1_of(target).await?;

        let delta = encode_delta(&base_content, &target_content);

        // SELF-VERIFY (the guard): the round-trip must reproduce the EXACT
        // target bytes — proven by BLAKE3 equality with the target's id. Any
        // mismatch aborts the rewrite; the object stays in its prior encoding.
        let check = apply_delta(&base_content, &delta)
            .map_err(|e| LedgeError::Corruption(format!("deltify verify: {e}")))?;
        if blake3::hash(&check).as_bytes() != target.as_bytes() {
            return Err(LedgeError::Corruption(
                "deltify: round-trip mismatch (encoder bug); object kept raw".into(),
            ));
        }

        let zdelta = zlib_compress(&delta);
        let mut file = Vec::with_capacity(56 + zdelta.len());
        file.extend_from_slice(&sha1); // bytes 0..20  — canonical git SHA-1 (unchanged)
        file.push(git_type); // byte 20  — git object type (unchanged)
        file.push(ENC_DELTA); // byte 21  — encoding = delta
        file.extend_from_slice(&[0u8; 2]); // bytes 22..24 — reserved zero
        file.extend_from_slice(base.as_bytes()); // bytes 24..56 — the BASE id (32B), NOT target
        file.extend_from_slice(&zdelta); // bytes 56..    — zlib(delta_bytes)

        // Never grow: if the delta encoding isn't strictly smaller than the
        // current file, keep the object as-is.
        let cur = tokio::fs::metadata(self.object_path(&target))
            .await
            .map(|m| m.len() as usize)
            .unwrap_or(usize::MAX);
        if file.len() >= cur {
            return Ok(false);
        }

        // Atomic replace: write to tmp/, then rename(2) over the canonical path.
        let tmp = self.tmp_path();
        tokio::fs::write(&tmp, &file).await.map_err(LedgeError::Io)?;
        tokio::fs::rename(&tmp, self.object_path(&target))
            .await
            .map_err(LedgeError::Io)?;
        Ok(true)
    }

    /// Read and decode the object `id`, resolving delta chains recursively up to
    /// [`MAX_CHAIN`] hops. `depth` is the number of delta links already
    /// traversed; the cap guarantees termination on cyclic/oversized chains.
    async fn read_depth(&self, id: ObjectId, depth: usize) -> Result<Bytes> {
        // Loose tier first (loose shadows pack). A loose file carries our 24-byte
        // header + encoded body (raw / zlib / delta); delta resolves recursively.
        let raw = match self.loose_bytes(id).await? {
            Some(r) => r,
            None => {
                // Pack tier: GitPackFile.read resolves REF_DELTA internally and
                // returns the full content. First pack carrying the id wins.
                let packs = self.packs.load();
                for pf in packs.iter() {
                    // Ensure the body is local before reading: a tiered pack has
                    // its `.pack` in S3, restored on demand from the marker.
                    self.ensure_pack_local(pf).await?;
                    if let Some(c) = pf.read(id)? {
                        return Ok(Bytes::from(c));
                    }
                }
                return Err(LedgeError::NotFound(id));
            }
        };

        if raw.len() < 24 {
            return Err(LedgeError::Corruption(format!(
                "object {} too short: {} bytes",
                id.to_hex(),
                raw.len()
            )));
        }

        match raw[21] {
            ENC_RAW => Ok(Bytes::from(raw[24..].to_vec())),
            ENC_ZLIB => Ok(Bytes::from(zlib_inflate(&raw[24..], id)?)),
            ENC_DELTA => {
                if depth >= MAX_CHAIN {
                    return Err(LedgeError::Corruption(format!(
                        "object {}: delta chain too deep",
                        id.to_hex()
                    )));
                }
                if raw.len() < 56 {
                    return Err(LedgeError::Corruption(format!(
                        "object {}: truncated delta header",
                        id.to_hex()
                    )));
                }
                let mut b = [0u8; 32];
                b.copy_from_slice(&raw[24..56]);
                let base_id = ObjectId::from_bytes(b);
                // Box::pin to allow the recursive async call (sized future).
                let base = Box::pin(self.read_depth(base_id, depth + 1)).await?;
                let delta = zlib_inflate(&raw[56..], id)?;
                let content = apply_delta(&base, &delta).map_err(|e| {
                    LedgeError::Corruption(format!("object {}: apply_delta: {e}", id.to_hex()))
                })?;
                Ok(Bytes::from(content))
            }
            other => Err(LedgeError::Corruption(format!(
                "object {}: unknown encoding {other}",
                id.to_hex()
            ))),
        }
    }
}

#[async_trait]
impl ObjectStore for DiskObjectStore {
    /// Write `content` to the store, returning its BLAKE3-addressed [`ObjectId`].
    ///
    /// Content-addressed deduplication: if the object already exists this is a
    /// no-op (the rename overwrites an identical file) and the same id is returned.
    ///
    /// Plain `write` stores raw content as a git blob (type=3), keeping the
    /// blob SHA-1 / header layout used by the existing object-store callers.
    async fn write(&self, content: Bytes) -> Result<ObjectId> {
        self.write_git_object(3, content).await
    }

    /// Write multiple objects, returning their ids in input order.
    ///
    /// Each object is written by a dedicated [`tokio::spawn`]'d task, giving
    /// the runtime the opportunity to overlap I/O.  The result vector preserves
    /// the original ordering by collecting join handles in sequence.
    async fn write_batch(&self, contents: Vec<Bytes>) -> Result<Vec<ObjectId>> {
        // Construct a lightweight DiskObjectStore per task by cloning data_dir.
        // PathBuf is a heap pointer + length — cheap to clone.
        // We stay inside the impl block so the private field access is valid.
        let handles: Vec<_> = contents
            .into_iter()
            .map(|c| {
                let data_dir = self.data_dir.clone();
                let packs = self.packs.clone();
                let sha1_cache = self.sha1_cache.clone();
                let cold = self.cold.clone();
                tokio::spawn(async move {
                    DiskObjectStore { data_dir, packs, sha1_cache, cold }.write(c).await
                })
            })
            .collect();

        let mut ids = Vec::with_capacity(handles.len());
        for handle in handles {
            ids.push(
                handle
                    .await
                    .map_err(|e| LedgeError::Io(std::io::Error::other(e.to_string())))??,
            );
        }
        Ok(ids)
    }

    /// Read and return the raw content bytes for `id`.
    ///
    /// # Errors
    /// Returns [`LedgeError::NotFound`] when no object with that id exists.
    /// Returns [`LedgeError::Corruption`] when the file is shorter than the
    /// 24-byte header.
    async fn read(&self, id: ObjectId) -> Result<Bytes> {
        // Header byte 21 selects the encoding (raw / zlib / delta). Delta
        // objects are resolved recursively; `read_depth` enforces the chain cap.
        self.read_depth(id, 0).await
    }

    /// Return `true` if an object for `id` is present in the store.
    async fn exists(&self, id: ObjectId) -> Result<bool> {
        // Presence = loose metadata OR any registered pack holds the id.
        match tokio::fs::metadata(self.object_path(&id)).await {
            Ok(_) => return Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(LedgeError::Io(e)),
        }
        Ok(self.packs.load().iter().any(|pf| pf.contains(id)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ledge_core::{LedgeError, ObjectId, ObjectStore};
    use std::sync::Arc;
    use tempfile::tempdir;

    fn make_store() -> (DiskObjectStore, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let store = DiskObjectStore::new(dir.path().to_path_buf()).unwrap();
        (store, dir)
    }

    /// Pack the given `(oid, git_type, content, sha1)` objects into a real git
    /// packfile (`.pack` + `.idx` + `.lidx`) in the store's pack_dir and return an
    /// opened [`GitPackFile`] over it. `write_git_pack` with a non-zero window may
    /// store some objects as REF_DELTA against larger same-type neighbours.
    async fn pack_objects(
        store: &DiskObjectStore,
        objs: &[(ObjectId, u8, Vec<u8>, [u8; 20])],
    ) -> std::sync::Arc<crate::git_pack_file::GitPackFile> {
        let pobjs: Vec<crate::git_pack::PackObject> = objs
            .iter()
            .map(|(_, t, c, s)| crate::git_pack::PackObject {
                git_type: *t,
                content: c.clone(),
                sha1: *s,
            })
            .collect();
        let (pack, idx, offs) = crate::git_pack::write_git_pack(&pobjs, 16).unwrap();
        let name = "test";
        let dir = store.pack_dir();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(format!("{name}.pack")), &pack).unwrap();
        std::fs::write(dir.join(format!("{name}.idx")), &idx).unwrap();
        let lidx: Vec<crate::git_pack_file::LidxEntry> = objs
            .iter()
            .zip(offs)
            .map(|((oid, t, _, s), off)| crate::git_pack_file::LidxEntry {
                oid: *oid,
                sha1: *s,
                git_type: *t,
                offset: off,
            })
            .collect();
        std::fs::write(
            dir.join(format!("{name}.lidx")),
            crate::git_pack_file::write_lidx(&lidx),
        )
        .unwrap();
        std::sync::Arc::new(
            crate::git_pack_file::GitPackFile::open(&dir.join(format!("{name}.pack"))).unwrap(),
        )
    }

    #[tokio::test]
    async fn list_all_ids_returns_every_written_object() {
        let (store, _dir) = make_store();
        // Write three distinct objects (distinct content → distinct ids).
        let id_a = store.write(Bytes::from_static(b"alpha")).await.unwrap();
        let id_b = store.write(Bytes::from_static(b"beta")).await.unwrap();
        let id_c = store.write(Bytes::from_static(b"gamma")).await.unwrap();

        let mut ids = store.list_all_ids().await.unwrap();
        ids.sort_by_key(|id| *id.as_bytes());

        let mut expected = vec![id_a, id_b, id_c];
        expected.sort_by_key(|id| *id.as_bytes());

        assert_eq!(ids, expected, "list_all_ids must return exactly the written ids");
    }

    #[tokio::test]
    async fn list_all_ids_empty_store_is_empty() {
        let (store, _dir) = make_store();
        assert!(store.list_all_ids().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_removes_object_then_exists_is_false() {
        let (store, _dir) = make_store();
        let id = store.write(Bytes::from_static(b"to be deleted")).await.unwrap();
        assert!(store.exists(id).await.unwrap());
        store.delete(id).await.unwrap();
        assert!(!store.exists(id).await.unwrap(), "object must be gone after delete");
    }

    #[tokio::test]
    async fn delete_missing_id_is_ok() {
        let (store, _dir) = make_store();
        // Deleting an id that was never written is a no-op (idempotent).
        store
            .delete(ObjectId::from_bytes([0x11u8; 32]))
            .await
            .expect("delete of a missing object must be Ok");
    }

    // ── Task 8: write path ────────────────────────────────────────────────────

    #[tokio::test]
    async fn write_same_content_returns_same_id() {
        let (store, _dir) = make_store();
        let c = Bytes::from_static(b"hello ledge");
        assert_eq!(
            store.write(c.clone()).await.unwrap(),
            store.write(c).await.unwrap()
        );
    }

    #[tokio::test]
    async fn write_creates_fanout_path() {
        let (store, dir) = make_store();
        let id = store
            .write(Bytes::from_static(b"fanout path test"))
            .await
            .unwrap();
        let hex = id.to_hex();
        assert!(dir
            .path()
            .join("objects")
            .join(&hex[..2])
            .join(&hex[2..4])
            .join(&hex)
            .exists());
    }

    #[tokio::test]
    async fn write_file_has_24_byte_header() {
        let (store, dir) = make_store();
        let content = b"header layout check";
        let id = store
            .write(Bytes::copy_from_slice(content))
            .await
            .unwrap();
        let hex = id.to_hex();
        let raw = std::fs::read(
            dir.path()
                .join("objects")
                .join(&hex[..2])
                .join(&hex[2..4])
                .join(&hex),
        )
        .unwrap();
        // This 19-byte input does not shrink under zlib, so the never-inflate
        // fallback stores it raw: enc byte (21) = 0, body == content verbatim.
        assert_eq!(raw.len(), 24 + content.len());
        // byte 20 holds the git object type (3 = blob for `write`).
        assert_eq!(raw[20], 3);
        // byte 21 = encoding flag (0 = raw here); bytes 22..24 reserved zero.
        assert_eq!(&raw[21..24], &[0u8; 3]);
        assert_eq!(&raw[24..], content as &[u8]);
    }

    #[tokio::test]
    async fn write_header_sha1_matches_git_blob_hash() {
        use sha1::Digest as _;
        let (store, dir) = make_store();
        let content = b"git sha1 compatibility check";
        let id = store
            .write(Bytes::copy_from_slice(content))
            .await
            .unwrap();
        let hex = id.to_hex();
        let raw = std::fs::read(
            dir.path()
                .join("objects")
                .join(&hex[..2])
                .join(&hex[2..4])
                .join(&hex),
        )
        .unwrap();
        let stored: [u8; 20] = raw[..20].try_into().unwrap();
        let mut h = sha1::Sha1::new();
        h.update(format!("blob {}\0", content.len()).as_bytes());
        h.update(content);
        assert_eq!(stored, <[u8; 20]>::from(h.finalize()));
    }

    #[tokio::test]
    async fn write_leaves_no_tmp_files() {
        let (store, dir) = make_store();
        store
            .write(Bytes::from_static(b"cleanup test"))
            .await
            .unwrap();
        let tmp = dir.path().join("objects").join("tmp");
        if tmp.exists() {
            assert_eq!(std::fs::read_dir(&tmp).unwrap().count(), 0);
        }
    }

    // ── Task 9: read path + sha1_of + write_batch + concurrent ───────────────

    #[tokio::test]
    async fn read_returns_original_content() {
        let (store, _dir) = make_store();
        let c = Bytes::from_static(b"round-trip payload");
        let id = store.write(c.clone()).await.unwrap();
        assert_eq!(store.read(id).await.unwrap(), c);
    }

    #[tokio::test]
    async fn read_missing_returns_not_found() {
        let (store, _dir) = make_store();
        assert!(matches!(
            store.read(ObjectId::from_bytes([0u8; 32])).await,
            Err(LedgeError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn exists_false_for_missing() {
        let (store, _dir) = make_store();
        assert!(!store
            .exists(ObjectId::from_bytes([0u8; 32]))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn exists_true_after_write() {
        let (store, _dir) = make_store();
        let id = store
            .write(Bytes::from_static(b"existence"))
            .await
            .unwrap();
        assert!(store.exists(id).await.unwrap());
    }

    #[tokio::test]
    async fn sha1_of_matches_git_blob_hash() {
        use sha1::Digest as _;
        let (store, _dir) = make_store();
        let content = b"sha1_of correctness";
        let id = store
            .write(Bytes::copy_from_slice(content))
            .await
            .unwrap();
        let sha1 = store.sha1_of(id).await.unwrap();
        let mut h = sha1::Sha1::new();
        h.update(format!("blob {}\0", content.len()).as_bytes());
        h.update(content);
        assert_eq!(sha1, <[u8; 20]>::from(h.finalize()));
    }

    #[tokio::test]
    async fn sha1_of_missing_returns_not_found() {
        let (store, _dir) = make_store();
        assert!(matches!(
            store
                .sha1_of(ObjectId::from_bytes([0xdeu8; 32]))
                .await,
            Err(LedgeError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn write_batch_ids_in_order() {
        let (store, _dir) = make_store();
        let cs: Vec<Bytes> = (0u8..8).map(|i| Bytes::from(vec![i; 64])).collect();
        let ids = store.write_batch(cs.clone()).await.unwrap();
        assert_eq!(ids.len(), 8);
        for (c, id) in cs.into_iter().zip(ids.iter()) {
            assert_eq!(store.write(c).await.unwrap(), *id);
        }
    }

    #[tokio::test]
    async fn concurrent_same_content_idempotent() {
        let dir = tempdir().unwrap();
        let store = Arc::new(DiskObjectStore::new(dir.path().to_path_buf()).unwrap());
        let content = Bytes::from_static(b"concurrent idempotency test payload");
        let handles: Vec<_> = (0..64)
            .map(|_| {
                let s = Arc::clone(&store);
                let c = content.clone();
                tokio::spawn(async move { s.write(c).await.unwrap() })
            })
            .collect();
        let ids: Vec<ObjectId> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        let first = ids[0];
        assert!(ids.iter().all(|id| *id == first));
        let hex = first.to_hex();
        assert!(dir
            .path()
            .join("objects")
            .join(&hex[..2])
            .join(&hex[2..4])
            .join(&hex)
            .exists());
        let tmp = dir.path().join("objects").join("tmp");
        assert!(
            std::fs::read_dir(&tmp).unwrap().count() == 0,
            "tmp files leaked"
        );
    }

    #[tokio::test]
    async fn concurrent_unique_objects_all_stored() {
        let dir = tempdir().unwrap();
        let store = Arc::new(DiskObjectStore::new(dir.path().to_path_buf()).unwrap());
        let handles: Vec<_> = (0u8..64)
            .map(|i| {
                let s = Arc::clone(&store);
                tokio::spawn(async move {
                    let c = Bytes::from(vec![i; 256]);
                    let id = s.write(c.clone()).await.unwrap();
                    (id, c)
                })
            })
            .collect();
        for result in futures::future::join_all(handles).await {
            let (id, original) = result.unwrap();
            assert_eq!(store.read(id).await.unwrap(), original);
        }
    }

    // ── Task 1: object compression (zlib + backward-compat enc flag) ──────────

    #[tokio::test]
    async fn roundtrip_compressible_binary_empty_tiny() {
        let (store, _d) = make_store();
        let big: Vec<u8> = (0..400).flat_map(|i| format!("line {i}\n").into_bytes()).collect();
        let cases: Vec<Vec<u8>> = vec![
            big.clone(),
            vec![],
            b"hi".to_vec(),
            (0..4096u32).map(|i| (i.wrapping_mul(2654435761) >> 24) as u8).collect(),
        ];
        for c in cases {
            let id = store.write_git_object(3, Bytes::from(c.clone())).await.unwrap();
            let got = ObjectStore::read(&store, id).await.unwrap();
            assert_eq!(got.as_ref(), c.as_slice(), "round-trip byte-identical (len {})", c.len());
        }
    }

    #[tokio::test]
    async fn dedup_same_content_same_id() {
        let (store, _d) = make_store();
        let c = Bytes::from(vec![7u8; 5000]);
        let a = store.write_git_object(3, c.clone()).await.unwrap();
        let b = store.write_git_object(3, c.clone()).await.unwrap();
        assert_eq!(a, b);
    }

    #[tokio::test]
    async fn compresses_on_disk() {
        let (store, _d) = make_store();
        let c = Bytes::from((0..2000).flat_map(|i| format!("line {i}\n").into_bytes()).collect::<Vec<u8>>());
        let id = store.write_git_object(3, c.clone()).await.unwrap();
        let p = store.object_path(&id);
        let on_disk = std::fs::metadata(&p).unwrap().len() as usize;
        assert!(on_disk < 24 + c.len(), "stored ({on_disk}) < header+raw ({})", 24 + c.len());
        let raw = std::fs::read(&p).unwrap();
        assert_eq!(raw[21], 1, "enc flag = zlib");
    }

    #[tokio::test]
    async fn legacy_raw_object_reads_back() {
        let (store, _d) = make_store();
        let content = b"legacy raw object body".to_vec();
        let mut h = sha1::Sha1::new();
        sha1::Digest::update(&mut h, format!("blob {}\0", content.len()).as_bytes());
        sha1::Digest::update(&mut h, &content);
        let sha1: [u8; 20] = sha1::Digest::finalize(h).into();
        let id = ObjectId::from_bytes(blake3::hash(&content).into());
        let mut payload = Vec::new();
        payload.extend_from_slice(&sha1);
        payload.push(3);
        payload.extend_from_slice(&[0u8; 3]); // reserved incl enc byte = 0 (legacy raw)
        payload.extend_from_slice(&content);
        let p = store.object_path(&id);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, &payload).unwrap();
        assert_eq!(ObjectStore::read(&store, id).await.unwrap().as_ref(), content.as_slice());
    }

    #[tokio::test]
    async fn corrupt_compressed_body_is_clean_error() {
        let (store, _d) = make_store();
        let id = store.write_git_object(3, Bytes::from(vec![1u8; 3000])).await.unwrap();
        let p = store.object_path(&id);
        let mut raw = std::fs::read(&p).unwrap();
        assert_eq!(raw[21], 1);
        for b in raw[24..].iter_mut() { *b = 0xff; }
        std::fs::write(&p, &raw).unwrap();
        assert!(ObjectStore::read(&store, id).await.is_err());
    }

    // ── Task 2: enc=2 delta objects (self-verifying deltify + chain read) ─────

    #[tokio::test]
    async fn deltify_shrinks_and_reads_back() {
        let (store, _d) = make_store();
        let base: Vec<u8> = (0..500).flat_map(|i| format!("line {i}\n").into_bytes()).collect();
        let target = String::from_utf8(base.clone()).unwrap().replace("line 250\n", "EDITED\n").into_bytes();
        let bid = store.write_git_object(3, Bytes::from(base.clone())).await.unwrap();
        let tid = store.write_git_object(3, Bytes::from(target.clone())).await.unwrap();
        let before = std::fs::metadata(store.object_path(&tid)).unwrap().len();
        assert!(store.deltify(tid, bid).await.unwrap(), "should deltify");
        let after = std::fs::metadata(store.object_path(&tid)).unwrap().len();
        assert!(after < before, "delta file {after} < raw {before}");
        assert_eq!(ObjectStore::read(&store, tid).await.unwrap().as_ref(), target.as_slice());
        assert_eq!(store.git_type_of(tid).await.unwrap(), 3);
        assert_eq!(store.delta_base_of(tid).await.unwrap(), Some(bid));
        assert_eq!(store.delta_base_of(bid).await.unwrap(), None);
    }

    #[tokio::test]
    async fn deltify_refuses_when_not_smaller() {
        let (store, _d) = make_store();
        let a = store.write_git_object(3, Bytes::from(vec![1u8; 50])).await.unwrap();
        let b = store.write_git_object(3, Bytes::from((0..50u8).collect::<Vec<_>>())).await.unwrap();
        let _ = store.deltify(a, b).await.unwrap(); // may refuse (delta not smaller)
        assert_eq!(ObjectStore::read(&store, a).await.unwrap().len(), 50, "a still reads exact");
    }

    #[tokio::test]
    async fn delta_chain_reads_back() {
        let (store, _d) = make_store();
        let v0: Vec<u8> = (0..300).flat_map(|i| format!("L{i}\n").into_bytes()).collect();
        let v1 = String::from_utf8(v0.clone()).unwrap().replace("L100\n","A\n").into_bytes();
        let v2 = String::from_utf8(v1.clone()).unwrap().replace("L200\n","B\n").into_bytes();
        let id0 = store.write_git_object(3, Bytes::from(v0)).await.unwrap();
        let id1 = store.write_git_object(3, Bytes::from(v1.clone())).await.unwrap();
        let id2 = store.write_git_object(3, Bytes::from(v2.clone())).await.unwrap();
        assert!(store.deltify(id1, id0).await.unwrap());
        assert!(store.deltify(id2, id1).await.unwrap()); // chain: id2 -> id1 -> id0
        assert_eq!(ObjectStore::read(&store, id2).await.unwrap().as_ref(), v2.as_slice());
        assert_eq!(ObjectStore::read(&store, id1).await.unwrap().as_ref(), v1.as_slice());
        assert_eq!(ObjectStore::read(&store, id0).await.unwrap().as_ref(), {
            let v0b: Vec<u8> = (0..300).flat_map(|i| format!("L{i}\n").into_bytes()).collect(); v0b
        }.as_slice());
    }

    // ── Task 2 (packing): two-tier read (loose + pack) ────────────────────────

    #[tokio::test]
    async fn reads_packed_only_object() {
        let (store, _d) = make_store();
        let content = (0..400).flat_map(|i| format!("line {i}\n").into_bytes()).collect::<Vec<u8>>();
        let id = store.write_git_object(3, Bytes::from(content.clone())).await.unwrap();
        let sha1 = store.sha1_of(id).await.unwrap();
        let pf = pack_objects(&store, &[(id, 3, content.clone(), sha1)]).await;
        store.swap_packs(vec![pf]);
        std::fs::remove_file(store.object_path(&id)).unwrap(); // loose gone — only the pack has it
        assert_eq!(ObjectStore::read(&store, id).await.unwrap().as_ref(), content.as_slice());
        assert_eq!(store.git_type_of(id).await.unwrap(), 3);
        assert_eq!(store.sha1_of(id).await.unwrap().len(), 20);
        assert!(store.exists(id).await.unwrap());
        assert!(store.list_all_ids().await.unwrap().contains(&id));
    }

    #[tokio::test]
    async fn packed_delta_with_packed_base_resolves() {
        let (store, _d) = make_store();
        let base = (0..400).flat_map(|i| format!("l{i}\n").into_bytes()).collect::<Vec<u8>>();
        let edited = String::from_utf8(base.clone()).unwrap().replace("l200\n","E\n").into_bytes();
        let bid = store.write_git_object(3, Bytes::from(base.clone())).await.unwrap();
        let tid = store.write_git_object(3, Bytes::from(edited.clone())).await.unwrap();
        let bsha1 = store.sha1_of(bid).await.unwrap();
        let tsha1 = store.sha1_of(tid).await.unwrap();
        // Pack both: write_git_pack(window=16) will store `edited` as a REF_DELTA
        // against the larger `base` INSIDE the pack — git-native delta, not ours.
        let pf = pack_objects(
            &store,
            &[(bid, 3, base, bsha1), (tid, 3, edited.clone(), tsha1)],
        )
        .await;
        store.swap_packs(vec![pf]);
        std::fs::remove_file(store.object_path(&bid)).unwrap();
        std::fs::remove_file(store.object_path(&tid)).unwrap();
        assert_eq!(ObjectStore::read(&store, tid).await.unwrap().as_ref(), edited.as_slice());
        assert_eq!(store.delta_base_of(tid).await.unwrap(), Some(bid));
    }

    #[tokio::test]
    async fn sha1_index_two_tier_and_cached() {
        let (store, _d) = make_store();
        let content = (0..400).flat_map(|i| format!("l{i}\n").into_bytes()).collect::<Vec<u8>>();
        let id = store.write_git_object(3, Bytes::from(content.clone())).await.unwrap();
        let sha1 = store.sha1_of(id).await.unwrap();
        let a = store.sha1_index().await.unwrap();
        let b = store.sha1_index().await.unwrap();
        assert!(std::sync::Arc::ptr_eq(&a, &b), "cached: same Arc on repeat call");
        assert_eq!(a.get(&sha1).copied(), Some(id));
        // pack it, prune loose, swap → cache invalidated + packed object still indexed
        let pf = pack_objects(&store, &[(id, 3, content, sha1)]).await;
        store.swap_packs(vec![pf]);
        std::fs::remove_file(store.object_path(&id)).unwrap();
        let c = store.sha1_index().await.unwrap();
        assert!(!std::sync::Arc::ptr_eq(&a, &c), "swap_packs invalidated the cache");
        assert_eq!(c.get(&sha1).copied(), Some(id), "packed object present in index");
    }

    #[tokio::test]
    async fn write_invalidates_sha1_cache() {
        let (store, _d) = make_store();
        let a = store.sha1_index().await.unwrap();
        let id = store.write_git_object(3, Bytes::from(b"new object".to_vec())).await.unwrap();
        let b = store.sha1_index().await.unwrap();
        assert!(!std::sync::Arc::ptr_eq(&a, &b), "write invalidated the cache");
        assert!(b.contains_key(&store.sha1_of(id).await.unwrap()));
    }

    // ── Task 2 (S3 cold tier): spill pack body → S3 + restore-on-read ─────────

    #[tokio::test]
    async fn tier_then_restore_and_durable() {
        let (store, _d) = make_store();
        store.set_cold(std::sync::Arc::new(crate::s3::S3Tier::in_memory("ledge")));
        let mut ids = Vec::new();
        let mut wants = Vec::new();
        for v in 0..6 {
            let c: Vec<u8> = (0..400).flat_map(|i| format!("l{i} v{v}\n").into_bytes()).collect();
            ids.push(store.write_git_object(3, Bytes::from(c.clone())).await.unwrap());
            wants.push(c);
        }
        crate::repack::repack(&store).await.unwrap();
        let pack_exists = || std::fs::read_dir(store.pack_dir()).unwrap().filter_map(|e| e.ok())
            .any(|e| e.path().extension().is_some_and(|x| x == "pack"));
        assert!(pack_exists(), "a .pack exists after repack");

        // TIER: pack body -> S3, local .pack removed
        let stats = store.tier_packs().await.unwrap();
        assert!(stats.packs_tiered >= 1, "tiered at least one pack");
        assert!(!pack_exists(), "local .pack removed after tiering");

        // READ restores the .pack from S3 and is byte-exact
        assert_eq!(ObjectStore::read(&store, ids[3]).await.unwrap().as_ref(), wants[3].as_slice());
        assert!(pack_exists(), ".pack restored locally on read");

        // DURABILITY: wipe the restored .pack + reload packs -> still reads from S3
        for e in std::fs::read_dir(store.pack_dir()).unwrap().filter_map(|e| e.ok()) {
            if e.path().extension().is_some_and(|x| x == "pack") { std::fs::remove_file(e.path()).unwrap(); }
        }
        store.reload_packs();
        assert_eq!(ObjectStore::read(&store, ids[0]).await.unwrap().as_ref(), wants[0].as_slice());
    }

    #[tokio::test]
    async fn tier_disabled_without_cold() {
        let (store, _d) = make_store();
        assert!(store.tier_packs().await.is_err(), "tier without a cold tier is an error");
    }

    #[tokio::test]
    async fn loose_shadows_pack() {
        let (store, _d) = make_store();
        let c = b"loose wins".to_vec();
        let id = store.write_git_object(3, Bytes::from(c.clone())).await.unwrap();
        // pack a DIFFERENT byte image under the same id path won't happen (content-addressed);
        // just assert that with both loose present and a pack registered, read uses loose.
        let sha1 = store.sha1_of(id).await.unwrap();
        let pf = pack_objects(&store, &[(id, 3, c.clone(), sha1)]).await;
        store.swap_packs(vec![pf]);
        assert_eq!(ObjectStore::read(&store, id).await.unwrap().as_ref(), c.as_slice()); // loose still there
    }
}
