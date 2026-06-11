//! Phase B1: read git-pack objects back by Ledge's BLAKE3 `ObjectId`.
//!
//! A git pack is SHA-1-keyed; Ledge addresses by `ObjectId = blake3(content)`.
//! [`write_lidx`] emits a sidecar `.lidx` that bridges the two namespaces, and
//! [`GitPackFile`] reads objects from the `.pack` by `ObjectId`, resolving
//! `REF_DELTA` chains against bases located via their SHA-1 (which lives inline
//! in the pack). We deliberately use OUR `.lidx` — not git's `.idx` — for the
//! read maps: it carries blake3 ids, sha1s, types, and offsets in one table.
//! The `.idx` is kept only so git tooling (`verify-pack`, serve) still works.

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use ledge_core::delta::apply_delta;
use ledge_core::{LedgeError, ObjectId, Result};

/// One row of the `.lidx` sidecar: a blake3 ObjectId mapped to its git identity
/// (sha1 + type) and its byte-offset within the companion `.pack`.
pub struct LidxEntry {
    pub oid: ObjectId,
    pub sha1: [u8; 20],
    pub git_type: u8,
    pub offset: u64,
}

/// Serialised row width: 32 (blake3) + 20 (sha1) + 1 (type) + 8 (offset).
const LIDX_ROW: usize = 61;

/// Cap on `REF_DELTA` chain recursion. Malformed/cyclic packs must not hang or
/// blow the stack; git's own `--depth` ceiling is 50, so we match it.
const MAX_DELTA_DEPTH: usize = 50;

/// Serialise `.lidx` = `[count:u32 LE]` + count × `[oid:32][sha1:20][type:1][offset:u64 LE]`.
pub fn write_lidx(entries: &[LidxEntry]) -> Vec<u8> {
    let mut b = Vec::with_capacity(4 + entries.len() * LIDX_ROW);
    b.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in entries {
        b.extend_from_slice(e.oid.as_bytes()); // 32
        b.extend_from_slice(&e.sha1); // 20
        b.push(e.git_type); // 1
        b.extend_from_slice(&e.offset.to_le_bytes()); // 8
    }
    b
}

/// Parse a `.lidx` blob into its entries, bounds-checking every field.
fn read_lidx(data: &[u8]) -> Result<Vec<LidxEntry>> {
    if data.len() < 4 {
        return Err(LedgeError::Corruption("lidx: truncated header".into()));
    }
    let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let need = 4usize
        .checked_add(count.checked_mul(LIDX_ROW).ok_or_else(|| {
            LedgeError::Corruption("lidx: count overflow".into())
        })?)
        .ok_or_else(|| LedgeError::Corruption("lidx: size overflow".into()))?;
    if data.len() < need {
        return Err(LedgeError::Corruption("lidx: truncated entries".into()));
    }
    let mut out = Vec::with_capacity(count);
    let mut p = 4;
    for _ in 0..count {
        let mut oid = [0u8; 32];
        oid.copy_from_slice(&data[p..p + 32]);
        let mut sha1 = [0u8; 20];
        sha1.copy_from_slice(&data[p + 32..p + 52]);
        let git_type = data[p + 52];
        let mut off = [0u8; 8];
        off.copy_from_slice(&data[p + 53..p + 61]);
        out.push(LidxEntry {
            oid: ObjectId::from_bytes(oid),
            sha1,
            git_type,
            offset: u64::from_le_bytes(off),
        });
        p += LIDX_ROW;
    }
    Ok(out)
}

/// Reader over a git `.pack` + Ledge `.lidx`, addressing objects by `ObjectId`.
pub struct GitPackFile {
    pack_path: PathBuf,
    by_oid: HashMap<ObjectId, (u8 /*type*/, u64 /*offset*/)>,
    sha1_to_offset: HashMap<[u8; 20], u64>, // REF_DELTA base resolution
    sha1_to_oid: HashMap<[u8; 20], ObjectId>, // delta_base_of + sha1 index
    oid_to_sha1: HashMap<ObjectId, [u8; 20]>,
}

impl GitPackFile {
    /// Open a `.pack` and its sibling `.lidx`, building the lookup maps.
    ///
    /// Validates the pack's `"PACK"` magic and parses the full `.lidx`. The
    /// `.idx` is intentionally untouched — every map is derived from `.lidx`.
    pub fn open(pack_path: &Path) -> Result<Self> {
        let pack = std::fs::read(pack_path)?;
        if pack.len() < 12 || &pack[0..4] != b"PACK" {
            return Err(LedgeError::Corruption(
                "git_pack_file: bad pack magic".into(),
            ));
        }
        let lidx_path = pack_path.with_extension("lidx");
        let lidx = std::fs::read(&lidx_path)?;
        let entries = read_lidx(&lidx)?;

        let mut by_oid = HashMap::with_capacity(entries.len());
        let mut sha1_to_offset = HashMap::with_capacity(entries.len());
        let mut sha1_to_oid = HashMap::with_capacity(entries.len());
        let mut oid_to_sha1 = HashMap::with_capacity(entries.len());
        for e in entries {
            by_oid.insert(e.oid, (e.git_type, e.offset));
            sha1_to_offset.insert(e.sha1, e.offset);
            sha1_to_oid.insert(e.sha1, e.oid);
            oid_to_sha1.insert(e.oid, e.sha1);
        }
        Ok(Self {
            pack_path: pack_path.to_path_buf(),
            by_oid,
            sha1_to_offset,
            sha1_to_oid,
            oid_to_sha1,
        })
    }

    /// Whether `id` is present in this pack.
    pub fn contains(&self, id: ObjectId) -> bool {
        self.by_oid.contains_key(&id)
    }

    /// All ObjectIds stored in this pack (unordered).
    pub fn oids(&self) -> Vec<ObjectId> {
        self.by_oid.keys().copied().collect()
    }

    /// All (sha1, ObjectId) pairs (unordered) — bridges git's namespace to Ledge's.
    pub fn sha1_pairs(&self) -> Vec<([u8; 20], ObjectId)> {
        self.sha1_to_oid.iter().map(|(s, o)| (*s, *o)).collect()
    }

    /// The git object type (1=commit 2=tree 3=blob 4=tag) for `id`, if present.
    pub fn git_type_of(&self, id: ObjectId) -> Option<u8> {
        self.by_oid.get(&id).map(|&(t, _)| t)
    }

    /// The git SHA-1 for `id`, if present.
    pub fn sha1_of(&self, id: ObjectId) -> Option<[u8; 20]> {
        self.oid_to_sha1.get(&id).copied()
    }

    /// Path to the backing `.pack`.
    pub fn pack_path(&self) -> &Path {
        &self.pack_path
    }

    /// Read the full (delta-resolved) content of `id`, or `None` if absent.
    pub fn read(&self, id: ObjectId) -> Result<Option<Vec<u8>>> {
        let Some(&(_, off)) = self.by_oid.get(&id) else {
            return Ok(None);
        };
        let pack = std::fs::read(&self.pack_path)?;
        Ok(Some(self.read_at(&pack, off, 0)?))
    }

    /// Materialise the object at pack `offset`, recursively resolving REF_DELTA.
    /// `depth` is capped at [`MAX_DELTA_DEPTH`] to reject cyclic/over-long chains
    /// without hanging or overflowing the stack.
    fn read_at(&self, pack: &[u8], offset: u64, depth: usize) -> Result<Vec<u8>> {
        if depth > MAX_DELTA_DEPTH {
            return Err(LedgeError::Corruption(
                "git_pack_file: delta chain too deep".into(),
            ));
        }
        let off = offset as usize;
        let data = pack
            .get(off..)
            .ok_or_else(|| LedgeError::Corruption("git_pack_file: offset out of range".into()))?;
        let (git_type, _size, hdr_len) = parse_pack_obj_header(data)?;
        match git_type {
            1..=4 => {
                // full object: header then zlib(content)
                let body = data
                    .get(hdr_len..)
                    .ok_or_else(|| LedgeError::Corruption("git_pack_file: truncated body".into()))?;
                zlib_inflate(body)
            }
            7 => {
                // REF_DELTA: header, 20-byte base sha1, zlib(delta)
                let base_sha1: [u8; 20] = data
                    .get(hdr_len..hdr_len + 20)
                    .ok_or_else(|| {
                        LedgeError::Corruption("git_pack_file: truncated ref-delta base".into())
                    })?
                    .try_into()
                    .expect("slice of length 20");
                let delta_z = data.get(hdr_len + 20..).ok_or_else(|| {
                    LedgeError::Corruption("git_pack_file: truncated ref-delta body".into())
                })?;
                let delta = zlib_inflate(delta_z)?;
                let base_off = *self.sha1_to_offset.get(&base_sha1).ok_or_else(|| {
                    LedgeError::Corruption("git_pack_file: ref-delta base not in pack".into())
                })?;
                let base = self.read_at(pack, base_off, depth + 1)?;
                apply_delta(&base, &delta)
            }
            6 => Err(LedgeError::Corruption(
                "git_pack_file: OFS_DELTA unsupported".into(),
            )),
            other => Err(LedgeError::Corruption(format!(
                "git_pack_file: unknown pack object type {other}"
            ))),
        }
    }

    /// If `id` is stored as a REF_DELTA, return the ObjectId of its base; else `None`.
    pub fn delta_base_of(&self, id: ObjectId) -> Result<Option<ObjectId>> {
        let Some(&(_, off)) = self.by_oid.get(&id) else {
            return Ok(None);
        };
        let pack = std::fs::read(&self.pack_path)?;
        let data = (pack.get(off as usize..))
            .ok_or_else(|| LedgeError::Corruption("git_pack_file: offset out of range".into()))?;
        let (git_type, _size, hdr_len) = parse_pack_obj_header(data)?;
        if git_type != 7 {
            return Ok(None);
        }
        let base_sha1: [u8; 20] = data
            .get(hdr_len..hdr_len + 20)
            .ok_or_else(|| LedgeError::Corruption("git_pack_file: truncated ref-delta base".into()))?
            .try_into()
            .expect("slice of length 20");
        Ok(self.sha1_to_oid.get(&base_sha1).copied())
    }
}

/// Decode a git pack object's type/size varint header (inverse of
/// `git_pack::write_obj_header`). Returns `(git_type, size, hdr_len)`. Every
/// byte access is bounds-checked so malformed input errors rather than panics.
fn parse_pack_obj_header(data: &[u8]) -> Result<(u8, usize, usize)> {
    let first = *data
        .first()
        .ok_or_else(|| LedgeError::Corruption("git_pack_file: empty object header".into()))?;
    let git_type = (first >> 4) & 0x07;
    let mut size = (first & 0x0f) as usize;
    let mut shift = 4u32;
    let mut i = 0usize;
    let mut byte = first;
    while byte & 0x80 != 0 {
        i += 1;
        byte = *data.get(i).ok_or_else(|| {
            LedgeError::Corruption("git_pack_file: truncated object header".into())
        })?;
        let chunk = (byte & 0x7f) as usize;
        // guard against a varint wider than usize before shifting
        let add = chunk
            .checked_shl(shift)
            .ok_or_else(|| LedgeError::Corruption("git_pack_file: header size overflow".into()))?;
        size |= add;
        shift += 7;
    }
    Ok((git_type, size, i + 1))
}

/// Inflate a zlib stream, reading exactly the bytes the stream consumes.
fn zlib_inflate(data: &[u8]) -> Result<Vec<u8>> {
    let mut dec = flate2::read::ZlibDecoder::new(data);
    let mut out = Vec::new();
    dec.read_to_end(&mut out)
        .map_err(|e| LedgeError::Corruption(format!("git_pack_file: zlib inflate failed: {e}")))?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git_pack::{write_git_pack, PackObject};

    // helper: run git, asserting success, returning stdout.
    fn git(args: &[&str], cwd: &std::path::Path) -> Vec<u8> {
        let o = std::process::Command::new("git")
            .args(args)
            .current_dir(cwd)
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .unwrap();
        assert!(
            o.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&o.stderr)
        );
        o.stdout
    }

    // Build a deltifiable object set via real git: a 500-line file edited across
    // 6 commits, enumerated as (sha,type,content) PackObjects.
    fn deltifiable_objects() -> Vec<PackObject> {
        let repo = tempfile::tempdir().unwrap();
        git(&["init", "--initial-branch=main", "."], repo.path());
        git(&["config", "user.email", "t@l"], repo.path());
        git(&["config", "user.name", "t"], repo.path());
        let base: String = (0..500).map(|i| format!("line {i}\n")).collect();
        for v in 0..6 {
            std::fs::write(
                repo.path().join("f.txt"),
                base.replace("line 5\n", &format!("V{v}\n")),
            )
            .unwrap();
            git(&["add", "."], repo.path());
            git(&["commit", "-m", &format!("c{v}")], repo.path());
        }
        let names = git(
            &[
                "cat-file",
                "--batch-all-objects",
                "--batch-check=%(objectname) %(objecttype)",
            ],
            repo.path(),
        );
        let mut objs = Vec::new();
        for line in String::from_utf8(names).unwrap().lines() {
            let mut it = line.split_whitespace();
            let sha_hex = it.next().unwrap();
            let ty = it.next().unwrap();
            let content = git(&["cat-file", ty, sha_hex], repo.path());
            let git_type = match ty {
                "commit" => 1u8,
                "tree" => 2,
                "blob" => 3,
                "tag" => 4,
                _ => panic!("type {ty}"),
            };
            let mut sha1 = [0u8; 20];
            for i in 0..20 {
                sha1[i] = u8::from_str_radix(&sha_hex[i * 2..i * 2 + 2], 16).unwrap();
            }
            objs.push(PackObject {
                git_type,
                content,
                sha1,
            });
        }
        objs
    }

    #[test]
    fn write_lidx_layout() {
        let e = LidxEntry {
            oid: ObjectId::from_bytes([7u8; 32]),
            sha1: [9u8; 20],
            git_type: 3,
            offset: 0x0102_0304_0506_0708,
        };
        let buf = write_lidx(&[e]);
        assert_eq!(buf.len(), 4 + LIDX_ROW);
        assert_eq!(&buf[0..4], &1u32.to_le_bytes());
        let back = read_lidx(&buf).unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].oid, ObjectId::from_bytes([7u8; 32]));
        assert_eq!(back[0].sha1, [9u8; 20]);
        assert_eq!(back[0].git_type, 3);
        assert_eq!(back[0].offset, 0x0102_0304_0506_0708);
    }

    #[test]
    fn git_pack_file_roundtrips_by_oid_and_verify_pack_accepts() {
        let objs = deltifiable_objects();
        assert!(objs.len() >= 3);
        let (pack, idx, offsets) = write_git_pack(&objs, 16).unwrap();

        // build the .lidx: oid = blake3(content), plus sha1/type/offset.
        let entries: Vec<LidxEntry> = objs
            .iter()
            .zip(&offsets)
            .map(|(po, &offset)| LidxEntry {
                oid: ObjectId::from_bytes(blake3::hash(&po.content).into()),
                sha1: po.sha1,
                git_type: po.git_type,
                offset,
            })
            .collect();
        let lidx = write_lidx(&entries);

        let dir = tempfile::tempdir().unwrap();
        let pack_path = dir.path().join("0.pack");
        let idx_path = dir.path().join("0.idx");
        let lidx_path = dir.path().join("0.lidx");
        std::fs::write(&pack_path, &pack).unwrap();
        std::fs::write(&idx_path, &idx).unwrap();
        std::fs::write(&lidx_path, &lidx).unwrap();

        let gpf = GitPackFile::open(&pack_path).unwrap();

        // every object reads back byte-identical THROUGH GitPackFile (incl deltified)
        for (i, po) in objs.iter().enumerate() {
            let oid = ObjectId::from_bytes(blake3::hash(&po.content).into());
            let got = gpf.read(oid).unwrap().expect("present");
            assert_eq!(got, po.content, "object {i} round-trips through GitPackFile");
            assert_eq!(gpf.git_type_of(oid).unwrap(), po.git_type);
            assert_eq!(gpf.sha1_of(oid).unwrap(), po.sha1);
        }
        assert_eq!(gpf.oids().len(), objs.len());

        // at least one object is a REF_DELTA (delta_base_of Some) — proves resolution
        let any_delta = objs.iter().any(|po| {
            let oid = ObjectId::from_bytes(blake3::hash(&po.content).into());
            gpf.delta_base_of(oid).unwrap().is_some()
        });
        assert!(any_delta, "the pack must contain a REF_DELTA to exercise resolution");

        // git still accepts the stored pack
        let vp = std::process::Command::new("git")
            .args(["verify-pack", "-v", idx_path.to_str().unwrap()])
            .output()
            .unwrap();
        assert!(
            vp.status.success(),
            "git verify-pack: {}",
            String::from_utf8_lossy(&vp.stderr)
        );
    }
}
