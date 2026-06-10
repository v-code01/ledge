//! Ledge internal pack format: many objects in one file + an offset index, to
//! eliminate per-object filesystem block overhead. A record holds the EXACT bytes
//! of a loose object file (`[24B header][stored…]`), so the reader parses packed
//! and loose objects identically. (Distinct from git's wire pack format.)
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use ledge_core::{LedgeError, ObjectId, Result};

const MAGIC: &[u8; 6] = b"PACKL\0";
const VERSION: u32 = 1;

/// A read-only pack: the `.pack` path + an in-memory `id → byte offset` index.
pub struct PackFile {
    pack_path: PathBuf,
    index: HashMap<ObjectId, u64>,
}

impl PackFile {
    pub fn pack_path(&self) -> PathBuf {
        self.pack_path.clone()
    }
    pub fn contains(&self, id: ObjectId) -> bool {
        self.index.contains_key(&id)
    }
    pub fn ids(&self) -> Vec<ObjectId> {
        self.index.keys().copied().collect()
    }

    /// Open `<base>.pack`; loads the sibling `<base>.idx`. Validates magic + index.
    pub fn open(pack_path: &Path) -> Result<Self> {
        let mut f = std::fs::File::open(pack_path).map_err(LedgeError::Io)?;
        let mut head = [0u8; 10];
        f.read_exact(&mut head)
            .map_err(|e| LedgeError::Corruption(format!("pack head: {e}")))?;
        if &head[..6] != MAGIC {
            return Err(LedgeError::Corruption("pack: bad magic".into()));
        }
        let idx_path = pack_path.with_extension("idx");
        let idx = std::fs::read(&idx_path).map_err(LedgeError::Io)?;
        if idx.len() < 4 {
            return Err(LedgeError::Corruption("idx: too short".into()));
        }
        let count = u32::from_le_bytes(idx[0..4].try_into().unwrap()) as usize;
        let mut index = HashMap::with_capacity(count);
        let mut p = 4usize;
        for _ in 0..count {
            if p + 40 > idx.len() {
                return Err(LedgeError::Corruption("idx: truncated".into()));
            }
            let mut b = [0u8; 32];
            b.copy_from_slice(&idx[p..p + 32]);
            let off = u64::from_le_bytes(idx[p + 32..p + 40].try_into().unwrap());
            index.insert(ObjectId::from_bytes(b), off);
            p += 40;
        }
        Ok(Self {
            pack_path: pack_path.to_path_buf(),
            index,
        })
    }

    /// The exact stored bytes (loose-file image) for `id`, or None.
    pub fn get(&self, id: ObjectId) -> Option<Vec<u8>> {
        let off = *self.index.get(&id)?;
        let mut f = std::fs::File::open(&self.pack_path).ok()?;
        f.seek(SeekFrom::Start(off)).ok()?;
        let mut len_buf = [0u8; 4];
        f.read_exact(&mut len_buf).ok()?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        f.read_exact(&mut buf).ok()?;
        Some(buf)
    }
}

/// Writes `(id, loose-file-bytes)` records into a new `<blake3>.pack` + `.idx`,
/// atomically (tmp + fsync + rename). Returns the opened `PackFile`.
pub struct PackWriter;

impl PackWriter {
    pub fn write(pack_dir: &Path, objects: Vec<(ObjectId, Vec<u8>)>) -> Result<PackFile> {
        std::fs::create_dir_all(pack_dir).map_err(LedgeError::Io)?;
        let mut pack = Vec::new();
        pack.extend_from_slice(MAGIC);
        pack.extend_from_slice(&VERSION.to_le_bytes());
        let mut idx_entries: Vec<(ObjectId, u64)> = Vec::with_capacity(objects.len());
        for (id, bytes) in &objects {
            let off = pack.len() as u64;
            pack.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            pack.extend_from_slice(bytes);
            idx_entries.push((*id, off));
        }
        let name = blake3::hash(&pack).to_hex().to_string();
        let pack_path = pack_dir.join(format!("{name}.pack"));
        let idx_path = pack_dir.join(format!("{name}.idx"));
        let mut idx = Vec::with_capacity(4 + idx_entries.len() * 40);
        idx.extend_from_slice(&(idx_entries.len() as u32).to_le_bytes());
        for (id, off) in &idx_entries {
            idx.extend_from_slice(id.as_bytes());
            idx.extend_from_slice(&off.to_le_bytes());
        }
        let tmp_pack = pack_dir.join(format!(".{name}.pack.tmp"));
        let tmp_idx = pack_dir.join(format!(".{name}.idx.tmp"));
        {
            let mut f = std::fs::File::create(&tmp_pack).map_err(LedgeError::Io)?;
            f.write_all(&pack).map_err(LedgeError::Io)?;
            f.sync_all().map_err(LedgeError::Io)?;
        }
        {
            let mut f = std::fs::File::create(&tmp_idx).map_err(LedgeError::Io)?;
            f.write_all(&idx).map_err(LedgeError::Io)?;
            f.sync_all().map_err(LedgeError::Io)?;
        }
        std::fs::rename(&tmp_idx, &idx_path).map_err(LedgeError::Io)?;
        std::fs::rename(&tmp_pack, &pack_path).map_err(LedgeError::Io)?;
        PackFile::open(&pack_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledge_core::ObjectId;

    fn id(n: u8) -> ObjectId {
        ObjectId::from_bytes([n; 32])
    }

    #[test]
    fn pack_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let objs = vec![
            (id(1), b"first record bytes (a loose object file image)".to_vec()),
            (id(2), vec![0u8; 5000]),
            (id(3), b"".to_vec()),
        ];
        let pf = PackWriter::write(dir.path(), objs.clone()).unwrap();
        for (i, bytes) in &objs {
            assert!(pf.contains(*i));
            assert_eq!(pf.get(*i).unwrap(), *bytes, "record bytes match for {}", i.to_hex());
        }
        assert!(pf.get(id(99)).is_none(), "absent id");
        assert_eq!(pf.ids().len(), 3);
        let reopened = PackFile::open(&pf.pack_path()).unwrap();
        assert_eq!(reopened.get(id(2)).unwrap(), vec![0u8; 5000]);
        assert!(reopened.contains(id(1)));
    }

    #[test]
    fn corrupt_pack_is_clean_error_no_panic() {
        let dir = tempfile::tempdir().unwrap();
        let pf = PackWriter::write(dir.path(), vec![(id(1), b"x".to_vec())]).unwrap();
        std::fs::write(pf.pack_path(), b"garbage").unwrap();
        let reopened = PackFile::open(&pf.pack_path());
        assert!(reopened.is_err() || reopened.unwrap().get(id(1)).is_none(), "no panic on corrupt pack");
    }

    #[test]
    fn missing_idx_is_error() {
        let dir = tempfile::tempdir().unwrap();
        let pf = PackWriter::write(dir.path(), vec![(id(7), b"y".to_vec())]).unwrap();
        std::fs::remove_file(pf.pack_path().with_extension("idx")).unwrap();
        assert!(PackFile::open(&pf.pack_path()).is_err());
    }
}
