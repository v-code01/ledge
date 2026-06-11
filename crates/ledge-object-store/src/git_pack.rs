//! Native git v2 packfile + index writer. NON-DELTA (every object zlib(content)).
//! Validated against git's own `verify-pack`/`unpack-objects`.

use std::io::Write;

use ledge_core::{LedgeError, Result};

pub struct PackObject {
    pub git_type: u8, // 1=commit 2=tree 3=blob 4=tag
    pub content: Vec<u8>,
    pub sha1: [u8; 20],
}

fn zlib(data: &[u8]) -> Vec<u8> {
    let mut e =
        flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(data).expect("zlib write to Vec infallible");
    e.finish().expect("zlib finish infallible")
}

/// git pack object header: type/size varint. First byte: bit7=continuation,
/// bits4-6=type, bits0-3=low 4 bits of size; subsequent bytes 7 bits of size each.
fn write_obj_header(out: &mut Vec<u8>, git_type: u8, mut size: usize) {
    let mut byte = (git_type << 4) | ((size & 0x0f) as u8);
    size >>= 4;
    while size > 0 {
        out.push(byte | 0x80);
        byte = (size & 0x7f) as u8;
        size >>= 7;
    }
    out.push(byte);
}

/// Write a git v2 pack + idx (non-delta). Returns (pack_bytes, idx_bytes).
///
/// Complexity: O(n log n) over object count (idx requires sha-sorted order);
/// each object is compressed once and its CRC-32 computed once.
/// Side effects: none (pure). Errors only on packs that would exceed the
/// 4-byte (2 GiB) offset table this Phase A1 writer supports.
pub fn write_git_pack(objects: &[PackObject]) -> Result<(Vec<u8>, Vec<u8>)> {
    // ---- pack ----
    let mut pack = Vec::new();
    pack.extend_from_slice(b"PACK");
    pack.extend_from_slice(&2u32.to_be_bytes());
    pack.extend_from_slice(&(objects.len() as u32).to_be_bytes());
    // record (sha1, offset, crc32) per object for the idx
    let mut entries: Vec<([u8; 20], u64, u32)> = Vec::with_capacity(objects.len());
    for o in objects {
        let offset = pack.len() as u64;
        if offset >= 0x8000_0000 {
            return Err(LedgeError::Corruption(
                "git_pack: large-offset packs unsupported (Phase A1)".into(),
            ));
        }
        let start = pack.len();
        write_obj_header(&mut pack, o.git_type, o.content.len());
        pack.extend_from_slice(&zlib(&o.content));
        // crc32 of the object's packed bytes (header + zlib)
        let mut crc = flate2::Crc::new();
        crc.update(&pack[start..]);
        entries.push((o.sha1, offset, crc.sum()));
    }
    // pack trailer = sha1 of all preceding pack bytes
    let pack_sha = {
        use sha1::{Digest, Sha1};
        let h: [u8; 20] = Sha1::digest(&pack).into();
        h
    };
    pack.extend_from_slice(&pack_sha);

    // ---- idx v2 ---- (entries sorted ascending by sha1)
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let mut idx = Vec::new();
    idx.extend_from_slice(&[0xff, 0x74, 0x4f, 0x63]); // \377tOc
    idx.extend_from_slice(&2u32.to_be_bytes());
    // fanout[256]: cumulative count with first byte <= i
    let mut fanout = [0u32; 256];
    for (sha, _, _) in &entries {
        fanout[sha[0] as usize] += 1;
    }
    let mut cum = 0u32;
    for f in fanout.iter_mut() {
        cum += *f;
        *f = cum;
    }
    for f in &fanout {
        idx.extend_from_slice(&f.to_be_bytes());
    }
    // sorted sha1s
    for (sha, _, _) in &entries {
        idx.extend_from_slice(sha);
    }
    // crc32s (sha-sorted order)
    for (_, _, crc) in &entries {
        idx.extend_from_slice(&crc.to_be_bytes());
    }
    // 4-byte offsets (sha-sorted order); high bit clear (no large-offset table in A1)
    for (_, off, _) in &entries {
        idx.extend_from_slice(&(*off as u32).to_be_bytes());
    }
    // trailers: pack sha + idx sha
    idx.extend_from_slice(&pack_sha);
    let idx_sha = {
        use sha1::{Digest, Sha1};
        let h: [u8; 20] = Sha1::digest(&idx).into();
        h
    };
    idx.extend_from_slice(&idx_sha);

    Ok((pack, idx))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Decode a git pack object header (inverse of `write_obj_header`).
    /// Returns (git_type, size, bytes_consumed).
    fn read_obj_header(buf: &[u8]) -> (u8, usize, usize) {
        let first = buf[0];
        let git_type = (first >> 4) & 0x07;
        let mut size = (first & 0x0f) as usize;
        let mut shift = 4;
        let mut i = 0;
        let mut byte = first;
        while byte & 0x80 != 0 {
            i += 1;
            byte = buf[i];
            size |= ((byte & 0x7f) as usize) << shift;
            shift += 7;
        }
        (git_type, size, i + 1)
    }

    #[test]
    fn obj_header_roundtrips() {
        // Cover boundary sizes around the 4-bit and each 7-bit varint chunk.
        for &size in &[
            0usize, 1, 15, 16, 17, 127, 128, 2047, 2048, 65_535, 1 << 20, (1 << 28) + 5,
        ] {
            for git_type in 1u8..=4 {
                let mut out = Vec::new();
                write_obj_header(&mut out, git_type, size);
                let (ty, sz, used) = read_obj_header(&out);
                assert_eq!(ty, git_type, "type mismatch for size {size}");
                assert_eq!(sz, size, "size mismatch for type {git_type}");
                assert_eq!(used, out.len(), "consumed all header bytes for size {size}");
            }
        }
    }

    // helper: run git, return stdout (asserting success)
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

    #[tokio::test]
    async fn git_accepts_our_pack_and_unpack_roundtrips() {
        // 1) Build a source repo with a few objects (blob+tree+commit) via real git.
        let repo = tempfile::tempdir().unwrap();
        git(&["init", "--initial-branch=main", "."], repo.path());
        git(&["config", "user.email", "t@l"], repo.path());
        git(&["config", "user.name", "t"], repo.path());
        std::fs::write(repo.path().join("a.txt"), b"hello pack\n").unwrap();
        git(&["add", "."], repo.path());
        git(&["commit", "-m", "c1"], repo.path());

        // 2) Enumerate every object as (sha1, type, content) via cat-file.
        let names = git(
            &[
                "cat-file",
                "--batch-all-objects",
                "--batch-check=%(objectname) %(objecttype)",
            ],
            repo.path(),
        );
        let mut objs: Vec<PackObject> = Vec::new();
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
        assert!(objs.len() >= 3);

        // 3) Write our pack+idx, name by the pack-trailer sha (git convention is
        //    flexible; we just place both).
        let (pack, idx) = write_git_pack(&objs).unwrap();
        let out = tempfile::tempdir().unwrap();
        let packdir = out.path().join("pk");
        std::fs::create_dir_all(&packdir).unwrap();
        std::fs::write(packdir.join("test.pack"), &pack).unwrap();
        std::fs::write(packdir.join("test.idx"), &idx).unwrap();

        // 4) ORACLE: git verify-pack must accept it + report all objects.
        let vp = std::process::Command::new("git")
            .args([
                "verify-pack",
                "-v",
                packdir.join("test.idx").to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert!(
            vp.status.success(),
            "git verify-pack rejected our pack:\nstdout={}\nstderr={}",
            String::from_utf8_lossy(&vp.stdout),
            String::from_utf8_lossy(&vp.stderr)
        );
        let vptxt = String::from_utf8_lossy(&vp.stdout);
        for o in &objs {
            let hex: String = o.sha1.iter().map(|b| format!("{b:02x}")).collect();
            assert!(vptxt.contains(&hex), "verify-pack missing object {hex}");
        }

        // 5) ORACLE: git unpack-objects round-trips every object byte-identically.
        let dst = tempfile::tempdir().unwrap();
        git(&["init", "--bare", "."], dst.path());
        let status = std::process::Command::new("git")
            .args(["unpack-objects"])
            .current_dir(dst.path())
            .stdin(std::process::Stdio::from(
                std::fs::File::open(packdir.join("test.pack")).unwrap(),
            ))
            .output()
            .unwrap();
        assert!(
            status.status.success(),
            "unpack-objects: {}",
            String::from_utf8_lossy(&status.stderr)
        );
        for o in &objs {
            let hex: String = o.sha1.iter().map(|b| format!("{b:02x}")).collect();
            let ty = match o.git_type {
                1 => "commit",
                2 => "tree",
                3 => "blob",
                4 => "tag",
                _ => unreachable!(),
            };
            let got = git(&["cat-file", ty, &hex], dst.path());
            assert_eq!(got, o.content, "unpacked object {hex} content mismatch");
        }
    }
}
