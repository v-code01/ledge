//! Git packfile delta codec: the two varint encodings, the copy/insert delta
//! applier (decoder), and a git-format delta encoder. Pure + bounds-checked
//! (never panics on malformed input).
//!
//! The decoder (`apply_delta`) is consumed by the pack-resolving decoder in
//! `ledge-git`'s `push.rs` to reconstruct OFS_DELTA / REF_DELTA objects. The
//! encoder (`encode_delta`) produces the exact same on-disk format so
//! `ledge-object-store` can store objects as deltas against a base.
use crate::{LedgeError, Result};

const MAX_OBJECT_SIZE: usize = 2 * 1024 * 1024 * 1024; // 2 GiB delta-bomb guard

/// LEB128 size/length varint (low 7 bits, little-endian). Returns (value, next_pos).
pub fn read_size_varint(data: &[u8], mut pos: usize) -> Result<(usize, usize)> {
    let mut result: usize = 0;
    let mut shift = 0u32;
    loop {
        let b = *data.get(pos).ok_or_else(|| LedgeError::Corruption("delta: truncated varint".into()))?;
        pos += 1;
        result |= ((b & 0x7f) as usize)
            .checked_shl(shift)
            .ok_or_else(|| LedgeError::Corruption("delta: varint overflow".into()))?;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 63 {
            return Err(LedgeError::Corruption("delta: varint too long".into()));
        }
    }
    Ok((result, pos))
}

/// Git's OFS_DELTA base-offset encoding (`((off+1)<<7)|x` continuation form).
/// Returns (offset, next_pos).
pub fn read_ofs_varint(data: &[u8], mut pos: usize) -> Result<(u64, usize)> {
    let mut c = *data.get(pos).ok_or_else(|| LedgeError::Corruption("delta: truncated ofs".into()))?;
    pos += 1;
    let mut off = (c & 0x7f) as u64;
    while c & 0x80 != 0 {
        c = *data.get(pos).ok_or_else(|| LedgeError::Corruption("delta: truncated ofs".into()))?;
        pos += 1;
        off = off
            .checked_add(1)
            .and_then(|v| v.checked_shl(7))
            .map(|v| v | (c & 0x7f) as u64)
            .ok_or_else(|| LedgeError::Corruption("delta: ofs overflow".into()))?;
    }
    Ok((off, pos))
}

/// Encode git's OFS_DELTA base-offset varint — the exact inverse of
/// [`read_ofs_varint`]. The value is the positive distance from the delta
/// object's pack offset back to its (earlier) base's offset. Uses git's
/// `((off+1)<<7)|x` continuation form: big-endian, with each continuation byte's
/// 7-bit group decremented so the decoder's `+1` on every continuation round-trips.
pub fn write_ofs_varint(out: &mut Vec<u8>, mut off: u64) {
    // Fill a scratch buffer from the back: the last byte has the low 7 bits and
    // no continuation flag; each preceding byte carries 7 more bits with 0x80 set.
    let mut tmp = [0u8; 16];
    let mut pos = tmp.len() - 1;
    tmp[pos] = (off & 0x7f) as u8;
    while off >> 7 != 0 {
        off = (off >> 7) - 1;
        pos -= 1;
        tmp[pos] = 0x80 | (off & 0x7f) as u8;
    }
    out.extend_from_slice(&tmp[pos..]);
}

/// Apply a git delta to `base`, producing the full reconstructed object content.
pub fn apply_delta(base: &[u8], delta: &[u8]) -> Result<Vec<u8>> {
    let (base_size, mut p) = read_size_varint(delta, 0)?;
    if base_size != base.len() {
        return Err(LedgeError::Corruption(format!(
            "delta: base size {base_size} != {}",
            base.len()
        )));
    }
    let (result_size, p2) = read_size_varint(delta, p)?;
    p = p2;
    if result_size > MAX_OBJECT_SIZE {
        return Err(LedgeError::Corruption("delta: result too large".into()));
    }
    let mut out = Vec::with_capacity(result_size.min(1 << 20));
    while p < delta.len() {
        let op = delta[p];
        p += 1;
        if op & 0x80 != 0 {
            // copy from base: offset bytes (bits 0-3), size bytes (bits 4-6)
            let mut cp_off = 0usize;
            for i in 0..4 {
                if op & (1 << i) != 0 {
                    cp_off |= (*delta
                        .get(p)
                        .ok_or_else(|| LedgeError::Corruption("delta: trunc copy off".into()))?
                        as usize)
                        << (8 * i);
                    p += 1;
                }
            }
            let mut cp_size = 0usize;
            for i in 0..3 {
                if op & (0x10 << i) != 0 {
                    cp_size |= (*delta
                        .get(p)
                        .ok_or_else(|| LedgeError::Corruption("delta: trunc copy size".into()))?
                        as usize)
                        << (8 * i);
                    p += 1;
                }
            }
            if cp_size == 0 {
                cp_size = 0x10000;
            }
            let end = cp_off
                .checked_add(cp_size)
                .ok_or_else(|| LedgeError::Corruption("delta: copy overflow".into()))?;
            if end > base.len() {
                return Err(LedgeError::Corruption("delta: copy out of base bounds".into()));
            }
            out.extend_from_slice(&base[cp_off..end]);
        } else if op != 0 {
            // insert literal: op = length 1..=127
            let len = op as usize;
            let end = p
                .checked_add(len)
                .ok_or_else(|| LedgeError::Corruption("delta: insert overflow".into()))?;
            if end > delta.len() {
                return Err(LedgeError::Corruption("delta: insert out of delta bounds".into()));
            }
            out.extend_from_slice(&delta[p..end]);
            p = end;
        } else {
            return Err(LedgeError::Corruption("delta: zero opcode".into()));
        }
        if out.len() > MAX_OBJECT_SIZE {
            return Err(LedgeError::Corruption("delta: result exceeded cap".into()));
        }
    }
    if out.len() != result_size {
        return Err(LedgeError::Corruption(format!(
            "delta: result size {} != {result_size}",
            out.len()
        )));
    }
    Ok(out)
}

/// Block-hash window width for the delta matcher.
const DELTA_WIN: usize = 16;

/// A reusable index over a delta *base*: the block-hash map is built ONCE in
/// [`DeltaIndex::new`], then [`DeltaIndex::delta`] can encode any number of
/// targets against it without rebuilding. This is the load-bearing optimization
/// for a wide pack delta-window: probing N targets against one base costs one
/// index build, not N (the per-probe rebuild was the wall that made a 250-wide
/// window unaffordable).
pub struct DeltaIndex<'a> {
    base: &'a [u8],
    index: std::collections::HashMap<u64, Vec<usize>>,
}

impl<'a> DeltaIndex<'a> {
    /// Build the block-hash index of `base` once.
    pub fn new(base: &'a [u8]) -> Self {
        let mut index: std::collections::HashMap<u64, Vec<usize>> =
            std::collections::HashMap::new();
        if base.len() >= DELTA_WIN {
            for i in 0..=base.len() - DELTA_WIN {
                index
                    .entry(hash_window(&base[i..i + DELTA_WIN]))
                    .or_default()
                    .push(i);
            }
        }
        Self { base, index }
    }

    /// Encode a git-format delta transforming this index's `base` into `target`.
    /// Output: `[base_size varint][target_size varint][ops]` — the exact format
    /// [`apply_delta`] consumes. Greedy block-hash matcher: not optimal, always valid.
    pub fn delta(&self, target: &[u8]) -> Vec<u8> {
        let base = self.base;
        let mut out = Vec::new();
        write_size_varint(&mut out, base.len());
        write_size_varint(&mut out, target.len());

        let mut pending: Vec<u8> = Vec::new();
        let mut t = 0usize;
        while t < target.len() {
            let mut best_off = 0usize;
            let mut best_len = 0usize;
            if base.len() >= DELTA_WIN && t + DELTA_WIN <= target.len() {
                if let Some(cands) = self.index.get(&hash_window(&target[t..t + DELTA_WIN])) {
                    for &b in cands.iter().take(64) {
                        if base[b..b + DELTA_WIN] != target[t..t + DELTA_WIN] {
                            continue;
                        }
                        let mut len = DELTA_WIN;
                        while b + len < base.len()
                            && t + len < target.len()
                            && base[b + len] == target[t + len]
                        {
                            len += 1;
                        }
                        if len > best_len {
                            best_len = len;
                            best_off = b;
                        }
                    }
                }
            }
            if best_len >= DELTA_WIN {
                flush_insert(&mut out, &mut pending);
                emit_copy(&mut out, best_off, best_len);
                t += best_len;
            } else {
                pending.push(target[t]);
                t += 1;
            }
        }
        flush_insert(&mut out, &mut pending);
        out
    }
}

/// Encode a git-format delta transforming `base` into `target`. Thin wrapper over
/// [`DeltaIndex`] (builds the base index, encodes once) — kept for callers that
/// delta a single target; the pack writer uses `DeltaIndex` directly to amortize.
pub fn encode_delta(base: &[u8], target: &[u8]) -> Vec<u8> {
    DeltaIndex::new(base).delta(target)
}

fn hash_window(w: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in w {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn write_size_varint(out: &mut Vec<u8>, mut v: usize) {
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if v == 0 {
            break;
        }
    }
}

fn flush_insert(out: &mut Vec<u8>, pending: &mut Vec<u8>) {
    let mut i = 0;
    while i < pending.len() {
        let chunk = (pending.len() - i).min(127);
        out.push(chunk as u8);
        out.extend_from_slice(&pending[i..i + chunk]);
        i += chunk;
    }
    pending.clear();
}

fn emit_copy(out: &mut Vec<u8>, offset: usize, size: usize) {
    let mut remaining = size;
    let mut off = offset;
    while remaining > 0 {
        let chunk = remaining.min(0xff_ffff);
        let mut op: u8 = 0x80;
        let mut bytes: Vec<u8> = Vec::new();
        for i in 0..4 {
            let b = ((off >> (8 * i)) & 0xff) as u8;
            if b != 0 {
                op |= 1 << i;
                bytes.push(b);
            }
        }
        for i in 0..3 {
            let b = ((chunk >> (8 * i)) & 0xff) as u8;
            if b != 0 {
                op |= 0x10 << i;
                bytes.push(b);
            }
        }
        out.push(op);
        out.extend_from_slice(&bytes);
        off += chunk;
        remaining -= chunk;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn size_varint_known() {
        assert_eq!(read_size_varint(&[0x80, 0x01], 0).unwrap(), (128, 2));
        assert_eq!(read_size_varint(&[0x05], 0).unwrap(), (5, 1));
        assert!(read_size_varint(&[0x80], 0).is_err()); // truncated
    }
    #[test]
    fn ofs_varint_known() {
        assert_eq!(read_ofs_varint(&[0x01], 0).unwrap(), (1, 1));
        assert_eq!(read_ofs_varint(&[0x80, 0x00], 0).unwrap(), (128, 2));
        assert!(read_ofs_varint(&[0x80], 0).is_err()); // truncated
    }
    #[test]
    fn write_ofs_varint_roundtrips_read() {
        for &off in &[0u64, 1, 2, 126, 127, 128, 129, 16_383, 16_384, 16_385, 1 << 21, (1 << 28) + 7] {
            let mut buf = Vec::new();
            write_ofs_varint(&mut buf, off);
            let (got, used) = read_ofs_varint(&buf, 0).unwrap();
            assert_eq!(got, off, "ofs varint roundtrip for {off}");
            assert_eq!(used, buf.len(), "consumed all bytes for {off}");
        }
    }
    #[test]
    fn apply_delta_copy_insert_copy() {
        // base "hello world" (11) -> result "hello RUST world" (16):
        //   copy(off=0,size=5)="hello"; insert " RUST"(5); copy(off=5,size=6)=" world"
        // delta bytes:
        //   base_size=11 -> 0x0b ; result_size=16 -> 0x10
        //   copy op 0x80|0x01|0x10 = 0x91, off-byte 0x00, size-byte 0x05
        //   insert op 0x05 then b" RUST"
        //   copy op 0x91, off-byte 0x05, size-byte 0x06
        let delta: &[u8] = &[
            0x0b, 0x10,
            0x91, 0x00, 0x05,
            0x05, b' ', b'R', b'U', b'S', b'T',
            0x91, 0x05, 0x06,
        ];
        assert_eq!(apply_delta(b"hello world", delta).unwrap(), b"hello RUST world".to_vec());
    }
    #[test]
    fn apply_delta_rejects_base_size_mismatch() {
        // base_size header says 99 but base is 1 byte ⇒ Corruption, no panic
        assert!(apply_delta(b"x", &[99u8, 1u8, b'\x01', b'y']).is_err());
    }
    #[test]
    fn apply_delta_rejects_oob_copy_and_truncation() {
        // copy off=0 size=200 from a 3-byte base ⇒ OOB ⇒ Corruption
        assert!(apply_delta(b"abc", &[3u8, 200u8, 0x91, 0x00, 200u8]).is_err());
        // insert says 5 bytes but none follow ⇒ Corruption
        assert!(apply_delta(b"abc", &[3u8, 2u8, 0x05]).is_err());
        // zero opcode ⇒ Corruption
        assert!(apply_delta(b"abc", &[3u8, 0u8, 0x00]).is_err());
    }

    #[test]
    fn encode_delta_roundtrips() {
        let base500: Vec<u8> = (0..500).flat_map(|i| format!("line {i}\n").into_bytes()).collect();
        let edited = String::from_utf8(base500.clone()).unwrap().replace("line 250\n", "LINE TWO FIFTY\n").into_bytes();
        let cases: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (base500.clone(), edited.clone()),
            (base500.clone(), base500.clone()),
            (base500.clone(), [base500.clone(), b"appended tail\n".to_vec()].concat()),
            ([b"prefix\n".to_vec(), base500.clone()].concat(), base500.clone()),
            (b"".to_vec(), b"hello".to_vec()),
            (b"hello".to_vec(), b"".to_vec()),
            ((0..2000u32).map(|i| (i.wrapping_mul(2654435761) >> 24) as u8).collect(),
             (0..2000u32).map(|i| (i.wrapping_mul(40503) >> 24) as u8).collect()),
        ];
        for (base, target) in cases {
            let d = encode_delta(&base, &target);
            let out = apply_delta(&base, &d).expect("apply");
            assert_eq!(out, target, "round-trip (base {} target {})", base.len(), target.len());
        }
    }
    proptest::proptest! {
        // apply_delta runs on attacker-controlled bytes (a REF_DELTA/OFS_DELTA in a
        // pushed pack). On ANY (base, delta) it must return Ok or Err — never panic,
        // never hang, never over-allocate (the MAX_OBJECT_SIZE guard bounds output).
        #[test]
        fn apply_delta_never_panics(
            base in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..256),
            delta in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..512),
        ) {
            let _ = apply_delta(&base, &delta);
        }
        // A valid encode→apply round-trips for arbitrary content.
        #[test]
        fn encode_apply_roundtrips_arbitrary(
            base in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..1024),
            target in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..1024),
        ) {
            let d = encode_delta(&base, &target);
            proptest::prop_assert_eq!(apply_delta(&base, &d).unwrap(), target);
        }
    }

    #[test]
    fn delta_index_matches_encode_delta() {
        // The reusable DeltaIndex must produce byte-identical deltas to the
        // one-shot encode_delta over the full roundtrip corpus.
        let base500: Vec<u8> = (0..500).flat_map(|i| format!("line {i}\n").into_bytes()).collect();
        let edited = String::from_utf8(base500.clone()).unwrap().replace("line 250\n", "X\n").into_bytes();
        let cases: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (base500.clone(), edited),
            (base500.clone(), base500.clone()),
            (base500.clone(), [base500.clone(), b"tail\n".to_vec()].concat()),
            (b"".to_vec(), b"hello".to_vec()),
            (b"hello".to_vec(), b"".to_vec()),
        ];
        for (base, target) in cases {
            let idx = DeltaIndex::new(&base);
            assert_eq!(idx.delta(&target), encode_delta(&base, &target));
            // and the index-built delta still round-trips
            assert_eq!(apply_delta(&base, &idx.delta(&target)).unwrap(), target);
        }
    }

    #[test]
    fn encode_delta_small_edit_is_small() {
        let base: Vec<u8> = (0..500).flat_map(|i| format!("line {i}\n").into_bytes()).collect();
        let target = String::from_utf8(base.clone()).unwrap().replace("line 250\n", "X\n").into_bytes();
        let d = encode_delta(&base, &target);
        assert!(d.len() < target.len() / 2, "delta {} should be << target {}", d.len(), target.len());
    }
}
