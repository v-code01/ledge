//! Git packfile delta decoding: the two varint encodings + the copy/insert delta
//! applier. Pure + bounds-checked (never panics on malformed input).
// Task 1 of delta-capable receive-pack: these primitives are consumed by the
// pack-resolving decoder in Task 2; until then they are exercised only by the
// in-module tests, so silence the not-yet-wired-in dead-code lint.
#![allow(dead_code)]
use ledge_core::{LedgeError, Result};

const MAX_OBJECT_SIZE: usize = 2 * 1024 * 1024 * 1024; // 2 GiB delta-bomb guard

/// LEB128 size/length varint (low 7 bits, little-endian). Returns (value, next_pos).
pub(crate) fn read_size_varint(data: &[u8], mut pos: usize) -> Result<(usize, usize)> {
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
pub(crate) fn read_ofs_varint(data: &[u8], mut pos: usize) -> Result<(u64, usize)> {
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

/// Apply a git delta to `base`, producing the full reconstructed object content.
pub(crate) fn apply_delta(base: &[u8], delta: &[u8]) -> Result<Vec<u8>> {
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
}
