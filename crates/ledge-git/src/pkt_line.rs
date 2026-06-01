use ledge_core::LedgeError;

#[derive(Debug, PartialEq, Eq)]
pub enum PktLine {
    Data(Vec<u8>),
    Flush,
    Delimiter,
}

/// Encode payload as pkt-line. Total length includes the 4-byte prefix.
/// Panics if payload > 65531 bytes.
pub fn encode(payload: &[u8]) -> Vec<u8> {
    let total = payload.len() + 4;
    assert!(total <= 0xFFFF, "pkt-line payload too large: {} bytes", payload.len());
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(format!("{:04x}", total).as_bytes());
    out.extend_from_slice(payload);
    out
}

pub fn encode_flush() -> Vec<u8> { b"0000".to_vec() }
pub fn encode_delimiter() -> Vec<u8> { b"0001".to_vec() }

/// Decode one pkt-line from the front of input. Returns (PktLine, remaining).
pub fn decode_line(input: &[u8]) -> Result<(PktLine, &[u8]), LedgeError> {
    if input.len() < 4 {
        return Err(LedgeError::Corruption(format!("pkt-line: need 4 bytes, got {}", input.len())));
    }
    let prefix = std::str::from_utf8(&input[..4])
        .map_err(|_| LedgeError::Corruption("pkt-line: non-UTF-8 prefix".into()))?;
    let total = u16::from_str_radix(prefix, 16)
        .map_err(|_| LedgeError::Corruption(format!("pkt-line: invalid hex {:?}", prefix)))? as usize;

    match total {
        0 => return Ok((PktLine::Flush, &input[4..])),
        1 => return Ok((PktLine::Delimiter, &input[4..])),
        2 | 3 => return Err(LedgeError::Corruption(format!("pkt-line: illegal length {}", total))),
        _ => {}
    }

    if input.len() < total {
        return Err(LedgeError::Corruption(format!(
            "pkt-line: declared {} bytes but only {} available", total, input.len()
        )));
    }
    Ok((PktLine::Data(input[4..total].to_vec()), &input[total..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_simple_payload() {
        let encoded = encode(b"hello\n");
        assert_eq!(&encoded[..4], b"000a");
        assert_eq!(&encoded[4..], b"hello\n");
        assert_eq!(encoded.len(), 10);
    }

    #[test]
    fn encode_flush_packet() { assert_eq!(encode_flush(), b"0000"); }

    #[test]
    fn encode_delimiter_packet() { assert_eq!(encode_delimiter(), b"0001"); }

    #[test]
    fn encode_empty_payload() {
        let encoded = encode(b"");
        assert_eq!(&encoded[..4], b"0004");
        assert_eq!(encoded.len(), 4);
    }

    #[test]
    fn encode_max_payload() {
        let payload = vec![0xABu8; 65531];
        let encoded = encode(&payload);
        assert_eq!(&encoded[..4], b"ffff");
        assert_eq!(encoded.len(), 65535);
    }

    #[test]
    fn decode_flush() {
        let (line, rest) = decode_line(b"0000rest").unwrap();
        assert!(matches!(line, PktLine::Flush));
        assert_eq!(rest, b"rest");
    }

    #[test]
    fn decode_delimiter() {
        let (line, rest) = decode_line(b"0001rest").unwrap();
        assert!(matches!(line, PktLine::Delimiter));
        assert_eq!(rest, b"rest");
    }

    #[test]
    fn decode_data() {
        let mut buf = encode(b"hello\n");
        buf.extend_from_slice(b"EXTRA");
        let (line, rest) = decode_line(&buf).unwrap();
        assert!(matches!(line, PktLine::Data(d) if d == b"hello\n"));
        assert_eq!(rest, b"EXTRA");
    }

    #[test]
    fn decode_too_short() { assert!(decode_line(b"00").is_err()); }

    #[test]
    fn decode_truncated_payload() { assert!(decode_line(b"000ahell").is_err()); }

    #[test]
    fn roundtrip_various_lengths() {
        for size in [0, 1, 7, 128, 1024, 65531] {
            let payload: Vec<u8> = (0..size).map(|i| (i & 0xFF) as u8).collect();
            let encoded = encode(&payload);
            let (line, rest) = decode_line(&encoded).unwrap();
            assert!(rest.is_empty());
            assert!(matches!(line, PktLine::Data(d) if d == payload));
        }
    }

    #[test]
    fn decode_stream_sequential() {
        let mut stream = Vec::new();
        stream.extend_from_slice(&encode(b"line1\n"));
        stream.extend_from_slice(&encode(b"line2\n"));
        stream.extend_from_slice(&encode_flush());
        let (l1, rem) = decode_line(&stream).unwrap();
        assert!(matches!(l1, PktLine::Data(d) if d == b"line1\n"));
        let (l2, rem) = decode_line(rem).unwrap();
        assert!(matches!(l2, PktLine::Data(d) if d == b"line2\n"));
        let (l3, rem) = decode_line(rem).unwrap();
        assert!(matches!(l3, PktLine::Flush));
        assert!(rem.is_empty());
    }
}
