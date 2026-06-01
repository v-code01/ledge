use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sha1::Digest as Sha1Digest;
use sha2::Digest as Sha2Digest;

/// LCG-based deterministic payload generator.
///
/// Uses the Knuth multiplicative hash constants for a cheap but non-trivial
/// byte sequence that avoids compression artifacts in the hash functions.
fn make_payload(size: usize) -> Vec<u8> {
    let mut buf = vec![0u8; size];
    let mut state: u64 = 0xdeadbeef_cafebabe;
    for b in buf.iter_mut() {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *b = (state >> 33) as u8;
    }
    buf
}

fn bench_blake3(c: &mut Criterion) {
    let mut g = c.benchmark_group("blake3");
    for size in [1_024usize, 65_536, 1_048_576] {
        let p = make_payload(size);
        g.throughput(Throughput::Bytes(size as u64));
        g.bench_with_input(
            BenchmarkId::from_parameter(format!("{}b", size)),
            &p,
            |b, p| {
                b.iter(|| black_box(blake3::hash(black_box(p))));
            },
        );
    }
    g.finish();
}

fn bench_sha1(c: &mut Criterion) {
    let mut g = c.benchmark_group("sha1");
    for size in [1_024usize, 65_536, 1_048_576] {
        let p = make_payload(size);
        // Prepend the Git "blob <len>\0" header to mirror actual Git hashing.
        let prefix = format!("blob {}\0", size);
        g.throughput(Throughput::Bytes(size as u64));
        g.bench_with_input(
            BenchmarkId::from_parameter(format!("{}b", size)),
            &p,
            |b, p| {
                b.iter(|| {
                    let mut h = sha1::Sha1::new();
                    Sha1Digest::update(&mut h, prefix.as_bytes());
                    Sha1Digest::update(&mut h, black_box(p));
                    black_box(Sha1Digest::finalize(h))
                });
            },
        );
    }
    g.finish();
}

fn bench_sha256(c: &mut Criterion) {
    let mut g = c.benchmark_group("sha256");
    for size in [1_024usize, 65_536, 1_048_576] {
        let p = make_payload(size);
        g.throughput(Throughput::Bytes(size as u64));
        g.bench_with_input(
            BenchmarkId::from_parameter(format!("{}b", size)),
            &p,
            |b, p| {
                b.iter(|| {
                    let mut h = sha2::Sha256::new();
                    Sha2Digest::update(&mut h, black_box(p));
                    black_box(Sha2Digest::finalize(h))
                });
            },
        );
    }
    g.finish();
}

criterion_group!(benches, bench_blake3, bench_sha1, bench_sha256);
criterion_main!(benches);
