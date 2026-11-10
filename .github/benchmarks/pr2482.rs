//! Temporary PR #2482 paired microbenchmark; copied into both revisions unchanged.
//! Each iteration writes the same framed byte pattern and flushes the same 8KiB Tokio BufWriter.
use pumpkin_protocol::java::packet_encoder::TCPNetworkEncoder;
use std::{env, hint::black_box, time::Instant};
use tokio::io::BufWriter;

fn parameters(name: &str) -> (usize, usize) {
    match name {
        "64b" => (64, 64),
        "1k" => (1024, 1024),
        "16k" => (16 * 1024, 16 * 1024),
        "64k" => (64 * 1024, 64 * 1024),
        "1m" => (1024 * 1024, 1024 * 1024),
        "64k-split-64b" => (64 * 1024, 64),
        "64k-split-1k" => (64 * 1024, 1024),
        "64k-split-16k" => (64 * 1024, 16 * 1024),
        _ => panic!("Unknown scenario: {name}"),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut args = env::args().skip(1);
    let name = args.next().expect("scenario");
    let iterations: usize = args.next().expect("iterations").parse().unwrap();
    assert!(iterations > 0);
    let (size, chunk) = parameters(&name);
    let key = [0x6du8; 16];

    // Deterministic, non-constant data. Both revisions use the identical source.
    let mut original = vec![0u8; size];
    let mut seed: u32 = 0x1234_abcd;
    for byte in &mut original {
        seed ^= seed << 13;
        seed ^= seed >> 17;
        seed ^= seed << 5;
        *byte = seed as u8;
    }

    // Exercise the same per-connection encryption-setup and write path.
    // Include the frame copy in BOTH versions because the new API mutates input.
    let run = |count: usize| async {
        for _ in 0..count {
            let mut frame = original.clone();
            let buffered = BufWriter::with_capacity(8192, tokio::io::sink());
            let mut encoder = TCPNetworkEncoder::new(buffered);
            encoder.set_encryption(&key).unwrap();
            for bytes in frame.chunks_mut(chunk) {
                encoder.write_frame(bytes).await.unwrap();
            }
            encoder.flush().await.unwrap();
            black_box(frame);
        }
    };

    run((iterations / 10).max(2)).await;
    let start = Instant::now();
    run(iterations).await;
    let ns_per_iter = start.elapsed().as_nanos() as f64 / iterations as f64;
    println!("{ns_per_iter:.3}");
}
