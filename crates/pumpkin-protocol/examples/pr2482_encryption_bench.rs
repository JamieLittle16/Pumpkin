use bytes::Bytes;
use pumpkin_protocol::java::packet_encoder::TCPNetworkEncoder;
use std::{io, pin::Pin, sync::{Arc, atomic::{AtomicUsize, Ordering}}, task::{Context, Poll}, time::Instant};
use tokio::io::{AsyncWrite, BufWriter};

#[derive(Default)]
struct Count {
    writes: AtomicUsize,
    bytes: AtomicUsize,
}
struct Sink(Arc<Count>);
impl AsyncWrite for Sink {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.0.writes.fetch_add(1, Ordering::Relaxed);
        self.0.bytes.fetch_add(buf.len(), Ordering::Relaxed);
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let size: usize = args[1].parse().unwrap();
    let count: usize = args[2].parse().unwrap();
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let stats = Arc::new(Count::default());
    let mut encoder = TCPNetworkEncoder::new(BufWriter::new(Sink(stats.clone())));
    encoder.set_encryption(&[0x42; 16]).unwrap();
    let packet = Bytes::from((0..size).map(|i| (i * 131 % 251) as u8).collect::<Vec<_>>());
    rt.block_on(async {
        for _ in 0..4 { encoder.write_packet(packet.clone()).await.unwrap(); }
        encoder.flush().await.unwrap();
    });
    stats.writes.store(0, Ordering::Relaxed);
    stats.bytes.store(0, Ordering::Relaxed);
    let before = Instant::now();
    rt.block_on(async {
        for _ in 0..count { encoder.write_packet(packet.clone()).await.unwrap(); }
        encoder.flush().await.unwrap();
    });
    println!("{size},{count},{},{},{}", before.elapsed().as_nanos(),
        stats.writes.load(Ordering::Relaxed), stats.bytes.load(Ordering::Relaxed));
}
