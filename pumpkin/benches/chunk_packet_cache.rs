use criterion::{Criterion, criterion_group, criterion_main};
use pumpkin::server::chunk_packet_cache::ChunkPacketCache;
use pumpkin_protocol::java::packet_encoder::CompressionProfile;
use pumpkin_util::background_cpu::BackgroundCpuBudget;
use pumpkin_util::version::JavaMinecraftVersion;
use pumpkin_world::chunk::ChunkData;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create bench runtime")
}

fn cache(capacity_mib: usize, preparation_threads: usize) -> Arc<ChunkPacketCache> {
    Arc::new(ChunkPacketCache::new(
        capacity_mib,
        preparation_threads,
        Arc::new(BackgroundCpuBudget::new(8, 7, preparation_threads)),
    ))
}

fn chunk(x: i32) -> Arc<ChunkData> {
    Arc::new(ChunkData::new_empty(x, 0, 1, 0))
}

fn bench_prepare_fresh(c: &mut Criterion) {
    let runtime = runtime();
    let cache = cache(64, 4);
    let next_x = AtomicI32::new(0);
    c.bench_function("prepare_fresh_empty_chunk", |b| {
        b.to_async(&runtime).iter(|| {
            let cache = Arc::clone(&cache);
            let chunk = chunk(next_x.fetch_add(1, Ordering::Relaxed));
            async move {
                cache
                    .prepare_chunk(chunk, JavaMinecraftVersion::V_26_2, None)
                    .await
            }
        });
    });
}

fn bench_cached_hit(c: &mut Criterion) {
    let runtime = runtime();
    let cache = cache(64, 4);
    let chunk = chunk(0);
    runtime.block_on(async {
        cache
            .prepare_chunk(Arc::clone(&chunk), JavaMinecraftVersion::V_26_2, None)
            .await
            .expect("warmup prepare failed");
    });
    c.bench_function("cached_hit", |b| {
        b.to_async(&runtime).iter(|| {
            let cache = Arc::clone(&cache);
            let chunk = Arc::clone(&chunk);
            async move {
                cache
                    .prepare_chunk(chunk, JavaMinecraftVersion::V_26_2, None)
                    .await
            }
        });
    });
}

fn bench_compressed_miss(c: &mut Criterion) {
    let runtime = runtime();
    let cache = cache(64, 4);
    let next_x = AtomicI32::new(0);
    let compression = Some(CompressionProfile {
        threshold: 0,
        level: 1,
    });
    c.bench_function("compressed_tier_miss", |b| {
        b.to_async(&runtime).iter(|| {
            let cache = Arc::clone(&cache);
            let chunk = chunk(next_x.fetch_add(1, Ordering::Relaxed));
            async move {
                cache
                    .prepare_chunk(Arc::clone(&chunk), JavaMinecraftVersion::V_26_2, None)
                    .await
                    .expect("serialized warmup failed");
                cache
                    .prepare_chunk(chunk, JavaMinecraftVersion::V_26_2, compression)
                    .await
            }
        });
    });
}

fn bench_concurrent_dedup(c: &mut Criterion) {
    let runtime = runtime();
    let cache = cache(64, 4);
    let next_x = AtomicI32::new(0);
    c.bench_function("concurrent_dedup_64", |b| {
        b.to_async(&runtime).iter(|| {
            let cache = Arc::clone(&cache);
            let chunk = chunk(next_x.fetch_add(1, Ordering::Relaxed));
            async move {
                let mut tasks = tokio::task::JoinSet::new();
                for _ in 0..64 {
                    let cache = Arc::clone(&cache);
                    let chunk = Arc::clone(&chunk);
                    tasks.spawn(async move {
                        cache
                            .prepare_chunk(chunk, JavaMinecraftVersion::V_26_2, None)
                            .await
                    });
                }
                while let Some(result) = tasks.join_next().await {
                    result
                        .expect("prepare task panicked")
                        .expect("prepare failed");
                }
            }
        });
    });
}

fn bench_eviction_churn(c: &mut Criterion) {
    let runtime = runtime();
    let cache = cache(1, 4);
    let chunks: Vec<Arc<ChunkData>> = (0..512)
        .map(|x| Arc::new(ChunkData::new_empty(x, 0, 1, 0)))
        .collect();
    c.bench_function("eviction_churn_512", |b| {
        b.to_async(&runtime).iter(|| {
            let cache = Arc::clone(&cache);
            let chunks = chunks.clone();
            async move {
                for chunk in &chunks {
                    cache
                        .prepare_chunk(Arc::clone(chunk), JavaMinecraftVersion::V_26_2, None)
                        .await
                        .expect("prepare failed");
                }
            }
        });
    });
}

criterion_group!(
    benches,
    bench_prepare_fresh,
    bench_cached_hit,
    bench_compressed_miss,
    bench_concurrent_dedup,
    bench_eviction_churn
);
criterion_main!(benches);
