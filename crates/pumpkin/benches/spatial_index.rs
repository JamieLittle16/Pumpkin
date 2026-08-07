use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use crossbeam::atomic::AtomicCell;
use pumpkin_util::math::boundingbox::BoundingBox;
use pumpkin_util::math::vector3::Vector3;
use std::sync::Arc;

use pumpkin::entity::living::LivingEntity;
use pumpkin::entity::spatial_metrics::SpatialMetrics;
use pumpkin::entity::spatial_pose::SpatialCategory;
use pumpkin::entity::world_spatial_index::WorldSpatialIndex;
use pumpkin::entity::{Entity, EntityBase, NBTStorage};

struct BenchEntity {
    bounds: AtomicCell<BoundingBox>,
}

impl BenchEntity {
    fn new(bounds: BoundingBox) -> Self {
        Self {
            bounds: AtomicCell::new(bounds),
        }
    }
}

impl NBTStorage for BenchEntity {}

impl EntityBase for BenchEntity {
    fn get_entity(&self) -> &Entity {
        unreachable!("query_candidates uses AtomicSpatialPose bounds, get_entity is never called")
    }

    fn get_living_entity(&self) -> Option<&LivingEntity> {
        None
    }

    fn cast_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_nbt_storage(&self) -> &dyn NBTStorage {
        self
    }
}

fn bench_spatial_queries_adaptive(c: &mut Criterion) {
    let mut group = c.benchmark_group("spatial_index_adaptive");

    // Fine-grained density spectrum to measure exact threshold crossover
    let sizes = [
        1, 4, 8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 192, 256, 512, 1_000, 10_000,
    ];

    for size in sizes {
        let metrics = Arc::new(SpatialMetrics::new());
        let index = Arc::new(WorldSpatialIndex::new(metrics));

        let mut entities: Vec<Arc<BenchEntity>> = Vec::with_capacity(size);

        for i in 0..size {
            // Entities placed in localized chunk clusters
            let x = (i % 50) as f64 * 4.0;
            let y = 64.0;
            let z = (i / 50) as f64 * 4.0;

            let bounds = BoundingBox::new(
                Vector3::new(x, y, z),
                Vector3::new(x + 1.0, y + 1.8, z + 1.0),
            );
            let bench_ent = Arc::new(BenchEntity::new(bounds));
            index.register_entity(1, bench_ent.clone(), bounds, SpatialCategory::LIVING);
            entities.push(bench_ent);
        }

        let query_box = BoundingBox::new(
            Vector3::new(0.0, 60.0, 0.0),
            Vector3::new(32.0, 70.0, 32.0),
        );

        // 1. Global linear scan baseline
        group.bench_with_input(
            BenchmarkId::new("linear_scan_baseline", size),
            &size,
            |b, _| {
                b.iter(|| {
                    let results: Vec<Arc<BenchEntity>> = entities
                        .iter()
                        .filter(|e| e.bounds.load().intersects(&query_box))
                        .cloned()
                        .collect();
                    results.len()
                });
            },
        );

        // 2. ACTUAL Public query_candidates with Local Scope Adaptive Dispatcher enabled!
        group.bench_with_input(
            BenchmarkId::new("world_spatial_index_adaptive", size),
            &size,
            |b, _| {
                b.iter(|| {
                    let candidates = index.query_candidates(1, &query_box, SpatialCategory::LIVING);
                    candidates.len()
                });
            },
        );
    }

    group.finish();
}

fn bench_spawn_despawn_churn(c: &mut Criterion) {
    let mut group = c.benchmark_group("spatial_index_churn");

    for size in [16, 100, 1_000, 10_000] {
        let metrics = Arc::new(SpatialMetrics::new());
        let index = WorldSpatialIndex::new(metrics);

        let mut keys = Vec::with_capacity(size);
        for i in 0..size {
            let x = (i % 50) as f64 * 4.0;
            let y = 64.0;
            let z = (i / 50) as f64 * 4.0;
            let bounds = BoundingBox::new(Vector3::new(x, y, z), Vector3::new(x + 1.0, y + 1.8, z + 1.0));
            let ent = Arc::new(BenchEntity::new(bounds));
            let k = index.register_entity(1, ent, bounds, SpatialCategory::LIVING);
            keys.push(k);
        }

        group.bench_with_input(
            BenchmarkId::new("spawn_despawn_cycle", size),
            &size,
            |b, _| {
                let mut idx = 0;
                b.iter(|| {
                    let bounds = BoundingBox::new(Vector3::new(10.0, 64.0, 10.0), Vector3::new(11.0, 65.8, 11.0));
                    let ent = Arc::new(BenchEntity::new(bounds));
                    let k = index.register_entity(1, ent, bounds, SpatialCategory::LIVING);
                    index.unregister_entity(k);
                    idx += 1;
                    idx
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_spatial_queries_adaptive,
    bench_spawn_despawn_churn
);
criterion_main!(benches);
