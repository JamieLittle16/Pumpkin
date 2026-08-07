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

fn bench_spatial_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("spatial_index_vs_linear");

    // Benchmark across Low-N (1, 5, 16) and High-N (100, 1,000, 10,000) entity counts
    for size in [1, 5, 16, 100, 1_000, 10_000] {
        let metrics = Arc::new(SpatialMetrics::new());
        let index = Arc::new(WorldSpatialIndex::new(metrics));

        let mut entities: Vec<Arc<BenchEntity>> = Vec::with_capacity(size);

        for i in 0..size {
            let x = (i % 100) as f64 * 10.0;
            let y = 64.0;
            let z = (i / 100) as f64 * 10.0;

            let bounds = BoundingBox::new(
                Vector3::new(x, y, z),
                Vector3::new(x + 1.0, y + 1.8, z + 1.0),
            );
            let bench_ent = Arc::new(BenchEntity::new(bounds));
            index.register_entity(1, bench_ent.clone(), bounds, SpatialCategory::LIVING);
            entities.push(bench_ent);
        }

        let query_box = BoundingBox::new(
            Vector3::new(250.0, 60.0, 250.0),
            Vector3::new(280.0, 70.0, 280.0),
        );

        // Global linear scan baseline: iterate all N entities in world and evaluate AABB intersection
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

        // WorldSpatialIndex query (Low-N fast path <= 16; microcells > 16)
        group.bench_with_input(
            BenchmarkId::new("world_spatial_index", size),
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

criterion_group!(benches, bench_spatial_queries);
criterion_main!(benches);
