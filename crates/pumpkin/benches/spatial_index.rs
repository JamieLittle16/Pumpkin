use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use pumpkin_util::math::boundingbox::BoundingBox;
use pumpkin_util::math::vector3::Vector3;
use std::sync::Arc;

use pumpkin::entity::living::LivingEntity;
use pumpkin::entity::spatial_metrics::SpatialMetrics;
use pumpkin::entity::spatial_pose::SpatialCategory;
use pumpkin::entity::world_spatial_index::WorldSpatialIndex;
use pumpkin::entity::{EntityBase, NBTStorage};

struct DummyEntity;

impl NBTStorage for DummyEntity {}

impl EntityBase for DummyEntity {
    fn get_entity(&self) -> &pumpkin::entity::Entity {
        panic!("Dummy benchmark entity")
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

    for size in [100, 1_000, 10_000] {
        let metrics = Arc::new(SpatialMetrics::new());
        let index = Arc::new(WorldSpatialIndex::new(metrics));

        let mut entities: Vec<Arc<dyn EntityBase>> = Vec::with_capacity(size);

        for i in 0..size {
            let entity: Arc<dyn EntityBase> = Arc::new(DummyEntity);
            let x = (i % 100) as f64 * 10.0;
            let y = 64.0;
            let z = (i / 100) as f64 * 10.0;

            let bounds = BoundingBox::new(
                Vector3::new(x, y, z),
                Vector3::new(x + 1.0, y + 1.8, z + 1.0),
            );
            index.register_entity(1, entity.clone(), bounds, SpatialCategory::LIVING);
            entities.push(entity);
        }

        let query_box = BoundingBox::new(
            Vector3::new(250.0, 60.0, 250.0),
            Vector3::new(280.0, 70.0, 280.0),
        );

        // Linear scan baseline: O(N) iteration over all entities
        group.bench_with_input(
            BenchmarkId::new("linear_scan_baseline", size),
            &size,
            |b, _| {
                b.iter(|| {
                    let candidates: Vec<&Arc<dyn EntityBase>> = entities
                        .iter()
                        .filter(|_| true) // O(N) linear iteration
                        .collect();
                    candidates.len()
                });
            },
        );

        // WorldSpatialIndex query: O(k + log N) spatial grid lookup
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
