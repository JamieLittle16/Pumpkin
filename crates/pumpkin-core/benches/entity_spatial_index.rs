//! Compare real broad-phase candidate selection against the previous linear scan.
//!
//! Run: cargo bench -p pumpkin-core --bench entity_spatial_index -- --noplot
//! Use identical hardware and a release profile for every comparison.
//! Query fixtures and setup are outside the timed closures.

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use crossbeam::atomic::AtomicCell;
use pumpkin_core::world::entity_spatial_index::EntityGrid;
use pumpkin_util::math::{boundingbox::BoundingBox, vector3::Vector3};
use std::{sync::Arc, time::Duration};

struct Mock {
    bounds: AtomicCell<BoundingBox>,
}

fn area(x: f64, z: f64) -> BoundingBox {
    BoundingBox::new(
        Vector3::new(x, 64.0, z),
        Vector3::new(x + 0.6, 65.0, z + 0.6),
    )
}

fn setup(count: usize, concentrated: bool) -> (Vec<Arc<Mock>>, EntityGrid<Arc<Mock>>, BoundingBox) {
    let mut entities = Vec::with_capacity(count);
    let mut index = EntityGrid::default();
    for i in 0..count {
        let (x, z) = if concentrated {
            ((i % 8) as f64, ((i / 8) % 8) as f64)
        } else {
            // Deterministic scattered map with no benchmark-time RNG.
            (((i * 73) % 65521) as f64 - 32760.0,
             ((i * 173) % 65519) as f64 - 32759.0)
        };
        let bounds = area(x, z);
        let entity = Arc::new(Mock { bounds: AtomicCell::new(bounds) });
        index.insert(i as i32, &bounds, entity.clone());
        entities.push(entity);
    }
    // Include one known hit to avoid a benchmark of only empty results.
    if !entities.is_empty() {
        let bounds = area(2.0, 2.0);
        index.update(0, &bounds);
        entities[0].bounds.store(bounds);
    }
    let query = BoundingBox::new(
        Vector3::new(1.0, 63.0, 1.0),
        Vector3::new(4.0, 67.0, 4.0),
    );
    (entities, index, query)
}

fn compare_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("entity_spatial_query");
    group.sample_size(50);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    for &concentrated in &[false, true] {
        let layout = if concentrated { "cluster" } else { "spread" };
        for count in [16, 32, 64, 128, 256, 512, 1024, 4096] {
            let (entities, index, bounds) = setup(count, concentrated);
            let baseline: Vec<usize> = entities.iter()
                .enumerate()
                .filter(|(_, entity)| entity.bounds.load().intersects(&bounds))
                .map(|(i, _)| i)
                .collect();
            let indexed = index.query(&bounds, false, |entity| {
                entity.bounds.load().intersects(&bounds)
            }).expect("small query must be indexable");
            assert_eq!(indexed.len(), baseline.len(), "fixture oracle mismatch");

            group.bench_with_input(
                BenchmarkId::new(format!("{layout}/linear"), count),
                &count,
                |b, _| b.iter(|| {
                    let hits: Vec<_> = entities.iter()
                        .filter(|entity| entity.bounds.load().intersects(black_box(&bounds)))
                        .cloned()
                        .collect();
                    black_box(hits)
                }),
            );
            group.bench_with_input(
                BenchmarkId::new(format!("{layout}/indexed"), count),
                &count,
                |b, _| b.iter(|| {
                    let hits = index.query(black_box(&bounds), false, |entity| {
                        entity.bounds.load().intersects(&bounds)
                    }).expect("small query must be indexable");
                    black_box(hits)
                }),
            );
            group.bench_with_input(
                BenchmarkId::new(format!("{layout}/adaptive"), count),
                &count,
                |b, _| b.iter(|| {
                    let hits = index.query(black_box(&bounds), true, |entity| {
                        entity.bounds.load().intersects(&bounds)
                    }).unwrap_or_else(|| {
                        entities.iter()
                            .filter(|entity| entity.bounds.load().intersects(&bounds))
                            .cloned()
                            .collect()
                    });
                    black_box(hits)
                }),
            );
        }
    }
    group.finish();
}

/// Separately measure the price of membership changes. This is crucial:
/// faster queries must repay their cost on a server with moving mobs.
fn compare_update_cost(c: &mut Criterion) {
    let mut group = c.benchmark_group("entity_spatial_maintenance");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(3));
    for count in [32, 128, 512, 2048] {
        let (entities, mut index, _) = setup(count, false);
        let mut toggle = false;
        let first = area(-0.4, 7.0);
        let second = area(16.4, 7.0);
        group.bench_with_input(BenchmarkId::new("boundary_crossing", count), &count, |b, _| {
            b.iter(|| {
                let new_bounds = if toggle { first } else { second };
                toggle = !toggle;
                index.update(0, black_box(&new_bounds));
                entities[0].bounds.store(new_bounds);
                black_box(&index);
            })
        });
        let first_inside = area(1.0, 7.0);
        let second_inside = area(1.4, 7.0);
        group.bench_with_input(BenchmarkId::new("same_cell_bbox_store", count), &count, |b, _| {
            b.iter(|| {
                let new_bounds = if toggle { first_inside } else { second_inside };
                toggle = !toggle;
                // Production fast path: same_cells() then publish the bbox, no grid mutation.
                if pumpkin_core::world::entity_spatial_index::same_cells(
                    &entities[0].bounds.load(),
                    &new_bounds,
                ) {
                    entities[0].bounds.store(new_bounds);
                } else {
                    index.update(0, &new_bounds);
                    entities[0].bounds.store(new_bounds);
                }
                black_box(&index);
            })
        });
    }
    group.finish();
}

criterion_group!(benches, compare_queries, compare_update_cost);
criterion_main!(benches);
