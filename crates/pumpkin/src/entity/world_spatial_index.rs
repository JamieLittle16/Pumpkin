use crate::entity::spatial_grid::{CellAddress, ChunkSpatialIndex, SpatialEntry};
use crate::entity::spatial_metrics::SpatialMetrics;
use crate::entity::spatial_pose::{SpatialCategory, SpatialPose, SpatialProxy};
use crate::entity::spatial_registry::{EntityKey, EntitySpatialRegistry};
use crate::entity::EntityBase;
use dashmap::DashMap;
use pumpkin_util::math::boundingbox::BoundingBox;
use pumpkin_util::math::vector2::Vector2;
use rustc_hash::FxHashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// World Spatial Index Façade managing hierarchical chunk microcells and entity lookup.
pub struct WorldSpatialIndex {
    pub registry: EntitySpatialRegistry,
    pub chunks: Arc<DashMap<Vector2<i32>, Arc<ChunkSpatialIndex>>>,
    pub proxies: DashMap<EntityKey, Arc<SpatialProxy>>,
    pub total_entities: AtomicUsize,
    pub metrics: Arc<SpatialMetrics>,
}

impl WorldSpatialIndex {
    pub fn new(metrics: Arc<SpatialMetrics>) -> Self {
        Self {
            registry: EntitySpatialRegistry::new(),
            chunks: Arc::new(DashMap::new()),
            proxies: DashMap::new(),
            total_entities: AtomicUsize::new(0),
            metrics,
        }
    }

    /// Register a spawned entity into the spatial index.
    pub fn register_entity(
        &self,
        world_id: u64,
        entity: Arc<dyn EntityBase>,
        bounds: BoundingBox,
        categories: SpatialCategory,
    ) -> EntityKey {
        let key = self.registry.allocate(entity);
        let coverage = SpatialPose::compute_coverage_bounds(bounds, 0.0);
        let pose = SpatialPose {
            world_id,
            exact_bounds: bounds,
            coverage_bounds: coverage,
            membership_revision: 1,
            categories,
            alive: true,
        };

        let proxy = Arc::new(SpatialProxy::new(key, pose));
        self.proxies.insert(key, proxy);
        self.total_entities.fetch_add(1, Ordering::Relaxed);

        self.insert_cell_memberships(key, bounds, 1, categories);
        key
    }

    /// Unregister a despawned or dead entity.
    pub fn unregister_entity(&self, key: EntityKey) {
        if let Some((_, proxy)) = self.proxies.remove(&key) {
            let mut pose = proxy.pose.read();
            pose.alive = false;
            proxy.pose.publish(pose);
            self.total_entities.fetch_sub(1, Ordering::Relaxed);

            let touched = proxy.touched_chunks.read().unwrap();
            for &chunk_pos in touched.iter() {
                if let Some(chunk_idx) = self.chunks.get(&chunk_pos) {
                    chunk_idx.remove_key(key);
                }
            }
        }
        self.registry.remove(key);
    }

    /// Insert cell memberships across microcells touched by bounds.
    fn insert_cell_memberships(
        &self,
        key: EntityKey,
        bounds: BoundingBox,
        revision: u32,
        categories: SpatialCategory,
    ) {
        let min_addr = CellAddress::from_block_coords(
            bounds.min.x.floor() as i32,
            bounds.min.y.floor() as i32,
            bounds.min.z.floor() as i32,
        );
        let max_addr = CellAddress::from_block_coords(
            bounds.max.x.ceil() as i32,
            bounds.max.y.ceil() as i32,
            bounds.max.z.ceil() as i32,
        );

        let entry = SpatialEntry {
            key,
            membership_revision: revision,
            categories,
        };

        if let Some(proxy) = self.proxies.get(&key) {
            let mut touched = proxy.touched_chunks.write().unwrap();
            touched.clear();
            for cx in min_addr.chunk_pos.x..=max_addr.chunk_pos.x {
                for cz in min_addr.chunk_pos.y..=max_addr.chunk_pos.y {
                    let chunk_pos = Vector2::new(cx, cz);
                    if !touched.contains(&chunk_pos) {
                        touched.push(chunk_pos);
                    }
                }
            }
        }

        for cx in min_addr.chunk_pos.x..=max_addr.chunk_pos.x {
            for cz in min_addr.chunk_pos.y..=max_addr.chunk_pos.y {
                let chunk_pos = Vector2::new(cx, cz);
                let chunk_idx = self
                    .chunks
                    .entry(chunk_pos)
                    .or_insert_with(|| Arc::new(ChunkSpatialIndex::new()))
                    .value()
                    .clone();

                chunk_idx.add_key(key);

                for sy in min_addr.section_y..=max_addr.section_y {
                    let sec = chunk_idx.get_or_create_section(sy);

                    for cell_idx in 0..8u8 {
                        sec.cells[cell_idx as usize].insert(entry);
                        sec.set_occupied(cell_idx, true);
                    }
                }
            }
        }
    }

    pub const DIRECT_SCAN_THRESHOLD: u32 = 96;

    /// Execute a candidates query with Selectivity-Aware Adaptive Dispatcher.
    pub fn query_candidates(
        &self,
        world_id: u64,
        bounds: &BoundingBox,
        mask: SpatialCategory,
    ) -> Vec<Arc<dyn EntityBase>> {
        self.metrics.total_queries.fetch_add(1, Ordering::Relaxed);

        let min_addr = CellAddress::from_block_coords(
            bounds.min.x.floor() as i32,
            bounds.min.y.floor() as i32,
            bounds.min.z.floor() as i32,
        );
        let max_addr = CellAddress::from_block_coords(
            bounds.max.x.ceil() as i32,
            bounds.max.y.ceil() as i32,
            bounds.max.z.ceil() as i32,
        );

        let mut touched_chunks = Vec::new();
        let mut scope_entity_count = 0u32;
        let mut estimated_candidates = 0u32;

        for cx in min_addr.chunk_pos.x..=max_addr.chunk_pos.x {
            for cz in min_addr.chunk_pos.y..=max_addr.chunk_pos.y {
                let chunk_pos = Vector2::new(cx, cz);
                if let Some(chunk_idx) = self.chunks.get(&chunk_pos) {
                    let cnt = chunk_idx.entity_count.load(Ordering::Relaxed);
                    scope_entity_count += cnt;
                    touched_chunks.push(chunk_idx.clone());

                    for sy in min_addr.section_y..=max_addr.section_y {
                        let sections = chunk_idx.sections.read().unwrap();
                        if let Some(sec) = sections.get(&sy) {
                            if sec.occupancy.load(Ordering::Relaxed) != 0 {
                                estimated_candidates += cnt.min(32);
                            }
                        }
                    }
                }
            }
        }

        // SELECTIVITY-AWARE ADAPTIVE DISPATCH:
        // 1. If scope_entity_count <= DIRECT_SCAN_THRESHOLD (96) -> direct scan
        // 2. If query is unselective (estimated_candidates >= 70% of scope_entity_count) -> direct scan
        if scope_entity_count <= Self::DIRECT_SCAN_THRESHOLD
            || (scope_entity_count > 0 && estimated_candidates * 10 >= scope_entity_count * 7)
        {
            let mut results = Vec::with_capacity(scope_entity_count as usize);
            for chunk_idx in touched_chunks {
                let keys = chunk_idx.active_keys.read().unwrap();
                for &key in keys.iter() {
                    let Some(proxy) = self.proxies.get(&key) else {
                        continue;
                    };
                    let pose = proxy.pose.read();

                    if !pose.alive || pose.world_id != world_id {
                        continue;
                    }
                    if mask.bits() != 0 && (pose.categories.bits() & mask.bits()) == 0 {
                        continue;
                    }
                    if pose.exact_bounds.intersects(bounds) {
                        if let Some(entity) = self.registry.resolve(key) {
                            results.push(entity);
                        }
                    }
                }
            }
            return results;
        }

        let mut candidate_keys = FxHashSet::default();

        for cx in min_addr.chunk_pos.x..=max_addr.chunk_pos.x {
            for cz in min_addr.chunk_pos.y..=max_addr.chunk_pos.y {
                let chunk_pos = Vector2::new(cx, cz);
                let Some(chunk_idx) = self.chunks.get(&chunk_pos) else {
                    continue;
                };

                for sy in min_addr.section_y..=max_addr.section_y {
                    let sections = chunk_idx.sections.read().unwrap();
                    let Some(sec) = sections.get(&sy) else {
                        continue;
                    };

                    if sec.occupancy.load(Ordering::Relaxed) == 0 {
                        continue;
                    }

                    for cell_idx in 0..8u8 {
                        let cell = &sec.cells[cell_idx as usize];
                        let category_union = cell.category_union.load(Ordering::Acquire);

                        if mask.bits() != 0 && (category_union & mask.bits()) == 0 {
                            continue;
                        }

                        let entries = cell.entries.read().unwrap();
                        for entry in entries.iter() {
                            if mask.bits() == 0 || (entry.categories.bits() & mask.bits()) != 0 {
                                candidate_keys.insert(entry.key);
                            }
                        }
                    }
                }
            }
        }

        let mut results = Vec::with_capacity(candidate_keys.len());
        for key in candidate_keys {
            let Some(proxy) = self.proxies.get(&key) else {
                continue;
            };
            let pose = proxy.pose.read();

            if !pose.alive || pose.world_id != world_id {
                continue;
            }

            if mask.bits() != 0 && (pose.categories.bits() & mask.bits()) == 0 {
                continue;
            }

            if pose.exact_bounds.intersects(bounds) {
                if let Some(entity) = self.registry.resolve(key) {
                    results.push(entity);
                }
            }
        }

        results
    }

    /// Update entity spatial position/bounds with loose coverage bounds optimization (Commit 10).
    pub fn update_entity_movement(
        &self,
        key: EntityKey,
        new_bounds: BoundingBox,
    ) -> bool {
        let Some(proxy) = self.proxies.get(&key) else {
            return false;
        };

        let _guard = proxy.update_lock.lock().unwrap();

        let mut current_pose = proxy.pose.read();
        if !current_pose.alive {
            return false;
        }

        current_pose.exact_bounds = new_bounds;

        // Fast-path: If movement remains within existing loose coverage bounds, update pose atomically without cell re-indexing
        if current_pose.is_contained_in(&current_pose.coverage_bounds) {
            proxy.pose.publish(current_pose);
            return true;
        }

        // Slow-path: Movement exceeded coverage -> compute new coverage bounds & re-index
        let new_coverage = SpatialPose::compute_coverage_bounds(new_bounds, 0.5);
        let new_rev = current_pose.membership_revision + 1;

        self.insert_cell_memberships(key, new_coverage, new_rev, current_pose.categories);

        current_pose.coverage_bounds = new_coverage;
        current_pose.membership_revision = new_rev;
        proxy.pose.publish(current_pose);

        true
    }

    /// Same-world or cross-world teleport protocol (Commit 10).
    pub fn teleport_entity(
        &self,
        key: EntityKey,
        target_world_id: u64,
        target_bounds: BoundingBox,
    ) -> bool {
        let Some(proxy) = self.proxies.get(&key) else {
            return false;
        };

        let _guard = proxy.update_lock.lock().unwrap();

        let mut current_pose = proxy.pose.read();
        let target_coverage = SpatialPose::compute_coverage_bounds(target_bounds, 0.0);
        let new_rev = current_pose.membership_revision + 1;

        self.insert_cell_memberships(key, target_coverage, new_rev, current_pose.categories);

        current_pose.world_id = target_world_id;
        current_pose.exact_bounds = target_bounds;
        current_pose.coverage_bounds = target_coverage;
        current_pose.membership_revision = new_rev;
        proxy.pose.publish(current_pose);

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::NBTStorage;
    use pumpkin_util::math::vector3::Vector3;

    struct DummyEntity;

    impl NBTStorage for DummyEntity {}

    impl crate::entity::EntityBase for DummyEntity {
        fn get_entity(&self) -> &crate::entity::Entity {
            panic!("Dummy test entity")
        }

        fn get_living_entity(&self) -> Option<&crate::entity::living::LivingEntity> {
            None
        }

        fn cast_any(&self) -> &dyn std::any::Any {
            self
        }

        fn as_nbt_storage(&self) -> &dyn NBTStorage {
            self
        }
    }

    #[test]
    fn test_world_spatial_index_shadow_registration_and_query() {
        let metrics = Arc::new(SpatialMetrics::new());
        let index = WorldSpatialIndex::new(metrics);

        let entity1: Arc<dyn EntityBase> = Arc::new(DummyEntity);
        let bounds1 = BoundingBox::new(Vector3::new(5.0, 64.0, 5.0), Vector3::new(6.0, 65.0, 6.0));
        let key1 = index.register_entity(1, entity1.clone(), bounds1, SpatialCategory::ITEM);

        let query_box = BoundingBox::new(Vector3::new(4.0, 63.0, 4.0), Vector3::new(7.0, 66.0, 7.0));
        let results = index.query_candidates(1, &query_box, SpatialCategory::ITEM);
        assert_eq!(results.len(), 1);

        index.unregister_entity(key1);
        let results_after = index.query_candidates(1, &query_box, SpatialCategory::ITEM);
        assert_eq!(results_after.len(), 0);
    }

    #[test]
    fn test_revisioned_movement_and_teleport_publication() {
        let metrics = Arc::new(SpatialMetrics::new());
        let index = WorldSpatialIndex::new(metrics);

        let entity: Arc<dyn EntityBase> = Arc::new(DummyEntity);
        let initial_bounds = BoundingBox::new(Vector3::new(0.0, 64.0, 0.0), Vector3::new(1.0, 65.0, 1.0));
        let key = index.register_entity(1, entity.clone(), initial_bounds, SpatialCategory::LIVING);

        // Move entity to new position (100.0, 64.0, 100.0)
        let new_bounds = BoundingBox::new(Vector3::new(100.0, 64.0, 100.0), Vector3::new(101.0, 65.0, 101.0));
        assert!(index.update_entity_movement(key, new_bounds));

        // Query new position -> should find candidate
        let query_new = BoundingBox::new(Vector3::new(99.0, 63.0, 99.0), Vector3::new(102.0, 66.0, 102.0));
        let results_new = index.query_candidates(1, &query_new, SpatialCategory::LIVING);
        assert_eq!(results_new.len(), 1);

        // Query old position -> should return zero (old bounds fail exact intersection check)
        let query_old = BoundingBox::new(Vector3::new(-1.0, 63.0, -1.0), Vector3::new(2.0, 66.0, 2.0));
        let results_old = index.query_candidates(1, &query_old, SpatialCategory::LIVING);
        assert_eq!(results_old.len(), 0);

        // Cross-world teleport to World 2
        let tp_bounds = BoundingBox::new(Vector3::new(50.0, 70.0, 50.0), Vector3::new(51.0, 71.0, 51.0));
        assert!(index.teleport_entity(key, 2, tp_bounds));

        // Query World 1 at tp_bounds -> should return 0 (different world ID)
        let query_tp = BoundingBox::new(Vector3::new(49.0, 69.0, 49.0), Vector3::new(52.0, 72.0, 52.0));
        assert_eq!(index.query_candidates(1, &query_tp, SpatialCategory::LIVING).len(), 0);

        // Query World 2 at tp_bounds -> should return 1
        assert_eq!(index.query_candidates(2, &query_tp, SpatialCategory::LIVING).len(), 1);
    }

    #[test]
    fn test_shadow_index_equivalence_against_oracle_trace() {
        use crate::entity::spatial_oracle::{OracleEntityKey, OracleEntityState, SpatialQueryOracle};

        let metrics = Arc::new(SpatialMetrics::new());
        let index = WorldSpatialIndex::new(metrics);
        let mut oracle = SpatialQueryOracle::new();

        let mut active_entities: Vec<(EntityKey, OracleEntityKey)> = Vec::new();

        // Perform 50 deterministic trace operations
        for step in 0..50 {
            let op = step % 5;
            match op {
                0 | 1 => {
                    // Spawn entity
                    let entity: Arc<dyn EntityBase> = Arc::new(DummyEntity);
                    let x = (step * 7 % 100) as f64;
                    let y = (step * 3 % 200) as f64;
                    let z = (step * 11 % 100) as f64;

                    let bounds = BoundingBox::new(Vector3::new(x, y, z), Vector3::new(x + 1.0, y + 1.8, z + 1.0));
                    let key = index.register_entity(1, entity, bounds, SpatialCategory::LIVING);
                    let oracle_key = OracleEntityKey {
                        slot: key.slot,
                        generation: key.generation,
                    };

                    oracle.upsert(OracleEntityState {
                        key: oracle_key,
                        world_id: 1,
                        bounds,
                        position: Vector3::new(x + 0.5, y, z + 0.5),
                        category_bits: SpatialCategory::LIVING.bits(),
                        alive: true,
                    });

                    active_entities.push((key, oracle_key));
                }
                2 => {
                    // Move existing entity
                    if let Some(&(key, oracle_key)) = active_entities.first() {
                        let new_x = (step * 13 % 100) as f64;
                        let new_y = (step * 5 % 200) as f64;
                        let new_z = (step * 17 % 100) as f64;

                        let new_bounds = BoundingBox::new(
                            Vector3::new(new_x, new_y, new_z),
                            Vector3::new(new_x + 1.0, new_y + 1.8, new_z + 1.0),
                        );

                        index.update_entity_movement(key, new_bounds);

                        oracle.upsert(OracleEntityState {
                            key: oracle_key,
                            world_id: 1,
                            bounds: new_bounds,
                            position: Vector3::new(new_x + 0.5, new_y, new_z + 0.5),
                            category_bits: SpatialCategory::LIVING.bits(),
                            alive: true,
                        });
                    }
                }
                3 => {
                    // Despawn entity
                    if let Some((key, oracle_key)) = active_entities.pop() {
                        index.unregister_entity(key);
                        oracle.remove(oracle_key);
                    }
                }
                _ => {
                    // Query check: verify ZERO false negatives between index and oracle
                    let q_box = BoundingBox::new(Vector3::new(0.0, 0.0, 0.0), Vector3::new(50.0, 100.0, 50.0));
                    let oracle_matches = oracle.query_aabb(1, &q_box, SpatialCategory::LIVING.bits());
                    let index_matches = index.query_candidates(1, &q_box, SpatialCategory::LIVING);

                    assert_eq!(
                        index_matches.len(),
                        oracle_matches.len(),
                        "Shadow index must produce zero false negatives compared to reference oracle"
                    );
                }
            }
        }
    }

    #[test]
    fn test_loose_coverage_fast_path_movement() {
        let metrics = Arc::new(SpatialMetrics::new());
        let index = WorldSpatialIndex::new(metrics);

        let entity: Arc<dyn EntityBase> = Arc::new(DummyEntity);
        let initial_bounds = BoundingBox::new(Vector3::new(0.0, 64.0, 0.0), Vector3::new(1.0, 65.0, 1.0));
        let key = index.register_entity(1, entity, initial_bounds, SpatialCategory::LIVING);

        let initial_rev = index.proxies.get(&key).unwrap().pose.read().membership_revision;
        assert_eq!(initial_rev, 1);

        // Micro-movement within 0.1 blocks (fits inside loose coverage padding 0.25)
        let micro_move = BoundingBox::new(Vector3::new(0.1, 64.0, 0.1), Vector3::new(1.1, 65.0, 1.1));
        assert!(index.update_entity_movement(key, micro_move));

        // Fast-path: Membership revision should remain 1 (no cell re-indexing!)
        let rev_after_micro = index.proxies.get(&key).unwrap().pose.read().membership_revision;
        assert_eq!(rev_after_micro, 1);

        // Large movement to (50.0, 64.0, 50.0) exceeding coverage
        let large_move = BoundingBox::new(Vector3::new(50.0, 64.0, 50.0), Vector3::new(51.0, 65.0, 51.0));
        assert!(index.update_entity_movement(key, large_move));

        // Slow-path: Membership revision should increment to 2
        let rev_after_large = index.proxies.get(&key).unwrap().pose.read().membership_revision;
        assert_eq!(rev_after_large, 2);
    }

    #[test]
    fn test_spatial_index_high_density_stress_and_concurrency() {
        let metrics = Arc::new(SpatialMetrics::new());
        let index = Arc::new(WorldSpatialIndex::new(metrics.clone()));

        // Register 1,000 entities across a 500x256x500 space
        let mut keys = Vec::new();
        for i in 0..1000 {
            let entity: Arc<dyn EntityBase> = Arc::new(DummyEntity);
            let x = (i % 50) as f64 * 10.0;
            let y = 64.0;
            let z = (i / 50) as f64 * 10.0;

            let bounds = BoundingBox::new(Vector3::new(x, y, z), Vector3::new(x + 1.0, y + 1.8, z + 1.0));
            let category = if i % 2 == 0 {
                SpatialCategory::ITEM
            } else {
                SpatialCategory::LIVING
            };
            let key = index.register_entity(1, entity, bounds, category);
            keys.push(key);
        }

        let keys = Arc::new(keys);
        let mut handles = Vec::new();

        // Spawn 8 concurrent worker threads
        for thread_id in 0..8 {
            let index_clone = index.clone();
            let keys_clone = keys.clone();

            handles.push(std::thread::spawn(move || {
                for step in 0..100 {
                    let key_idx = (thread_id * 100 + step) % keys_clone.len();
                    let key = keys_clone[key_idx];

                    if step % 2 == 0 {
                        // Query candidates
                        let q_box = BoundingBox::new(
                            Vector3::new(0.0, 60.0, 0.0),
                            Vector3::new(200.0, 70.0, 200.0),
                        );
                        let results = index_clone.query_candidates(1, &q_box, SpatialCategory::ITEM);
                        assert!(results.len() > 0);
                    } else {
                        // Micro-movement
                        let dx = (step % 5) as f64 * 0.05;
                        let new_bounds = BoundingBox::new(
                            Vector3::new(dx, 64.0, dx),
                            Vector3::new(dx + 1.0, 65.8, dx + 1.0),
                        );
                        index_clone.update_entity_movement(key, new_bounds);
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert!(metrics.total_queries.load(Ordering::Relaxed) > 0);
    }
}
