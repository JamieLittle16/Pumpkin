use pumpkin_util::math::boundingbox::BoundingBox;
use pumpkin_util::math::vector3::Vector3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OracleEntityKey {
    pub slot: u32,
    pub generation: u32,
}

#[derive(Debug, Clone)]
pub struct OracleEntityState {
    pub key: OracleEntityKey,
    pub world_id: u64,
    pub bounds: BoundingBox,
    pub position: Vector3<f64>,
    pub category_bits: u32,
    pub alive: bool,
}

/// A reference brute-force spatial query oracle.
///
/// Serves as the ground-truth specification for spatial queries during tests.
/// Evaluates queries by scanning all active entities without acceleration.
#[derive(Default, Debug, Clone)]
pub struct SpatialQueryOracle {
    entities: Vec<OracleEntityState>,
}

impl SpatialQueryOracle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or update an entity state in the oracle.
    pub fn upsert(&mut self, state: OracleEntityState) {
        if let Some(existing) = self.entities.iter_mut().find(|e| e.key == state.key) {
            *existing = state;
        } else {
            self.entities.push(state);
        }
    }

    /// Mark an entity as removed in the oracle.
    pub fn remove(&mut self, key: OracleEntityKey) {
        if let Some(existing) = self.entities.iter_mut().find(|e| e.key == key) {
            existing.alive = false;
        }
    }

    /// Brute-force AABB query returning matching entity keys.
    pub fn query_aabb(&self, world_id: u64, bounds: &BoundingBox, mask_bits: u32) -> Vec<OracleEntityKey> {
        self.entities
            .iter()
            .filter(|e| {
                e.alive
                    && e.world_id == world_id
                    && (mask_bits == 0 || (e.category_bits & mask_bits) != 0)
                    && e.bounds.intersects(bounds)
            })
            .map(|e| e.key)
            .collect()
    }

    /// Brute-force sphere query returning matching entity keys.
    pub fn query_sphere(&self, world_id: u64, center: Vector3<f64>, radius: f64, mask_bits: u32) -> Vec<OracleEntityKey> {
        let r_sq = radius * radius;
        self.entities
            .iter()
            .filter(|e| {
                if !e.alive || e.world_id != world_id {
                    return false;
                }
                if mask_bits != 0 && (e.category_bits & mask_bits) == 0 {
                    return false;
                }
                let dx = e.position.x - center.x;
                let dy = e.position.y - center.y;
                let dz = e.position.z - center.z;
                (dx * dx + dy * dy + dz * dz) <= r_sq
            })
            .map(|e| e.key)
            .collect()
    }

    /// Brute-force swept AABB query returning matching entity keys.
    pub fn query_swept_aabb(
        &self,
        world_id: u64,
        bounds: &BoundingBox,
        movement: Vector3<f64>,
        mask_bits: u32,
    ) -> Vec<OracleEntityKey> {
        let swept_box = bounds.expand(movement.x.abs(), movement.y.abs(), movement.z.abs());
        self.query_aabb(world_id, &swept_box, mask_bits)
    }

    /// Active entity count in the oracle.
    pub fn active_count(&self) -> usize {
        self.entities.iter().filter(|e| e.alive).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oracle_spatial_equivalence() {
        let mut oracle = SpatialQueryOracle::new();

        let key1 = OracleEntityKey { slot: 0, generation: 1 };
        let key2 = OracleEntityKey { slot: 1, generation: 1 };

        oracle.upsert(OracleEntityState {
            key: key1,
            world_id: 1,
            bounds: BoundingBox::new(Vector3::new(0.0, 0.0, 0.0), Vector3::new(1.0, 1.0, 1.0)),
            position: Vector3::new(0.5, 0.5, 0.5),
            category_bits: 0b0001,
            alive: true,
        });

        oracle.upsert(OracleEntityState {
            key: key2,
            world_id: 1,
            bounds: BoundingBox::new(Vector3::new(10.0, 10.0, 10.0), Vector3::new(11.0, 11.0, 11.0)),
            position: Vector3::new(10.5, 10.5, 10.5),
            category_bits: 0b0010,
            alive: true,
        });

        let query_box = BoundingBox::new(Vector3::new(-1.0, -1.0, -1.0), Vector3::new(2.0, 2.0, 2.0));
        let results = oracle.query_aabb(1, &query_box, 0);
        assert_eq!(results, vec![key1]);

        let sphere_results = oracle.query_sphere(1, Vector3::new(0.5, 0.5, 0.5), 1.0, 0);
        assert_eq!(sphere_results, vec![key1]);

        oracle.remove(key1);
        assert_eq!(oracle.query_aabb(1, &query_box, 0), vec![]);
    }
}
