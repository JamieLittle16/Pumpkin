use crate::entity::spatial_registry::EntityKey;
use bitflags::bitflags;
use pumpkin_util::math::boundingbox::BoundingBox;
use pumpkin_util::math::vector3::Vector3;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Mutex;

bitflags! {
    /// Spatial capability flags for entity filtering without dynamic dispatch.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct SpatialCategory: u32 {
        const PLAYER      = 1 << 0;
        const LIVING      = 1 << 1;
        const ITEM        = 1 << 2;
        const PROJECTILE  = 1 << 3;
        const COLLIDABLE  = 1 << 4;
        const PUSHABLE    = 1 << 5;
        const DAMAGEABLE  = 1 << 6;
        const TRACKABLE   = 1 << 7;
        const VEHICLE     = 1 << 8;
        const PICKABLE    = 1 << 9;
    }
}

/// Coherent spatial state snapshot for an indexed entity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SpatialPose {
    pub world_id: u64,
    pub exact_bounds: BoundingBox,
    pub coverage_bounds: BoundingBox,
    pub membership_revision: u32,
    pub categories: SpatialCategory,
    pub alive: bool,
}

impl SpatialPose {
    /// Compute loose coverage AABB to suppress microcell re-indexing on small movements.
    pub fn compute_coverage_bounds(exact: BoundingBox, horizontal_speed: f64) -> BoundingBox {
        const BASE_PADDING: f64 = 0.25;
        const LOOKAHEAD_TICKS: f64 = 2.0;
        const MIN_PADDING: f64 = 0.25;
        const MAX_PADDING: f64 = 2.0;

        let padding = (BASE_PADDING + horizontal_speed * LOOKAHEAD_TICKS).clamp(MIN_PADDING, MAX_PADDING);
        exact.expand(padding, padding * 0.5, padding)
    }

    /// Check if exact_bounds fits entirely inside a coverage AABB.
    pub fn is_contained_in(&self, coverage: &BoundingBox) -> bool {
        self.exact_bounds.min.x >= coverage.min.x
            && self.exact_bounds.min.y >= coverage.min.y
            && self.exact_bounds.min.z >= coverage.min.z
            && self.exact_bounds.max.x <= coverage.max.x
            && self.exact_bounds.max.y <= coverage.max.y
            && self.exact_bounds.max.z <= coverage.max.z
    }
}

/// Lock-free atomic sequence lock for publishing coherent SpatialPose updates.
pub struct AtomicSpatialPose {
    sequence: AtomicU64,
    min_x: AtomicU64,
    min_y: AtomicU64,
    min_z: AtomicU64,
    max_x: AtomicU64,
    max_y: AtomicU64,
    max_z: AtomicU64,
    cov_min_x: AtomicU64,
    cov_min_y: AtomicU64,
    cov_min_z: AtomicU64,
    cov_max_x: AtomicU64,
    cov_max_y: AtomicU64,
    cov_max_z: AtomicU64,
    world_id: AtomicU64,
    membership_revision: AtomicU32,
    categories: AtomicU32,
    alive: AtomicBool,
}

impl AtomicSpatialPose {
    pub fn new(pose: SpatialPose) -> Self {
        Self {
            sequence: AtomicU64::new(0),
            min_x: AtomicU64::new(pose.exact_bounds.min.x.to_bits()),
            min_y: AtomicU64::new(pose.exact_bounds.min.y.to_bits()),
            min_z: AtomicU64::new(pose.exact_bounds.min.z.to_bits()),
            max_x: AtomicU64::new(pose.exact_bounds.max.x.to_bits()),
            max_y: AtomicU64::new(pose.exact_bounds.max.y.to_bits()),
            max_z: AtomicU64::new(pose.exact_bounds.max.z.to_bits()),
            cov_min_x: AtomicU64::new(pose.coverage_bounds.min.x.to_bits()),
            cov_min_y: AtomicU64::new(pose.coverage_bounds.min.y.to_bits()),
            cov_min_z: AtomicU64::new(pose.coverage_bounds.min.z.to_bits()),
            cov_max_x: AtomicU64::new(pose.coverage_bounds.max.x.to_bits()),
            cov_max_y: AtomicU64::new(pose.coverage_bounds.max.y.to_bits()),
            cov_max_z: AtomicU64::new(pose.coverage_bounds.max.z.to_bits()),
            world_id: AtomicU64::new(pose.world_id),
            membership_revision: AtomicU32::new(pose.membership_revision),
            categories: AtomicU32::new(pose.categories.bits()),
            alive: AtomicBool::new(pose.alive),
        }
    }

    /// Read a consistent pose snapshot using sequence-locking (seqlock).
    pub fn read(&self) -> SpatialPose {
        loop {
            let seq1 = self.sequence.load(Ordering::Acquire);
            if seq1 & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }

            let min_x = f64::from_bits(self.min_x.load(Ordering::Relaxed));
            let min_y = f64::from_bits(self.min_y.load(Ordering::Relaxed));
            let min_z = f64::from_bits(self.min_z.load(Ordering::Relaxed));
            let max_x = f64::from_bits(self.max_x.load(Ordering::Relaxed));
            let max_y = f64::from_bits(self.max_y.load(Ordering::Relaxed));
            let max_z = f64::from_bits(self.max_z.load(Ordering::Relaxed));

            let cov_min_x = f64::from_bits(self.cov_min_x.load(Ordering::Relaxed));
            let cov_min_y = f64::from_bits(self.cov_min_y.load(Ordering::Relaxed));
            let cov_min_z = f64::from_bits(self.cov_min_z.load(Ordering::Relaxed));
            let cov_max_x = f64::from_bits(self.cov_max_x.load(Ordering::Relaxed));
            let cov_max_y = f64::from_bits(self.cov_max_y.load(Ordering::Relaxed));
            let cov_max_z = f64::from_bits(self.cov_max_z.load(Ordering::Relaxed));

            let world_id = self.world_id.load(Ordering::Relaxed);
            let revision = self.membership_revision.load(Ordering::Relaxed);
            let cat_bits = self.categories.load(Ordering::Relaxed);
            let alive = self.alive.load(Ordering::Relaxed);

            let seq2 = self.sequence.load(Ordering::Acquire);
            if seq1 == seq2 {
                return SpatialPose {
                    world_id,
                    exact_bounds: BoundingBox::new(
                        Vector3::new(min_x, min_y, min_z),
                        Vector3::new(max_x, max_y, max_z),
                    ),
                    coverage_bounds: BoundingBox::new(
                        Vector3::new(cov_min_x, cov_min_y, cov_min_z),
                        Vector3::new(cov_max_x, cov_max_y, cov_max_z),
                    ),
                    membership_revision: revision,
                    categories: SpatialCategory::from_bits_truncate(cat_bits),
                    alive,
                };
            }
        }
    }

    /// Write/publish a new pose snapshot atomically.
    pub fn publish(&self, pose: SpatialPose) {
        let seq = self.sequence.load(Ordering::Relaxed);
        self.sequence.store(seq + 1, Ordering::Release);

        self.min_x.store(pose.exact_bounds.min.x.to_bits(), Ordering::Relaxed);
        self.min_y.store(pose.exact_bounds.min.y.to_bits(), Ordering::Relaxed);
        self.min_z.store(pose.exact_bounds.min.z.to_bits(), Ordering::Relaxed);
        self.max_x.store(pose.exact_bounds.max.x.to_bits(), Ordering::Relaxed);
        self.max_y.store(pose.exact_bounds.max.y.to_bits(), Ordering::Relaxed);
        self.max_z.store(pose.exact_bounds.max.z.to_bits(), Ordering::Relaxed);

        self.cov_min_x.store(pose.coverage_bounds.min.x.to_bits(), Ordering::Relaxed);
        self.cov_min_y.store(pose.coverage_bounds.min.y.to_bits(), Ordering::Relaxed);
        self.cov_min_z.store(pose.coverage_bounds.min.z.to_bits(), Ordering::Relaxed);
        self.cov_max_x.store(pose.coverage_bounds.max.x.to_bits(), Ordering::Relaxed);
        self.cov_max_y.store(pose.coverage_bounds.max.y.to_bits(), Ordering::Relaxed);
        self.cov_max_z.store(pose.coverage_bounds.max.z.to_bits(), Ordering::Relaxed);

        self.world_id.store(pose.world_id, Ordering::Relaxed);
        self.membership_revision.store(pose.membership_revision, Ordering::Relaxed);
        self.categories.store(pose.categories.bits(), Ordering::Relaxed);
        self.alive.store(pose.alive, Ordering::Relaxed);

        self.sequence.store(seq + 2, Ordering::Release);
    }
}

/// Light proxy per indexed entity carrying spatial key and atomic pose.
pub struct SpatialProxy {
    pub key: EntityKey,
    pub update_lock: Mutex<()>,
    pub pose: AtomicSpatialPose,
}

impl SpatialProxy {
    pub fn new(key: EntityKey, initial_pose: SpatialPose) -> Self {
        Self {
            key,
            update_lock: Mutex::new(()),
            pose: AtomicSpatialPose::new(initial_pose),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seqlock_atomic_pose_reading_and_publishing() {
        let exact1 = BoundingBox::new(Vector3::new(1.0, 2.0, 3.0), Vector3::new(4.0, 5.0, 6.0));
        let initial = SpatialPose {
            world_id: 42,
            exact_bounds: exact1,
            coverage_bounds: SpatialPose::compute_coverage_bounds(exact1, 0.0),
            membership_revision: 1,
            categories: SpatialCategory::LIVING | SpatialCategory::PUSHABLE,
            alive: true,
        };

        let atomic_pose = AtomicSpatialPose::new(initial);
        let read1 = atomic_pose.read();
        assert_eq!(read1, initial);

        let exact2 = BoundingBox::new(Vector3::new(10.0, 20.0, 30.0), Vector3::new(14.0, 25.0, 36.0));
        let updated = SpatialPose {
            world_id: 42,
            exact_bounds: exact2,
            coverage_bounds: SpatialPose::compute_coverage_bounds(exact2, 0.0),
            membership_revision: 2,
            categories: SpatialCategory::PLAYER | SpatialCategory::DAMAGEABLE,
            alive: true,
        };

        atomic_pose.publish(updated);
        let read2 = atomic_pose.read();
        assert_eq!(read2, updated);
    }
}
