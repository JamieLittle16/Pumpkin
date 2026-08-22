use crate::entity::EntityBase;
use crate::entity::spatial_metrics::QueryCaller;
use pumpkin_util::math::boundingbox::BoundingBox;
use pumpkin_util::math::vector3::Vector3;
use std::sync::Arc;

/// Centralized trait for spatial entity queries across the world.
///
/// In this stage (Commit 2 abstraction), all query methods delegate to the
/// current world entity storage, establishing a unified interface for downstream systems.
pub trait SpatialQueries {
    /// Query all entities intersecting an Axis-Aligned Bounding Box (AABB).
    fn query_aabb(&self, bounds: &BoundingBox) -> Vec<Arc<dyn EntityBase>> {
        self.query_aabb_for(bounds, QueryCaller::Other)
    }

    /// Query all entities intersecting an AABB with caller telemetry classification.
    fn query_aabb_for(&self, bounds: &BoundingBox, caller: QueryCaller) -> Vec<Arc<dyn EntityBase>>;

    /// Query all entities within a spherical radius of a point.
    fn query_sphere(&self, center: Vector3<f64>, radius: f64) -> Vec<Arc<dyn EntityBase>> {
        self.query_sphere_for(center, radius, QueryCaller::Other)
    }

    /// Query all entities within a spherical radius with caller telemetry classification.
    fn query_sphere_for(&self, center: Vector3<f64>, radius: f64, caller: QueryCaller) -> Vec<Arc<dyn EntityBase>>;

    /// Query all entities intersecting a swept AABB volume over a movement vector.
    fn query_swept_aabb(&self, bounds: &BoundingBox, movement: Vector3<f64>) -> Vec<Arc<dyn EntityBase>> {
        self.query_swept_aabb_for(bounds, movement, QueryCaller::Other)
    }

    /// Query all entities intersecting a swept AABB volume with caller classification.
    fn query_swept_aabb_for(&self, bounds: &BoundingBox, movement: Vector3<f64>, caller: QueryCaller) -> Vec<Arc<dyn EntityBase>>;
}
