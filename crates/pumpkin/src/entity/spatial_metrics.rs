use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryCaller {
    ItemMerge,
    PushCollision,
    Projectile,
    Explosion,
    BlockInteraction,
    EntitySelector,
    VillagerAi,
    Other,
}

impl QueryCaller {
    pub const fn index(self) -> usize {
        match self {
            Self::ItemMerge => 0,
            Self::PushCollision => 1,
            Self::Projectile => 2,
            Self::Explosion => 3,
            Self::BlockInteraction => 4,
            Self::EntitySelector => 5,
            Self::VillagerAi => 6,
            Self::Other => 7,
        }
    }

    pub const COUNT: usize = 8;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryKind {
    Aabb,
    Sphere,
    PredicateScan,
}

#[derive(Default, Debug)]
struct CallerMetrics {
    query_count: AtomicU64,
    entities_examined: AtomicU64,
    candidates_returned: AtomicU64,
    total_duration_ns: AtomicU64,
}

/// Baseline telemetry metrics for spatial entity queries.
///
/// Records query volume, examined entities count vs. returned candidates,
/// total duration spent in broad entity scans, and caller classification.
#[derive(Default, Debug)]
pub struct SpatialMetrics {
    pub total_queries: AtomicU64,
    pub total_entities_examined: AtomicU64,
    pub total_candidates_returned: AtomicU64,
    pub total_duration_ns: AtomicU64,

    pub aabb_queries: AtomicU64,
    pub sphere_queries: AtomicU64,
    pub predicate_queries: AtomicU64,

    caller_metrics: [CallerMetrics; QueryCaller::COUNT],
}

impl SpatialMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the completion of a spatial query.
    pub fn record_query(
        &self,
        caller: QueryCaller,
        kind: QueryKind,
        examined: usize,
        returned: usize,
        duration_ns: u64,
    ) {
        let examined_u64 = examined as u64;
        let returned_u64 = returned as u64;

        self.total_queries.fetch_add(1, Ordering::Relaxed);
        self.total_entities_examined.fetch_add(examined_u64, Ordering::Relaxed);
        self.total_candidates_returned.fetch_add(returned_u64, Ordering::Relaxed);
        self.total_duration_ns.fetch_add(duration_ns, Ordering::Relaxed);

        match kind {
            QueryKind::Aabb => {
                self.aabb_queries.fetch_add(1, Ordering::Relaxed);
            }
            QueryKind::Sphere => {
                self.sphere_queries.fetch_add(1, Ordering::Relaxed);
            }
            QueryKind::PredicateScan => {
                self.predicate_queries.fetch_add(1, Ordering::Relaxed);
            }
        }

        let caller_m = &self.caller_metrics[caller.index()];
        caller_m.query_count.fetch_add(1, Ordering::Relaxed);
        caller_m.entities_examined.fetch_add(examined_u64, Ordering::Relaxed);
        caller_m.candidates_returned.fetch_add(returned_u64, Ordering::Relaxed);
        caller_m.total_duration_ns.fetch_add(duration_ns, Ordering::Relaxed);
    }

    /// Snapshot current metrics into a summary structure.
    pub fn snapshot(&self) -> SpatialMetricsSnapshot {
        let mut callers = [CallerSnapshot::default(); QueryCaller::COUNT];
        for (i, c) in self.caller_metrics.iter().enumerate() {
            callers[i] = CallerSnapshot {
                query_count: c.query_count.load(Ordering::Relaxed),
                entities_examined: c.entities_examined.load(Ordering::Relaxed),
                candidates_returned: c.candidates_returned.load(Ordering::Relaxed),
                total_duration_ns: c.total_duration_ns.load(Ordering::Relaxed),
            };
        }

        SpatialMetricsSnapshot {
            total_queries: self.total_queries.load(Ordering::Relaxed),
            total_entities_examined: self.total_entities_examined.load(Ordering::Relaxed),
            total_candidates_returned: self.total_candidates_returned.load(Ordering::Relaxed),
            total_duration_ns: self.total_duration_ns.load(Ordering::Relaxed),
            aabb_queries: self.aabb_queries.load(Ordering::Relaxed),
            sphere_queries: self.sphere_queries.load(Ordering::Relaxed),
            predicate_queries: self.predicate_queries.load(Ordering::Relaxed),
            callers,
        }
    }

    /// Reset all metric counters to zero.
    pub fn reset(&self) {
        self.total_queries.store(0, Ordering::Relaxed);
        self.total_entities_examined.store(0, Ordering::Relaxed);
        self.total_candidates_returned.store(0, Ordering::Relaxed);
        self.total_duration_ns.store(0, Ordering::Relaxed);
        self.aabb_queries.store(0, Ordering::Relaxed);
        self.sphere_queries.store(0, Ordering::Relaxed);
        self.predicate_queries.store(0, Ordering::Relaxed);

        for c in &self.caller_metrics {
            c.query_count.store(0, Ordering::Relaxed);
            c.entities_examined.store(0, Ordering::Relaxed);
            c.candidates_returned.store(0, Ordering::Relaxed);
            c.total_duration_ns.store(0, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CallerSnapshot {
    pub query_count: u64,
    pub entities_examined: u64,
    pub candidates_returned: u64,
    pub total_duration_ns: u64,
}

#[derive(Debug, Clone)]
pub struct SpatialMetricsSnapshot {
    pub total_queries: u64,
    pub total_entities_examined: u64,
    pub total_candidates_returned: u64,
    pub total_duration_ns: u64,
    pub aabb_queries: u64,
    pub sphere_queries: u64,
    pub predicate_queries: u64,
    pub callers: [CallerSnapshot; QueryCaller::COUNT],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spatial_metrics_accumulation() {
        let metrics = SpatialMetrics::new();
        metrics.record_query(QueryCaller::ItemMerge, QueryKind::Aabb, 100, 5, 500);
        metrics.record_query(QueryCaller::PushCollision, QueryKind::Sphere, 50, 2, 300);

        let snap = metrics.snapshot();
        assert_eq!(snap.total_queries, 2);
        assert_eq!(snap.total_entities_examined, 150);
        assert_eq!(snap.total_candidates_returned, 7);
        assert_eq!(snap.total_duration_ns, 800);
        assert_eq!(snap.aabb_queries, 1);
        assert_eq!(snap.sphere_queries, 1);

        let item_caller = snap.callers[QueryCaller::ItemMerge.index()];
        assert_eq!(item_caller.query_count, 1);
        assert_eq!(item_caller.entities_examined, 100);
        assert_eq!(item_caller.candidates_returned, 5);
        assert_eq!(item_caller.total_duration_ns, 500);

        metrics.reset();
        let reset_snap = metrics.snapshot();
        assert_eq!(reset_snap.total_queries, 0);
        assert_eq!(reset_snap.total_entities_examined, 0);
    }
}
