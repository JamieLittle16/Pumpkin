//! Broad-phase search for non-player entities by occupied 16x16 X/Z cells.
//!
//! Keeps insertion order and checks precise bounding boxes after selecting candidates.
//! Oversized hitboxes are kept separately so they can never be missed.

use pumpkin_util::math::boundingbox::BoundingBox;
use rustc_hash::{FxHashMap, FxHashSet};

const MAX_INDEXED_CELLS: i64 = 16;
const MAX_QUERY_CELLS: i64 = 64;
const MIN_INDEX_ENTITIES: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CellSpan {
    min_x: i32,
    max_x: i32,
    min_z: i32,
    max_z: i32,
}

impl CellSpan {
    fn from_box(bounds: &BoundingBox) -> Option<Self> {
        if !bounds.min.x.is_finite()
            || !bounds.max.x.is_finite()
            || !bounds.min.z.is_finite()
            || !bounds.max.z.is_finite()
            || bounds.min.x > bounds.max.x
            || bounds.min.z > bounds.max.z
        {
            return None;
        }
        Some(Self {
            min_x: (bounds.min.x.floor() as i32) >> 4,
            max_x: (bounds.max.x.floor() as i32) >> 4,
            min_z: (bounds.min.z.floor() as i32) >> 4,
            max_z: (bounds.max.z.floor() as i32) >> 4,
        })
    }

    fn count(self) -> i64 {
        (i64::from(self.max_x) - i64::from(self.min_x) + 1)
            * (i64::from(self.max_z) - i64::from(self.min_z) + 1)
    }

    fn cells(self) -> impl Iterator<Item = (i32, i32)> {
        (self.min_x..=self.max_x)
            .flat_map(move |x| (self.min_z..=self.max_z).map(move |z| (x, z)))
    }
}

/// Same indexed cell footprint, even if an entity moves inside a cell.
#[must_use]
pub fn same_cells(before: &BoundingBox, after: &BoundingBox) -> bool {
    indexed_span(before) == indexed_span(after)
}

fn indexed_span(bounds: &BoundingBox) -> Option<CellSpan> {
    CellSpan::from_box(bounds).filter(|span| span.count() <= MAX_INDEXED_CELLS)
}

struct Entry<T> {
    item: T,
    span: Option<CellSpan>,
    sequence: u64,
}

/// The index is not synchronized internally: World guards it with an RwLock.
/// Queries return None when scanning the original entity vector is likely cheaper.
pub struct EntityGrid<T> {
    entries: FxHashMap<i32, Entry<T>>,
    cells: FxHashMap<(i32, i32), Vec<i32>>,
    oversized: FxHashSet<i32>,
    next_sequence: u64,
}

impl<T: Clone> Default for EntityGrid<T> {
    fn default() -> Self {
        Self {
            entries: FxHashMap::default(),
            cells: FxHashMap::default(),
            oversized: FxHashSet::default(),
            next_sequence: 0,
        }
    }
}

impl<T: Clone> EntityGrid<T> {
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn insert(&mut self, id: i32, bounds: &BoundingBox, item: T) {
        if self.entries.contains_key(&id) {
            self.remove(id);
        }
        let span = indexed_span(bounds);
        self.add_cells(id, span);
        self.entries.insert(id, Entry {
            item,
            span,
            sequence: self.next_sequence,
        });
        self.next_sequence = self.next_sequence.wrapping_add(1);
    }

    pub fn remove(&mut self, id: i32) {
        if let Some(entry) = self.entries.remove(&id) {
            self.remove_cells(id, entry.span);
        }
    }

    /// Called under the World write lock *before* publishing changed hitbox coordinates.
    pub fn update(&mut self, id: i32, bounds: &BoundingBox) {
        let Some(old_span) = self.entries.get(&id).map(|e| e.span) else {
            return;
        };
        let new_span = indexed_span(bounds);
        if old_span == new_span {
            return;
        }
        self.remove_cells(id, old_span);
        self.add_cells(id, new_span);
        if let Some(entry) = self.entries.get_mut(&id) {
            entry.span = new_span;
        }
    }

    fn add_cells(&mut self, id: i32, span: Option<CellSpan>) {
        if let Some(span) = span {
            for cell in span.cells() {
                self.cells.entry(cell).or_default().push(id);
            }
        } else {
            self.oversized.insert(id);
        }
    }

    fn remove_cells(&mut self, id: i32, span: Option<CellSpan>) {
        if let Some(span) = span {
            for cell in span.cells() {
                if let std::collections::hash_map::Entry::Occupied(mut entry) =
                    self.cells.entry(cell)
                {
                    entry.get_mut().retain(|&candidate| candidate != id);
                    if entry.get().is_empty() {
                        entry.remove();
                    }
                }
            }
        } else {
            self.oversized.remove(&id);
        }
    }

    /// If adaptive is true, bypass the index when the input is tiny or
    /// the query covers too many of the indexed entities.
    /// Exact-hit testing is done *while World holds the index read lock*.
    pub fn query(
        &self,
        bounds: &BoundingBox,
        adaptive: bool,
        mut intersects: impl FnMut(&T) -> bool,
    ) -> Option<Vec<T>> {
        if adaptive && self.len() < MIN_INDEX_ENTITIES {
            return None;
        }
        let span = CellSpan::from_box(bounds)?;
        if span.count() > MAX_QUERY_CELLS {
            return None;
        }

        let candidate_count = self.oversized.len()
            + span.cells()
                .map(|cell| self.cells.get(&cell).map_or(0, Vec::len))
                .sum::<usize>();
        if adaptive && candidate_count > self.len() / 2 {
            return None;
        }

        let mut seen = FxHashSet::default();
        let mut found = Vec::new();
        for id in self.oversized.iter().copied().chain(
            span.cells()
                .filter_map(|cell| self.cells.get(&cell))
                .flat_map(|ids| ids.iter().copied()),
        ) {
            if !seen.insert(id) {
                continue;
            }
            if let Some(entry) = self.entries.get(&id)
                && intersects(&entry.item)
            {
                found.push((entry.sequence, entry.item.clone()));
            }
        }
        // Preserve the original Vec's insertion order for callers where
        // collision tie-breaking or merging depends on iteration order.
        found.sort_unstable_by_key(|(sequence, _)| *sequence);
        Some(found.into_iter().map(|(_, item)| item).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_util::math::vector3::Vector3;

    fn area(x: f64, z: f64, width: f64) -> BoundingBox {
        BoundingBox::new(
            Vector3::new(x, 0.0, z),
            Vector3::new(x + width, 1.0, z + width),
        )
    }

    fn original(
        entries: &[(i32, BoundingBox)],
        query: &BoundingBox,
    ) -> Vec<i32> {
        entries.iter()
            .filter(|(_, bounds)| bounds.intersects(query))
            .map(|(id, _)| *id)
            .collect()
    }

    fn check(grid: &EntityGrid<i32>, entries: &[(i32, BoundingBox)], box_: &BoundingBox) {
        let expected = original(entries, box_);
        let actual = grid.query(box_, false, |id| {
            entries.iter().find(|(candidate, _)| candidate == id)
                .is_some_and(|(_, bounds)| bounds.intersects(box_))
        }).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn oracle_matches_full_scan_for_boundaries_moves_removals_and_large_hitboxes() {
        let mut grid = EntityGrid::default();
        let mut entries = Vec::new();
        for i in 0..240 {
            let x = f64::from(((i * 79) % 819) - 409);
            let z = f64::from(((i * 131) % 911) - 455);
            let bounds = area(x, z, if i == 17 { 140.0 } else { 0.75 });
            grid.insert(i, &bounds, i);
            entries.push((i, bounds));
        }
        for x in [-416.0, -32.0, -16.0, -0.1, 0.0, 15.9, 16.0, 192.0] {
            for z in [-416.0, -16.0, 0.0, 16.0, 192.0] {
                check(&grid, &entries, &area(x, z, 3.0));
            }
        }
        for i in (0..240).step_by(3) {
            let new_bounds = area(f64::from(i * 3 - 240), f64::from(i - 120), 0.75);
            grid.update(i, &new_bounds);
            if let Some(entry) = entries.iter_mut().find(|(id, _)| *id == i) {
                entry.1 = new_bounds;
            }
        }
        for i in (0..240).step_by(5) {
            grid.remove(i);
            entries.retain(|(id, _)| *id != i);
        }
        for x in [-300.0, -16.0, 0.0, 16.0, 128.0, 500.0] {
            for z in [-256.0, 0.0, 16.0, 144.0, 512.0] {
                check(&grid, &entries, &area(x, z, 12.0));
            }
        }
    }

    #[test]
    fn dense_queries_use_linear_fallback_but_sparse_queries_do_not() {
        let mut grid = EntityGrid::default();
        for i in 0..100 {
            let bounds = area(0.0, 0.0, 0.6);
            grid.insert(i, &bounds, i);
        }
        assert!(grid.query(&area(0.0, 0.0, 1.0), true, |_| true).is_none());
        assert_eq!(
            grid.query(&area(100.0, 100.0, 1.0), true, |_| true).unwrap().len(),
            0
        );
    }

    #[test]
    fn cell_footprint_changes_on_boundary_and_size_growth() {
        assert!(same_cells(&area(1.0, 1.0, 0.5), &area(2.0, 2.0, 0.5)));
        assert!(!same_cells(&area(15.0, 2.0, 0.5), &area(16.0, 2.0, 0.5)));
        assert!(!same_cells(&area(1.0, 1.0, 0.5), &area(1.0, 1.0, 32.0)));
    }
}
