use crate::entity::spatial_pose::SpatialCategory;
use crate::entity::spatial_registry::EntityKey;
use pumpkin_util::math::vector2::Vector2;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};

/// 8x8x8 microcell address inside a chunk column and 16-block vertical section.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CellAddress {
    pub chunk_pos: Vector2<i32>,
    pub section_y: i8,
    pub cell_index: u8,
}

impl PartialOrd for CellAddress {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CellAddress {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.chunk_pos
            .x
            .cmp(&other.chunk_pos.x)
            .then_with(|| self.chunk_pos.y.cmp(&other.chunk_pos.y))
            .then_with(|| self.section_y.cmp(&other.section_y))
            .then_with(|| self.cell_index.cmp(&other.cell_index))
    }
}

impl CellAddress {
    /// Convert world block coordinates to a CellAddress.
    pub fn from_block_coords(block_x: i32, block_y: i32, block_z: i32) -> Self {
        let chunk_x = block_x >> 4;
        let chunk_z = block_z >> 4;
        let section_y = (block_y >> 4) as i8;

        let micro_x = ((block_x >> 3) & 1) as u8;
        let micro_y = ((block_y >> 3) & 1) as u8;
        let micro_z = ((block_z >> 3) & 1) as u8;

        let cell_index = (micro_y << 2) | (micro_z << 1) | micro_x;

        Self {
            chunk_pos: Vector2::new(chunk_x, chunk_z),
            section_y,
            cell_index,
        }
    }
}

/// Compact spatial entry stored inside microcells (~16 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpatialEntry {
    pub key: EntityKey,
    pub membership_revision: u32,
    pub categories: SpatialCategory,
}

/// Compact microcell storing entries and conservative category OR union.
#[derive(Default)]
pub struct SpatialCell {
    pub entries: RwLock<Vec<SpatialEntry>>,
    pub category_union: AtomicU32,
    pub revision: AtomicU64,
    pub stale_hint: AtomicU32,
}

impl SpatialCell {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert an entry into the cell, updating category_union and revision.
    pub fn insert(&self, entry: SpatialEntry) {
        let mut entries = self.entries.write().unwrap();
        entries.push(entry);
        self.category_union.fetch_or(entry.categories.bits(), Ordering::Release);
        self.revision.fetch_add(1, Ordering::Release);
    }

    /// Clear cell entries and reset category union.
    pub fn clear(&self) {
        let mut entries = self.entries.write().unwrap();
        entries.clear();
        self.category_union.store(0, Ordering::Release);
        self.stale_hint.store(0, Ordering::Relaxed);
        self.revision.fetch_add(1, Ordering::Release);
    }

    /// Budgeted compaction removing stale entries and recomputing category union.
    pub fn compact<F>(&self, is_valid: F)
    where
        F: Fn(EntityKey, u32) -> bool,
    {
        let mut entries = self.entries.write().unwrap();
        let mut new_union = 0u32;
        entries.retain(|entry| {
            if is_valid(entry.key, entry.membership_revision) {
                new_union |= entry.categories.bits();
                true
            } else {
                false
            }
        });
        self.category_union.store(new_union, Ordering::Release);
        self.stale_hint.store(0, Ordering::Relaxed);
        self.revision.fetch_add(1, Ordering::Release);
    }
}

/// 16-block section spatial index containing eight 8x8x8 microcells + overflow cell.
pub struct SectionSpatialIndex {
    pub occupancy: AtomicU8,
    pub cells: [SpatialCell; 8],
    pub large_entities: SpatialCell,
}

impl Default for SectionSpatialIndex {
    fn default() -> Self {
        Self {
            occupancy: AtomicU8::new(0),
            cells: [
                SpatialCell::new(),
                SpatialCell::new(),
                SpatialCell::new(),
                SpatialCell::new(),
                SpatialCell::new(),
                SpatialCell::new(),
                SpatialCell::new(),
                SpatialCell::new(),
            ],
            large_entities: SpatialCell::new(),
        }
    }
}

impl SectionSpatialIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Update occupancy bit for a microcell.
    pub fn set_occupied(&self, cell_index: u8, occupied: bool) {
        let bit = 1u8 << (cell_index & 7);
        if occupied {
            self.occupancy.fetch_or(bit, Ordering::Release);
        } else {
            self.occupancy.fetch_and(!bit, Ordering::Release);
        }
    }
}

/// Sparse chunk column spatial index.
#[derive(Default)]
pub struct ChunkSpatialIndex {
    pub sections: RwLock<HashMap<i8, Arc<SectionSpatialIndex>>>,
    pub revision: AtomicU64,
}

impl ChunkSpatialIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Retrieve or create a section spatial index.
    pub fn get_or_create_section(&self, section_y: i8) -> Arc<SectionSpatialIndex> {
        {
            let sections = self.sections.read().unwrap();
            if let Some(sec) = sections.get(&section_y) {
                return sec.clone();
            }
        }
        let mut sections = self.sections.write().unwrap();
        sections
            .entry(section_y)
            .or_insert_with(|| Arc::new(SectionSpatialIndex::default()))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cell_address_coordinate_conversion() {
        // Positive origin block (0, 0, 0)
        let addr1 = CellAddress::from_block_coords(0, 0, 0);
        assert_eq!(addr1.chunk_pos, Vector2::new(0, 0));
        assert_eq!(addr1.section_y, 0);
        assert_eq!(addr1.cell_index, 0);

        // Positive boundary block (15, 63, 15)
        let addr2 = CellAddress::from_block_coords(15, 63, 15);
        assert_eq!(addr2.chunk_pos, Vector2::new(0, 0));
        assert_eq!(addr2.section_y, 3); // 63 >> 4 = 3
        assert_eq!(addr2.cell_index, 7); // micro_y=1, micro_z=1, micro_x=1 -> 4+2+1=7

        // Negative coordinates (-1, -64, -1)
        let addr3 = CellAddress::from_block_coords(-1, -64, -1);
        assert_eq!(addr3.chunk_pos, Vector2::new(-1, -1));
        assert_eq!(addr3.section_y, -4); // -64 >> 4 = -4
        assert_eq!(addr3.cell_index, 3); // micro_y=0 (-64>>3=-8), micro_z=1, micro_x=1 -> 0+2+1=3
    }

    #[test]
    fn test_spatial_cell_category_union_and_compaction() {
        let cell = SpatialCell::new();
        assert_eq!(cell.category_union.load(Ordering::Relaxed), 0);

        let entry = SpatialEntry {
            key: EntityKey { slot: 5, generation: 1 },
            membership_revision: 1,
            categories: SpatialCategory::ITEM | SpatialCategory::PICKABLE,
        };

        cell.insert(entry);
        let bits = cell.category_union.load(Ordering::Relaxed);
        assert_ne!(bits & SpatialCategory::ITEM.bits(), 0);
        assert_ne!(bits & SpatialCategory::PICKABLE.bits(), 0);
        assert_eq!(bits & SpatialCategory::PLAYER.bits(), 0);

        cell.clear();
        assert_eq!(cell.category_union.load(Ordering::Relaxed), 0);
        assert!(cell.entries.read().unwrap().is_empty());
    }
}
