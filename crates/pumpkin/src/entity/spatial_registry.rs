use crate::entity::EntityBase;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// A generational entity handle ensuring slot reuse cannot revive stale references (Invariant 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EntityKey {
    pub slot: u32,
    pub generation: u32,
}

impl EntityKey {
    pub const NULL: Self = Self {
        slot: u32::MAX,
        generation: 0,
    };

    pub const fn is_null(self) -> bool {
        self.slot == u32::MAX
    }
}

pub struct EntitySlot {
    pub generation: AtomicU32,
    pub entity: RwLock<Option<Arc<dyn EntityBase>>>,
}

use arc_swap::ArcSwap;

/// Registry mapping generational `EntityKey` handles to active world entities.
pub struct EntitySpatialRegistry {
    slots: RwLock<Vec<EntitySlot>>,
    free: Mutex<Vec<u32>>,
    pub active_flat: ArcSwap<Vec<Arc<dyn EntityBase>>>,
}

impl Default for EntitySpatialRegistry {
    fn default() -> Self {
        Self {
            slots: RwLock::new(Vec::new()),
            free: Mutex::new(Vec::new()),
            active_flat: ArcSwap::new(Arc::new(Vec::new())),
        }
    }
}

impl EntitySpatialRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate a generational key for a new entity.
    pub fn allocate(&self, entity: Arc<dyn EntityBase>) -> EntityKey {
        let key = {
            let mut free = self.free.lock().unwrap();
            if let Some(slot_idx) = free.pop() {
                let slots = self.slots.read().unwrap();
                let slot = &slots[slot_idx as usize];
                let gen_val = slot.generation.load(Ordering::Relaxed);
                *slot.entity.write().unwrap() = Some(entity.clone());
                EntityKey {
                    slot: slot_idx,
                    generation: gen_val,
                }
            } else {
                let mut slots = self.slots.write().unwrap();
                let slot_idx = slots.len() as u32;
                slots.push(EntitySlot {
                    generation: AtomicU32::new(1),
                    entity: RwLock::new(Some(entity.clone())),
                });
                EntityKey {
                    slot: slot_idx,
                    generation: 1,
                }
            }
        };

        // Lock-free atomic RCU update of active flat array
        let mut active = (**self.active_flat.load()).clone();
        active.push(entity);
        self.active_flat.store(Arc::new(active));

        key
    }

    /// Resolve an EntityKey to its entity reference, returning None if key is stale or removed.
    pub fn resolve(&self, key: EntityKey) -> Option<Arc<dyn EntityBase>> {
        if key.is_null() {
            return None;
        }
        let slots = self.slots.read().unwrap();
        let slot = slots.get(key.slot as usize)?;
        if slot.generation.load(Ordering::Relaxed) != key.generation {
            return None;
        }
        slot.entity.read().unwrap().clone()
    }

    /// Lock-free array scan over active entities (used for low-N fast path).
    pub fn for_each_active<F: FnMut(&Arc<dyn EntityBase>)>(&self, mut f: F) {
        let active = self.active_flat.load();
        for entity in active.iter() {
            f(entity);
        }
    }

    /// Remove an entity by key, advancing generation and marking slot for reuse.
    pub fn remove(&self, key: EntityKey) -> bool {
        if key.is_null() {
            return false;
        }
        let removed_entity = {
            let slots = self.slots.read().unwrap();
            let Some(slot) = slots.get(key.slot as usize) else {
                return false;
            };
            if slot.generation.load(Ordering::Relaxed) != key.generation {
                return false;
            }
            slot.generation.fetch_add(1, Ordering::Relaxed);
            let entity = slot.entity.write().unwrap().take();
            self.free.lock().unwrap().push(key.slot);
            entity
        };

        if let Some(entity) = removed_entity {
            let mut active = (**self.active_flat.load()).clone();
            active.retain(|e| !Arc::ptr_eq(e, &entity));
            self.active_flat.store(Arc::new(active));
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::NBTStorage;

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
    fn test_generational_slot_reuse_invariance() {
        let registry = EntitySpatialRegistry::new();
        let entity1: Arc<dyn EntityBase> = Arc::new(DummyEntity);
        let key1 = registry.allocate(entity1.clone());

        assert_eq!(key1.slot, 0);
        assert_eq!(key1.generation, 1);

        assert!(registry.resolve(key1).is_some());

        assert!(registry.remove(key1));
        assert!(registry.resolve(key1).is_none());

        let entity2: Arc<dyn EntityBase> = Arc::new(DummyEntity);
        let key2 = registry.allocate(entity2.clone());

        assert_eq!(key2.slot, 0);
        assert_eq!(key2.generation, 2);

        // Invariant 4: Key1 (generation 1) cannot resolve key2 (generation 2)
        assert!(registry.resolve(key1).is_none());
        assert!(registry.resolve(key2).is_some());
    }
}
