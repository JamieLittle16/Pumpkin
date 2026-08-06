use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackgroundCpuCategory {
    Generation,
    PacketPreparation,
}

#[derive(Default)]
struct Usage {
    total: usize,
    generation: usize,
    packet_preparation: usize,
}

/// Coordinates sustained CPU work performed by otherwise independent executors.
pub struct BackgroundCpuBudget {
    capacity: usize,
    generation_limit: usize,
    packet_preparation_limit: usize,
    usage: Mutex<Usage>,
    available: Condvar,
    generation_wait_nanos: AtomicU64,
    packet_preparation_wait_nanos: AtomicU64,
    generation_acquisitions: AtomicU64,
    packet_preparation_acquisitions: AtomicU64,
}

impl BackgroundCpuBudget {
    #[must_use]
    pub fn automatic(packet_preparation_limit: usize) -> Self {
        let capacity = std::thread::available_parallelism()
            .map_or(1, std::num::NonZero::get)
            .saturating_sub(2)
            .max(1);
        // Symmetric category reservation: each category may use at most
        // `capacity - 1` slots, so neither chunk generation nor packet
        // preparation can starve the other.
        let category_max = capacity.saturating_sub(1).max(1);
        let generation_limit = category_max;
        let packet_preparation_limit = packet_preparation_limit.min(category_max);
        Self::new(capacity, generation_limit, packet_preparation_limit)
    }

    #[must_use]
    pub fn new(capacity: usize, generation_limit: usize, packet_preparation_limit: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            capacity,
            generation_limit: generation_limit.clamp(1, capacity),
            packet_preparation_limit: packet_preparation_limit.clamp(1, capacity),
            usage: Mutex::new(Usage::default()),
            available: Condvar::new(),
            generation_wait_nanos: AtomicU64::new(0),
            packet_preparation_wait_nanos: AtomicU64::new(0),
            generation_acquisitions: AtomicU64::new(0),
            packet_preparation_acquisitions: AtomicU64::new(0),
        }
    }

    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    #[must_use]
    pub const fn generation_limit(&self) -> usize {
        self.generation_limit
    }

    #[must_use]
    pub const fn packet_preparation_limit(&self) -> usize {
        self.packet_preparation_limit
    }

    pub fn acquire(&self, category: BackgroundCpuCategory) -> BackgroundCpuPermit<'_> {
        let started = Instant::now();
        let mut usage = self.usage.lock().unwrap();
        while usage.total >= self.capacity
            || match category {
                BackgroundCpuCategory::Generation => usage.generation >= self.generation_limit,
                BackgroundCpuCategory::PacketPreparation => {
                    usage.packet_preparation >= self.packet_preparation_limit
                }
            }
        {
            usage = self.available.wait(usage).unwrap();
        }
        usage.total += 1;
        match category {
            BackgroundCpuCategory::Generation => usage.generation += 1,
            BackgroundCpuCategory::PacketPreparation => usage.packet_preparation += 1,
        }
        drop(usage);

        let waited = started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        match category {
            BackgroundCpuCategory::Generation => {
                self.generation_acquisitions.fetch_add(1, Ordering::Relaxed);
                self.generation_wait_nanos
                    .fetch_add(waited, Ordering::Relaxed);
            }
            BackgroundCpuCategory::PacketPreparation => {
                self.packet_preparation_acquisitions
                    .fetch_add(1, Ordering::Relaxed);
                self.packet_preparation_wait_nanos
                    .fetch_add(waited, Ordering::Relaxed);
            }
        }
        BackgroundCpuPermit {
            budget: self,
            category,
        }
    }

    #[must_use]
    pub fn wait_nanos(&self, category: BackgroundCpuCategory) -> u64 {
        match category {
            BackgroundCpuCategory::Generation => self.generation_wait_nanos.load(Ordering::Relaxed),
            BackgroundCpuCategory::PacketPreparation => {
                self.packet_preparation_wait_nanos.load(Ordering::Relaxed)
            }
        }
    }

    #[must_use]
    pub fn acquisitions(&self, category: BackgroundCpuCategory) -> u64 {
        match category {
            BackgroundCpuCategory::Generation => {
                self.generation_acquisitions.load(Ordering::Relaxed)
            }
            BackgroundCpuCategory::PacketPreparation => {
                self.packet_preparation_acquisitions.load(Ordering::Relaxed)
            }
        }
    }
}

pub struct BackgroundCpuPermit<'a> {
    budget: &'a BackgroundCpuBudget,
    category: BackgroundCpuCategory,
}

impl Drop for BackgroundCpuPermit<'_> {
    fn drop(&mut self) {
        let mut usage = self.budget.usage.lock().unwrap();
        usage.total -= 1;
        match self.category {
            BackgroundCpuCategory::Generation => usage.generation -= 1,
            BackgroundCpuCategory::PacketPreparation => usage.packet_preparation -= 1,
        }
        drop(usage);
        self.budget.available.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::{BackgroundCpuBudget, BackgroundCpuCategory};
    use std::sync::Arc;

    #[test]
    fn automatic_reserves_capacity_for_both_categories() {
        let budget = BackgroundCpuBudget::automatic(usize::MAX);
        let category_max = budget.capacity().saturating_sub(1).max(1);
        assert_eq!(budget.generation_limit(), category_max);
        assert_eq!(budget.packet_preparation_limit(), category_max);
    }

    #[test]
    fn generation_limit_reserves_packet_capacity() {
        let budget = Arc::new(BackgroundCpuBudget::new(2, 1, 2));
        let generation = budget.acquire(BackgroundCpuCategory::Generation);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let waiting_budget = Arc::clone(&budget);
        let waiter = std::thread::spawn(move || {
            let _second_generation = waiting_budget.acquire(BackgroundCpuCategory::Generation);
            started_tx.send(()).unwrap();
        });

        let packet = budget.acquire(BackgroundCpuCategory::PacketPreparation);
        assert!(started_rx.try_recv().is_err());
        drop(generation);
        started_rx.recv().unwrap();
        drop(packet);
        waiter.join().unwrap();
    }
}
