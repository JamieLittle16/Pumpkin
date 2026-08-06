use bytes::Bytes;
use dashmap::DashMap;
use pumpkin_protocol::codec::var_int::VarInt;
use pumpkin_protocol::java::client::play::CChunkData;
use pumpkin_protocol::java::packet_encoder::{
    CompressionProfile, PreparedPacket, SerializedPacket, prepare_packet,
};
use pumpkin_protocol::{
    ClientPacket, MultiVersionJavaPacket, PacketEncodeError, ser::NetworkWriteExt,
};
use pumpkin_util::background_cpu::{BackgroundCpuBudget, BackgroundCpuCategory};
use pumpkin_util::version::JavaMinecraftVersion;
use pumpkin_world::chunk::{ChunkNetworkSnapshot, ChunkSnapshotIdentity};
use pumpkin_world::level::SyncChunk;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Instant;
use tokio::sync::{Notify, oneshot};

const SERIALIZED_ENTRY_OVERHEAD: usize = 256;
const PREPARED_ENTRY_OVERHEAD: usize = 192;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SerializationKey {
    pub instance_id: u64,
    pub revision: u64,
    pub protocol_version: i32,
}

#[derive(Clone)]
pub struct CachedSerializedPacket {
    pub key: SerializationKey,
    pub packet: Arc<SerializedPacket>,
}

#[derive(Clone)]
pub struct PreparedChunkPacket {
    pub serialized: CachedSerializedPacket,
    pub prepared: Arc<PreparedPacket>,
}

#[derive(Clone)]
enum PreparationFailure {
    Encode(Arc<PacketEncodeError>),
    Obsolete,
}

struct PreparationCell<T> {
    result: OnceLock<Result<Arc<T>, PreparationFailure>>,
    started: AtomicBool,
    notify: Notify,
}

impl<T> Default for PreparationCell<T> {
    fn default() -> Self {
        Self {
            result: OnceLock::new(),
            started: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }
}

impl<T> PreparationCell<T> {
    async fn wait(&self) -> Result<Arc<T>, PreparationFailure> {
        loop {
            let notified = self.notify.notified();
            if let Some(result) = self.result.get() {
                return result.clone();
            }
            notified.await;
        }
    }

    fn finish(&self, result: Result<Arc<T>, PreparationFailure>) {
        let _ = self.result.set(result);
        self.notify.notify_waiters();
    }
}

struct ChunkCacheEntry {
    revision: u64,
    serialized: DashMap<i32, Arc<PreparationCell<SerializedPacket>>>,
    prepared: DashMap<(i32, Option<CompressionProfile>), Arc<PreparationCell<PreparedPacket>>>,
}

struct ChunkEntryLease {
    cache: Weak<ChunkPacketCache>,
    instance_id: u64,
    entry: Arc<ChunkCacheEntry>,
}

impl std::ops::Deref for ChunkEntryLease {
    type Target = ChunkCacheEntry;

    fn deref(&self) -> &Self::Target {
        &self.entry
    }
}

impl Drop for ChunkEntryLease {
    fn drop(&mut self) {
        let Some(cache) = self.cache.upgrade() else {
            return;
        };
        cache.remove_empty_entry(self.instance_id, &self.entry, 2);
    }
}

impl ChunkCacheEntry {
    fn new(revision: u64) -> Self {
        Self {
            revision,
            serialized: DashMap::new(),
            prepared: DashMap::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum RetainedKey {
    Serialized(SerializationKey),
    Prepared(SerializationKey, Option<CompressionProfile>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Segment {
    Probationary,
    Protected,
}

#[derive(Clone, Copy)]
struct RetainedValue {
    bytes: usize,
    segment: Segment,
}

struct Retention {
    capacity: usize,
    target_serialized: usize,
    retained_bytes: usize,
    serialized_bytes: usize,
    values: HashMap<RetainedKey, RetainedValue>,
    probationary: VecDeque<RetainedKey>,
    protected: VecDeque<RetainedKey>,
}

impl Retention {
    fn new(capacity: usize, serialized_percent: usize) -> Self {
        Self {
            capacity,
            target_serialized: capacity.saturating_mul(serialized_percent) / 100,
            retained_bytes: 0,
            serialized_bytes: 0,
            values: HashMap::new(),
            probationary: VecDeque::new(),
            protected: VecDeque::new(),
        }
    }

    fn remove(&mut self, key: RetainedKey) {
        if let Some(value) = self.values.remove(&key) {
            self.retained_bytes = self.retained_bytes.saturating_sub(value.bytes);
            if matches!(key, RetainedKey::Serialized(_)) {
                self.serialized_bytes = self.serialized_bytes.saturating_sub(value.bytes);
            }
            self.probationary.retain(|candidate| *candidate != key);
            self.protected.retain(|candidate| *candidate != key);
        }
    }

    fn remove_revision(&mut self, instance_id: u64, revision: u64) {
        let keys: Vec<_> = self
            .values
            .keys()
            .copied()
            .filter(|key| match key {
                RetainedKey::Serialized(key) | RetainedKey::Prepared(key, _) => {
                    key.instance_id == instance_id && key.revision == revision
                }
            })
            .collect();
        for key in keys {
            self.remove(key);
        }
    }

    fn record_hit(&mut self, key: RetainedKey) {
        let Some(value) = self.values.get_mut(&key) else {
            return;
        };
        if value.segment == Segment::Protected {
            return;
        }
        value.segment = Segment::Protected;
        self.probationary.retain(|candidate| *candidate != key);
        self.protected.push_back(key);
    }

    fn admit(&mut self, key: RetainedKey, bytes: usize) -> Vec<RetainedKey> {
        self.remove(key);
        if bytes > self.capacity / 8 || self.capacity == 0 {
            return vec![key];
        }
        self.values.insert(
            key,
            RetainedValue {
                bytes,
                segment: Segment::Probationary,
            },
        );
        self.probationary.push_back(key);
        self.retained_bytes += bytes;
        if matches!(key, RetainedKey::Serialized(_)) {
            self.serialized_bytes += bytes;
        }

        let mut evicted = Vec::new();
        while self.retained_bytes > self.capacity {
            let prefer_serialized = self.serialized_bytes > self.target_serialized;
            let candidate = self
                .pop_oldest(prefer_serialized)
                .or_else(|| self.pop_oldest(!prefer_serialized));
            let Some(candidate) = candidate else {
                break;
            };
            self.remove(candidate);
            evicted.push(candidate);
        }
        evicted
    }

    fn pop_oldest(&mut self, serialized: bool) -> Option<RetainedKey> {
        for (queue, segment) in [
            (&mut self.probationary, Segment::Probationary),
            (&mut self.protected, Segment::Protected),
        ] {
            let candidates = queue.len();
            for _ in 0..candidates {
                let Some(key) = queue.pop_front() else {
                    break;
                };
                if !self.values.contains_key(&key) {
                    continue;
                }
                let Some(value) = self.values.get(&key) else {
                    continue;
                };
                if value.segment == segment
                    && matches!(key, RetainedKey::Serialized(_)) == serialized
                {
                    return Some(key);
                }
                if value.segment == segment {
                    queue.push_back(key);
                }
            }
        }
        None
    }
}

pub struct ChunkPacketCache {
    chunks: DashMap<u64, Arc<ChunkCacheEntry>>,
    retention: Mutex<Retention>,
    retention_enabled: bool,
    executor: Arc<rayon::ThreadPool>,
    background_cpu_budget: Arc<BackgroundCpuBudget>,
    serialization_hits: AtomicU64,
    serialization_misses: AtomicU64,
    serialization_waiters: AtomicU64,
    preparation_hits: AtomicU64,
    preparation_misses: AtomicU64,
    preparation_waiters: AtomicU64,
    snapshot_captures: AtomicU64,
    obsolete_snapshot_attempts: AtomicU64,
    snapshot_nanos: AtomicU64,
    serialization_nanos: AtomicU64,
    compression_nanos: AtomicU64,
    preparations_in_flight: AtomicU64,
    capacity_bytes: usize,
    #[cfg(test)]
    #[expect(clippy::type_complexity)]
    before_snapshot: Mutex<Option<Arc<dyn Fn(&SyncChunk) + Send + Sync>>>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ChunkPacketCacheMetrics {
    pub serialization_hits: u64,
    pub serialization_misses: u64,
    pub serialization_waiters: u64,
    pub preparation_hits: u64,
    pub preparation_misses: u64,
    pub preparation_waiters: u64,
    pub snapshot_captures: u64,
    pub obsolete_snapshot_attempts: u64,
    pub snapshot_nanos: u64,
    pub serialization_nanos: u64,
    pub compression_nanos: u64,
    pub background_permit_acquisitions: u64,
    pub background_permit_wait_nanos: u64,
    pub retained_bytes: usize,
    pub capacity_bytes: usize,
    pub retention_entries: usize,
    pub cache_cells: usize,
    pub preparations_in_flight: u64,
}

impl ChunkPacketCache {
    #[must_use]
    pub fn new(
        capacity_mib: usize,
        preparation_threads: usize,
        background_cpu_budget: Arc<BackgroundCpuBudget>,
    ) -> Self {
        let preparation_threads = preparation_threads.max(1);
        Self {
            chunks: DashMap::new(),
            retention: Mutex::new(Retention::new(capacity_mib.saturating_mul(1024 * 1024), 40)),
            retention_enabled: capacity_mib > 0,
            capacity_bytes: capacity_mib.saturating_mul(1024 * 1024),
            executor: Arc::new(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(preparation_threads)
                    .thread_name(|index| format!("Chunk-Packet-{index}"))
                    .build()
                    .expect("failed to create chunk packet preparation pool"),
            ),
            background_cpu_budget,
            serialization_hits: AtomicU64::new(0),
            serialization_misses: AtomicU64::new(0),
            serialization_waiters: AtomicU64::new(0),
            preparation_hits: AtomicU64::new(0),
            preparation_misses: AtomicU64::new(0),
            preparation_waiters: AtomicU64::new(0),
            snapshot_captures: AtomicU64::new(0),
            obsolete_snapshot_attempts: AtomicU64::new(0),
            snapshot_nanos: AtomicU64::new(0),
            serialization_nanos: AtomicU64::new(0),
            compression_nanos: AtomicU64::new(0),
            preparations_in_flight: AtomicU64::new(0),
            #[cfg(test)]
            before_snapshot: Mutex::new(None),
        }
    }

    #[must_use]
    pub fn metrics(&self) -> ChunkPacketCacheMetrics {
        let (retained_bytes, retention_entries) = {
            let retention = self.retention.lock().unwrap();
            (retention.retained_bytes, retention.values.len())
        };
        ChunkPacketCacheMetrics {
            serialization_hits: self.serialization_hits.load(Ordering::Relaxed),
            serialization_misses: self.serialization_misses.load(Ordering::Relaxed),
            serialization_waiters: self.serialization_waiters.load(Ordering::Relaxed),
            preparation_hits: self.preparation_hits.load(Ordering::Relaxed),
            preparation_misses: self.preparation_misses.load(Ordering::Relaxed),
            preparation_waiters: self.preparation_waiters.load(Ordering::Relaxed),
            snapshot_captures: self.snapshot_captures.load(Ordering::Relaxed),
            obsolete_snapshot_attempts: self.obsolete_snapshot_attempts.load(Ordering::Relaxed),
            snapshot_nanos: self.snapshot_nanos.load(Ordering::Relaxed),
            serialization_nanos: self.serialization_nanos.load(Ordering::Relaxed),
            compression_nanos: self.compression_nanos.load(Ordering::Relaxed),
            background_permit_acquisitions: self
                .background_cpu_budget
                .acquisitions(BackgroundCpuCategory::PacketPreparation),
            background_permit_wait_nanos: self
                .background_cpu_budget
                .wait_nanos(BackgroundCpuCategory::PacketPreparation),
            retained_bytes,
            capacity_bytes: self.capacity_bytes,
            retention_entries,
            cache_cells: self.chunks.len(),
            preparations_in_flight: self.preparations_in_flight.load(Ordering::Relaxed),
        }
    }

    fn entry(self: &Arc<Self>, identity: ChunkSnapshotIdentity) -> Option<ChunkEntryLease> {
        if let Some(entry) = self.chunks.get(&identity.instance_id)
            && entry.revision == identity.revision
        {
            return Some(ChunkEntryLease {
                cache: Arc::downgrade(self),
                instance_id: identity.instance_id,
                entry: Arc::clone(entry.value()),
            });
        }

        let replacement = Arc::new(ChunkCacheEntry::new(identity.revision));
        match self.chunks.entry(identity.instance_id) {
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                if occupied.get().revision == identity.revision {
                    return Some(ChunkEntryLease {
                        cache: Arc::downgrade(self),
                        instance_id: identity.instance_id,
                        entry: Arc::clone(occupied.get()),
                    });
                }
                if occupied.get().revision > identity.revision {
                    return None;
                }
                let old_revision = occupied.get().revision;
                occupied.insert(Arc::clone(&replacement));
                self.retention
                    .lock()
                    .unwrap()
                    .remove_revision(identity.instance_id, old_revision);
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                vacant.insert(Arc::clone(&replacement));
            }
        }
        Some(ChunkEntryLease {
            cache: Arc::downgrade(self),
            instance_id: identity.instance_id,
            entry: replacement,
        })
    }

    pub async fn prepare_chunk(
        self: &Arc<Self>,
        chunk: SyncChunk,
        version: JavaMinecraftVersion,
        compression: Option<CompressionProfile>,
    ) -> Result<PreparedChunkPacket, Arc<PacketEncodeError>> {
        if !self.retention_enabled {
            return self.prepare_uncached(chunk, version, compression).await;
        }

        loop {
            let identity = chunk.network_identity();
            let key = SerializationKey {
                instance_id: identity.instance_id,
                revision: identity.revision,
                protocol_version: version.protocol_version(),
            };
            let Some(entry) = self.entry(identity) else {
                continue;
            };
            let serialized_cell = Arc::clone(
                entry
                    .serialized
                    .entry(key.protocol_version)
                    .or_default()
                    .value(),
            );
            let prepared_cell = Arc::clone(
                entry
                    .prepared
                    .entry((key.protocol_version, compression))
                    .or_default()
                    .value(),
            );

            if serialized_cell.result.get().is_some() {
                self.serialization_hits.fetch_add(1, Ordering::Relaxed);
                self.retention
                    .lock()
                    .unwrap()
                    .record_hit(RetainedKey::Serialized(key));
            }

            if serialized_cell
                .started
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.serialization_misses.fetch_add(1, Ordering::Relaxed);
                self.preparation_misses.fetch_add(1, Ordering::Relaxed);
                prepared_cell.started.store(true, Ordering::Release);
                self.spawn_serialization_winner(
                    Arc::clone(&chunk),
                    identity,
                    key,
                    version,
                    compression,
                    Arc::clone(&serialized_cell),
                    Arc::clone(&prepared_cell),
                );
            } else if serialized_cell.result.get().is_none() {
                self.serialization_waiters.fetch_add(1, Ordering::Relaxed);
            }

            let serialized = match serialized_cell.wait().await {
                Ok(packet) => CachedSerializedPacket { key, packet },
                Err(PreparationFailure::Obsolete) => continue,
                Err(PreparationFailure::Encode(error)) => {
                    self.evict(RetainedKey::Serialized(key));
                    self.evict(RetainedKey::Prepared(key, compression));
                    return Err(error);
                }
            };

            if prepared_cell.result.get().is_some() {
                self.preparation_hits.fetch_add(1, Ordering::Relaxed);
                self.retention
                    .lock()
                    .unwrap()
                    .record_hit(RetainedKey::Prepared(key, compression));
            }
            if prepared_cell
                .started
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.preparation_misses.fetch_add(1, Ordering::Relaxed);
                self.spawn_compression_winner(
                    serialized.clone(),
                    compression,
                    Arc::clone(&prepared_cell),
                );
            } else if prepared_cell.result.get().is_none() {
                self.preparation_waiters.fetch_add(1, Ordering::Relaxed);
            }

            match prepared_cell.wait().await {
                Ok(prepared) => {
                    return Ok(PreparedChunkPacket {
                        serialized,
                        prepared,
                    });
                }
                Err(PreparationFailure::Obsolete) => {}
                Err(PreparationFailure::Encode(error)) => {
                    self.evict(RetainedKey::Prepared(key, compression));
                    return Err(error);
                }
            }
        }
    }

    async fn prepare_uncached(
        self: &Arc<Self>,
        chunk: SyncChunk,
        version: JavaMinecraftVersion,
        compression: Option<CompressionProfile>,
    ) -> Result<PreparedChunkPacket, Arc<PacketEncodeError>> {
        self.serialization_misses.fetch_add(1, Ordering::Relaxed);
        self.preparation_misses.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        let cache = Arc::clone(self);

        self.preparations_in_flight.fetch_add(1, Ordering::Relaxed);
        self.executor.spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _permit = cache
                    .background_cpu_budget
                    .acquire(BackgroundCpuCategory::PacketPreparation);

                let snapshot_started = Instant::now();
                let snapshot = chunk.network_snapshot();
                cache.snapshot_captures.fetch_add(1, Ordering::Relaxed);
                cache.add_duration(&cache.snapshot_nanos, snapshot_started);

                let key = SerializationKey {
                    instance_id: snapshot.instance_id,
                    revision: snapshot.revision,
                    protocol_version: version.protocol_version(),
                };
                let serialization_started = Instant::now();
                let serialized = serialize_snapshot(&snapshot, version).map(Arc::new);
                cache.add_duration(&cache.serialization_nanos, serialization_started);
                let serialized = serialized.map_err(Arc::new)?;

                let compression_started = Instant::now();
                let prepared = prepare_packet(&serialized, compression).map(Arc::new);
                cache.add_duration(&cache.compression_nanos, compression_started);
                let prepared = prepared.map_err(Arc::new)?;

                Ok(PreparedChunkPacket {
                    serialized: CachedSerializedPacket {
                        key,
                        packet: serialized,
                    },
                    prepared,
                })
            }))
            .unwrap_or_else(|_| {
                Err(Arc::new(PacketEncodeError::Message(
                    "uncached chunk packet preparation panicked".into(),
                )))
            });
            let _ = sender.send(result);
            cache.preparations_in_flight.fetch_sub(1, Ordering::Relaxed);
        });

        receiver.await.map_err(|_| {
            Arc::new(PacketEncodeError::Message(
                "uncached chunk packet preparation task stopped".into(),
            ))
        })?
    }

    #[expect(clippy::too_many_arguments)]
    fn spawn_serialization_winner(
        self: &Arc<Self>,
        chunk: SyncChunk,
        expected_identity: ChunkSnapshotIdentity,
        key: SerializationKey,
        version: JavaMinecraftVersion,
        compression: Option<CompressionProfile>,
        serialized_cell: Arc<PreparationCell<SerializedPacket>>,
        prepared_cell: Arc<PreparationCell<PreparedPacket>>,
    ) {
        let cache = Arc::clone(self);
        self.preparations_in_flight.fetch_add(1, Ordering::Relaxed);
        self.executor.spawn(move || {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _permit = cache
                    .background_cpu_budget
                    .acquire(BackgroundCpuCategory::PacketPreparation);
                #[cfg(test)]
                let before_snapshot = cache
                    .before_snapshot
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                #[cfg(test)]
                if let Some(hook) = before_snapshot {
                    hook(&chunk);
                }
                let snapshot_started = Instant::now();
                let snapshot = chunk.network_snapshot();
                cache.snapshot_captures.fetch_add(1, Ordering::Relaxed);
                cache.add_duration(&cache.snapshot_nanos, snapshot_started);

                if snapshot.instance_id != expected_identity.instance_id
                    || snapshot.revision != expected_identity.revision
                {
                    cache
                        .obsolete_snapshot_attempts
                        .fetch_add(1, Ordering::Relaxed);
                    // Evict the cells before publishing the failure so any waiter that
                    // wakes on `finish` does not immediately loop and re-observe the same
                    // completed Obsolete cell while the next serialization is still queued.
                    cache.evict(RetainedKey::Serialized(key));
                    cache.evict(RetainedKey::Prepared(key, compression));
                    serialized_cell.finish(Err(PreparationFailure::Obsolete));
                    prepared_cell.finish(Err(PreparationFailure::Obsolete));
                    return;
                }

                let serialization_started = Instant::now();
                let serialized = match serialize_snapshot(&snapshot, version) {
                    Ok(packet) => Arc::new(packet),
                    Err(error) => {
                        let error = PreparationFailure::Encode(Arc::new(error));
                        serialized_cell.finish(Err(error.clone()));
                        prepared_cell.finish(Err(error));
                        cache.evict(RetainedKey::Serialized(key));
                        cache.evict(RetainedKey::Prepared(key, compression));
                        return;
                    }
                };
                cache.add_duration(&cache.serialization_nanos, serialization_started);

                let compression_started = Instant::now();
                let prepared = match prepare_packet(&serialized, compression) {
                    Ok(packet) => Arc::new(packet),
                    Err(error) => {
                        cache.retain_if_current(
                            RetainedKey::Serialized(key),
                            serialized.len().saturating_add(SERIALIZED_ENTRY_OVERHEAD),
                        );
                        cache.evict(RetainedKey::Prepared(key, compression));
                        serialized_cell.finish(Ok(Arc::clone(&serialized)));
                        prepared_cell.finish(Err(PreparationFailure::Encode(Arc::new(error))));
                        return;
                    }
                };
                cache.add_duration(&cache.compression_nanos, compression_started);

                cache.retain_if_current(
                    RetainedKey::Serialized(key),
                    serialized.len().saturating_add(SERIALIZED_ENTRY_OVERHEAD),
                );
                cache.retain_if_current(
                    RetainedKey::Prepared(key, compression),
                    prepared.len().saturating_add(PREPARED_ENTRY_OVERHEAD),
                );
                serialized_cell.finish(Ok(Arc::clone(&serialized)));
                prepared_cell.finish(Ok(Arc::clone(&prepared)));
            }));
            if outcome.is_err() {
                let failure = PreparationFailure::Encode(Arc::new(PacketEncodeError::Message(
                    "chunk packet preparation panicked".into(),
                )));
                serialized_cell.finish(Err(failure.clone()));
                prepared_cell.finish(Err(failure));
                cache.evict(RetainedKey::Serialized(key));
                cache.evict(RetainedKey::Prepared(key, compression));
            }
            cache.preparations_in_flight.fetch_sub(1, Ordering::Relaxed);
        });
    }

    fn spawn_compression_winner(
        self: &Arc<Self>,
        serialized: CachedSerializedPacket,
        compression: Option<CompressionProfile>,
        prepared_cell: Arc<PreparationCell<PreparedPacket>>,
    ) {
        let cache = Arc::clone(self);
        self.preparations_in_flight.fetch_add(1, Ordering::Relaxed);
        self.executor.spawn(move || {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _permit = cache
                    .background_cpu_budget
                    .acquire(BackgroundCpuCategory::PacketPreparation);
                let started = Instant::now();
                let result = prepare_packet(&serialized.packet, compression)
                    .map(Arc::new)
                    .map_err(|error| PreparationFailure::Encode(Arc::new(error)));
                cache.add_duration(&cache.compression_nanos, started);
                if let Ok(packet) = &result {
                    cache.retain_if_current(
                        RetainedKey::Prepared(serialized.key, compression),
                        packet.len().saturating_add(PREPARED_ENTRY_OVERHEAD),
                    );
                } else {
                    cache.evict(RetainedKey::Prepared(serialized.key, compression));
                }
                prepared_cell.finish(result);
            }));
            if outcome.is_err() {
                cache.evict(RetainedKey::Prepared(serialized.key, compression));
                prepared_cell.finish(Err(PreparationFailure::Encode(Arc::new(
                    PacketEncodeError::Message("chunk packet compression panicked".into()),
                ))));
            }
            cache.preparations_in_flight.fetch_sub(1, Ordering::Relaxed);
        });
    }

    #[expect(clippy::unused_self)]
    fn add_duration(&self, counter: &AtomicU64, started: Instant) {
        counter.fetch_add(
            started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    fn retain_if_current(&self, key: RetainedKey, bytes: usize) {
        let serialization = match key {
            RetainedKey::Serialized(key) | RetainedKey::Prepared(key, _) => key,
        };
        let Some(entry) = self.chunks.get(&serialization.instance_id) else {
            return;
        };
        if entry.revision != serialization.revision {
            return;
        }
        // Keep the chunk guard while admitting so revision replacement cannot
        // remove the old revision and then race with stale readmission.
        let evicted = self.retention.lock().unwrap().admit(key, bytes);
        drop(entry);
        for key in evicted {
            self.evict(key);
        }
    }

    fn evict(&self, key: RetainedKey) {
        let serialization = match key {
            RetainedKey::Serialized(key) | RetainedKey::Prepared(key, _) => key,
        };
        let Some(entry) = self.chunks.get(&serialization.instance_id) else {
            return;
        };
        if entry.revision != serialization.revision {
            return;
        }
        match key {
            RetainedKey::Serialized(key) => {
                entry.serialized.remove(&key.protocol_version);
            }
            RetainedKey::Prepared(key, compression) => {
                entry.prepared.remove(&(key.protocol_version, compression));
            }
        }
        drop(entry);
        let entry = self
            .chunks
            .get(&serialization.instance_id)
            .map(|entry| Arc::clone(entry.value()));
        if let Some(entry) = entry {
            self.remove_empty_entry(serialization.instance_id, &entry, 2);
        }
    }

    fn remove_empty_entry(
        &self,
        instance_id: u64,
        expected: &Arc<ChunkCacheEntry>,
        max_strong_count: usize,
    ) {
        self.chunks.remove_if(&instance_id, |_, entry| {
            Arc::ptr_eq(entry, expected)
                && Arc::strong_count(entry) <= max_strong_count
                && entry.serialized.is_empty()
                && entry.prepared.is_empty()
        });
    }
}

fn serialize_snapshot(
    snapshot: &ChunkNetworkSnapshot,
    version: JavaMinecraftVersion,
) -> Result<SerializedPacket, PacketEncodeError> {
    let mut bytes = Vec::new();
    bytes
        .write_var_int(&VarInt(CChunkData::to_id(version)))
        .map_err(|error| PacketEncodeError::Message(error.to_string()))?;
    CChunkData(snapshot)
        .write_packet_data(&mut bytes, &version)
        .map_err(|error| PacketEncodeError::Message(error.to_string()))?;
    SerializedPacket::try_from_bytes(Bytes::from(bytes))
}

#[cfg(test)]
mod tests {
    use super::{ChunkPacketCache, RetainedKey};
    use pumpkin_data::Block;
    use pumpkin_protocol::java::packet_encoder::CompressionProfile;
    use pumpkin_util::background_cpu::BackgroundCpuBudget;
    use pumpkin_util::version::JavaMinecraftVersion;
    use pumpkin_world::chunk::ChunkData;
    use pumpkin_world::chunk::ChunkSnapshotIdentity;
    use std::sync::Arc;

    fn cache(capacity_mib: usize, preparation_threads: usize) -> Arc<ChunkPacketCache> {
        Arc::new(ChunkPacketCache::new(
            capacity_mib,
            preparation_threads,
            Arc::new(BackgroundCpuBudget::new(2, 1, preparation_threads)),
        ))
    }

    fn chunk() -> Arc<ChunkData> {
        Arc::new(ChunkData::new_empty(0, 0, 1, 0))
    }

    #[test]
    fn older_revision_cannot_displace_newer_slot() {
        let cache = cache(64, 1);
        let newer = cache
            .entry(ChunkSnapshotIdentity {
                instance_id: 4,
                revision: 9,
            })
            .unwrap();
        assert!(
            cache
                .entry(ChunkSnapshotIdentity {
                    instance_id: 4,
                    revision: 8,
                })
                .is_none()
        );
        assert_eq!(cache.chunks.get(&4).unwrap().revision, 9);
        drop(newer);
    }

    #[tokio::test]
    async fn simultaneous_callers_share_one_snapshot_and_preparation() {
        let cache = cache(64, 2);
        let chunk = chunk();
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..32 {
            let cache = Arc::clone(&cache);
            let chunk = Arc::clone(&chunk);
            tasks.spawn(async move {
                cache
                    .prepare_chunk(chunk, JavaMinecraftVersion::V_26_2, None)
                    .await
                    .unwrap()
            });
        }
        let mut packets = Vec::new();
        while let Some(packet) = tasks.join_next().await {
            packets.push(packet.unwrap());
        }

        assert!(
            packets
                .windows(2)
                .all(|pair| Arc::ptr_eq(&pair[0].prepared, &pair[1].prepared))
        );
        assert_eq!(cache.metrics().snapshot_captures, 1);
        assert_eq!(cache.metrics().serialization_misses, 1);
        assert_eq!(cache.metrics().preparation_misses, 1);
        let acquisitions = cache.metrics().background_permit_acquisitions;

        cache
            .prepare_chunk(chunk, JavaMinecraftVersion::V_26_2, None)
            .await
            .unwrap();
        assert_eq!(cache.metrics().snapshot_captures, 1);
        assert_eq!(cache.metrics().background_permit_acquisitions, acquisitions);
    }

    #[tokio::test]
    async fn cancelling_first_waiter_does_not_cancel_preparation() {
        let cache = cache(64, 1);
        let chunk = chunk();
        let first_cache = Arc::clone(&cache);
        let first_chunk = Arc::clone(&chunk);
        let first = tokio::spawn(async move {
            first_cache
                .prepare_chunk(first_chunk, JavaMinecraftVersion::V_26_2, None)
                .await
        });
        tokio::task::yield_now().await;
        first.abort();

        let packet = cache
            .prepare_chunk(chunk, JavaMinecraftVersion::V_26_2, None)
            .await
            .unwrap();
        assert!(!packet.prepared.is_empty());
        assert_eq!(cache.metrics().snapshot_captures, 1);
        assert_eq!(cache.metrics().preparation_misses, 1);
    }

    #[tokio::test]
    async fn compression_profiles_have_separate_tier_two_entries() {
        let cache = cache(64, 2);
        let chunk = chunk();
        cache
            .prepare_chunk(Arc::clone(&chunk), JavaMinecraftVersion::V_26_2, None)
            .await
            .unwrap();
        cache
            .prepare_chunk(
                chunk,
                JavaMinecraftVersion::V_26_2,
                Some(CompressionProfile {
                    threshold: 0,
                    level: 1,
                }),
            )
            .await
            .unwrap();

        assert_eq!(cache.metrics().snapshot_captures, 1);
        assert_eq!(cache.metrics().serialization_misses, 1);
        assert_eq!(cache.metrics().preparation_misses, 2);
    }

    #[tokio::test]
    async fn changed_identity_retries_winner_snapshot() {
        let cache = cache(64, 1);
        let chunk = chunk();
        *cache.before_snapshot.lock().unwrap() = Some(Arc::new(|chunk| {
            chunk.set_block_absolute_y(0, 0, 0, Block::STONE.default_state.id);
        }));

        let result = cache
            .prepare_chunk(chunk, JavaMinecraftVersion::V_26_2, None)
            .await
            .unwrap();
        assert!(!result.prepared.is_empty());
        assert_eq!(cache.metrics().obsolete_snapshot_attempts, 1);
        assert_eq!(cache.metrics().snapshot_captures, 2);
    }

    #[tokio::test]
    async fn capacity_zero_bypasses_retention_and_single_flight() {
        let cache = cache(0, 1);
        let chunk = chunk();
        cache
            .prepare_chunk(Arc::clone(&chunk), JavaMinecraftVersion::V_26_2, None)
            .await
            .unwrap();
        cache
            .prepare_chunk(chunk, JavaMinecraftVersion::V_26_2, None)
            .await
            .unwrap();
        let metrics = cache.metrics();
        assert_eq!(metrics.retained_bytes, 0);
        assert_eq!(metrics.snapshot_captures, 2);
        assert_eq!(metrics.serialization_misses, 2);
        assert_eq!(metrics.preparation_misses, 2);
        assert_eq!(metrics.serialization_hits, 0);
        assert_eq!(metrics.preparation_hits, 0);
        assert_eq!(metrics.serialization_waiters, 0);
        assert_eq!(metrics.preparation_waiters, 0);
        assert!(cache.chunks.is_empty());
    }

    #[tokio::test]
    async fn panicking_winner_releases_work_and_allows_retry() {
        let cache = cache(64, 1);
        let chunk = chunk();
        *cache.before_snapshot.lock().unwrap() =
            Some(Arc::new(|_| panic!("deterministic preparation panic")));

        assert!(
            cache
                .prepare_chunk(Arc::clone(&chunk), JavaMinecraftVersion::V_26_2, None)
                .await
                .is_err()
        );
        assert!(
            cache
                .prepare_chunk(chunk, JavaMinecraftVersion::V_26_2, None)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn capacity_eviction_bounds_retained_bytes_and_drops_oldest() {
        let cache = cache(1, 2);
        let first = chunk();
        cache
            .prepare_chunk(Arc::clone(&first), JavaMinecraftVersion::V_26_2, None)
            .await
            .unwrap();
        let footprint = cache.metrics().retained_bytes;
        assert!(footprint > 0);

        let target = 1024 * 1024 / footprint.max(1) + 8;
        let mut distinct = Vec::with_capacity(target);
        for _ in 0..target {
            distinct.push(chunk());
        }
        for c in &distinct {
            cache
                .prepare_chunk(Arc::clone(c), JavaMinecraftVersion::V_26_2, None)
                .await
                .unwrap();
        }
        assert!(cache.metrics().retained_bytes <= 1024 * 1024);

        let misses_before = cache.metrics().serialization_misses;
        cache
            .prepare_chunk(first, JavaMinecraftVersion::V_26_2, None)
            .await
            .unwrap();
        assert_eq!(cache.metrics().serialization_misses, misses_before + 1);

        let entry = cache.chunks.get(&distinct[target - 1].instance_id).unwrap();
        assert!(!entry.serialized.is_empty());
        assert!(!entry.prepared.is_empty());
    }

    #[tokio::test]
    async fn revision_bump_drops_retained_entries_for_old_revision() {
        let cache = cache(64, 1);
        let chunk = chunk();
        cache
            .prepare_chunk(Arc::clone(&chunk), JavaMinecraftVersion::V_26_2, None)
            .await
            .unwrap();
        let old_revision = chunk.network_revision();

        chunk.set_block_absolute_y(0, 0, 0, Block::STONE.default_state.id);
        assert!(chunk.network_revision() > old_revision);

        cache
            .prepare_chunk(Arc::clone(&chunk), JavaMinecraftVersion::V_26_2, None)
            .await
            .unwrap();

        let retention = cache.retention.lock().unwrap();
        assert!(retention.values.keys().all(|key| match key {
            RetainedKey::Serialized(identity) | RetainedKey::Prepared(identity, _) => {
                identity.instance_id != chunk.instance_id || identity.revision != old_revision
            }
        }));
    }

    #[tokio::test]
    async fn disabled_bypass_does_no_single_flight() {
        let cache = cache(0, 2);
        let chunk = chunk();
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..16 {
            let cache = Arc::clone(&cache);
            let chunk = Arc::clone(&chunk);
            tasks.spawn(async move {
                cache
                    .prepare_chunk(chunk, JavaMinecraftVersion::V_26_2, None)
                    .await
                    .unwrap()
            });
        }
        while tasks.join_next().await.is_some() {}

        assert_eq!(cache.metrics().snapshot_captures, 16);
        assert_eq!(cache.metrics().serialization_misses, 16);
        assert_eq!(cache.metrics().serialization_hits, 0);
    }

    #[tokio::test]
    async fn concurrent_distinct_chunks_prepare_once_each() {
        let cache = cache(64, 4);
        let mut tasks = tokio::task::JoinSet::new();
        for i in 0..128 {
            let cache = Arc::clone(&cache);
            tasks.spawn(async move {
                let chunk = Arc::new(ChunkData::new_empty(i, 0, 1, 0));
                cache
                    .prepare_chunk(chunk, JavaMinecraftVersion::V_26_2, None)
                    .await
                    .unwrap()
            });
        }
        let mut packets = Vec::new();
        while let Some(packet) = tasks.join_next().await {
            packets.push(packet.unwrap());
        }

        assert_eq!(packets.len(), 128);
        assert_eq!(cache.metrics().snapshot_captures, 128);
        assert_eq!(cache.metrics().serialization_misses, 128);
    }

    #[tokio::test]
    async fn concurrent_same_chunk_mixed_compressions_are_isolated() {
        let cache = cache(64, 4);
        let chunk = chunk();
        let mut tasks = tokio::task::JoinSet::new();
        for i in 0..64 {
            let cache = Arc::clone(&cache);
            let chunk = Arc::clone(&chunk);
            tasks.spawn(async move {
                let compression = if i % 2 == 0 {
                    None
                } else {
                    Some(CompressionProfile {
                        threshold: 0,
                        level: 1,
                    })
                };
                cache
                    .prepare_chunk(chunk, JavaMinecraftVersion::V_26_2, compression)
                    .await
                    .unwrap()
            });
        }
        while tasks.join_next().await.is_some() {}

        assert_eq!(cache.metrics().snapshot_captures, 1);
        assert_eq!(cache.metrics().serialization_misses, 1);
        assert_eq!(cache.metrics().preparation_misses, 2);
    }

    #[tokio::test]
    async fn in_flight_metric_includes_queued_preparations() {
        let cache = cache(64, 1);
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (second_entered_tx, second_entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = std::sync::Mutex::new(release_rx);
        let second_entered_tx = std::sync::Mutex::new(second_entered_tx);
        let count = std::sync::atomic::AtomicUsize::new(0);

        *cache.before_snapshot.lock().unwrap() = Some(Arc::new(move |_| {
            let n = count.fetch_add(1, Ordering::Relaxed);
            if n == 0 {
                entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            } else {
                second_entered_tx.lock().unwrap().send(()).unwrap();
            }
        }));

        let first_cache = Arc::clone(&cache);
        let first = tokio::spawn(async move {
            first_cache
                .prepare_chunk(chunk(), JavaMinecraftVersion::V_26_2, None)
                .await
                .unwrap();
        });
        tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
            .await
            .unwrap();

        let second_cache = Arc::clone(&cache);
        let second = tokio::spawn(async move {
            second_cache
                .prepare_chunk(
                    Arc::new(ChunkData::new_empty(1, 0, 1, 0)),
                    JavaMinecraftVersion::V_26_2,
                    None,
                )
                .await
                .unwrap();
        });

        // The second preparation is queued to in-flight metrics deterministically.
        while cache.metrics().preparations_in_flight < 2 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(cache.metrics().preparations_in_flight, 2);

        release_tx.send(()).unwrap();
        first.await.unwrap();
        second.await.unwrap();
        assert_eq!(cache.metrics().preparations_in_flight, 0);
    }

    #[tokio::test]
    async fn eviction_churn_with_concurrency_bounds_retained_bytes() {
        let cache = cache(1, 4);
        let result = tokio::time::timeout(std::time::Duration::from_secs(20), async {
            let mut tasks = tokio::task::JoinSet::new();
            for i in 0..2048 {
                let cache = Arc::clone(&cache);
                tasks.spawn(async move {
                    let chunk = Arc::new(ChunkData::new_empty(i, 0, 1, 0));
                    cache
                        .prepare_chunk(chunk, JavaMinecraftVersion::V_26_2, None)
                        .await
                        .unwrap()
                });
            }
            while tasks.join_next().await.is_some() {}
        })
        .await;
        assert!(result.is_ok(), "eviction churn deadlocked");

        let metrics = cache.metrics();
        assert!(metrics.retained_bytes <= 1024 * 1024);
        assert!(metrics.cache_cells <= metrics.retention_entries);
        assert_eq!(metrics.snapshot_captures, 2048);
        assert_eq!(metrics.serialization_misses, 2048);
    }
}

#[cfg(test)]
mod retention_tests {
    use super::{RetainedKey, Retention, Segment, SerializationKey};
    use pumpkin_protocol::java::packet_encoder::CompressionProfile;

    fn key(instance: u64, revision: u64, protocol: i32) -> SerializationKey {
        SerializationKey {
            instance_id: instance,
            revision,
            protocol_version: protocol,
        }
    }

    fn sk(instance: u64, revision: u64, protocol: i32) -> RetainedKey {
        RetainedKey::Serialized(key(instance, revision, protocol))
    }

    fn pk(instance: u64, revision: u64, protocol: i32) -> RetainedKey {
        RetainedKey::Prepared(
            key(instance, revision, protocol),
            Some(CompressionProfile {
                threshold: 0,
                level: 1,
            }),
        )
    }

    /// Returns `(retained_bytes, serialized_bytes)` as tracked by the live map.
    fn values_total(retention: &Retention) -> (usize, usize) {
        let mut retained = 0;
        let mut serialized = 0;
        for (key, value) in &retention.values {
            retained += value.bytes;
            if matches!(key, RetainedKey::Serialized(_)) {
                serialized += value.bytes;
            }
        }
        (retained, serialized)
    }

    fn accounting(retention: &Retention) -> (usize, usize) {
        (retention.retained_bytes, retention.serialized_bytes)
    }

    #[test]
    fn admit_accounts_serialized_bytes() {
        let mut retention = Retention::new(4096, 40);
        let evicted = retention.admit(sk(1, 1, 1), 300);
        assert!(evicted.is_empty());
        assert_eq!(retention.retained_bytes, 300);
        assert_eq!(retention.serialized_bytes, 300);
        assert_eq!(accounting(&retention), values_total(&retention));
    }

    #[test]
    fn oversized_entries_are_rejected() {
        let mut retention = Retention::new(800, 40);
        let evicted = retention.admit(sk(1, 1, 1), 200);
        assert_eq!(evicted, vec![sk(1, 1, 1)]);
        assert!(retention.values.is_empty());
        assert_eq!(retention.retained_bytes, 0);
    }

    #[test]
    fn zero_capacity_admit_rejects() {
        let mut retention = Retention::new(0, 40);
        let evicted = retention.admit(sk(1, 1, 1), 10);
        assert_eq!(evicted, vec![sk(1, 1, 1)]);
        assert!(retention.values.is_empty());
    }

    #[test]
    fn eviction_removes_oldest_serialized_first() {
        let mut retention = Retention::new(10_000, 40);
        let mut evicted_all = Vec::new();
        for revision in 1..=18 {
            evicted_all.extend(retention.admit(sk(1, revision, 1), 600));
        }
        assert_eq!(evicted_all, vec![sk(1, 1, 1), sk(1, 2, 1)]);
        assert!(!retention.values.contains_key(&sk(1, 1, 1)));
        assert!(!retention.values.contains_key(&sk(1, 2, 1)));
        assert!(retention.values.contains_key(&sk(1, 18, 1)));
        assert_eq!(retention.retained_bytes, 9600);
    }

    #[test]
    fn hits_promote_probationary_to_protected() {
        let mut retention = Retention::new(10_000, 40);
        retention.admit(sk(1, 1, 1), 600);
        retention.admit(sk(1, 2, 1), 600);
        retention.record_hit(sk(1, 1, 1));
        assert_eq!(
            retention.values.get(&sk(1, 1, 1)).unwrap().segment,
            Segment::Protected
        );
        let mut evicted_all = Vec::new();
        for revision in 3..=18 {
            evicted_all.extend(retention.admit(sk(1, revision, 1), 600));
        }
        assert_eq!(evicted_all, vec![sk(1, 2, 1), sk(1, 3, 1)]);
        assert!(retention.values.contains_key(&sk(1, 1, 1)));
        assert!(!retention.values.contains_key(&sk(1, 2, 1)));
        assert_eq!(retention.retained_bytes, 9600);
        assert_eq!(accounting(&retention), values_total(&retention));
    }

    #[test]
    fn repeated_hits_keep_promotion_queues_bounded() {
        let mut retention = Retention::new(10_000, 40);
        let key = sk(1, 1, 1);
        retention.admit(key, 600);
        for _ in 0..10_000 {
            retention.record_hit(key);
        }
        assert!(retention.probationary.is_empty());
        assert_eq!(retention.protected, [key]);
    }

    #[test]
    fn prepared_over_target_is_evicted_first() {
        let mut retention = Retention::new(10_000, 40);
        retention.admit(sk(1, 1, 1), 600);
        let mut evicted_all = Vec::new();
        for revision in 1..=18 {
            evicted_all.extend(retention.admit(pk(1, revision, 1), 600));
        }
        assert_eq!(evicted_all, vec![pk(1, 1, 1), pk(1, 2, 1), pk(1, 3, 1)]);
        assert!(retention.values.contains_key(&sk(1, 1, 1)));
        assert_eq!(retention.serialized_bytes, 600);
    }

    #[test]
    fn serialized_over_target_is_evicted_first() {
        let mut retention = Retention::new(10_000, 40);
        for revision in 1..=7 {
            retention.admit(sk(1, revision, 1), 600);
        }
        let mut evicted_all = Vec::new();
        for revision in 1..=18 {
            evicted_all.extend(retention.admit(pk(1, revision, 1), 600));
        }
        assert_eq!(evicted_all.first(), Some(&sk(1, 1, 1)));
        assert!(!retention.values.contains_key(&sk(1, 1, 1)));
        assert!(retention.values.contains_key(&sk(1, 7, 1)));
    }

    #[test]
    fn pop_oldest_falls_back_to_protected_segment() {
        let mut retention = Retention::new(4096, 40);
        retention.admit(pk(1, 1, 1), 300);
        retention.record_hit(pk(1, 1, 1));
        assert_eq!(retention.pop_oldest(false), Some(pk(1, 1, 1)));
    }

    #[test]
    fn remove_revision_clears_instance_revision() {
        let mut retention = Retention::new(100_000, 40);
        retention.admit(sk(1, 5, 1), 100);
        retention.admit(pk(1, 5, 1), 200);
        retention.admit(sk(1, 6, 1), 300);
        retention.admit(sk(2, 5, 1), 400);
        retention.remove_revision(1, 5);
        assert!(!retention.values.contains_key(&sk(1, 5, 1)));
        assert!(!retention.values.contains_key(&pk(1, 5, 1)));
        assert!(retention.values.contains_key(&sk(1, 6, 1)));
        assert!(retention.values.contains_key(&sk(2, 5, 1)));
        assert_eq!(accounting(&retention), values_total(&retention));
    }

    #[test]
    fn removal_of_absent_key_is_a_noop() {
        let mut retention = Retention::new(1000, 40);
        retention.admit(sk(1, 1, 1), 100);
        retention.remove(sk(9, 9, 9));
        assert_eq!(retention.retained_bytes, 100);
    }

    #[test]
    fn hit_on_absent_key_is_a_noop() {
        let mut retention = Retention::new(1000, 40);
        retention.admit(sk(1, 1, 1), 100);
        retention.record_hit(sk(9, 9, 9));
        assert_eq!(retention.values.len(), 1);
        assert_eq!(
            retention.values.get(&sk(1, 1, 1)).unwrap().segment,
            Segment::Probationary
        );
    }

    #[test]
    fn readmission_replaces_existing_entry() {
        let mut retention = Retention::new(1000, 40);
        retention.admit(sk(1, 1, 1), 100);
        let evicted = retention.admit(sk(1, 1, 1), 50);
        assert!(evicted.is_empty());
        assert_eq!(retention.values.get(&sk(1, 1, 1)).unwrap().bytes, 50);
        assert_eq!(retention.retained_bytes, 50);
        assert_eq!(retention.serialized_bytes, 50);
    }

    #[test]
    fn oversized_readmission_removes_existing_accounting() {
        let mut retention = Retention::new(1000, 40);
        let key = sk(1, 1, 1);
        retention.admit(key, 100);
        assert_eq!(retention.admit(key, 126), vec![key]);
        assert!(!retention.values.contains_key(&key));
        assert_eq!(retention.retained_bytes, 0);
        assert_eq!(retention.serialized_bytes, 0);
        assert!(retention.probationary.is_empty());
        assert!(retention.protected.is_empty());
    }

    #[test]
    fn pop_oldest_tolerates_stale_entries() {
        let mut retention = Retention::new(4096, 40);
        retention.admit(sk(1, 1, 1), 300);
        retention.admit(sk(1, 2, 1), 300);
        retention.remove(sk(1, 1, 1));
        assert_eq!(retention.pop_oldest(true), Some(sk(1, 2, 1)));
    }

    #[test]
    fn accounting_stays_consistent_after_mixed_operations() {
        let mut retention = Retention::new(10_000, 40);
        for revision in 1..=5 {
            retention.admit(sk(1, revision, 1), 300);
            retention.admit(pk(1, revision, 1), 200);
        }
        retention.record_hit(sk(1, 3, 1));
        retention.record_hit(pk(1, 5, 1));
        retention.remove_revision(1, 2);
        retention.remove(sk(1, 5, 1));
        retention.admit(sk(2, 1, 1), 1200);
        retention.remove(sk(1, 4, 1));
        assert_eq!(accounting(&retention), values_total(&retention));
        assert!(retention.retained_bytes <= retention.capacity);
    }
}
