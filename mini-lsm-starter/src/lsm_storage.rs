#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::{map_bound, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        unimplemented!()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        // 释放读锁：当进入 let snapshot = ... 之后，guard 就会被丢弃（释放），因为 guard 只在这个内部作用域中有效。
        // 一旦这个作用域结束，Rust 的所有权系统会自动调用 Drop 来释放锁，允许其他线程获取这个锁。
        // 因此，虽然我们克隆了 Arc，但我们已经释放了对 state.read() 的锁。
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        // 在当前的 memtable 中查找
        if let Some(value) = snapshot.memtable.get(_key) {
            if value.is_empty() {
                // 说明找到的是这个 key 的 tome stone
                return Ok(None);
            }
            return Ok(Some(value));
        }

        // 在 immutable memtables 中查找
        for memtable in snapshot.imm_memtables.iter() {
            if let Some(value) = memtable.get(_key) {
                if value.is_empty() {
                    // 说明找到的是这个 key 的 tome stone
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        let mut iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for table in snapshot.l0_sstables.iter() {
            iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                snapshot.sstables[table].clone(),
                KeySlice::from_slice(_key),
            )?));
        }
        let iter = MergeIterator::create(iters);
        if iter.is_valid() && iter.key() == KeySlice::from_slice(_key) && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        assert!(!_value.is_empty(), "value cannot be empty");
        assert!(!_key.is_empty(), "key cannot be empty");

        let size;
        {
            let guard = self.state.read();
            guard.memtable.put(_key, _value)?;
            size = guard.memtable.approximate_size();
        }

        self.try_freeze(size)?;

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        assert!(!_key.is_empty(), "key cannot be empty");

        let size;
        {
            let guard = self.state.read();
            guard.memtable.put(_key, b"")?;
            size = guard.memtable.approximate_size();
        }

        self.try_freeze(size)?;

        Ok(())
    }

    // try_freeze 方法的作用是根据内存表的大小判断是否需要冻结当前的内存表。
    /*
    具体操作：
    检查大小是否超过阈值：

    首先判断 estimated_size 是否超过了系统配置中的 target_sst_size。如果当前内存表的估计大小大于或等于这个目标值，就可能需要冻结。
    加锁和再次确认：

    获取全局锁 self.state_lock.lock()，确保冻结操作的独占性。
    再次获取读锁 self.state.read()，并检查当前内存表的实际大小是否仍然大于阈值（防止其他线程已经处理过冻结操作）。
    执行冻结：

    如果内存表的大小确实超过了阈值，释放读锁（drop(guard)），然后调用 self.force_freeze_memtable(&state_lock) 执行内存表的冻结操作。
    总结：
    该方法是一个保护机制，用于确保内存表不会超过目标大小。如果内存表过大，会触发冻结操作，保证性能和内存占用在合理范围内。
     */
    fn try_freeze(&self, estimated_size: usize) -> Result<()> {
        if estimated_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let guard = self.state.read();
            // the memtable could have already been frozen, check again to ensure we really need to freeze
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                // drop(guard) 是用于显式释放读锁，确保接下来调用 force_freeze_memtable 时可以顺利获取到写锁，避免死锁或锁竞争。
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    /// 该方法的核心功能是将当前的可变内存表（memtable）冻结，并且用一个新的内存表替换它。
    /// 旧的内存表被插入到不可变内存表（imm_memtables）中，表明它已经准备好进行下一步处理（例如持久化或合并）。
    /// 这个操作通常在内存表达到某个大小阈值或其他条件时触发。
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        // 创建一个新的 memtable
        let memtable_id = self.next_sst_id();
        let memtable = Arc::new(MemTable::create(memtable_id));

        // 这里使用了一个代码块 {}，这个块主要是为了控制作用域，确保 state.write() 的锁只在这个块内生效。
        let old_memtable;
        {
            let mut guard = self.state.write();
            // Swap the current memtable with a new one. 获取当前状态的快照并克隆一份，目的是为了在更改之前先复制当前状态。
            let mut snapshot = guard.as_ref().clone();
            // 使用 std::mem::replace 将当前的可变内存表替换为新的内存表，并将旧的内存表保存到 old_memtable 中
            old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
            // Add the memtable to the immutable memtables.
            snapshot.imm_memtables.insert(0, old_memtable.clone());
            // Update the snapshot.最后，将更新后的快照重新存回 state 中，确保状态变更正确。
            *guard = Arc::new(snapshot);
        }

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        // 这里是为了获取一个快照，确保在迭代器的生命周期内状态不会发生变化。
        // 放在代码块里，是为了保证锁的释放，避免锁的持有时间过长。
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        // 创建一个迭代器，用于合并多个 memtable 的迭代器。
        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        // 将当前的 memtable 和所有的不可变 memtable 都加入到 memtable_iters 中。
        memtable_iters.push(Box::new(snapshot.memtable.scan(_lower, _upper)));
        // 遍历所有的不可变 memtable，将它们的迭代器也加入到 memtable_iters 中。
        for memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(memtable.scan(_lower, _upper)));
        }

        let memtable_iter = MergeIterator::create(memtable_iters);

        let mut table_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for table_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table_id].clone();
            let iter = match _lower {
                Bound::Included(key) => SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key))?,
                Bound::Excluded(key) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key))?;
                    if iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
            };

            table_iters.push(Box::new(iter));
        }

        let table_iter = MergeIterator::create(table_iters);

        let iter = TwoMergeIterator::create(memtable_iter, table_iter)?;

        Ok(FusedIterator::new(LsmIterator::new(
            iter,
            map_bound(_upper),
        )?))
    }
}
