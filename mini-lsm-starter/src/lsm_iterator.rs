#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;
use anyhow::{bail, Result};
use bytes::Bytes;
use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::table::SsTableIterator;

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner =  TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    // Note that our SST iterator does not support passing an end bound to it.
    // Therefore, you will need to handle the end_bound manually in LsmIterator.
    // You will need to modify your LsmIterator logic to stop when the key from the inner iterator reaches the end boundary.
    // end_bound: Bound<Bytes>: 表示遍历的结束边界。Bound<Bytes> 可以是三种情况：
    // Bound::Unbounded：没有结束边界，迭代器可以遍历所有键。
    // Bound::Included(Bytes)：包含结束键，即迭代器遍历到这个键时停止。
    // Bound::Excluded(Bytes)：不包含结束键，即迭代器在遇到这个键之前停止。
    end_bound: Bound<Bytes>,
    // is_valid: bool: 表示当前迭代器是否有效。无效的迭代器意味着它不能返回任何有效的键值对（可能已到达边界或没有更多元素）。
    is_valid: bool,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        let mut iter = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            end_bound,
        };
        iter.move_to_non_delete()?;
        Ok(iter)
    }
}

impl LsmIterator {
    fn next_inner(&mut self) -> Result<()> {
        self.inner.next()?;
        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }
        // 如果 end_bound 是有限的（Bound::Included 或 Bound::Excluded），则根据 inner 当前键和 end_bound 进行比较，更新 is_valid：
        // Bound::Included(key)：表示包含 key，所以如果当前键小于等于 key，is_valid 保持为 true，否则为 false。
        // Bound::Excluded(key)：表示不包含 key，所以当前键必须小于 key，否则 is_valid 为 false。
        // 边界检查：这个逻辑确保迭代器在遇到结束边界时变得无效，从而停止遍历。
        match self.end_bound.as_ref() {
            Bound::Unbounded => {}
            Bound::Included(key) => self.is_valid = self.inner.key().raw_ref() <= key.as_ref(),
            Bound::Excluded(key) => self.is_valid = self.inner.key().raw_ref() < key.as_ref(),
        }
        Ok(())
    }

    fn move_to_non_delete(&mut self) -> Result<()> {
        while self.is_valid() && self.inner.value().is_empty() {
            self.next_inner()?;
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.next_inner()?;
        self.move_to_non_delete()?;
        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if self.has_errored || !self.iter.is_valid() {
            panic!("invalid access to the underlying iterator");
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if self.has_errored || !self.iter.is_valid() {
            panic!("invalid access to the underlying iterator");
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        // only move when the iterator is valid and not errored
        if self.has_errored {
            bail!("the iterator is tainted");
        }
        if self.iter.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_errored = true;
                return Err(e);
            }
        }
        Ok(())
    }
}
