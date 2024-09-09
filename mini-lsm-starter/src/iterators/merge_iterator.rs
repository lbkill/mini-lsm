#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

/// HeapWrapper<I> 是一个泛型元组结构体，用于封装实现了 StorageIterator 的迭代器 I，
/// 同时它还有一个 usize 字段，用来存储与迭代器相关的索引、优先级或计数器。
// Box<I> 使得这个结构体可以动态分配迭代器，并封装不同类型的 StorageIterator 实现。
struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

/// 实现 HeapWrapper 的 PartialEq、Eq、PartialOrd 和 Ord trait，用于比较两个 HeapWrapper 实例。
impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// 优先级队列的比较规则是：首先比较 key，然后比较 HeapWrapper 的 usize 字段。
/// 默认是升序，这里使用 reverse() 方法将其改为降序。就是最小堆。就是说
impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
/// 为什么需要 MergeIterator ？
/// 1. 为了合并多个迭代器，这样可以将多个迭代器的结果合并成一个有序的迭代器。
/// 2. 为了避免重复的 key，如果多个迭代器中有相同的 key，那么只保留一个。
/// 3. 这样将局部有序的迭代器合并成一个全局有序的迭代器。
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        // 如果迭代器列表为空，直接返回一个空的 MergeIterator
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        // 创建一个堆
        let mut heap = BinaryHeap::new();

        // 如果所有迭代器当前item都是无效的，那么选择最后一个作为当前迭代器
        if iters.iter().all(|x| !x.is_valid()) {
            // All invalid, select the last one as the current.
            let mut iters = iters;
            return Self {
                iters: heap,
                current: Some(HeapWrapper(0, iters.pop().unwrap())),
            };
        }

        // 选择迭代器元素是有效的迭代器加入到堆中
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }

        // 选择堆顶元素作为当前迭代器
        let current = heap.pop().unwrap();
        Self {
            iters: heap,
            current: Some(current),
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|x| x.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();
        // Pop the item out of the heap if they have the same value.
        // 堆顶元素的 key 与当前迭代器的 key 进行比较
        while let Some(mut inner_iter) = self.iters.peek_mut() {
            debug_assert!(
                inner_iter.1.key() >= current.1.key(),
                "heap invariant violated"
            );
            // 如果当前迭代器的 key 与堆顶迭代器的 key 相等，那么调用 next 方法，跳过这个 key
            // 如果 next 方法返回错误，那么将堆顶迭代器弹出，然后返回错误
            if inner_iter.1.key() == current.1.key() {
                // Case 1: an error occurred when calling `next`.
                if let e @ Err(_) = inner_iter.1.next() {
                    // 报错的话，会将堆顶迭代器弹出，然后返回错误
                    PeekMut::pop(inner_iter);
                    return e;
                }

                // Case 2: iter is no longer valid.
                if !inner_iter.1.is_valid() {
                    PeekMut::pop(inner_iter);
                }
            } else {
                break;
            }
        }

        // 将 current 迭代器前进到下一个元素
        current.1.next()?;

        // If the current iterator is invalid, pop it out of the heap and select the next one.
        // 检查 current 是否仍然有效：如果 current 迭代器已经无效（即没有剩余元素），需要从堆中弹出下一个迭代器，作为新的 current。
        if !current.1.is_valid() {
            // 从堆中弹出堆顶元素，即堆中当前键值最小的迭代器
            if let Some(iter) = self.iters.pop() {
                // 将堆中弹出的迭代器替换当前的 current。
                *current = iter;
            }
            // 如果 current 已经无效且堆为空，说明所有迭代器都已经处理完，函数返回 Ok(())。
            return Ok(());
        }

        // Otherwise, compare with heap top and swap if necessary.
        // 目的：在 current 有效的情况下，比较 current 和堆顶迭代器，确保堆的有序性。如果 current 比堆顶大，交换它们的位置，以保持堆顶是最小元素。
        if let Some(mut inner_iter) = self.iters.peek_mut() {
            // 比较当前的 current 迭代器和堆顶的迭代器。如果 current 比堆顶大，意味着堆的有序性被破坏，因此需要交换它们的位置。
            // 解引用 *current 和 *inner_iter：current 和 inner_iter 是 HeapWrapper<I> 类型的实例。*current 和 *inner_iter 是通过解引用获得实际的 HeapWrapper<I> 值。
            // 比较 current 和 inner_iter：
            // current 和 inner_iter 都是 HeapWrapper<I> 类型，HeapWrapper<I> 实现了 Ord trait，因此可以进行大小比较。
            // 比较的核心逻辑是在之前 Ord 实现中定义的：首先比较它们内部的 StorageIterator 键（即 key() 返回的值），然后根据键的大小决定比较结果。
            // 如果 current 中的键值比 inner_iter 中的键值大，则条件为 true，表示 current 并不比堆顶的元素小，违反了堆的性质，此时需要交换 current 和堆顶的元素。
            if *current < *inner_iter {
                // 交换 current 和堆顶的 inner_iter，以确保 current 始终是最小的迭代器。
                std::mem::swap(&mut *inner_iter, current);
            }
        }

        Ok(())
    }
}
