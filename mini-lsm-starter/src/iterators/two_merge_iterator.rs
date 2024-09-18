#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    // 决定在合并迭代器时，当前应该从 A 还是 B 中获取下一个键值对。
    choose_a: bool,
}

impl<
        // A 是泛型类型，表示 TwoMergeIterator 的第一个迭代器。
        // 'static：这个约束表示 A 必须具有 'static 生命周期。这意味着 A 类型的数据必须在程序的整个生命周期内有效，通常用来确保迭代器不会引用临时数据。'static 生命周期是 Rust 中最长的生命周期，表示数据不会被提前释放。
        // StorageIterator：A 必须实现了 StorageIterator trait。这个 trait 是用户定义的，用来表示一种可以遍历存储数据的迭代器。这个约束确保了 A 可以像标准迭代器那样执行遍历、获取键和值等操作。
        A: 'static + StorageIterator,
        // B 是泛型类型，表示 TwoMergeIterator 的第二个迭代器。
        // 'static：同样要求 B 的生命周期为 'static，这意味着 B 的数据在程序的整个生命周期内有效，不会提前释放。
        // for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>：
        // for<'a> 是 Rust 中的一种高阶生命周期约束，表示 B 迭代器的 StorageIterator trait 实现对任意生命周期 'a 都有效。
        // StorageIterator<KeyType<'a> = A::KeyType<'a>>：这意味着 B 迭代器的键类型 KeyType<'a> 必须与 A 迭代器的键类型 KeyType<'a> 相同。这一约束确保了 A 和 B 迭代器的键类型是一致的，才能在合并时进行比较和操作。
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self {
            choose_a: false,
            a,
            b,
        };
        // 确保在两个迭代器中具有相同键时跳过 B 中的值。
        iter.skip_b()?;
        // 根据 A 和 B 当前的状态和键，决定是否优先选择 A。
        iter.choose_a = Self::choose_a(&iter.a, &iter.b);
        Ok(iter)
    }

    fn choose_a(a: &A, b: &B) -> bool {
        // 如果 A 迭代器无效（没有更多数据），则返回 false，表示应选择 B。
        if !a.is_valid() {
            return false;
        }
        // 如果 B 迭代器无效，则返回 true，表示应选择 A。
        if !b.is_valid() {
            return true;
        }
        // 如果 A 和 B 都有效，则比较它们当前的键，返回 true 表示 A 的键更小，应选择 A。
        a.key() < b.key()
    }

    // skip_b 方法确保如果 A 和 B 的当前键相同，迭代器 B 会跳过该键，不会返回重复的键值对。
    fn skip_b(&mut self) -> Result<()> {
        if self.a.is_valid() {
            if self.b.is_valid() && self.b.key() == self.a.key() {
                self.b.next()?; // 如果 A 和 B 的键相同，跳过 B
            }
        }
        Ok(())
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.choose_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.choose_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.choose_a {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.choose_a {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        // 跳过 B 中的重复键
        self.skip_b()?;
        // 选择下一个迭代器
        self.choose_a = Self::choose_a(&self.a, &self.b);
        Ok(())
    }
}
