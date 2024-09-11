use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::{Block, SIZEOF_U16};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

// 这个方法 compute_overlap 的目的是计算两个键 first_key 和 key 之间的前缀重叠长度。
// 换句话说，它会返回两个键的公共前缀的字节长度，直到它们的字符不再相同或其中一个键到达末尾为止。
// 这种计算前缀重叠长度的方法在数据库或存储系统中很常见，尤其是在 LSM 树、B 树或索引结构中。例如：
//
// 压缩存储：当多个键有相同的前缀时，存储系统可以利用前缀压缩来减少存储空间。
// 加速比较：前缀重叠长度可以用于优化键的比较、排序等操作。
// 数据分区：根据键的前缀重叠情况，可以将数据分为不同的区块或层级，优化查询效率。
fn compute_overlap(first_key: KeySlice, key: KeySlice) -> usize {
    let mut i = 0;
    loop {
        if i >= first_key.len() || i >= key.len() {
            break;
        }
        if first_key.raw_ref()[i] != key.raw_ref()[i] {
            break;
        }
        i += 1;
    }
    i
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    fn estimated_size(&self) -> usize {
        SIZEOF_U16 /* number of key-value pairs in the block */ +  self.offsets.len() * SIZEOF_U16 /* offsets */ + self.data.len()
        // key-value pairs
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        // 检查块大小是否足够：
        // self.estimated_size()：计算当前数据块的已使用大小。
        // key.len() 和 value.len()：表示即将添加的键和值的长度。
        // SIZEOF_U16 * 3：这里需要额外的 6 个字节，分别用于存储 key_len（键长度）、value_len（值长度）和 offset（偏移量）。
        if self.estimated_size() + key.len() + value.len() + SIZEOF_U16 * 3 /* key_len, value_len and offset */ > self.block_size
            && !self.is_empty()
        {
            return false;
        }
        // Add the offset of the data into the offset array.
        self.offsets.push(self.data.len() as u16);
        // 这是一个辅助函数，计算当前要添加的键和 first_key（块中的第一个键）之间的公共前缀长度。前缀压缩可以减少存储空间，因为相同的前缀只需要存储一次。
        let overlap = compute_overlap(self.first_key.as_key_slice(), key);
        // Encode key overlap.
        // 将键与 first_key 共享的前缀长度编码为 2 个字节（u16），存储到 data 中。
        self.data.put_u16(overlap as u16);
        // Encode key length.
        // 将键的剩余部分（不包含前缀）长度编码为 2 个字节，并存储到 data 中。
        self.data.put_u16((key.len() - overlap) as u16);
        // Encode key content.
        // 将键的剩余部分（不包含前缀）存储到 data 中。这只会存储从 overlap 开始的后缀部分，避免重复存储前缀。
        self.data.put(&key.raw_ref()[overlap..]);
        // Encode value length.
        self.data.put_u16(value.len() as u16);
        // Encode value content.
        self.data.put(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        true
    }

    /// Check if there are no key-value pairs in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
