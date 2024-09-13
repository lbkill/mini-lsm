#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

// +-----------------------------+
// | Number of BlockMeta Entries  | 4 bytes (u32)
// +-----------------------------+
// |         BlockMeta #1         |
// +-----------------------------+
// |        Offset (u32)          | 4 bytes
// +-----------------------------+
// |   First Key Length (u16)     | 2 bytes
// +-----------------------------+
// |       First Key Data         | variable (depends on First Key Length)
// +-----------------------------+
// |   Last Key Length (u16)      | 2 bytes
// +-----------------------------+
// |       Last Key Data          | variable (depends on Last Key Length)
// +-----------------------------+
// |         BlockMeta #2         |
// +-----------------------------+
// |        Offset (u32)          | 4 bytes
// +-----------------------------+
// |   First Key Length (u16)     | 2 bytes
// +-----------------------------+
// |       First Key Data         | variable
// +-----------------------------+
// |   Last Key Length (u16)      | 2 bytes
// +-----------------------------+
// |       Last Key Data          | variable
// +-----------------------------+
// |            ...               | Repeat for each BlockMeta
// +-----------------------------+
// |         CRC32 Checksum       | 4 bytes (u32)
// +-----------------------------+

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        // The estimated size of the buffer
        // The size of the number of block meta
        let mut estimated_size = size_of::<u32>();
        for meta in block_meta {
            // The size of offset
            estimated_size += size_of::<u32>();
            // The size of key length
            estimated_size += size_of::<u16>();
            // The size of actual key
            estimated_size += meta.first_key.len();
            // The size of key length
            estimated_size += size_of::<u16>();
            // The size of actual key
            estimated_size += meta.last_key.len();
        }
        // The size of the CRC32 checksum
        estimated_size += size_of::<u32>();
        // Reserve the space to improve performance, especially when the size of incoming data is
        // large
        // 预先分配空间，避免多次扩容
        buf.reserve(estimated_size);
        // The original length of the buffer
        // 这里是可以看做记录 buf 的起始位置
        let original_len = buf.len();
        // The number of block meta
        buf.put_u32(block_meta.len() as u32);
        // Encode each block meta
        for meta in block_meta {
            // The offset of the block
            buf.put_u32(meta.offset as u32);
            // The length of the first key
            buf.put_u16(meta.first_key.len() as u16);
            // The actual first key
            buf.put_slice(meta.first_key.raw_ref());
            // The length of the last key
            buf.put_u16(meta.last_key.len() as u16);
            // The actual last key
            buf.put_slice(meta.last_key.raw_ref());
        }
        // Calculate the CRC32 checksum of the buffer
        // 用于验证整个 BlockMeta 数据的完整性
        // original_len + 4 表示从 BlockMeta 的数量开始计算，跳过记录 BlockMeta 数量的位置。确保在计算 CRC32 时，校验的只是数据本身，而不是包含元数据的前导部分。
        // 这样就可以计算出 BlockMeta 数据的 CRC32 校验和
        buf.put_u32(crc32fast::hash(&buf[original_len + 4..]));
        assert_eq!(estimated_size, buf.len() - original_len);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: &[u8]) -> Result<Vec<BlockMeta>> {
        let mut block_meta = Vec::new();
        // The number of block meta
        let num = buf.get_u32() as usize;
        // Calculate the CRC32 checksum of the buffer
        let checksum = crc32fast::hash(&buf[..buf.remaining() - 4]);
        for _ in 0..num {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_len));
            let last_key_len: usize = buf.get_u16() as usize;
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_len));
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        // Verify the CRC32 checksum
        if buf.get_u32() != checksum {
            bail!("meta checksum mismatched");
        }

        Ok(block_meta)
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
/// -------------------------------------------------------------------------------------------
// |         Block Section         |          Meta Section         |          Extra          |
// -------------------------------------------------------------------------------------------
// | data block | ... | data block |            metadata           | meta block offset (u32) |
// -------------------------------------------------------------------------------------------
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let len = file.size();
        let raw_meta_offset = file.read(len - 4, 4)?;
        let block_meta_offset = (&raw_meta_offset[..]).get_u32() as u64;
        let raw_meta = file.read(block_meta_offset, len - 4 - block_meta_offset)?;
        let block_meta = BlockMeta::decode_block_meta(&raw_meta[..])?;
        Ok(Self {
            file,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            bloom: None,
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        // Get the offset of the block
        let offset = self.block_meta[block_idx].offset;
        // Get the offset of the next block
        let offset_end = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset);
        let block_data = self
            .file
            .read(offset as u64, (offset_end - offset) as u64)?;
        Ok(Arc::new(Block::decode(&block_data[..])))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            let blk = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;
            Ok(blk)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.block_meta
            // partition_point 是标准库提供的方法，用于在一个排序的序列中找到一个分割点，使得前面的元素都小于或等于给定条件，而后面的元素都大于给定条件。其时间复杂度为 O(log n)，适合在有序列表中进行二分查找。
            // meta.first_key.as_key_slice() <= key 这个闭包用于比较块的第一个键 first_key 和目标键 key。它找到第一个块的 first_key 大于 key 的位置（即返回的 partition_point 是大于 key 的第一个位置）。
            .partition_point(|meta| meta.first_key.as_key_slice() <= key)
            // saturating_sub 是安全的减法操作，防止下溢出。它会尝试将结果减去 1，但如果结果为 0 或更小，它返回 0，而不会产生负数。这样可以避免块索引为负的情况。
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
