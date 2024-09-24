#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;
use super::{BlockMeta, FileObject, SsTable};
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};
use crate::key::KeyVec;
use crate::table::bloom::Bloom;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    // 用于存储每个 key 的 hash 值
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        // if the first key is empty, set it to the current key
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        // 计算 key 的 hash 值，并将其添加到 key_hashes 中
        self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));

        // if the current block can hold the key-value pair, add it to the block
        if self.builder.add(key, value) {
            self.last_key.set_from_slice(key);
            return;
        }

        // create a new block builder and append block data
        self.finish_block();

        // add the key-value pair to the next block
        assert!(self.builder.add(key, value));
        self.first_key.set_from_slice(key);
        self.last_key.set_from_slice(key);
    }

    fn finish_block(&mut self) {
        // 采用 std::mem::replace 方法，将当前的 BlockBuilder 替换为一个新的 BlockBuilder。
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        // 将当前的 BlockBuilder 构建成一个 Block，并将其编码后的数据追加到 SSTable 的 data 字段中。
        let encoded_block = builder.build().encode();
        // 将 Block 的元信息（BlockMeta）添加到 SSTable 的 meta 字段中。
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key).into_key_bytes(),
            last_key: std::mem::take(&mut self.last_key).into_key_bytes(),
        });
        self.data.extend(encoded_block);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_block();
        let mut buf = self.data;
        let meta_offset = buf.len();
        // 将 SSTable 的元信息（BlockMeta）编码后，追加到 SSTable 的 data 字段中。
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        // 将 SSTable 的元信息的偏移量（meta_offset）写入 SSTable 的文件头部。
        buf.put_u32(meta_offset as u32);
        // 根据 SSTable 的 key_hashes 字段构建一个 Bloom 过滤器。
        let bloom = Bloom::build_from_key_hashes(
            &self.key_hashes,
            Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01),
        );
        // 将 Bloom 过滤器编码后，追加到 SSTable 的 data 字段中。
        let bloom_offset = buf.len();
        bloom.encode(&mut buf);
        buf.put_u32(bloom_offset as u32);
        // 创建一个 FileObject 对象，将 SSTable 的数据写入到文件中。
        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            id,
            file,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            bloom: Some(bloom),
            block_meta: self.meta,
            block_meta_offset: meta_offset,
            block_cache,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
