#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{KeySlice, KeyVec};

use super::Block;

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

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // Calculate the total size required to add the key-value pair
        let key_len = key.len() as u16;
        let value_len = value.len() as u16;
        let entry_size = 2 + key_len as usize + 2 + value_len as usize; // 2B key_len + key + 2B value_len + value

        // Check if adding this entry would exceed block_size
        if self.data.len() + entry_size > self.block_size {
            return false;
        }

        // Record the offset of this entry (which is the current length of data)
        self.offsets.push(self.data.len() as u16);

        // Append key_len, key, value_len, value to the data
        self.data.extend_from_slice(&key_len.to_le_bytes());
        self.data.extend_from_slice(key.raw_ref());
        self.data.extend_from_slice(&value_len.to_le_bytes());
        self.data.extend_from_slice(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
