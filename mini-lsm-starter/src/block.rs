#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
/// ----------------------------------------------------------------------------------------------------
// |             Data Section             |              Offset Section             |      Extra      |
// ----------------------------------------------------------------------------------------------------
// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
// ----------------------------------------------------------------------------------------------------
// -----------------------------------------------------------------------
// |                           Entry #1                            | ... |
// -----------------------------------------------------------------------
// | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
// -----------------------------------------------------------------------
// -------------------------------
// |offset|offset|num_of_elements|
// -------------------------------
// |   0  |  12  |       2       |
// -------------------------------
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut encoded = Vec::new();

        // Append all key-value data entries
        encoded.extend_from_slice(&self.data);

        // Append all offsets (each offset is a u16 value)
        for &offset in &self.offsets {
            encoded.extend_from_slice(&offset.to_le_bytes());
        }

        // Append the number of elements (num_of_elements is a u16 value)
        let num_of_elements = self.offsets.len() as u16;
        encoded.extend_from_slice(&num_of_elements.to_le_bytes());

        // Convert Vec<u8> to Bytes and return
        Bytes::from(encoded)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let total_len = data.len();

        // Read the number of elements from the end of the block
        let num_of_elements_offset = total_len - 2;
        let num_of_elements = u16::from_le_bytes([data[num_of_elements_offset], data[num_of_elements_offset + 1]]) as usize;

        // Read the offsets before the num_of_elements
        let offset_section_start = total_len - 2 - (num_of_elements * 2);
        let mut offsets = Vec::new();
        for i in 0..num_of_elements {
            let offset_pos = offset_section_start + i * 2;
            let offset = u16::from_le_bytes([data[offset_pos], data[offset_pos + 1]]);
            offsets.push(offset);
        }

        // Read the data section
        let data_section = data[..offset_section_start].to_vec();

        Block {
            data: data_section,
            offsets,
        }
    }
}
