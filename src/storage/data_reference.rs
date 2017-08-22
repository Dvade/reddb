use std::cmp::Ordering;
use std::sync::Arc;

use data_type::DataType;
use storage::MemoryPage;
use protocol::deserialize_stream::DeserializeStream;

/// Reference to indexed data.
pub struct DataReference {
    data_type: DataType,
    page: Arc<MemoryPage>,
    pos: usize,
}

impl DataReference {
    /// Create new data reference.
    pub fn new(page: Arc<MemoryPage>, pos: usize, data_type: DataType) -> Self {
        DataReference {
            data_type: data_type,
            page: page,
            pos: pos,
        }
    }

    /// Deserializes value as an integer.
    pub fn to_int(&self) -> i32 {
        assert!(self.data_type == DataType::INTEGER);

        let mut rs = DeserializeStream::new(&self.page, self.pos);
        rs.read_int().unwrap()
    }
}

impl PartialOrd for DataReference {
    fn partial_cmp(&self, other: &DataReference) -> Option<Ordering> {
        if self.data_type != other.data_type {
            None
        } else {
            match self.data_type {
                DataType::INTEGER => self.to_int().partial_cmp(&other.to_int()),
                _ => unimplemented!(),
            }
        }
    }
}

impl PartialEq for DataReference {
    fn eq(&self, other: &DataReference) -> bool {
        self.data_type == other.data_type
    }
}
