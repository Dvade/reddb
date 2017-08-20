use skiplist::ordered_skiplist::OrderedSkipList;
use std::sync::Arc;

use data_type::DataType;
use storage::MemoryPage;
use storage::DataReference;

/// Index.
/// Stores references to data in a sorted order.
/// Only single column index is supported for now.
struct Index {
    data_type: DataType,
    index: OrderedSkipList<DataReference>,
}

impl Index {
    /// Create new index instance.
    pub fn new(data_type: DataType) -> Self {
        Index {
            data_type: data_type,
            index: OrderedSkipList::new(),
        }
    }

    /// Add new value to index.
    pub fn add(&mut self, page: Arc<MemoryPage>, pos: usize) {
        let dref = DataReference::new(page, pos, self.data_type);
        self.index.insert(dref);
    }
}