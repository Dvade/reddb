use std::sync::Mutex;
use std::sync::Arc;

use data_type::Data;

#[derive(Debug)]
pub struct Storage {
    root: Vec<Arc<Mutex<Vec<Data>>>>,
}

impl Storage {
    /// Create new Storage
    pub fn new() -> Self {
        Storage { root: Vec::new() }
    }
}
