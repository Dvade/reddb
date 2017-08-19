use std::sync::Mutex;
use std::sync::Arc;

use storage::MemoryPage;

#[derive(Debug)]
pub struct Storage {
    root: Vec<Arc<Mutex<MemoryPage>>>,
}

impl Storage {
    /// Create new Storage
    pub fn new() -> Self {
        Storage { root: Vec::new() }
    }
}
