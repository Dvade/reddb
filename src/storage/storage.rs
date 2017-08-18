use std::sync::Mutex;
use std::sync::Arc;

use storage::memory_page::MemoryPage;

#[derive(Debug)]
pub struct Storage {
    root: Vec<Arc<Mutex<MemoryPage>>>,
}

impl Storage {
    /// Create new Storage
    pub fn new() -> Storage {
        Storage { root: Vec::new() }
    }
}
