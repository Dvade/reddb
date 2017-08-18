#[derive(Debug)]
pub struct MemoryPage {
    mem: Box<[u8]>,
}

/// Returns the size of memory page in OS
fn os_page_size() -> usize {
    // Temporary
    64 * 1024
}

impl MemoryPage {
    /// Creates new instance of MemoryPage with the specific size
    pub fn new(len: usize) -> MemoryPage {
        let vmem = vec![0; len];
        MemoryPage { mem: vmem.into_boxed_slice() }
    }

    /// Creates new instance of MemoryPage
    pub fn new_os() -> MemoryPage {
        MemoryPage::new(os_page_size())
    }
}