use std::borrow::BorrowMut;
use std::borrow::Borrow;

#[derive(Debug)]
pub struct MemoryPage {
    mem: Box<[u8]>,
}

impl MemoryPage {
    /// Creates new instance of MemoryPage with the specific size
    pub fn new(len: usize) -> Self {
        let vmem = vec![0; len];
        MemoryPage { mem: vmem.into_boxed_slice() }
    }

    /// Gets mutable reference to page data
    pub fn data(&self) -> &[u8] {
        self.mem.borrow()
    }

    /// Gets mutable reference to page data
    pub fn data_mut(&mut self) -> &mut [u8] {
        self.mem.borrow_mut()
    }
}
