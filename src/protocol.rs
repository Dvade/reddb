use data_type::DataType;
use storage::memory_page::MemoryPage;

#[derive(Debug)]
struct SerializeStream<'a> {
    position: usize,
    page: &'a mut MemoryPage,
}

/// Serializes and writes values to MemoryPage.
/// Always writes valus in the architertural endian currently.
impl<'a> SerializeStream<'a> {
    pub fn new(page: &'a mut MemoryPage) -> SerializeStream {
        SerializeStream {
            position: 0,
            page: page,
        }
    }

    /// Check if we have enough memory in page to write a value.
    fn check_space(&self, len: usize) -> Result<(), String> {
        let avail: isize = self.page.data().len() as isize - self.position as isize;

        if avail < len as isize {
            Err(format!("Not enough memory to serialize INTEGER value: required={}, available={}",
                        len,
                        avail))
        } else {
            Ok(())
        }
    }

    /// Write integer to stream.
    pub fn write_int(&mut self, val: i32) -> Result<(), String> {
        self.check_space(DataType::INTEGER.static_len())?;

        let mem = self.page.data_mut();

        mem[0] = val as u8;
        mem[1] = (val >> 8) as u8;
        mem[2] = (val >> 16) as u8;
        mem[3] = (val >> 24) as u8;

        self.position += 4;

        Ok(())
    }
}

#[test]
fn write_signle_int() {
    let mut page = MemoryPage::new(4);
    let mut stream = SerializeStream::new(&mut page);

    stream.write_int(42).expect("Should not fail");
    stream.write_int(999).unwrap_err();
}

#[test]
fn write_several_ints() {
    let mut page = MemoryPage::new(4 + 4 + 4 + 3);
    let mut stream = SerializeStream::new(&mut page);

    stream.write_int(2093608745).expect("Should not fail");
    stream.write_int(0).expect("Should not fail");
    stream.write_int(-1294).expect("Should not fail");
    stream.write_int(0).unwrap_err();
}