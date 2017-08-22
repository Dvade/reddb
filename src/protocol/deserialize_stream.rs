use data_type::DataType;
use storage::MemoryPage;

/// Reads and deserializes values from MemoryPage.
/// Always reads values in the architertural endian currently.
#[derive(Debug)]
pub struct DeserializeStream<'a> {
    position: usize,
    page: &'a MemoryPage,
}

impl<'a> DeserializeStream<'a> {
    /// Creates new stream.
    pub fn new(page: &'a MemoryPage, pos: usize) -> Self {
        DeserializeStream {
            position: pos,
            page: page,
        }
    }

    /// Check if we have enough memory in page to read a value.
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

    /// Read integer from stream.
    pub fn read_int(&mut self) -> Result<i32, String> {
        self.check_space(DataType::INTEGER.static_len())?;

        let mem = self.page.data();
        let p = self.position;

        let res: i32 = (mem[p] as i32 & 0xFFi32) | (mem[p + 1] as i32 & 0xFFi32) << 8 |
                       (mem[p + 2] as i32 & 0xFFi32) << 16 |
                       (mem[p + 3] as i32 & 0xFFi32) << 24;

        self.position += 4;

        Ok(res)
    }
}


#[cfg(test)]
mod test {
    use storage::MemoryPage;
    use protocol::serialize_stream::SerializeStream;
    use protocol::deserialize_stream::DeserializeStream;

    #[test]
    fn write_read_single_int() {
        let mut page = MemoryPage::new(4);
        {
            let mut ws = SerializeStream::new(&mut page, 0);
            ws.write_int(42).expect("Should not fail");
        }

        let mut rs = DeserializeStream::new(&mut page, 0);
        let val = rs.read_int().unwrap();

        assert_eq!(val, 42);
    }

    #[test]
    fn write_read_several_ints() {
        let mut page = MemoryPage::new(4 + 4 + 4 + 3);
        {
            let mut ws = SerializeStream::new(&mut page, 0);

            ws.write_int(2093608745).expect("Should not fail");
            ws.write_int(0).expect("Should not fail");
            ws.write_int(-1294).expect("Should not fail");
        }

        let mut rs = DeserializeStream::new(&mut page, 0);
        assert_eq!(rs.read_int().unwrap(), 2093608745);
        assert_eq!(rs.read_int().unwrap(), 0);
        assert_eq!(rs.read_int().unwrap(), -1294);
    }
}
