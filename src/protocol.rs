use data_type::DataType;
use storage::MemoryPage;
use std::mem;

/// Serializes and writes values to MemoryPage.
/// Always writes valus in the architertural endian currently.
#[derive(Debug)]
struct SerializeStream<'a> {
    position: usize,
    page: &'a mut MemoryPage,
}

impl<'a> SerializeStream<'a> {
    /// Creates new streamer.
    pub fn new(page: &'a mut MemoryPage) -> Self {
        SerializeStream {
            position: 0,
            page: page,
        }
    }

    /// Check if we have enough memory in page to write a static typed value.
    fn check_static_type_len(&self, field_type: DataType) -> Result<(), String> {
        let avail: isize = self.page.data().len() as isize - self.position as isize;
        let field_len: isize = field_type.static_len() as isize;

        if avail < field_len as isize {
            Err(format!("Not enough memory to serialize {:?} value: required={}, available={}",
                        field_type,
                        field_len,
                        avail))
        } else {
            Ok(())
        }
    }

    /// Check if we have enough memory in page to write a dynamic typed value.
    fn check_dynamic_type_len(&self, field_type: DataType, size: usize) -> Result<(), String> {
        let avail: isize = self.page.data().len() as isize - self.position as isize;
        let field_len: isize = field_type.static_len() as isize + size as isize;

        if avail < field_len as isize {
            Err(format!("Not enough memory to serialize {:?} value: required={}, available={}",
                        field_type,
                        field_len,
                        avail))
        } else {
            Ok(())
        }
    }

    /// Write INTEGER to stream.
    pub fn write_int(&mut self, val: i32) -> Result<(), String> {
        self.check_static_type_len(DataType::INTEGER)?;

        let mem = self.page.data_mut();

        mem[self.position] = val as u8;
        mem[self.position + 1] = (val >> 8) as u8;
        mem[self.position + 2] = (val >> 16) as u8;
        mem[self.position + 3] = (val >> 24) as u8;

        self.position += 4;

        Ok(())
    }

    /// Write SMALLINT to stream.
    pub fn write_smallint(&mut self, val: i16) -> Result<(), String> {
        self.check_static_type_len(DataType::SMALLINT)?;

        let mem = self.page.data_mut();

        mem[self.position] = val as u8;
        mem[self.position + 1] = (val >> 8) as u8;

        self.position += 2;

        Ok(())
    }

    /// Write BIGINT to stream.
    pub fn write_bigint(&mut self, val: i64) -> Result<(), String> {
        self.check_static_type_len(DataType::BIGINT)?;

        let mem = self.page.data_mut();

        mem[self.position] = val as u8;
        mem[self.position + 1] = (val >> 8) as u8;
        mem[self.position + 2] = (val >> 16) as u8;
        mem[self.position + 3] = (val >> 24) as u8;
        mem[self.position + 4] = (val >> 32) as u8;
        mem[self.position + 5] = (val >> 40) as u8;
        mem[self.position + 6] = (val >> 48) as u8;
        mem[self.position + 7] = (val >> 56) as u8;

        self.position += 8;

        Ok(())
    }

    /// Write BOOLEAN to stream.
    pub fn write_bool(&mut self, val: bool) -> Result<(), String> {
        self.check_static_type_len(DataType::BOOLEAN)?;

        let mem = self.page.data_mut();

        mem[self.position] = val as u8;

        self.position += 1;

        Ok(())
    }

    /// Write VARCHAR to stream.
    pub fn write_float(&mut self, val: f64) -> Result<(), String> {
        self.check_static_type_len(DataType::FLOAT)?;

        let transumuted = unsafe {
            mem::transmute::<f64, i64>(val)
        };

        self.write_bigint(transumuted)
    }


    /// Write usize to stream.
    fn write_usize(&mut self, val: usize) -> Result<(), String> {
        self.check_static_type_len(DataType::VARBINARY)?;

        let mem = self.page.data_mut();
        let val_size = mem::size_of_val(&val);

        for byte_number in 0..val_size {
            mem[self.position + byte_number] = (val >> (8 * byte_number)) as u8;
        }

        self.position += val_size;

        Ok(())
    }

    /// Write VARCHAR to stream.
    pub fn write_varchar(&mut self, val: String) -> Result<(), String> {
        self.check_dynamic_type_len(DataType::VARCHAR, val.len())?;

        self.write_usize(val.len())?;

        let mem = self.page.data_mut();

        for b in val.as_bytes() {
            mem[self.position] = *b as u8;
            self.position += 1;
        }

        Ok(())
    }

    /// Write VARBINARY to stream.
    pub fn write_varbinary(&mut self, val: &[u8]) -> Result<(), String> {
        self.check_dynamic_type_len(DataType::VARCHAR, val.len())?;

        self.write_usize(val.len())?;

        let mem = self.page.data_mut();

        for b in val {
            mem[self.position] = *b as u8;
            self.position += 1;
        }

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