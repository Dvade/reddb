use data_type::DataType;
use storage::MemoryPage;
use std::mem::transmute;

/// Serializes and writes values to MemoryPage.
/// Always writes values in the architertural endian currently.
#[derive(Debug)]
pub struct SerializeStream<'a> {
    position: usize,
    page: &'a mut MemoryPage,
}

impl<'a> SerializeStream<'a> {
    /// Creates new stream.
    pub fn new(page: &'a mut MemoryPage, pos: usize) -> Self {
        SerializeStream {
            position: pos,
            page: page,
        }
    }

    fn check_available_space(&self, val: isize) -> Result<(), String> {
        let avail: isize = self.page.data().len() as isize - self.position as isize;

        if avail < val as isize {
            Err(format!("Not enough memory to serialize value: required={}, available={}",
                        val,
                        avail))
        } else {
            Ok(())
        }
    }

    /// Check if we have enough memory in page to write a static typed value.
    fn check_static_type_len(&self, field_type: DataType) -> Result<(), String> {
        let field_len: isize = field_type.static_len() as isize;

        match self.check_available_space(field_len) {
            Err(e) => return Err(format!("Unable to write {:?}: {:?}", field_type, e)),
            Ok(_) => ()
        }

        Ok(())
    }

    /// Check if we have enough memory in page to write a dynamic typed value.
    fn check_dynamic_type_len(&self, field_type: DataType, size: usize) -> Result<(), String> {
        let field_len: isize = field_type.static_len() as isize;

        match self.check_available_space(field_len + size as isize) {
            Err(e) => return Err(format!("Unable to write {:?}: {:?}", field_type, e)),
            Ok(_) => ()
        }

        Ok(())
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
            transmute::<f64, i64>(val)
        };

        self.write_bigint(transumuted)
    }


    /// Write u32 to stream.
    fn write_u32(&mut self, val: u32) -> Result<(), String> {
        self.check_available_space(4)?;

        let mem = self.page.data_mut();

        mem[self.position] = val as u8;
        mem[self.position + 1] = (val >> 8) as u8;
        mem[self.position + 2] = (val >> 16) as u8;
        mem[self.position + 3] = (val >> 24) as u8;

        self.position += 4;

        Ok(())
    }

    /// Write VARCHAR to stream.
    pub fn write_varchar(&mut self, val: &str) -> Result<(), String> {
        self.check_dynamic_type_len(DataType::VARCHAR, val.len())?;

        self.write_u32(val.len() as u32)?;

        let mem = self.page.data_mut();

        for b in val.as_bytes() {
            mem[self.position] = *b as u8;
            self.position += 1;
        }

        Ok(())
    }

    /// Write VARBINARY to stream.
    pub fn write_varbinary(&mut self, val: &[u8]) -> Result<(), String> {
        self.check_dynamic_type_len(DataType::VARBINARY, val.len())?;

        self.write_u32(val.len() as u32)?;

        let mem = self.page.data_mut();

        for b in val {
            mem[self.position] = *b as u8;
            self.position += 1;
        }

        Ok(())
    }
}

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

#[test]
fn write_single_int() {
    let mut page = MemoryPage::new(4);
    let mut stream = SerializeStream::new(&mut page, 0);

    stream.write_int(42).expect("Should not fail");
    stream.write_int(999).unwrap_err();
}

#[test]
fn write_several_ints() {
    let mut page = MemoryPage::new(4 + 4 + 4 + 3);
    let mut stream = SerializeStream::new(&mut page, 0);

    stream.write_int(2093608745).expect("Should not fail");
    stream.write_int(0).expect("Should not fail");
    stream.write_int(-1294).expect("Should not fail");
    stream.write_int(0).unwrap_err();
}

#[test]
fn write_signle_smallint() {
    let mut page = MemoryPage::new(2);
    let mut stream = SerializeStream::new(&mut page, 0);

    stream.write_smallint(42).expect("Should not fail");
    stream.write_smallint(999).unwrap_err();
}

#[test]
fn write_several_smallints() {
    let mut page = MemoryPage::new(3 * 2 + 1);
    let mut stream = SerializeStream::new(&mut page, 0);

    stream.write_smallint(11).expect("Should not fail");
    stream.write_smallint(0).expect("Should not fail");
    stream.write_smallint(-1294).expect("Should not fail");
    stream.write_smallint(0).unwrap_err();
}

#[test]
fn write_signle_bigint() {
    let mut page = MemoryPage::new(8);
    let mut stream = SerializeStream::new(&mut page, 0);

    stream.write_bigint(42).expect("Should not fail");
    stream.write_bigint(999).unwrap_err();
}

#[test]
fn write_several_bigints() {
    let mut page = MemoryPage::new(3 * 8 + 7);
    let mut stream = SerializeStream::new(&mut page, 0);

    stream.write_bigint(11).expect("Should not fail");
    stream.write_bigint(0).expect("Should not fail");
    stream.write_bigint(-1294).expect("Should not fail");
    stream.write_bigint(0).unwrap_err();
}

#[test]
fn write_signle_bool() {
    let mut page = MemoryPage::new(1);
    let mut stream = SerializeStream::new(&mut page, 0);

    stream.write_bool(true).expect("Should not fail");
    stream.write_bool(false).unwrap_err();
}

#[test]
fn write_several_bools() {
    let mut page = MemoryPage::new(3 * 1);
    let mut stream = SerializeStream::new(&mut page, 0);

    stream.write_bool(true).expect("Should not fail");
    stream.write_bool(true).expect("Should not fail");
    stream.write_bool(true).expect("Should not fail");
    stream.write_bool(false).unwrap_err();
}

#[test]
fn write_signle_float() {
    let mut page = MemoryPage::new(8);
    let mut stream = SerializeStream::new(&mut page, 0);

    stream.write_float(42.05).expect("Should not fail");
    stream.write_float(999.05).unwrap_err();
}

#[test]
fn write_several_float() {
    let mut page = MemoryPage::new(3 * 8 + 7);
    let mut stream = SerializeStream::new(&mut page, 0);

    stream.write_float(11.05).expect("Should not fail");
    stream.write_float(0.05).expect("Should not fail");
    stream.write_float(-1294.05).expect("Should not fail");
    stream.write_float(0.05).unwrap_err();
}

#[test]
fn write_signle_string() {
    let test = "test";
    let test1 = "test";
    let mut page = MemoryPage::new(test.len() + 4);
    let mut stream = SerializeStream::new(&mut page, 0);

    stream.write_varchar(test).expect("Should not fail");
    stream.write_varchar(test1).unwrap_err();
}

#[test]
fn write_several_strings() {
    let test = "test";
    let test1 = "test1";
    let test2 = "test2";
    let mut page = MemoryPage::new(3 * 4 + test.len() + test1.len() + test2.len());
    let mut stream = SerializeStream::new(&mut page, 0);

    stream.write_varchar(test).expect("Should not fail");
    stream.write_varchar(test1).expect("Should not fail");
    stream.write_varchar(test2).expect("Should not fail");
    stream.write_varchar("error").unwrap_err();
}

#[test]
fn write_signle_array() {
    let arr: [u8; 5] = [1, 2, 3, 4, 5];
    let mut page = MemoryPage::new(arr.len() + 4);
    let mut stream = SerializeStream::new(&mut page, 0);

    stream.write_varbinary(&arr).expect("Should not fail");
    stream.write_varbinary(&[1, 1]).unwrap_err();
}

#[test]
fn write_several_arrays() {
    let arr: [u8; 5] = [1, 2, 3, 4, 5];

    let mut page = MemoryPage::new(3 * 4 + arr.len() + arr.len() + arr.len());
    let mut stream = SerializeStream::new(&mut page, 0);

    stream.write_varbinary(&arr).expect("Should not fail");
    stream.write_varbinary(&arr).expect("Should not fail");
    stream.write_varbinary(&arr).expect("Should not fail");
    stream.write_varbinary(&[2, 2]).unwrap_err();
}

#[test]
fn write_all_types() {
    let arr: [u8; 5] = [1, 2, 3, 4, 5];
    let test = "test";

    let mut page = MemoryPage::new(2 * 4 + arr.len() + test.len() + 
                                   1 + 4 + 
                                   2 + 8 + 
                                   8);
    let mut stream = SerializeStream::new(&mut page, 0);

    stream.write_bool(true).expect("Should not fail");
    stream.write_float(2.2).expect("Should not fail");
    stream.write_int(2).expect("Should not fail");
    stream.write_smallint(2).expect("Should not fail");
    stream.write_bigint(2).expect("Should not fail");
    stream.write_varbinary(&arr).expect("Should not fail");
    stream.write_varchar(test).expect("Should not fail");
    stream.write_varbinary(&[2, 2]).unwrap_err();
}
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
