use data_type::DataType;
use storage::MemoryPage;
use protocol::pack;

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
            Err(e) => Err(format!("Unable to write {:?}: {:?}", field_type, e)),
            Ok(_) => Ok(())
        }
    }

    /// Check if we have enough memory in page to write a dynamic typed value.
    fn check_dynamic_type_len(&self, field_type: DataType, size: usize) -> Result<(), String> {
        let field_len: isize = field_type.static_len() as isize;

        match self.check_available_space(field_len + size as isize) {
            Err(e) => return Err(format!("Unable to write {:?}: {:?}", field_type, e)),
            Ok(_) => Ok(())
        }
    }

    /// Write INTEGER to stream.
    pub fn write_int(&mut self, val: i32) -> Result<(), String> {
        self.check_static_type_len(DataType::INTEGER)?;

        let mem = self.page.data_mut();
        let len = mem.len();

        self.position += pack::pack_int(&mut mem[self.position..len], val);

        Ok(())
    }

    /// Write SMALLINT to stream.
    pub fn write_smallint(&mut self, val: i16) -> Result<(), String> {
        self.check_static_type_len(DataType::SMALLINT)?;

        let mem = self.page.data_mut();
        let len = mem.len();

        self.position += pack::pack_smallint(&mut mem[self.position..len], val);

        Ok(())
    }

    /// Write BIGINT to stream.
    pub fn write_bigint(&mut self, val: i64) -> Result<(), String> {
        self.check_static_type_len(DataType::BIGINT)?;

        let mem = self.page.data_mut();
        let len = mem.len();

        self.position += pack::pack_bigint(&mut mem[self.position..len], val);

        Ok(())
    }

    /// Write BOOLEAN to stream.
    pub fn write_bool(&mut self, val: bool) -> Result<(), String> {
        self.check_static_type_len(DataType::BOOLEAN)?;

        let mem = self.page.data_mut();
        let len = mem.len();

        self.position += pack::pack_bool(&mut mem[self.position..len], val);

        Ok(())
    }

    /// Write VARCHAR to stream.
    pub fn write_float(&mut self, val: f64) -> Result<(), String> {
        self.check_static_type_len(DataType::FLOAT)?;

        let mem = self.page.data_mut();
        let len = mem.len();

        self.position += pack::pack_float(&mut mem[self.position..len], val);

        Ok(())
    }

    /// Write VARCHAR to stream.
    pub fn write_varchar(&mut self, val: &str) -> Result<(), String> {
        self.check_dynamic_type_len(DataType::VARCHAR, val.len())?;

        let mem = self.page.data_mut();
        let len = mem.len();

        self.position += pack::pack_string(&mut mem[self.position..len], val);

        Ok(())
    }

    /// Write VARBINARY to stream.
    pub fn write_varbinary(&mut self, val: &[u8]) -> Result<(), String> {
        self.check_dynamic_type_len(DataType::VARBINARY, val.len())?;

        let mem = self.page.data_mut();
        let len = mem.len();

        self.position += pack::pack_array(&mut mem[self.position..len], val);

        Ok(())
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
fn write_single_bigint() {
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
fn write_single_bool() {
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
fn write_single_float() {
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
fn write_single_string() {
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
                                   1 + 4 + 2 + 8 + 8);
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