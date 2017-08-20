use std::cmp::Ordering;
use std::sync::Arc;

use data_type::DataType;
use storage::MemoryPage;
use protocol::DeserializeStream;

/// Reference to indexed data.
pub struct DataReference {
    data_type: DataType,
    page: Arc<MemoryPage>,
    pos: usize,
}

impl DataReference {
    /// Create new data reference.
    pub fn new(page: Arc<MemoryPage>, pos: usize, data_type: DataType) -> Self {
        DataReference {
            data_type: data_type,
            page: page,
            pos: pos,
        }
    }

    /// Deserializes value as an integer.
    pub fn to_int(&self) -> i32 {
        assert!(self.data_type == DataType::INTEGER);

        let mut rs = DeserializeStream::new(&self.page, self.pos);
        rs.read_int().unwrap()
    }
}

impl PartialOrd for DataReference {
    fn partial_cmp(&self, other: &DataReference) -> Option<Ordering> {
        if self.data_type != other.data_type {
            None
        } else {
            let spp = self.page.as_ref() as *const MemoryPage;
            let opp = other.page.as_ref() as *const MemoryPage;

            if spp == opp && self.pos == other.pos {
                return Some(Ordering::Equal);
            }

            match self.data_type {
                DataType::INTEGER => self.to_int().partial_cmp(&other.to_int()),
                _ => unimplemented!(),
            }
        }
    }
}

impl PartialEq for DataReference {
    fn eq(&self, other: &DataReference) -> bool {
        self.data_type == other.data_type
    }
}

#[test]
fn int_cmp() {
    use protocol::SerializeStream;

    let mut page = MemoryPage::new(1024);

    {
        let mut ws = SerializeStream::new(&mut page, 0);

        ws.write_int(1).expect("Should not fail");
        ws.write_int(2).expect("Should not fail");
        ws.write_int(42).expect("Should not fail");
        ws.write_int(0).expect("Should not fail");
        ws.write_int(42).expect("Should not fail");
    }

    let pref = Arc::new(page);

    let dref1 = DataReference::new(pref.clone(), 0, DataType::INTEGER);
    let dref2 = DataReference::new(pref.clone(), 4, DataType::INTEGER);
    let dref42_1 = DataReference::new(pref.clone(), 8, DataType::INTEGER);
    let dref0 = DataReference::new(pref.clone(), 12, DataType::INTEGER);
    let dref42_2 = DataReference::new(pref.clone(), 16, DataType::INTEGER);

    assert!(dref1 == dref1);
    assert!(dref1 < dref2);
    assert!(dref1 < dref42_1);
    assert!(dref1 > dref0);
    assert!(dref1 < dref42_2);

    assert!(dref2 > dref1);
    assert!(dref2 == dref2);
    assert!(dref2 < dref42_1);
    assert!(dref2 > dref0);
    assert!(dref2 < dref42_2);

    assert!(dref42_1 > dref1);
    assert!(dref42_1 > dref2);
    assert!(dref42_1 == dref42_1);
    assert!(dref42_1 > dref0);
    assert!(dref42_1 == dref42_2);

    assert!(dref0 < dref1);
    assert!(dref0 < dref2);
    assert!(dref0 < dref42_1);
    assert!(dref0 == dref0);
    assert!(dref0 < dref42_2);

    assert!(dref42_2 > dref1);
    assert!(dref42_2 > dref2);
    assert!(dref42_2 == dref42_1);
    assert!(dref42_2 > dref0);
    assert!(dref42_2 == dref42_2);
}