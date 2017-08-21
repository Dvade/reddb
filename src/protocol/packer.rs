use std::mem::transmute;

pub fn pack_int(data: &mut [u8], value: i32) {
    assert!(data.len() >= 4, "Unable to write i32 into buffer. Not enough space, available {}", data.len());

    data[0] = value as u8;
    data[1] = (value >> 8) as u8;
    data[2] = (value >> 16) as u8;
    data[3] = (value >> 24) as u8;
}

#[test]
fn test_pack_int_success() {
    let mut array: [u8; 4] = [0; 4];
    pack_int(&mut array, 42);
}

#[test]
#[should_panic]
fn test_pack_int_fail() {
    let mut array: [u8; 3] = [0; 3];
    pack_int(&mut array, 42);
}

pub fn pack_smallint(data: &mut [u8], value: i16) {
    assert!(data.len() >= 2, "Unable to write i16 into buffer. Not enough space");

    data[0] = value as u8;
    data[1] = (value >> 8) as u8;
}

#[test]
fn test_pack_smallint_success() {
    let mut array: [u8; 2] = [0; 2];
    pack_smallint(&mut array, 42);
}

#[test]
#[should_panic]
fn test_pack_smallint_fail() {
    let mut array: [u8; 1] = [0; 1];
    pack_smallint(&mut array, 42);
}

pub fn pack_bigint(data: &mut [u8], value: i64) {
    assert!(data.len() >= 8, "Unable to write i64 into buffer. Not enough space");

    data[0] = value as u8;
    data[1] = (value >> 8) as u8;
    data[2] = (value >> 16) as u8;
    data[3] = (value >> 24) as u8;
    data[4] = (value >> 32) as u8;
    data[5] = (value >> 40) as u8;
    data[6] = (value >> 48) as u8;
    data[7] = (value >> 56) as u8;
}

#[test]
fn test_pack_bigint_success() {
    let mut array: [u8; 8] = [0; 8];
    pack_bigint(&mut array, 42);
}

#[test]
#[should_panic]
fn test_pack_bigint_fail() {
    let mut array: [u8; 7] = [0; 7];
    pack_bigint(&mut array, 42);
}

pub fn pack_float(data: &mut [u8], value: f64) {
    assert!(data.len() >= 8, "Unable to write f64 into buffer. Not enough space");

    let transumuted = unsafe {
        transmute::<f64, i64>(value)
    };

    data[0] = transumuted as u8;
    data[1] = (transumuted >> 8) as u8;
    data[2] = (transumuted >> 16) as u8;
    data[3] = (transumuted >> 24) as u8;
    data[4] = (transumuted >> 32) as u8;
    data[5] = (transumuted >> 40) as u8;
    data[6] = (transumuted >> 48) as u8;
    data[7] = (transumuted >> 56) as u8;
}

#[test]
fn test_pack_float_success() {
    let mut array: [u8; 8] = [0; 8];
    pack_float(&mut array, 42.02);
}

#[test]
#[should_panic]
fn test_pack_float_fail() {
    let mut array: [u8; 7] = [0; 7];
    pack_float(&mut array, 42.02);
}

pub fn pack_bool(data: &mut [u8], value: bool) {
    assert!(data.len() >= 1, "Unable to write bool into buffer. Not enough space");

    data[0] = value as u8;
}

#[test]
fn test_pack_bool_success() {
    let mut array: [u8; 1] = [0; 1];
    pack_bool(&mut array, true);
}

#[test]
#[should_panic]
fn test_pack_bool_fail() {
    let mut array: [u8; 0] = [0; 0];
    pack_bool(&mut array, true);
}

pub fn pack_string(data: &mut [u8], value: &str) {
    assert!(data.len() >= value.len() + 4, "Unable to write string into buffer. Not enough space");

    pack_int(data, value.len() as i32);

    let mut pos = 0;
    for b in value.as_bytes() {
        data[pos] = *b as u8;
        pos += 1;
    }
}

#[test]
fn test_pack_string_success() {
    let mut array: [u8; 8] = [0; 8];
    pack_string(&mut array, "test");
}

#[test]
#[should_panic]
fn test_pack_string_fail() {
    let mut array: [u8; 7] = [0; 7];
    pack_string(&mut array, "test");
}

pub fn pack_array(data: &mut [u8], value: &[u8]) {
    assert!(data.len() >= value.len() + 4, "Unable to write binary into buffer. Not enough space");

    pack_int(data, value.len() as i32);

    let mut pos = 0;
    for b in value {
        data[pos] = *b as u8;
        pos += 1;
    }
}

#[test]
fn test_pack_array_success() {
    let mut array: [u8; 8] = [0; 8];
    let test: [u8; 4] = [1; 4];

    pack_array(&mut array, &test);
}

#[test]
#[should_panic]
fn test_pack_array_fail() {
    let mut array: [u8; 7] = [0; 7];
    let test: [u8; 4] = [1; 4];

    pack_array(&mut array, &test);
}