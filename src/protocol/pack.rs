use std::mem::transmute;

pub fn pack_int(data: &mut [u8], value: i32) -> usize {
    assert!(data.len() >= 4, "Unable to pack i32 into buffer. Not enough space, available {}", data.len());

    data[0] = value as u8;
    data[1] = (value >> 8) as u8;
    data[2] = (value >> 16) as u8;
    data[3] = (value >> 24) as u8;

    4
}

#[test]
fn test_pack_int_success() {
    let mut array: [u8; 4] = [0; 4];
    assert_eq!(4, pack_int(&mut array, 42));
}

#[test]
#[should_panic]
fn test_pack_int_fail() {
    let mut array: [u8; 3] = [0; 3];
    pack_int(&mut array, 42);
}

pub fn pack_smallint(data: &mut [u8], value: i16) -> usize {
    assert!(data.len() >= 2, "Unable to pack i16 into buffer. Not enough space");

    data[0] = value as u8;
    data[1] = (value >> 8) as u8;

    2
}

#[test]
fn test_pack_smallint_success() {
    let mut array: [u8; 2] = [0; 2];
    assert_eq!(2, pack_smallint(&mut array, 42));
}

#[test]
#[should_panic]
fn test_pack_smallint_fail() {
    let mut array: [u8; 1] = [0; 1];
    pack_smallint(&mut array, 42);
}

pub fn pack_bigint(data: &mut [u8], value: i64) -> usize {
    assert!(data.len() >= 8, "Unable to pack i64 into buffer. Not enough space");

    data[0] = value as u8;
    data[1] = (value >> 8) as u8;
    data[2] = (value >> 16) as u8;
    data[3] = (value >> 24) as u8;
    data[4] = (value >> 32) as u8;
    data[5] = (value >> 40) as u8;
    data[6] = (value >> 48) as u8;
    data[7] = (value >> 56) as u8;

    8
}

#[test]
fn test_pack_bigint_success() {
    let mut array: [u8; 8] = [0; 8];
    assert_eq!(8, pack_bigint(&mut array, 42));
}

#[test]
#[should_panic]
fn test_pack_bigint_fail() {
    let mut array: [u8; 7] = [0; 7];
    pack_bigint(&mut array, 42);
}

pub fn pack_float(data: &mut [u8], value: f64) -> usize {
    assert!(data.len() >= 8, "Unable to pack f64 into buffer. Not enough space");

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

    8
}

#[test]
fn test_pack_float_success() {
    let mut array: [u8; 8] = [0; 8];
    assert_eq!(8, pack_float(&mut array, 42.02));
}

#[test]
#[should_panic]
fn test_pack_float_fail() {
    let mut array: [u8; 7] = [0; 7];
    pack_float(&mut array, 42.02);
}

pub fn pack_bool(data: &mut [u8], value: bool) -> usize {
    assert!(data.len() >= 1, "Unable to pack bool into buffer. Not enough space");

    data[0] = value as u8;

    1
}

#[test]
fn test_pack_bool_success() {
    let mut array: [u8; 1] = [0; 1];
    assert_eq!(1, pack_bool(&mut array, true));
}

#[test]
#[should_panic]
fn test_pack_bool_fail() {
    let mut array: [u8; 0] = [0; 0];
    pack_bool(&mut array, true);
}

pub fn pack_unsigned(data: &mut [u8], value: u32) -> usize {
    assert!(data.len() >= 4, "Unable to pack u32 into buffer. Not enough space");

    data[0] = value as u8;
    data[1] = (value >> 8) as u8;
    data[2] = (value >> 16) as u8;
    data[3] = (value >> 24) as u8;

    4
}

#[test]
fn test_pack_unsigned_success() {
    let mut array: [u8; 4] = [0; 4];
    assert_eq!(4, pack_unsigned(&mut array, 42));
}

#[test]
#[should_panic]
fn test_pack_unsigned_fail() {
    let mut array: [u8; 3] = [0; 3];
    pack_unsigned(&mut array, 42);
}

pub fn pack_string(data: &mut [u8], value: &str) -> usize {
    assert!(data.len() >= value.len() + 4, "Unable to pack string into buffer. Not enough space");

    pack_array(data, value.as_bytes())
}

#[test]
fn test_pack_string_success() {
    let mut array: [u8; 8] = [0; 8];
    let test = "test";
    assert_eq!(4 + test.len(), pack_string(&mut array, &test));
}

#[test]
#[should_panic]
fn test_pack_string_fail() {
    let mut array: [u8; 7] = [0; 7];
    pack_string(&mut array, "test");
}

fn copy_array(dst: &mut [u8], src: &[u8]) {
    assert!(dst.len() >= src.len(), "Not enough space in destination array.");

    let mut pos = 0;
    for b in src {
        dst[pos] = *b;
        pos += 1;
    }
}

#[test]
fn test_copy_array_success() {
    let mut dst: [u8; 8] = [0; 8];
    let src: [u8; 8] = [0; 8];
    copy_array(&mut dst, &src);
}

#[test]
#[should_panic]
fn test_copy_array_fail() {
    let mut dst: [u8; 1] = [0; 1];
    let src: [u8; 8] = [0; 8];
    copy_array(&mut dst, &src);
}

pub fn pack_array(data: &mut [u8], value: &[u8]) -> usize {
    assert!(data.len() >= value.len() + 4, "Unable to pack binary into buffer. Not enough space");

    let array_len = pack_unsigned(data, value.len() as u32);

    copy_array(&mut data[4..], value);

    array_len + value.len()
}

#[test]
fn test_pack_array_success() {
    let mut array: [u8; 8] = [0; 8];
    let test: [u8; 4] = [1; 4];

    assert_eq!(4 + test.len(), pack_array(&mut array, &test));
}

#[test]
#[should_panic]
fn test_pack_array_fail() {
    let mut array: [u8; 7] = [0; 7];
    let test: [u8; 4] = [1; 4];

    pack_array(&mut array, &test);
}