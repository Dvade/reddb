use std::mem::transmute;
use std::str;

pub fn unpack_int(data: &[u8]) -> i32 {
    assert!(data.len() >= 4, "Unable to unpack i32 from provided buffer. Not enough space, available {}", data.len());

    (data[0] as i32 & 0xFFi32) |
    (data[1] as i32 & 0xFFi32) << 8 |
    (data[2] as i32 & 0xFFi32) << 16 |
    (data[3] as i32 & 0xFFi32) << 24
}

#[test]
fn test_unpack_int_success() {
    let array: [u8; 4] = [0x11, 0x22, 0x33, 0x44];
    let res: i32 = unpack_int(&array);
    assert_eq!(res, 1144201745);
}

#[test]
#[should_panic]
fn test_unpack_int_fail() {
    let array: [u8; 3] = [0; 3];
    unpack_int(&array);
}

pub fn unpack_smallint(data: &[u8]) -> i16 {
    assert!(data.len() >= 2, "Unable to unpack i16 from provided buffer. Not enough space");

    (data[0] as i16 & 0xFFi16) |
    (data[1] as i16 & 0xFFi16) << 8
}

#[test]
fn test_unpack_smallint_success() {
    let array: [u8; 2] = [0x11, 0x22];
    let res: i16 = unpack_smallint(&array);
    assert_eq!(res, 8721);
}

#[test]
#[should_panic]
fn test_unpack_smallint_fail() {
    let array: [u8; 1] = [0; 1];
    unpack_smallint(&array);
}

pub fn unpack_bigint(data: &[u8]) -> i64 {
    assert!(data.len() >= 8, "Unable to unpack i16 from provided buffer. Not enough space");

    (data[0] as i64 & 0xFFi64) |
    (data[1] as i64 & 0xFFi64) << 8 |
    (data[2] as i64 & 0xFFi64) << 16 |
    (data[3] as i64 & 0xFFi64) << 24 |
    (data[4] as i64 & 0xFFi64) << 32 |
    (data[5] as i64 & 0xFFi64) << 40 |
    (data[6] as i64 & 0xFFi64) << 48 |
    (data[7] as i64 & 0xFFi64) << 56
}

#[test]
fn test_unpack_bigint_success() {
    let array: [u8; 8] = [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
    let res: i64 = unpack_bigint(&array);
    assert_eq!(res, -8613303245920329199);
}

#[test]
#[should_panic]
fn test_unpack_bigint_fail() {
    let array: [u8; 7] = [0; 7];
    unpack_bigint(&array);
}

pub fn unpack_float(data: &[u8]) -> f64 {
    assert!(data.len() >= 8, "Unable to unpack f64 from provided buffer. Not enough space");

    let tmp: i64 = 
        (data[0] as i64 & 0xFFi64) |
        (data[1] as i64 & 0xFFi64) << 8 |
        (data[2] as i64 & 0xFFi64) << 16 |
        (data[3] as i64 & 0xFFi64) << 24 |
        (data[4] as i64 & 0xFFi64) << 32 |
        (data[5] as i64 & 0xFFi64) << 40 |
        (data[6] as i64 & 0xFFi64) << 48 |
        (data[7] as i64 & 0xFFi64) << 56;
    
    let transumuted = unsafe {
        transmute::<i64, f64>(tmp)
    };

    transumuted
}

#[test]
fn test_unpack_float_success() {
    let array: [u8; 8] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xE0, 0x3F];
    let res: f64 = unpack_float(&array);
    assert_eq!(res, 0.5);
}

#[test]
#[should_panic]
fn test_unpack_float_fail() {
    let array: [u8; 7] = [0; 7];
    unpack_float(&array);
}

pub fn unpack_bool(data: &[u8]) -> bool {
    assert!(data.len() >= 1, "Unable to unpack bool from provided buffer. Not enough space");

    data[0] != 0
}

#[test]
fn test_unpack_bool_success() {
    let array: [u8; 1] = [0x01];
    let res: bool = unpack_bool(&array);
    assert_eq!(res, true);
}

#[test]
#[should_panic]
fn test_unpack_bool_fail() {
    let array: [u8; 0] = [0; 0];
    unpack_bool(&array);
}

pub fn unpack_string(data: &[u8]) -> &str {
    assert!(data.len() >= 4, "Unable to unpack string length from provided buffer. Not enough space");

    let len = unpack_int(data) as usize;

    assert!(data.len() - 4 >= len as usize, "Unable to unpack string(with length: {}) from provided buffer. Not enough space", len);

    let result = match str::from_utf8(&data[4..4 + len]) {
        Ok(v) => v,
        Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
    };

    result
}

#[test]
fn test_unpack_string_success() {
    let array: [u8; 8] = [4, 0, 0, 0, 240, 159, 146, 150];
    let res = unpack_string(&array);
    assert_eq!(res, "💖");
}

#[test]
#[should_panic]
fn test_unpack_string_fail_on_length() {
    let array: [u8; 3] = [0; 3];
    unpack_string(&array);
}

#[test]
#[should_panic]
fn test_unpack_string_fail_on_string() {
    let array: [u8; 7] = [4, 0, 0, 0, 0, 0, 0];
    unpack_string(&array);
}

pub fn unpack_array(data: &[u8]) -> &[u8] {
    assert!(data.len() >= 4, "Unable to unpack array size from provided buffer. Not enough space");

    let len = unpack_int(data) as usize;

    assert!(data.len() - 4 >= len as usize, "Unable to unpack array(with size: {}) from provided buffer. Not enough space", len);

    &data[4..4 + len]
}

#[test]
fn test_unpack_array_success() {
    let array: [u8; 8] = [4, 0, 0, 0, 11, 22, 33, 44];
    let res: &[u8] = unpack_array(&array);

    assert_eq!(&array[4..], res, "rest len = {}", res.len())
}

#[test]
#[should_panic]
fn test_unpack_array_fail_on_size() {
    let array: [u8; 3] = [0, 0, 0];

    unpack_array(&array);
}

#[test]
#[should_panic]
fn test_unpack_array_fail_on_array() {
    let array: [u8; 7] = [4, 0, 0, 0, 0, 0, 0];

    unpack_array(&array);
}