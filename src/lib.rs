#![allow(dead_code)]

extern crate skiplist;

pub mod database;
pub mod data_type;

mod storage;
mod protocol;
mod indexing;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
