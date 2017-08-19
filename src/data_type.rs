use std::mem;

#[derive(Debug, Copy, Clone)]
pub enum DataType {
    VARCHAR,
    VARBINARY,
    BOOLEAN,
    SMALLINT,
    INTEGER,
    BIGINT,
    FLOAT,
}

impl DataType {
    /// Get length of the static part of the column.
    pub fn static_len(self) -> usize {
        match self {
            DataType::VARCHAR => mem::size_of::<usize>(),
            DataType::VARBINARY => mem::size_of::<usize>(),
            DataType::BOOLEAN => mem::size_of::<bool>(),
            DataType::SMALLINT => mem::size_of::<i16>(),
            DataType::INTEGER => mem::size_of::<i32>(),
            DataType::BIGINT => mem::size_of::<i64>(),
            DataType::FLOAT => mem::size_of::<f64>(),
        }
    }
}