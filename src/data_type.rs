use std::mem;

#[derive(Debug, Copy, Clone, Ord, Eq, PartialOrd, PartialEq)]
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
            DataType::VARCHAR => 8,
            DataType::VARBINARY => 8,
            DataType::BOOLEAN => 1,
            DataType::SMALLINT => 2,
            DataType::INTEGER => 4,
            DataType::BIGINT => 8,
            DataType::FLOAT => 8,
        }
    }
}
