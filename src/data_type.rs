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
            DataType::VARCHAR => unimplemented!(), //16?
            DataType::VARBINARY => unimplemented!(), // 16?
            DataType::BOOLEAN => 1,
            DataType::SMALLINT => 2,
            DataType::INTEGER => 4,
            DataType::BIGINT => 8,
            DataType::FLOAT => 8,
        }
    }
}