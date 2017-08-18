use std::collections::BTreeMap;
use std::sync::Arc;
use data_type::DataType;

#[derive(Debug)]
pub struct Column {
    name: String,
    data_type: DataType,
    system: bool,
}

impl Column {
    fn new(name: &str, data_type: DataType, system: bool) -> Column {
        Column {
            name: name.to_owned(),
            data_type: data_type,
            system: system,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableConfiguration {
    name: String,
    columns: BTreeMap<String, Arc<Column>>,
}

impl TableConfiguration {
    /// Creates new TableConfiguration.
    pub fn new(name: &str) -> TableConfiguration {
        TableConfiguration {
            name: name.to_owned(),
            columns: BTreeMap::new(),
        }
    }

    // Adds new column to the table.
    pub fn add_column(&mut self, column: Column) -> Result<(), String> {
        let name = column.name.clone();

        if self.columns.contains_key(&name) {
            return Err(format!("Column with the name '{}' already exists in table '{}'",
                               name,
                               self.name));
        }

        self.columns.insert(name.clone(), Arc::new(column));
        Ok(())
    }
}

#[derive(Debug)]
pub struct Table {
    name: String,
    columns: BTreeMap<String, Arc<Column>>,
}

impl Table {
    /// Creates new table.
    fn new(cfg: TableConfiguration) -> Table {
        let mut table = Table {
            name: cfg.name,
            columns: cfg.columns,
        };

        table.add_system_columns();

        return table;
    }

    /// Adds system columns to the new table.
    fn add_system_columns(&mut self) {
        self.add_column(Column::new("_flags", DataType::INTEGER, true));
    }

    /// Adds new column to the table.
    fn add_column(&mut self, column: Column) {
        let name = column.name.clone();
        let prev = self.columns.insert(name.clone(), Arc::new(column));

        assert!(prev.is_none(),
                format!("Column with the name '{}' already exists in table '{}'",
                        name,
                        self.name));
    }
}

#[derive(Debug)]
pub struct Database {
    tables: BTreeMap<String, Arc<Table>>,
}

impl Database {
    /// Creates new Database.
    pub fn new() -> Database {
        Database { tables: BTreeMap::new() }
    }

    /// Creates new table in Database using provided configuration.
    pub fn create_table(&mut self, cfg: TableConfiguration) -> Arc<Table> {
        let name = cfg.name.clone();

        let table = Table::new(cfg);
        self.tables.insert(name.clone(), Arc::new(table));

        return self.tables.get(&name).unwrap().clone();
    }
}

#[test]
fn create_empty_table() {
    let mut Database = Database::new();

    let cfg = TableConfiguration::new("Table1");

    let table = Database.create_table(cfg);

    assert!(table.name == "Table1");
}

#[test]
fn create_table_with_columns() {
    let mut Database = Database::new();

    let mut cfg = TableConfiguration::new("SomeTable");
    cfg.add_column(Column::new("foo", DataType::INTEGER, false)).expect("should not fail");
    cfg.add_column(Column::new("bar", DataType::BOOLEAN, false)).expect("should not fail");
    cfg.add_column(Column::new("baz", DataType::VARCHAR, false)).expect("should not fail");

    let table = Database.create_table(cfg);

    assert!(table.name == "SomeTable");
}