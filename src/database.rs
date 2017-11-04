use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use data_type::DataType;
use data_type::Data;
use storage::Storage;

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
    storage: Storage,
}

impl Table {
    /// Creates new table.
    fn new(cfg: TableConfiguration) -> Table {
        let mut table = Table {
            name: cfg.name,
            columns: cfg.columns,
            storage: Storage::new(),
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

    /// Insert row into table.
    fn insert_row(&mut self, row: Vec<Data>) -> Result<(), String> {
        self.storage.insert(row);

        Ok(())
    }
}

#[derive(Debug)]
pub struct Database {
    tables: BTreeMap<String, Arc<Mutex<Table>>>,
}

impl Database {
    /// Creates new Database.
    pub fn new() -> Database {
        Database { tables: BTreeMap::new() }
    }

    /// Creates new table in Database using provided configuration.
    pub fn create_table(&mut self, cfg: TableConfiguration) -> Arc<Mutex<Table>> {
        let name = cfg.name.clone();

        let table = Table::new(cfg);
        self.tables.insert(name.clone(), Arc::new(Mutex::new(table)));

        return self.tables.get(&name).unwrap().clone();
    }

    /// Get table.
    pub fn get_table(&self, name: &str) -> Option<&Arc<Mutex<Table>>> {
        self.tables.get(name)
    }
}

#[test]
fn create_empty_table() {
    let mut database = Database::new();

    let cfg = TableConfiguration::new("Table1");

    let table = database.create_table(cfg);

    assert!(table.lock().unwrap().name == "Table1");
}

#[test]
fn create_table_with_columns() {
    let mut database = Database::new();

    let mut cfg = TableConfiguration::new("SomeTable");
    cfg.add_column(Column::new("foo", DataType::INTEGER, false)).expect("should not fail");
    cfg.add_column(Column::new("bar", DataType::BOOLEAN, false)).expect("should not fail");
    cfg.add_column(Column::new("baz", DataType::VARCHAR, false)).expect("should not fail");

    let table = database.create_table(cfg);

    assert!(table.lock().unwrap().name == "SomeTable");
}

#[test]
fn insert_single_row() {
    let table_name = "TestTable";

    let mut database = Database::new();

    let mut cfg = TableConfiguration::new(table_name);
    cfg.add_column(Column::new("c1", DataType::INTEGER, false)).expect("should not fail");
    cfg.add_column(Column::new("c2", DataType::VARCHAR, false)).expect("should not fail");
    cfg.add_column(Column::new("c3", DataType::BOOLEAN, false)).expect("should not fail");

    database.create_table(cfg);

    let table = database.get_table(table_name).expect("should return table");

    let row = vec![Data::INTEGER(6), Data::EMPTY, Data::BOOLEAN(true)];

    table.lock().unwrap().insert_row(row).expect("should not fail");
}
