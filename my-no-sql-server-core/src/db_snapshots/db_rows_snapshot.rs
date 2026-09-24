use std::sync::Arc;

use my_no_sql_core::db::DbRow;
use my_no_sql_core::my_json::json_writer::JsonArrayWriter;

pub struct DbRowsSnapshot {
    pub db_rows: Vec<Arc<DbRow>>,
}

impl DbRowsSnapshot {
    pub fn new() -> Self {
        Self {
            db_rows: Vec::new(),
        }
    }

    pub fn new_from_snapshot(db_rows: Vec<Arc<DbRow>>) -> Self {
        Self { db_rows }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            db_rows: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, db_row: Arc<DbRow>) {
        self.db_rows.push(db_row);
    }

    pub fn len(&self) -> usize {
        self.db_rows.len()
    }

    pub fn as_json_array(&self) -> JsonArrayWriter {
        let mut json_array_writer = JsonArrayWriter::new();
        for db_row in &self.db_rows {
            json_array_writer = json_array_writer.write(db_row.as_ref());
        }

        json_array_writer
    }
}
