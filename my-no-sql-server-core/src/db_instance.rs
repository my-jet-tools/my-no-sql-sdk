use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use my_no_sql_core::db::DbTableName;
use rust_extensions::sorted_vec::SortedVecOfArcWithStrKey;

use super::DbTable;

struct DbInstanceInner {
    sorted: SortedVecOfArcWithStrKey<DbTable>,
    as_vec: Arc<Vec<Arc<DbTable>>>,
    names: Arc<Vec<DbTableName>>,
}

impl DbInstanceInner {
    fn empty() -> Self {
        Self {
            sorted: SortedVecOfArcWithStrKey::new(),
            as_vec: Arc::new(Vec::new()),
            names: Arc::new(Vec::new()),
        }
    }

    fn from_sorted(sorted: SortedVecOfArcWithStrKey<DbTable>) -> Self {
        let as_vec: Vec<Arc<DbTable>> = sorted.iter().cloned().collect();
        let names: Vec<DbTableName> = as_vec.iter().map(|t| t.name.clone()).collect();
        Self {
            sorted,
            as_vec: Arc::new(as_vec),
            names: Arc::new(names),
        }
    }
}

pub struct DbInstance {
    inner: ArcSwap<DbInstanceInner>,
    write_lock: Mutex<()>,
}

impl DbInstance {
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::from_pointee(DbInstanceInner::empty()),
            write_lock: Mutex::new(()),
        }
    }

    pub fn get_table(&self, table_name: &str) -> Option<Arc<DbTable>> {
        self.inner.load().sorted.get(table_name).cloned()
    }

    pub fn get_tables(&self) -> Arc<Vec<Arc<DbTable>>> {
        self.inner.load().as_vec.clone()
    }

    pub fn get_table_names(&self) -> Arc<Vec<DbTableName>> {
        self.inner.load().names.clone()
    }

    pub fn insert(&self, table: Arc<DbTable>) {
        let _guard = self.write_lock.lock().unwrap();
        let current = self.inner.load_full();
        let mut new_sorted = current.sorted.clone();
        new_sorted.insert_or_replace(table);
        self.inner
            .store(Arc::new(DbInstanceInner::from_sorted(new_sorted)));
    }

    pub fn get_or_create<F: FnOnce() -> Arc<DbTable>>(
        &self,
        table_name: &str,
        factory: F,
    ) -> (Arc<DbTable>, bool) {
        if let Some(existing) = self.inner.load().sorted.get(table_name) {
            return (existing.clone(), false);
        }

        let _guard = self.write_lock.lock().unwrap();
        let current = self.inner.load_full();

        if let Some(existing) = current.sorted.get(table_name) {
            return (existing.clone(), false);
        }

        let table = factory();
        let mut new_sorted = current.sorted.clone();
        new_sorted.insert_or_replace(table.clone());
        self.inner
            .store(Arc::new(DbInstanceInner::from_sorted(new_sorted)));

        (table, true)
    }

    pub fn delete_table(&self, table_name: &str) -> Option<Arc<DbTable>> {
        let _guard = self.write_lock.lock().unwrap();
        let current = self.inner.load_full();
        let mut new_sorted = current.sorted.clone();
        let removed = new_sorted.remove(table_name)?;
        self.inner
            .store(Arc::new(DbInstanceInner::from_sorted(new_sorted)));
        Some(removed)
    }
}
