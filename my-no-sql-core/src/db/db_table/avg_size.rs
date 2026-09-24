use crate::db::DbRow;

pub struct AvgSize {
    pub total_size: usize,
    pub count: usize,
}

impl AvgSize {
    pub fn new() -> Self {
        Self {
            total_size: 0,
            count: 0,
        }
    }

    pub fn add(&mut self, db_row: &DbRow) {
        self.total_size += db_row.get_content_size();
        self.count += 1;
    }

    pub fn get(&self) -> usize {
        if self.count == 0 {
            return 0;
        }

        self.total_size / self.count
    }
}
