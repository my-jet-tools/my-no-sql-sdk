use my_json::json_reader::JsonFirstLineIterator;

use crate::db::DbRow;

use super::{DbEntityParseFail, DbJsonEntity, JsonTimeStamp};

pub struct DbJsonEntityWithContent<'s> {
    entity: DbJsonEntity,
    raw: &'s [u8],
    time_stamp: &'s JsonTimeStamp,
}

impl<'s> DbJsonEntityWithContent<'s> {
    pub fn new(raw: &'s [u8], time_stamp: &'s JsonTimeStamp, entity: DbJsonEntity) -> Self {
        Self {
            entity,
            raw,
            time_stamp,
        }
    }

    pub fn get_partition_key(&self) -> &str {
        self.entity.get_partition_key(self.raw)
    }

    pub fn get_row_key(&self) -> &str {
        self.entity.get_row_key(self.raw)
    }

    pub fn get_expires(&self) -> Option<&str> {
        self.entity.get_expires(self.raw)
    }

    pub fn get_time_stamp(&self) -> Option<&str> {
        self.entity.get_time_stamp(self.raw)
    }

    pub fn into_db_row(self) -> Result<DbRow, DbEntityParseFail> {
        let first_line_reader = JsonFirstLineIterator::new(self.raw);
        DbJsonEntity::parse_into_db_row(first_line_reader, &self.time_stamp)
    }
}
