use std::{collections::BTreeMap, sync::Arc};

use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};
use my_no_sql_tcp_shared::sync_to_main::UpdateEntityStatisticsData;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::subscribers::MyNoSqlDataReaderMockInner;

pub struct GetEntitiesBuilderMock<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
> {
    partition_key: String,
    update_statistic_data: UpdateEntityStatisticsData,
    inner: Arc<MyNoSqlDataReaderMockInner<TMyNoSqlEntity>>,
}

impl<TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static>
    GetEntitiesBuilderMock<TMyNoSqlEntity>
{
    pub fn new(
        partition_key: String,
        inner: Arc<MyNoSqlDataReaderMockInner<TMyNoSqlEntity>>,
    ) -> Self {
        Self {
            partition_key,
            update_statistic_data: UpdateEntityStatisticsData::default(),
            inner,
        }
    }

    pub fn set_partition_last_read_moment(&mut self) {
        self.update_statistic_data.partition_last_read_moment = true;
    }

    pub fn set_row_last_read_moment(&mut self) {
        self.update_statistic_data.row_last_read_moment = true;
    }

    pub fn set_partition_expiration_moment(&mut self, value: Option<DateTimeAsMicroseconds>) {
        self.update_statistic_data.partition_expiration_moment = Some(value);
    }

    pub fn set_row_expiration_moment(&mut self, value: Option<DateTimeAsMicroseconds>) {
        self.update_statistic_data.row_expiration_moment = Some(value);
    }

    pub  fn get_as_vec(&self) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        self.inner
            .get_by_partition_key_as_vec(&self.partition_key)
    }

    pub  fn get_as_vec_with_filter(
        &self,
        filter: impl Fn(&TMyNoSqlEntity) -> bool,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        self.inner
            .get_by_partition_key_as_vec_with_filter(&self.partition_key, filter)
    }

    pub  fn get_as_btree_map(&self) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        let items = self
            .inner
            .get_by_partition_key_as_vec(self.partition_key.as_str())?;

        let mut result = BTreeMap::new();

        for item in items {
            result.insert(item.get_row_key().to_string(), item);
        }

        Some(result)
    }

    pub  fn get_as_btree_map_with_filter(
        &self,
        filter: impl Fn(&TMyNoSqlEntity) -> bool,
    ) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        let items = self
            .inner
            .get_by_partition_key_as_vec_with_filter(self.partition_key.as_str(), filter)?;

        let mut result = BTreeMap::new();

        for item in items {
            result.insert(item.get_row_key().to_string(), item);
        }

        Some(result)
    }
}
