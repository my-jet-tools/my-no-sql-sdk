use std::sync::Arc;

use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};
use my_no_sql_tcp_shared::sync_to_main::UpdateEntityStatisticsData;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use super::super::my_no_sql_data_reader_tcp::MyNoSqlDataReaderInner;

pub struct GetEntityBuilderInner<
    's,
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
> {
    partition_key: &'s str,
    row_key: &'s str,
    update_statistic_data: UpdateEntityStatisticsData,
    inner: Arc<MyNoSqlDataReaderInner<TMyNoSqlEntity>>,
}

impl<'s, TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static>
    GetEntityBuilderInner<'s, TMyNoSqlEntity>
{
    pub fn new(
        partition_key: &'s str,
        row_key: &'s str,
        inner: Arc<MyNoSqlDataReaderInner<TMyNoSqlEntity>>,
    ) -> Self {
        Self {
            partition_key,
            row_key,
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

    pub async fn execute(&self) -> Option<Arc<TMyNoSqlEntity>> {
        let result = {
            let mut reader = self.inner.get_data().lock();
            reader.get_entity(self.partition_key, self.row_key)
        };

        if result.is_some() {
            self.inner
                .get_sync_handler()
                .update(
                    TMyNoSqlEntity::TABLE_NAME,
                    self.partition_key,
                    || [self.row_key].into_iter(),
                    &self.update_statistic_data,
                );
        }

        result
    }
}
