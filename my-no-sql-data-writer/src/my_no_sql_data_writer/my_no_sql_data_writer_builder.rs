use std::{marker::PhantomData, sync::Arc};

use my_no_sql_abstractions::{DataSynchronizationPeriod, MyNoSqlEntity, MyNoSqlEntitySerializer};

use crate::{CreateTableParams, MyNoSqlDataWriter, MyNoSqlWriterSettings};

pub struct MyNoSqlDataWriterBuilder<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>
{
    settings: Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static>,
    phantom: PhantomData<TEntity>,
    sync_period: DataSynchronizationPeriod,
    create_table_params: Option<CreateTableParams>,
    use_h1: bool,
}

impl<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>
    MyNoSqlDataWriterBuilder<TEntity>
{
    pub fn new(settings: Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static>) -> Self {
        Self {
            settings,
            phantom: Default::default(),
            sync_period: DataSynchronizationPeriod::Sec5,
            create_table_params: CreateTableParams {
                persist: true,
                max_partitions_amount: None,
                max_rows_per_partition_amount: None,
            }
            .into(),
            use_h1: false,
        }
    }

    /// Falls back to HTTP/1.1 - for MyNoSqlServer instances which do not support HTTP/2.
    pub fn use_h1(mut self) -> Self {
        self.use_h1 = true;
        self
    }
    pub fn set_sync_period(mut self, sync_period: DataSynchronizationPeriod) -> Self {
        self.sync_period = sync_period;
        self
    }

    pub fn persist_table(mut self, value: bool) -> Self {
        if let Some(params) = self.create_table_params.as_mut() {
            params.persist = value;
        }
        self
    }

    pub fn set_max_partitions_amount(mut self, value: usize) -> Self {
        if let Some(params) = self.create_table_params.as_mut() {
            params.max_partitions_amount = Some(value);
        }
        self
    }

    pub fn set_max_row_per_partitions_amount(mut self, value: usize) -> Self {
        if let Some(params) = self.create_table_params.as_mut() {
            params.max_rows_per_partition_amount = Some(value);
        }
        self
    }

    pub fn do_not_auto_create_table(mut self) -> Self {
        self.create_table_params = None;
        self
    }

    pub fn build(self) -> MyNoSqlDataWriter<TEntity> {
        let mut result =
            MyNoSqlDataWriter::new(self.settings, self.create_table_params, self.sync_period);

        if self.use_h1 {
            result.use_h1();
        }

        result
    }
}
