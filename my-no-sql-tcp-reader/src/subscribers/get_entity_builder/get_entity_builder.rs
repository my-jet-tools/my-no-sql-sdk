use std::sync::Arc;

use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};
use rust_extensions::date_time::DateTimeAsMicroseconds;

#[cfg(feature = "mocks")]
use super::GetEntityBuilderMock;
use super::{super::my_no_sql_data_reader_tcp::MyNoSqlDataReaderInner, GetEntityBuilderInner};
pub enum GetEntityBuilder<
    's,
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
> {
    Inner(GetEntityBuilderInner<'s, TMyNoSqlEntity>),
    #[cfg(feature = "mocks")]
    Mock(GetEntityBuilderMock<'s, TMyNoSqlEntity>),
}

impl<'s, TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static>
    GetEntityBuilder<'s, TMyNoSqlEntity>
{
    pub fn new(
        partition_key: &'s str,
        row_key: &'s str,
        inner: Arc<MyNoSqlDataReaderInner<TMyNoSqlEntity>>,
    ) -> Self {
        Self::Inner(GetEntityBuilderInner::new(partition_key, row_key, inner))
    }
    #[cfg(feature = "mocks")]
    pub fn new_mock(
        partition_key: &'s str,
        row_key: &'s str,
        inner: Arc<crate::subscribers::MyNoSqlDataReaderMockInner<TMyNoSqlEntity>>,
    ) -> Self {
        Self::Mock(GetEntityBuilderMock::new(partition_key, row_key, inner))
    }

    pub fn set_partition_last_read_moment(mut self) -> Self {
        match &mut self {
            GetEntityBuilder::Inner(inner) => inner.set_partition_last_read_moment(),
            #[cfg(feature = "mocks")]
            GetEntityBuilder::Mock(_) => {}
        }

        self
    }

    pub fn set_row_last_read_moment(mut self) -> Self {
        match &mut self {
            GetEntityBuilder::Inner(inner) => inner.set_row_last_read_moment(),
            #[cfg(feature = "mocks")]
            GetEntityBuilder::Mock(_) => {}
        }

        self
    }

    pub fn set_partition_expiration_moment(
        mut self,
        value: Option<DateTimeAsMicroseconds>,
    ) -> Self {
        match &mut self {
            GetEntityBuilder::Inner(inner) => inner.set_partition_expiration_moment(value),
            #[cfg(feature = "mocks")]
            GetEntityBuilder::Mock(_) => {}
        }
        self
    }

    pub fn set_row_expiration_moment(mut self, value: Option<DateTimeAsMicroseconds>) -> Self {
        match &mut self {
            GetEntityBuilder::Inner(inner) => inner.set_row_expiration_moment(value),
            #[cfg(feature = "mocks")]
            GetEntityBuilder::Mock(_) => {}
        }

        self
    }

    pub async fn execute(&self) -> Option<Arc<TMyNoSqlEntity>> {
        match self {
            GetEntityBuilder::Inner(inner) => inner.execute().await,
            #[cfg(feature = "mocks")]
            GetEntityBuilder::Mock(inner) => inner.execute().await,
        }
    }
}
