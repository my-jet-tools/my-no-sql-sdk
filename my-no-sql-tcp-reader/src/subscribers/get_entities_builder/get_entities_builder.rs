use std::{collections::BTreeMap, sync::Arc};

use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};
use rust_extensions::date_time::DateTimeAsMicroseconds;

use super::{super::my_no_sql_data_reader_tcp::MyNoSqlDataReaderInner, GetEntitiesBuilderInner};

#[cfg(feature = "mocks")]
use super::GetEntitiesBuilderMock;

pub enum GetEntitiesBuilder<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
> {
    Inner(GetEntitiesBuilderInner<TMyNoSqlEntity>),
    #[cfg(feature = "mocks")]
    Mock(GetEntitiesBuilderMock<TMyNoSqlEntity>),
}

impl<TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static>
    GetEntitiesBuilder<TMyNoSqlEntity>
{
    pub fn new(partition_key: String, inner: Arc<MyNoSqlDataReaderInner<TMyNoSqlEntity>>) -> Self {
        Self::Inner(GetEntitiesBuilderInner::new(partition_key, inner))
    }
    #[cfg(feature = "mocks")]
    pub fn new_mock(
        partition_key: String,
        inner: Arc<crate::subscribers::MyNoSqlDataReaderMockInner<TMyNoSqlEntity>>,
    ) -> Self {
        Self::Mock(GetEntitiesBuilderMock::new(partition_key, inner))
    }

    pub fn set_partition_last_read_moment(mut self) -> Self {
        match &mut self {
            GetEntitiesBuilder::Inner(inner) => inner.set_partition_last_read_moment(),
            #[cfg(feature = "mocks")]
            GetEntitiesBuilder::Mock(inner) => inner.set_partition_last_read_moment(),
        }
        self
    }

    pub fn set_row_last_read_moment(mut self) -> Self {
        match &mut self {
            GetEntitiesBuilder::Inner(inner) => inner.set_row_last_read_moment(),
            #[cfg(feature = "mocks")]
            GetEntitiesBuilder::Mock(inner) => inner.set_row_last_read_moment(),
        }
        self
    }

    pub fn set_partition_expiration_moment(
        mut self,
        value: Option<DateTimeAsMicroseconds>,
    ) -> Self {
        match &mut self {
            GetEntitiesBuilder::Inner(inner) => inner.set_partition_expiration_moment(value),
            #[cfg(feature = "mocks")]
            GetEntitiesBuilder::Mock(inner) => inner.set_partition_expiration_moment(value),
        }
        self
    }

    pub fn set_row_expiration_moment(mut self, value: Option<DateTimeAsMicroseconds>) -> Self {
        match &mut self {
            GetEntitiesBuilder::Inner(inner) => inner.set_row_expiration_moment(value),
            #[cfg(feature = "mocks")]
            GetEntitiesBuilder::Mock(inner) => inner.set_row_expiration_moment(value),
        }
        self
    }

    pub async fn get_as_vec(&self) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        match &self {
            GetEntitiesBuilder::Inner(inner) => inner.get_as_vec().await,
            #[cfg(feature = "mocks")]
            GetEntitiesBuilder::Mock(inner) => inner.get_as_vec(),
        }
    }

    pub async fn get_as_vec_with_filter(
        &self,
        filter: impl Fn(&TMyNoSqlEntity) -> bool,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        match &self {
            GetEntitiesBuilder::Inner(inner) => inner.get_as_vec_with_filter(filter).await,
            #[cfg(feature = "mocks")]
            GetEntitiesBuilder::Mock(inner) => inner.get_as_vec_with_filter(filter),
        }
    }

    pub async fn get_as_btree_map(&self) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        match &self {
            GetEntitiesBuilder::Inner(inner) => inner.get_as_btree_map().await,
            #[cfg(feature = "mocks")]
            GetEntitiesBuilder::Mock(inner) => inner.get_as_btree_map(),
        }
    }

    pub async fn get_as_btree_map_with_filter(
        &self,
        filter: impl Fn(&TMyNoSqlEntity) -> bool,
    ) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        match &self {
            GetEntitiesBuilder::Inner(inner) => inner.get_as_btree_map_with_filter(filter).await,
            #[cfg(feature = "mocks")]
            GetEntitiesBuilder::Mock(inner) => inner.get_as_btree_map_with_filter(filter),
        }
    }
}
