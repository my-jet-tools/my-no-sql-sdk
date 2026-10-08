use super::WriterFlUrl;
use my_no_sql_abstractions::DataSynchronizationPeriod;

pub trait FlUrlExt {
    fn with_table_name_as_query_param(self, table_name: &str) -> Self;
    fn append_data_sync_period(self, sync_period: &DataSynchronizationPeriod) -> Self;
    fn with_partition_key_as_query_param(self, partition_key: &str) -> Self;
    fn with_partition_keys_as_query_param(self, partition_keys: &[&str]) -> Self;
    fn with_row_key_as_query_param(self, partition_key: &str) -> Self;

    fn with_skip_as_query_param(self, skip: Option<i32>) -> Self;
    fn with_limit_as_query_param(self, limit: Option<i32>) -> Self;
}

impl FlUrlExt for WriterFlUrl {
    fn with_table_name_as_query_param(self, table_name: &str) -> Self {
        self.append_query_param("tableName", Some(table_name))
    }

    fn append_data_sync_period(self, sync_period: &DataSynchronizationPeriod) -> Self {
        let value = match sync_period {
            DataSynchronizationPeriod::Immediately => "i",
            DataSynchronizationPeriod::Sec1 => "1",
            DataSynchronizationPeriod::Sec5 => "5",
            DataSynchronizationPeriod::Sec15 => "15",
            DataSynchronizationPeriod::Sec30 => "30",
            DataSynchronizationPeriod::Min1 => "60",
            DataSynchronizationPeriod::Asap => "a",
        };

        self.append_query_param("syncPeriod", Some(value))
    }

    fn with_partition_key_as_query_param(self, partition_key: &str) -> Self {
        self.append_query_param("partitionKey", Some(partition_key))
    }

    /// The list of `DELETE /api/Rows/DeletePartitions`: the parameter is `partitionKeys` and it
    /// is repeated once per key - the server takes every pair of that name as one element. A
    /// single `partitionKey` (the name of the one-partition parameter) is not read at all there.
    fn with_partition_keys_as_query_param(self, partition_keys: &[&str]) -> Self {
        let mut s = self;
        for partition_key in partition_keys {
            s = s.append_query_param("partitionKeys", Some(*partition_key));
        }
        s
    }

    fn with_row_key_as_query_param(self, row_key: &str) -> Self {
        self.append_query_param("rowKey", Some(row_key))
    }

    fn with_skip_as_query_param(self, skip: Option<i32>) -> Self {
        if let Some(skip) = skip {
            self.append_query_param("skip", Some(skip.to_string()))
        } else {
            self
        }
    }

    fn with_limit_as_query_param(self, limit: Option<i32>) -> Self {
        if let Some(limit) = limit {
            self.append_query_param("limit", Some(limit.to_string()))
        } else {
            self
        }
    }
}
