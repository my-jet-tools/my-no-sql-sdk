use super::PartitionKey;

pub trait PartitionKeyParameter {
    fn as_str(&self) -> &str;
    fn into_partition_key(self) -> PartitionKey;

    fn to_partition_key(&self) -> PartitionKey;
}

impl PartitionKeyParameter for String {
    fn as_str(&self) -> &str {
        self.as_str()
    }

    fn into_partition_key(self) -> PartitionKey {
        PartitionKey::new(self)
    }
    fn to_partition_key(&self) -> PartitionKey {
        PartitionKey::new(self.clone())
    }
}
