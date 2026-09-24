use std::sync::Arc;

use rust_extensions::sorted_vec::EntityWithStrKey;

use super::PartitionKeyParameter;

#[derive(Clone)]
pub struct PartitionKey(Arc<String>);

impl PartitionKey {
    pub fn new(partition_key: String) -> Self {
        Self(Arc::new(partition_key))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn as_ref_of_string(&self) -> &String {
        &self.0
    }

    pub fn to_string(&self) -> String {
        self.0.to_string()
    }

    pub fn to_arc_of_string(&self) -> Arc<String> {
        self.0.clone()
    }
}

impl EntityWithStrKey for PartitionKey {
    fn get_key(&self) -> &str {
        &self.0
    }
}
/*

impl ExpirationItem for PartitionKey {
    fn get_id(&self) -> &str {
        self.0.as_str()
    }
}
 */

impl<'s> Into<PartitionKey> for &'s str {
    fn into(self) -> PartitionKey {
        PartitionKey::new(self.to_string())
    }
}

impl Into<PartitionKey> for String {
    fn into(self) -> PartitionKey {
        PartitionKey::new(self)
    }
}

impl Into<PartitionKey> for Arc<String> {
    fn into(self) -> PartitionKey {
        PartitionKey(self)
    }
}

impl PartitionKeyParameter for PartitionKey {
    fn as_str(&self) -> &str {
        self.as_str()
    }

    fn into_partition_key(self) -> PartitionKey {
        self
    }

    fn to_partition_key(&self) -> PartitionKey {
        self.clone()
    }
}
