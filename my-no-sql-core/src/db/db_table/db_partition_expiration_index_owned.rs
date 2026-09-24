use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{db::PartitionKey, ExpirationIndex};

#[derive(Clone)]
pub struct DbPartitionExpirationIndexOwned {
    pub partition_key: PartitionKey,
    pub expires: Option<DateTimeAsMicroseconds>,
}

impl ExpirationIndex<DbPartitionExpirationIndexOwned> for DbPartitionExpirationIndexOwned {
    fn get_id_as_str(&self) -> &str {
        self.partition_key.as_str()
    }

    fn to_owned(&self) -> DbPartitionExpirationIndexOwned {
        self.clone()
    }
    fn get_expiration_moment(&self) -> Option<DateTimeAsMicroseconds> {
        self.expires
    }
}
