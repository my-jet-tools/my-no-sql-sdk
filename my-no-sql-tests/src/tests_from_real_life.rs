use my_no_sql_macros::my_no_sql_entity;
use serde::*;

#[my_no_sql_entity("payout-withdrawal-settings")]
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct PayoutWithdrawalSettingsMyNoSqlEntity {
    pub value: f64,
    pub currency: String,
}

#[cfg(test)]
mod tests {

    use my_no_sql_sdk::{
        abstractions::MyNoSqlEntitySerializer, core::rust_extensions::date_time::DateTimeStruct,
    };

    use super::PayoutWithdrawalSettingsMyNoSqlEntity;

    #[test]
    fn test() {
        let src = "{\"PartitionKey\":\"bank-transfer\",\"RowKey\":\"max\",\"TimeStamp\":\"2024-11-29T14:59:15.6145\",\"Value\":15000.0,\"Currency\":\"USD\"}";

        let entity =
            PayoutWithdrawalSettingsMyNoSqlEntity::deserialize_entity(src.as_bytes()).unwrap();

        assert_eq!(entity.value, 15000.0);
        assert_eq!(entity.currency, "USD");
        assert_eq!(entity.partition_key, "bank-transfer");
        assert_eq!(entity.row_key, "max");

        let dt_struct: DateTimeStruct = entity.time_stamp.to_date_time().into();

        assert_eq!(dt_struct.year, 2024);
        assert_eq!(dt_struct.month, 11);
        assert_eq!(dt_struct.day, 29);

        assert_eq!(dt_struct.time.hour, 14);
        assert_eq!(dt_struct.time.min, 59);
        assert_eq!(dt_struct.time.sec, 15);
        assert_eq!(dt_struct.time.micros, 614500);
    }
}
