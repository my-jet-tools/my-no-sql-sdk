# How to use MyNoSqlDataWriter

First - set up the settings model and settings reader. The crate https://github.com/MyJetTools/my-settings-reader is recommended.

#### Cargo.toml
```toml
[dependencies]
my-no-sql-sdk = { tag = "0.5.1", git = "https://github.com/my-jet-tools/my-no-sql-sdk.git", features = [
    "macros",
    "data-writer",
] }
my-settings-reader = { tag = "0.5.0", git = "https://github.com/MyJetTools/my-settings-reader.git", features = [
    "background-reader",
] }

async-trait = "*"
serde = { version = "*", features = ["derive"] }
tokio = { version = "*", features = ["full"] }
```

#### settings.rs
```rust
use my_no_sql_sdk::data_writer::MyNoSqlWriterSettings;
use serde::{Deserialize, Serialize};

#[derive(my_settings_reader::SettingsModel, Serialize, Deserialize, Debug, Clone)]
pub struct SettingsModel {

    #[serde(rename = "MyNoSqlWriterUrl")]
    pub my_no_sql_writer_url: String,
}

#[async_trait::async_trait]
impl MyNoSqlWriterSettings for SettingsReader {
    async fn get_url(&self) -> String {
        let read_access = self.settings.read().await;
        read_access.my_no_sql_writer_url.clone()
    }

    fn get_app_name(&self) -> &'static str {
        env!("CARGO_PKG_NAME")
    }

    fn get_app_version(&self) -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
}
```


Then MyNoSqlDataWriter can be created.


#### main.rs
```rust
use std::sync::Arc;

use my_no_sql_sdk::abstractions::DataSynchronizationPeriod;
use my_no_sql_sdk::data_writer::{CreateTableParams, MyNoSqlDataWriter};
use my_no_sql_sdk::macros::my_no_sql_entity;
use serde::{Deserialize, Serialize};

mod settings;
use settings::SettingsReader;

#[my_no_sql_entity("test")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestEntity {
    pub value: String,
}

#[tokio::main]
async fn main() {
    
    let settings_reader = SettingsReader::new(".my-settings").await;
    let settings_reader = Arc::new(settings_reader);
    
    let my_no_sql_writer: MyNoSqlDataWriter<TestEntity> = MyNoSqlDataWriter::new(
        settings_reader.clone(),
        CreateTableParams {
            persist: true,
            max_partitions_amount: None,
            max_rows_per_partition_amount: None,
        }.into(),
        DataSynchronizationPeriod::Sec5,
    );
}

```

How to use the writer — `with_retries`, reads, writes, deletes — is described in the [repository README](../README.md#writing-data).
