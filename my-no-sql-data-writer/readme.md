# How to use MyNoSqlDataWriter

First - setup the settings model and settings reader. Create https://github.com/MyJetTools/my-settings-reader is recommended.

#### Cargo.toml
```yaml
[dependencies]
my-settings-reader = { tag = "xxx", git = "https://github.com/MyJetTools/my-settings-reader.git", features = [
    "background-reader",
] }

async-trait = "*"
flurl = { tag = "xxx", git = "https://github.com/MyJetTools/fl-url.git" }
serde = { version = "*", features = ["derive"] }
serde_json = "*"
tokio = { version = "*", features = ["full"] }
```

#### settings.rs
```rust
use my_no_sql_data_writer::MyNoSqlWriterSettings;
use serde::{Deserialize, Serialize};

#[derive(my_settings_reader::SettingsModel, Serialize, Deserialize, Debug, Clone)]
pub struct SettingsModel {

    #[serde(rename = "MyNoSqlDataWriterUrl")]
    pub my_no_sql_data_writer_url: String,
}

#[async_trait::async_trait]
impl MyNoSqlWriterSettings for SettingsReader {
    async fn get_url(&self) -> String {
        let read_access = self.settings.read().await;
        read_access.my_no_sql_data_writer_url.clone()
    }
}
```


Then MyNoSqlDataWriter can be created.


#### main.rs
```rust

#[tokio::main]
async fn main() {
    
    let settings_reader = SettingsReader::new(".my-settings").await;
    let settings_reader = Arc::new(settings_reader);
    
    let my_no_sql_writer: MyNoSqlDataWriter<TMyNoSqlEntity> = MyNoSqlDataWriter::new(
        settings_reader.clone(),
        CreateTableParams {
            persist: true,
            max_partitions_amount: None,
            max_rows_per_partition_amount: None,
        }.into(),
        my_no_sql_abstractions::DataSynchronizationPeriod::Sec5,
    );
}

```
