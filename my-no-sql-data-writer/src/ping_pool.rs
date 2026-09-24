use std::{collections::HashMap, sync::Arc, time::Duration};

use flurl::body::HttpRequestBody;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::{FlUrlFactory, MyNoSqlWriterSettings};

#[derive(Clone)]
pub struct PingTableItem {
    pub table: String,
    pub settings: Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static>,
    pub use_h1: bool,
}

pub struct PingDataItem {
    pub name: &'static str,
    pub version: &'static str,

    pub table_settings: Vec<PingTableItem>,
}

pub struct PingPoolInner {
    items: Vec<PingDataItem>,
    started: bool,
}

impl PingPoolInner {
    pub fn new() -> Self {
        Self {
            items: Vec::new(),
            started: false,
        }
    }
}

pub struct PingPool {
    data: Mutex<PingPoolInner>,
}

impl PingPool {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(PingPoolInner::new()),
        }
    }

    pub fn register(
        &self,

        settings: Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static>,
        table: &str,
    ) {
        let mut data = self.data.lock();
        if !data.started {
            tokio::spawn(async move { ping_loop().await });
            data.started = true;
        }

        let index = data.items.iter().position(|x| {
            x.name == settings.get_app_name() && x.version == settings.get_app_version()
        });

        let table_item = PingTableItem {
            table: table.to_string(),
            settings: settings.clone(),
            use_h1: false,
        };

        if let Some(index) = index {
            let item = &mut data.items[index];
            item.table_settings.push(table_item);
        } else {
            let item = PingDataItem {
                name: settings.get_app_name(),
                version: settings.get_app_version(),

                table_settings: vec![table_item],
            };

            data.items.push(item);
        }
    }

    /// Pings the endpoint of the given table over HTTP/1.1 - to stay in sync with
    /// the writer which was switched to h1.
    pub fn use_h1(
        &self,
        settings: &Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static>,
        table: &str,
    ) {
        let mut data = self.data.lock();

        for item in data.items.iter_mut() {
            if item.name != settings.get_app_name() || item.version != settings.get_app_version() {
                continue;
            }

            for table_item in item.table_settings.iter_mut() {
                if table_item.table == table {
                    table_item.use_h1 = true;
                }
            }
        }
    }
}

struct PingSnapshotItem {
    name: &'static str,
    version: &'static str,
    table_settings: Vec<PingTableItem>,
}

async fn ping_loop() {
    let delay = Duration::from_secs(30);
    loop {
        tokio::time::sleep(delay).await;

        let snapshot: Vec<PingSnapshotItem> = {
            let access = crate::PING_POOL.data.lock();
            access
                .items
                .iter()
                .map(|itm| PingSnapshotItem {
                    name: itm.name,
                    version: itm.version,
                    table_settings: itm.table_settings.clone(),
                })
                .collect()
        };

        for itm in snapshot {
            let mut url_to_ping = HashMap::new();
            for table_item in itm.table_settings.iter() {
                let connection_string = table_item.settings.get_url().await;

                // Ping is grouped by the endpoint it is sent to - and the namespace is a part
                // of that endpoint identity, since the request carries it as a header.
                let connection_string =
                    match my_no_sql_abstractions::parse_connection_string(connection_string.as_str())
                    {
                        Ok(result) => result,
                        Err(err) => {
                            println!(
                                "{}:{} ping error: invalid connection string. {}",
                                itm.name, itm.version, err
                            );
                            continue;
                        }
                    };

                let entry = url_to_ping
                    .entry((connection_string.host, connection_string.namespace))
                    .or_insert_with(|| (table_item.settings.clone(), Vec::new(), false));
                entry.1.push(table_item.table.to_string());
                // The endpoint either speaks h2 or it does not - if any writer of it
                // was switched to h1, we ping it over h1 as well.
                entry.2 |= table_item.use_h1;
            }

            for (_, (settings, tables, use_h1)) in url_to_ping {
                let mut factory = FlUrlFactory::new(settings, None, "");

                if use_h1 {
                    factory.use_h1();
                }

                let ping_model = PingModel {
                    name: itm.name.to_string(),
                    version: itm.version.to_string(),
                    tables,
                };

                let fl_url = factory.get_fl_url().await;

                if let Err(err) = &fl_url {
                    println!("{}:{} ping error: {:?}", itm.name, itm.version, err);
                    continue;
                }

                let fl_url_response = fl_url
                    .unwrap()
                    .0
                    .with_retries(3)
                    .append_path_segment("api")
                    .append_path_segment("ping")
                    .post(HttpRequestBody::as_json(&ping_model))
                    .await;

                if let Err(err) = &fl_url_response {
                    println!("{}:{} ping error: {:?}", itm.name, itm.version, err);
                    continue;
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingModel {
    pub name: String,
    pub version: String,
    pub tables: Vec<String>,
}
