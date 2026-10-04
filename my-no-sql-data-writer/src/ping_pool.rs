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

            // A writer of a table is registered once, however many times it is built: an
            // application which builds its writers again and again would grow this list
            // without end, and the table would be named in the ping body once per writer.
            // The url is not known here - it is what the settings answer, asked anew before
            // every ping - so a writer is told from another by the settings it was built with.
            let is_registered = item
                .table_settings
                .iter()
                .any(|x| x.table == table && Arc::ptr_eq(&x.settings, &settings));

            if !is_registered {
                item.table_settings.push(table_item);
            }
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

/// What is pinged at one endpoint: the settings it is reached by, the tables which are written
/// there and whether it is pinged over HTTP/1.1.
type PingOfEndpoint = (
    Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static>,
    Vec<String>,
    bool,
);

/// The tables of an application by the endpoint they are pinged at - its host and its
/// namespace.
///
/// A table is named once per endpoint. The registry can tell a writer from another only by the
/// settings it was built with; here the url those settings answer is known, and writers of one
/// table which were built with different settings for the same endpoint are one table to the
/// server.
async fn group_by_endpoint(
    itm: &PingSnapshotItem,
) -> HashMap<(String, Option<String>), PingOfEndpoint> {
    let mut url_to_ping: HashMap<(String, Option<String>), PingOfEndpoint> = HashMap::new();

    for table_item in itm.table_settings.iter() {
        let connection_string = table_item.settings.get_url().await;

        // Ping is grouped by the endpoint it is sent to - and the namespace is a part
        // of that endpoint identity, since the request carries it as a header.
        let connection_string =
            match my_no_sql_abstractions::parse_connection_string(connection_string.as_str()) {
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

        if !entry.1.contains(&table_item.table) {
            entry.1.push(table_item.table.to_string());
        }

        // The endpoint either speaks h2 or it does not - if any writer of it
        // was switched to h1, we ping it over h1 as well.
        entry.2 |= table_item.use_h1;
    }

    url_to_ping
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

        ping_round(snapshot).await;
    }
}

/// One round of the loop: a ping to every endpoint of every application.
async fn ping_round(snapshot: Vec<PingSnapshotItem>) {
    for itm in snapshot {
        let url_to_ping = group_by_endpoint(&itm).await;

        for (_, (settings, tables, use_h1)) in url_to_ping {
            // In a task of its own: this loop is a single task which nothing starts again, so a
            // panic of one ping would end the ping of every writer of the process. (An `https://`
            // url in a build without a TLS feature no longer panics - the HTTP client returns
            // `UnsupportedScheme`, which is an error of that ping.)
            let ping = tokio::spawn(ping_endpoint(
                itm.name,
                itm.version,
                settings,
                tables,
                use_h1,
            ));

            if let Err(err) = ping.await {
                println!("{}:{} ping error: {}", itm.name, itm.version, err);
            }
        }
    }
}

async fn ping_endpoint(
    name: &'static str,
    version: &'static str,
    settings: Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static>,
    tables: Vec<String>,
    use_h1: bool,
) {
    let mut factory = FlUrlFactory::new(settings, None, "");

    if use_h1 {
        factory.use_h1();
    }

    let ping_model = PingModel {
        name: name.to_string(),
        version: version.to_string(),
        tables,
    };

    let fl_url = factory.get_fl_url().await;

    if let Err(err) = &fl_url {
        println!("{}:{} ping error: {:?}", name, version, err);
        return;
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
        println!("{}:{} ping error: {:?}", name, version, err);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingModel {
    pub name: String,
    pub version: String,
    pub tables: Vec<String>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{group_by_endpoint, PingPool, PingSnapshotItem, PingTableItem};
    use crate::MyNoSqlWriterSettings;

    /// Settings which answer the connection string they were built with.
    struct TestSettings(&'static str);

    #[async_trait::async_trait]
    impl MyNoSqlWriterSettings for TestSettings {
        async fn get_url(&self) -> String {
            self.0.to_string()
        }

        fn get_app_name(&self) -> &'static str {
            "test-app"
        }

        fn get_app_version(&self) -> &'static str {
            "0.0.0"
        }
    }

    fn settings_of(
        connection_string: &'static str,
    ) -> Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static> {
        Arc::new(TestSettings(connection_string))
    }

    fn new_settings() -> Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static> {
        settings_of("http://127.0.0.1:5123")
    }

    /// The tables registered for the application of [`TestSettings`], each with its h1 flag.
    fn registered(pool: &PingPool) -> Vec<(String, bool)> {
        let data = pool.data.lock();

        assert_eq!(data.items.len(), 1);

        data.items[0]
            .table_settings
            .iter()
            .map(|itm| (itm.table.to_string(), itm.use_h1))
            .collect()
    }

    #[tokio::test]
    async fn a_writer_which_is_built_again_is_registered_once() {
        let pool = PingPool::new();
        let settings = new_settings();

        // An application which builds the writer of a table whenever it needs one.
        for _ in 0..3 {
            pool.register(settings.clone(), "table-a");
        }

        assert_eq!(registered(&pool), vec![("table-a".to_string(), false)]);

        // Another table of the same writer settings is another entry.
        pool.register(settings.clone(), "table-b");
        pool.register(settings.clone(), "table-b");

        assert_eq!(
            registered(&pool),
            vec![
                ("table-a".to_string(), false),
                ("table-b".to_string(), false)
            ]
        );

        // The switch to h1 is kept by the entry when the writer is built once more.
        pool.use_h1(&settings, "table-a");
        pool.register(settings.clone(), "table-a");

        assert_eq!(
            registered(&pool),
            vec![
                ("table-a".to_string(), true),
                ("table-b".to_string(), false)
            ]
        );

        // Other settings are another writer - of another server, for all that is known here:
        // the url is what the settings answer when they are asked.
        pool.register(new_settings(), "table-a");

        assert_eq!(registered(&pool).len(), 3);
    }

    fn table_item(
        settings: &Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static>,
        table: &str,
        use_h1: bool,
    ) -> PingTableItem {
        PingTableItem {
            table: table.to_string(),
            settings: settings.clone(),
            use_h1,
        }
    }

    /// A ping which fails - with an error or with a panic - is the end of that ping, not of the
    /// round: the loop is a single task which nothing starts again, so the writers behind it
    /// would never be pinged again. The failing url is `https://` - without a TLS feature the
    /// HTTP client returns `UnsupportedScheme` (it used to panic), with one the connection is
    /// refused.
    #[tokio::test]
    async fn a_ping_which_fails_does_not_end_the_round() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // a server which writes down the request line of what it gets and answers 204
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url: &'static str =
            Box::leak(format!("http://{}", listener.local_addr().unwrap()).into_boxed_str());

        let requests = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));

        {
            let requests = requests.clone();
            tokio::spawn(async move {
                while let Ok((mut socket, _)) = listener.accept().await {
                    let requests = requests.clone();
                    tokio::spawn(async move {
                        let mut received = Vec::new();

                        loop {
                            // the head, and the body its content-length announces
                            let head_end = received.windows(4).position(|w| w == b"\r\n\r\n");

                            let whole = head_end.map(|head_end| {
                                let head = String::from_utf8_lossy(&received[..head_end]);

                                let body_len = head
                                    .lines()
                                    .filter_map(|line| line.split_once(':'))
                                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                                    .and_then(|(_, value)| value.trim().parse::<usize>().ok())
                                    .unwrap_or(0);

                                (
                                    head.lines().next().unwrap_or("").to_string(),
                                    head_end + 4 + body_len,
                                )
                            });

                            match whole {
                                Some((request_line, len)) if received.len() >= len => {
                                    requests.lock().push(request_line);
                                    let _ =
                                        socket.write_all(b"HTTP/1.1 204 No Content\r\n\r\n").await;
                                    received.drain(..len);
                                }
                                _ => match socket.read_buf(&mut received).await {
                                    Ok(0) | Err(_) => return,
                                    Ok(_) => {}
                                },
                            }
                        }
                    });
                }
            });
        }

        // Nothing listens there. Without a TLS feature the request fails with
        // `UnsupportedScheme` before it gets that far; with one it is refused
        let failing = settings_of("https://127.0.0.1:1");
        let healthy = settings_of(url);

        let snapshot = vec![
            PingSnapshotItem {
                name: "first-app",
                version: "0.0.0",
                table_settings: vec![table_item(&failing, "table-a", true)],
            },
            PingSnapshotItem {
                name: "second-app",
                version: "0.0.0",
                table_settings: vec![table_item(&healthy, "table-a", true)],
            },
        ];

        super::ping_round(snapshot).await;

        assert_eq!(
            *requests.lock(),
            vec!["POST /api/ping HTTP/1.1".to_string()]
        );
    }

    #[tokio::test]
    async fn a_table_is_named_once_in_the_ping_of_an_endpoint() {
        // What the registry holds of an application whose writers were built with settings of
        // their own: the first two answer one and the same endpoint.
        let first = settings_of("http://127.0.0.1:5123");
        let second = settings_of("http://127.0.0.1:5123");
        let in_a_namespace = settings_of("host=http://127.0.0.1:5123;ns=alpha");
        let elsewhere = settings_of("http://127.0.0.1:5124");

        let itm = PingSnapshotItem {
            name: "test-app",
            version: "0.0.0",
            table_settings: vec![
                table_item(&first, "table-a", false),
                table_item(&second, "table-a", true),
                table_item(&second, "table-b", false),
                table_item(&in_a_namespace, "table-a", false),
                table_item(&elsewhere, "table-a", false),
            ],
        };

        let mut pings: Vec<_> = group_by_endpoint(&itm)
            .await
            .into_iter()
            .map(|((host, namespace), (_, tables, use_h1))| (host, namespace, tables, use_h1))
            .collect();
        pings.sort();

        let tables = |tables: &[&str]| -> Vec<String> {
            tables.iter().map(|table| table.to_string()).collect()
        };

        assert_eq!(
            pings,
            vec![
                // One table of two writers - and h1, since one of the two speaks it.
                (
                    "http://127.0.0.1:5123".to_string(),
                    None,
                    tables(&["table-a", "table-b"]),
                    true
                ),
                // The namespace is a part of the endpoint.
                (
                    "http://127.0.0.1:5123".to_string(),
                    Some("alpha".to_string()),
                    tables(&["table-a"]),
                    false
                ),
                (
                    "http://127.0.0.1:5124".to_string(),
                    None,
                    tables(&["table-a"]),
                    false
                ),
            ]
        );
    }
}
