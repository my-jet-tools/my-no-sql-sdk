use std::{sync::Arc, time::Duration};

use arc_swap::ArcSwapOption;
use my_no_sql_abstractions::{parse_connection_string, MyNoSqlEntity, MyNoSqlEntitySerializer};
use my_no_sql_tcp_shared::{sync_to_main::SyncToMainNodeHandler, MyNoSqlTcpSerializerFactory};
use my_tcp_sockets::{TcpClient, TlsSettings};
use rust_extensions::StrOrString;

use crate::{
    subscribers::MyNoSqlDataReaderTcp, tcp_events::TcpEvents, MyNoSqlTcpConnectionSettings,
};

pub struct TcpConnectionSettings {
    settings: Arc<dyn MyNoSqlTcpConnectionSettings + Sync + Send + 'static>,
    /// Namespace parsed out of the connection string. Shared with [`TcpEvents`] which sends it
    /// to the server right after the Greeting.
    namespace: Arc<ArcSwapOption<String>>,
}

#[async_trait::async_trait]
impl my_tcp_sockets::TcpClientSocketSettings for TcpConnectionSettings {
    async fn get_host_port(&self) -> Option<String> {
        let connection_string = self.settings.get_host_port().await;

        // Connection string is resolved before every connect attempt - so is the namespace,
        // which the application can change in its settings without a restart.
        let connection_string = match parse_connection_string(connection_string.as_str()) {
            Ok(result) => result,
            Err(err) => {
                // A panic here would end the connect loop - it is a single task, nothing starts
                // it again - and the readers would stay on the data they have for good. `None`
                // fails this attempt only: the next one reads the settings again.
                my_logger::LOGGER.write_error(
                    "MyNoSqlTcpConnection",
                    format!(
                        "Invalid MyNoSqlServer connection string '{}'. {}",
                        connection_string, err
                    ),
                    None.into(),
                );

                return None;
            }
        };

        self.namespace.store(
            connection_string
                .get_namespace_to_send()
                .map(|namespace| Arc::new(namespace.to_string())),
        );

        connection_string.host.into()
    }

    async fn get_tls_settings(&self) -> Option<TlsSettings> {
        None
    }
}

pub struct MyNoSqlTcpConnection {
    tcp_client: TcpClient,
    pub ping_timeout: Duration,
    pub connect_timeout: Duration,
    pub tcp_events: TcpEvents,
}

impl MyNoSqlTcpConnection {
    pub fn new(
        app_name: impl Into<StrOrString<'static>>,
        settings: Arc<dyn MyNoSqlTcpConnectionSettings + Sync + Send + 'static>,
    ) -> Self {
        let namespace = Arc::new(ArcSwapOption::empty());

        let settings = TcpConnectionSettings {
            settings,
            namespace: namespace.clone(),
        };

        let app_name: StrOrString<'static> = app_name.into();

        Self {
            tcp_client: TcpClient::new("MyNoSqlClient".to_string(), Arc::new(settings)),
            ping_timeout: Duration::from_secs(3),
            connect_timeout: Duration::from_secs(3),
            tcp_events: TcpEvents::new(
                app_name.to_string(),
                Arc::new(SyncToMainNodeHandler::new(my_logger::LOGGER.clone())),
                namespace,
            ),
        }
    }

    pub fn get_reader<
        TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
    >(
        &self,
    ) -> Arc<MyNoSqlDataReaderTcp<TMyNoSqlEntity>> {
        self.tcp_events
            .subscribers
            .create_subscriber(self.tcp_events.sync_handler.clone())
    }

    pub async fn start(&self) {
        self.tcp_client
            .start(
                Arc::new(MyNoSqlTcpSerializerFactory),
                self.tcp_events.clone(),
                my_logger::LOGGER.clone(),
            )
            .await;

        self.tcp_events.sync_handler.start();
    }
}

#[cfg(test)]
mod tests {
    //! The connect loop of my-tcp-sockets asks for host:port before every attempt. A connection
    //! string which could not be parsed used to panic there - inside the loop, which is a single
    //! task nothing starts again. Not one more attempt was made: the readers stayed on the data
    //! they had, however the settings were changed afterwards.

    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use arc_swap::ArcSwapOption;
    use my_logger::{LogLevel, MyLogEvent, MyLoggerConsumer};
    use my_no_sql_abstractions::ConnectionStringError;
    use my_tcp_sockets::TcpClientSocketSettings;
    use parking_lot::Mutex;

    use crate::MyNoSqlTcpConnectionSettings;

    use super::{MyNoSqlTcpConnection, TcpConnectionSettings};

    /// The settings of an application. The connection string can be changed while the connection
    /// is running - the way a settings file is - and the reads of it are counted.
    struct TestSettings {
        connection_string: Mutex<String>,
        reads: AtomicUsize,
    }

    impl TestSettings {
        fn new(connection_string: &str) -> Arc<Self> {
            Arc::new(Self {
                connection_string: Mutex::new(connection_string.to_string()),
                reads: AtomicUsize::new(0),
            })
        }

        fn set(&self, connection_string: String) {
            *self.connection_string.lock() = connection_string;
        }

        /// An attempt of the connect loop starts with a read of the settings - so the reads keep
        /// coming for as long as the loop is alive.
        async fn wait_for_reads(&self, amount: usize) {
            for _ in 0..500 {
                if self.reads.load(Ordering::SeqCst) >= amount {
                    return;
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            panic!(
                "the settings were read {} time(s), not {} - the connect loop is gone",
                self.reads.load(Ordering::SeqCst),
                amount
            );
        }
    }

    #[async_trait::async_trait]
    impl MyNoSqlTcpConnectionSettings for TestSettings {
        async fn get_host_port(&self) -> String {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.connection_string.lock().clone()
        }
    }

    /// Keeps the messages of the errors written to my_logger.
    struct TestLogs {
        errors: Mutex<Vec<String>>,
    }

    impl MyLoggerConsumer for TestLogs {
        fn write_log(&self, log_event: Arc<MyLogEvent>) {
            if let LogLevel::Error = log_event.level {
                self.errors.lock().push(log_event.message.to_string());
            }
        }
    }

    #[tokio::test]
    async fn a_connection_string_which_can_not_be_parsed_fails_the_attempt_and_is_logged() {
        // `namespace` is not a key of a connection string - the key is `ns`
        let can_not_be_parsed = "host=127.0.0.1:5125;namespace=alpha";

        let logs = Arc::new(TestLogs {
            errors: Mutex::new(Vec::new()),
        });
        my_logger::LOGGER.plug_consumer(logs.clone());

        let settings = TestSettings::new(can_not_be_parsed);
        let namespace = Arc::new(ArcSwapOption::empty());

        let tcp_settings = TcpConnectionSettings {
            settings: settings.clone(),
            namespace: namespace.clone(),
        };

        // used to panic here
        assert_eq!(tcp_settings.get_host_port().await, None);

        // The logger is shared with the tests which run next to this one - only the errors about
        // the string of this test are looked at. One attempt - one error, which says what is
        // wrong with the string.
        let errors: Vec<String> = logs
            .errors
            .lock()
            .iter()
            .filter(|message| message.contains(can_not_be_parsed))
            .cloned()
            .collect();

        assert_eq!(
            errors,
            vec![format!(
                "Invalid MyNoSqlServer connection string '{}'. {}",
                can_not_be_parsed,
                ConnectionStringError::UnknownKey("namespace".to_string())
            )]
        );

        // the next attempt reads the settings again - fixing them is all it takes
        settings.set("host=127.0.0.1:5125;ns=alpha".to_string());

        assert_eq!(
            tcp_settings.get_host_port().await,
            Some("127.0.0.1:5125".to_string())
        );
        assert_eq!(namespace.load_full().unwrap().as_str(), "alpha");
    }

    /// The same through the connect loop itself: `None` is an attempt which has failed - the
    /// loop waits and asks again.
    #[tokio::test]
    async fn the_connect_loop_outlives_a_connection_string_which_can_not_be_parsed() {
        // Stands for the server - all it has to do is to see the reader come.
        let server = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();

        // an upper case letter is not allowed in the name of a namespace
        let settings = TestSettings::new("host=127.0.0.1:5125;ns=Alpha");

        let mut connection = MyNoSqlTcpConnection::new("test-app", settings.clone());
        // An application has 3 seconds between the attempts - too long for a test to wait.
        connection.tcp_client = connection
            .tcp_client
            .set_reconnect_timeout(Duration::from_millis(10));

        connection.start().await;

        // The second read used to never come: the first one panicked inside the loop, and the
        // loop was gone with it.
        settings.wait_for_reads(2).await;

        settings.set(format!("host={};ns=alpha", server.local_addr().unwrap()));

        tokio::time::timeout(Duration::from_secs(5), server.accept())
            .await
            .expect("the reader did not come after its connection string was fixed")
            .unwrap();
    }
}
