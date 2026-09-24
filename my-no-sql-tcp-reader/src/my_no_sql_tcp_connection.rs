use std::{sync::Arc, time::Duration};

use arc_swap::ArcSwapOption;
use my_no_sql_abstractions::{parse_connection_string, MyNoSqlEntity, MyNoSqlEntitySerializer};
use my_no_sql_tcp_shared::{sync_to_main::SyncToMainNodeHandler, MyNoSqlTcpSerializerFactory};
use my_tcp_sockets::{TcpClient, TlsSettings};
use rust_extensions::{AppStates, StrOrString};

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
            Err(err) => panic!(
                "Invalid MyNoSqlServer connection string '{}'. {}",
                connection_string, err
            ),
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
    app_states: Arc<AppStates>,
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
                Arc::new(SyncToMainNodeHandler::new()),
                namespace,
            ),
            app_states: Arc::new(AppStates::create_un_initialized()),
        }
    }

    pub fn get_reader<
        TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
    >(
        &self,
    ) -> Arc<MyNoSqlDataReaderTcp<TMyNoSqlEntity>> {
        self.tcp_events
            .subscribers
            .create_subscriber(
                self.app_states.clone(),
                self.tcp_events.sync_handler.clone(),
            )
            
    }

    pub async fn start(&self) {
        self.app_states.set_initialized();

        self.tcp_client
            .start(
                Arc::new(MyNoSqlTcpSerializerFactory),
                self.tcp_events.clone(),
                my_logger::LOGGER.clone(),
            )
            .await;

        self.tcp_events
            .sync_handler
            .start(my_logger::LOGGER.clone(), self.app_states.clone());
    }
}
