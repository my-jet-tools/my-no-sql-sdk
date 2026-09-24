use std::sync::Arc;

use flurl::FlUrl;

use my_no_sql_abstractions::{parse_connection_string, ConnectionString};
use rust_extensions::UnsafeValue;

use super::{CreateTableParams, DataWriterError, MyNoSqlWriterSettings};

#[derive(Clone)]
pub struct FlUrlFactory {
    settings: Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static>,
    auto_create_table_params: Option<Arc<CreateTableParams>>,

    #[cfg(all(unix, feature = "with-ssh"))]
    pub ssh_security_credentials_resolver:
        Option<Arc<dyn flurl::my_ssh::ssh_settings::SshSecurityCredentialsResolver + Send + Sync>>,

    create_table_is_called: Arc<UnsafeValue<bool>>,
    table_name: &'static str,
    mode: flurl::FlUrlMode,
}

impl FlUrlFactory {
    pub fn new(
        settings: Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static>,
        auto_create_table_params: Option<Arc<CreateTableParams>>,
        table_name: &'static str,
    ) -> Self {
        Self {
            auto_create_table_params,

            create_table_is_called: UnsafeValue::new(false).into(),
            settings,
            table_name,
            // HTTP/2 multiplexes all our requests over a single connection per
            // endpoint. Servers which do not speak h2 can be handled with use_h1().
            mode: flurl::FlUrlMode::H2,

            #[cfg(all(unix, feature = "with-ssh"))]
            ssh_security_credentials_resolver: None,
        }
    }

    /// Falls back to HTTP/1.1 - for MyNoSqlServer instances which do not support HTTP/2.
    pub fn use_h1(&mut self) {
        self.mode = flurl::FlUrlMode::Http1Hyper;
    }

    pub fn get_settings(&self) -> &Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static> {
        &self.settings
    }

    /// The only place where FlUrl is created - so every outgoing request carries both the
    /// session and the namespace of this writer. `connection_string` is the parsed one: its
    /// host is a clean url, never the whole `host=...;ns=...` string.
    async fn create_fl_url(&self, connection_string: &ConnectionString) -> FlUrl {
        // Replay this process's session id on every request so the server can
        // attribute all of our traffic (data requests and the Ping handshake
        // alike) to a single writer.
        let mut fl_url = flurl::FlUrl::new(connection_string.host.as_str())
            .update_mode(self.mode)
            .with_header("session", super::get_writer_session_id());

        // Nothing is sent when the connection works with the default namespace - which is what
        // the server uses anyway when the header is absent.
        if let Some(namespace) = connection_string.get_namespace_to_send() {
            fl_url = fl_url.with_header("ns", namespace);
        }

        #[cfg(all(unix, feature = "with-ssh"))]
        if let Some(ssh_security_credentials_resolver) = &self.ssh_security_credentials_resolver {
            return fl_url
                .set_ssh_security_credentials_resolver(ssh_security_credentials_resolver.clone());
        }

        fl_url
    }

    pub async fn get_fl_url(&self) -> Result<(FlUrl, String), DataWriterError> {
        let connection_string = parse_connection_string(self.settings.get_url().await.as_str())?;

        if !self.create_table_is_called.get_value() {
            if let Some(crate_table_params) = &self.auto_create_table_params {
                self.create_table_if_not_exists(&connection_string, crate_table_params)
                    .await?;
            }

            self.create_table_is_called.set_value(true);
        }

        let result = self.create_fl_url(&connection_string).await;

        Ok((result, connection_string.host))
    }

    /// The FlUrl of a request which must NOT bring the table into existence as a side effect.
    ///
    /// [`Self::get_fl_url`] auto-creates the table on the first request when the writer was
    /// built with auto-creation on - which is the default, and which is right for a write and
    /// wrong for a question about whether the table is there at all. Asking through that path
    /// would make the answer true: the table would be created empty and the count would come
    /// back as `0` for a table which did not exist a moment earlier.
    pub async fn get_fl_url_without_auto_create_table(
        &self,
    ) -> Result<(FlUrl, String), DataWriterError> {
        let connection_string = parse_connection_string(self.settings.get_url().await.as_str())?;

        let result = self.create_fl_url(&connection_string).await;

        Ok((result, connection_string.host))
    }

    pub async fn create_table_if_not_exists(
        &self,
        connection_string: &ConnectionString,
        create_table_params: &CreateTableParams,
    ) -> Result<(), DataWriterError> {
        let fl_url = self.create_fl_url(connection_string).await;
        super::execution::create_table_if_not_exists(
            fl_url,
            connection_string.host.as_str(),
            self.table_name,
            create_table_params,
            my_no_sql_abstractions::DataSynchronizationPeriod::Sec1,
        )
        .await
    }
}
