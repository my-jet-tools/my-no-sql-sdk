use std::sync::Arc;

use flurl::FlUrl;

use my_no_sql_abstractions::{parse_connection_string, ConnectionString};
use rust_extensions::UnsafeValue;

use super::{CreateTableParams, DataWriterError, MyNoSqlWriterSettings};

/// The largest answer a writer reads, in bytes, unless it is given another limit by
/// `set_body_size_limit`: 100 MB. FlUrl reads no more than 10 MB of an answer by default, and a
/// table - a partition even - is easily bigger than that.
pub const DEFAULT_BODY_SIZE_LIMIT: usize = 100 * 1024 * 1024;

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
    body_size_limit: usize,
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
            body_size_limit: DEFAULT_BODY_SIZE_LIMIT,

            #[cfg(all(unix, feature = "with-ssh"))]
            ssh_security_credentials_resolver: None,
        }
    }

    /// Falls back to HTTP/1.1 - for MyNoSqlServer instances which do not support HTTP/2.
    pub fn use_h1(&mut self) {
        self.mode = flurl::FlUrlMode::Http1Hyper;
    }

    /// The largest answer the writer reads, in bytes - see [`DEFAULT_BODY_SIZE_LIMIT`].
    pub fn set_body_size_limit(&mut self, value: usize) {
        self.body_size_limit = value;
    }

    pub fn get_settings(&self) -> &Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static> {
        &self.settings
    }

    /// The only place where FlUrl is created - so every outgoing request carries both the
    /// session and the namespace of this writer. `connection_string` is the parsed one: its
    /// host is a clean url, never the whole `host=...;ns=...` string.
    ///
    /// A host which is not an url is an error of the call. A legacy connection string is taken
    /// whole, so that is what a string like `ns=alpha;host=http://..` comes to; a string which
    /// names no host - an empty setting, a bare `http://` - is not an url either.
    ///
    /// `FlUrl::new` never fails and never panics: a url it can not use becomes the error the
    /// request would fail with. It is asked for here, before anything is sent, so that the call
    /// returns it - and so does the ping loop, which is a single task nothing starts again.
    fn create_fl_url(
        &self,
        connection_string: &ConnectionString,
    ) -> Result<FlUrl, DataWriterError> {
        let fl_url = FlUrl::new(connection_string.host.as_str());

        if let Some(err) = fl_url.get_error() {
            return Err(DataWriterError::InvalidConnectionString(format!(
                "Host '{}' is not an url. {:?}",
                connection_string.host, err
            )));
        }

        // Replay this process's session id on every request so the server can
        // attribute all of our traffic (data requests and the Ping handshake
        // alike) to a single writer.
        let mut fl_url = fl_url
            .update_mode(self.mode)
            .set_max_response_body_size(self.body_size_limit)
            .with_header("session", super::get_writer_session_id());

        // Nothing is sent when the connection works with the default namespace - which is what
        // the server uses anyway when the header is absent.
        if let Some(namespace) = connection_string.get_namespace_to_send() {
            fl_url = fl_url.with_header("ns", namespace);
        }

        #[cfg(all(unix, feature = "with-ssh"))]
        if let Some(ssh_security_credentials_resolver) = &self.ssh_security_credentials_resolver {
            return Ok(fl_url
                .set_ssh_security_credentials_resolver(ssh_security_credentials_resolver.clone()));
        }

        Ok(fl_url)
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

        let result = self.create_fl_url(&connection_string)?;

        Ok((result, connection_string.host))
    }

    /// The FlUrl of a request which must NOT bring the table into existence as a side effect.
    ///
    /// [`Self::get_fl_url`] auto-creates the table on the first request when the writer was
    /// built with auto-creation on - which is the default, and which is right for a write and
    /// wrong for a question about whether the table is there at all. Asking through that path
    /// would make the answer true: the table would be created empty and the count would come
    /// back as `0` for a table which did not exist a moment earlier.
    ///
    /// It is wrong for the calls which create the table themselves as well: the auto-creation
    /// in front of them would create it with the auto-create parameters instead of theirs.
    pub async fn get_fl_url_without_auto_create_table(
        &self,
    ) -> Result<(FlUrl, String), DataWriterError> {
        let connection_string = parse_connection_string(self.settings.get_url().await.as_str())?;

        let result = self.create_fl_url(&connection_string)?;

        Ok((result, connection_string.host))
    }

    /// The writer has created the table by a call of its own, with the parameters of that
    /// call: the auto-creation of [`Self::get_fl_url`] has nothing left to do. Its
    /// `CreateIfNotExists` must not go out after that - the server applies the parameters of
    /// that request to a table which is already there, so the auto-create parameters would
    /// replace the ones the table was just created with.
    pub(crate) fn table_is_created(&self) {
        self.create_table_is_called.set_value(true);
    }

    pub async fn create_table_if_not_exists(
        &self,
        connection_string: &ConnectionString,
        create_table_params: &CreateTableParams,
    ) -> Result<(), DataWriterError> {
        let fl_url = self.create_fl_url(connection_string)?;
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::FlUrlFactory;
    use crate::{CreateTableParams, DataWriterError, MyNoSqlWriterSettings};

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

    fn factory_of(
        connection_string: &'static str,
        auto_create_table_params: Option<CreateTableParams>,
    ) -> FlUrlFactory {
        FlUrlFactory::new(
            Arc::new(TestSettings(connection_string)),
            auto_create_table_params.map(Arc::new),
            "test",
        )
    }

    fn assert_is_refused<T>(result: Result<T, DataWriterError>, host: &str) {
        match result {
            Err(DataWriterError::InvalidConnectionString(message)) => {
                assert!(message.contains(host), "{}", message)
            }
            Err(err) => panic!("expected InvalidConnectionString, got {:?}", err),
            Ok(_) => panic!("expected InvalidConnectionString, got Ok"),
        }
    }

    /// `ns=..;host=..` does not start with `host=` - it is a legacy string and the whole of it is
    /// the host. It used to panic in `FlUrl::new`: in every call of the writer, and in the ping
    /// loop, which nothing starts again.
    #[tokio::test]
    async fn a_host_which_is_not_an_url_is_an_error_and_not_a_panic() {
        for host in [
            "ns=alpha;host=http://127.0.0.1:5123",
            "htp://127.0.0.1:5123",
        ] {
            // a writer which does not create the table - and the ping loop, which builds its
            // factory the same way
            let factory = factory_of(host, None);

            assert_is_refused(factory.get_fl_url().await, host);
            assert_is_refused(factory.get_fl_url_without_auto_create_table().await, host);

            // a writer with the auto-creation: the request in front of the first call
            let factory = factory_of(
                host,
                Some(CreateTableParams {
                    persist: false,
                    max_partitions_amount: None,
                    max_rows_per_partition_amount: None,
                }),
            );

            assert_is_refused(factory.get_fl_url().await, host);
        }
    }

    /// A string which names no host - a setting which is empty, a scheme with nothing behind it -
    /// used to be taken, and the request built on it panicked: in every call of the writer and
    /// in the ping loop, like the host which is not an url at all.
    #[tokio::test]
    async fn a_connection_string_which_names_no_host_is_an_error_and_not_a_panic() {
        // ...and a unix socket without a path: an empty host is refused whatever the url is. So
        // is a blank name next to an ip, which FlUrl used to connect to the ip with
        for host in [
            "",
            " ",
            "http://",
            "https://",
            "HTTP://",
            "http+unix:/",
            " @10.0.0.5:5123",
            "http:// @10.0.0.5:5123",
        ] {
            let factory = factory_of(host, None);

            assert_is_refused(factory.get_fl_url().await, "names no host");
            assert_is_refused(
                factory.get_fl_url_without_auto_create_table().await,
                "names no host",
            );

            let factory = factory_of(
                host,
                Some(CreateTableParams {
                    persist: false,
                    max_partitions_amount: None,
                    max_rows_per_partition_amount: None,
                }),
            );

            assert_is_refused(factory.get_fl_url().await, "names no host");
        }
    }

    /// FlUrl used to panic on some of what it does not take: the parser of an ssh part (`..->..`)
    /// had no error for a part which does not begin with `ssh` and has two `:`, nor - once the
    /// part is read, which takes the `with-ssh` feature - for a port which is not a number. That
    /// was a panic in every call of the writer and in the ping loop as well.
    #[tokio::test]
    async fn a_host_on_which_fl_url_panics_is_an_error_and_not_a_panic() {
        let mut hosts = vec![
            "http://user@10.0.0.2:22->http://127.0.0.1:5123",
            "user@10.0.0.2:22:22->http://127.0.0.1:5123",
        ];

        if cfg!(all(unix, feature = "with-ssh")) {
            hosts.push("ssh://user@10.0.0.2:22x->http://127.0.0.1:5123");
            hosts.push("ssh://user@10.0.0.2:99999->http://127.0.0.1:5123");
        }

        for host in hosts {
            let factory = factory_of(host, None);

            assert_is_refused(factory.get_fl_url().await, host);
            assert_is_refused(factory.get_fl_url_without_auto_create_table().await, host);

            let factory = factory_of(
                host,
                Some(CreateTableParams {
                    persist: false,
                    max_partitions_amount: None,
                    max_rows_per_partition_amount: None,
                }),
            );

            assert_is_refused(factory.get_fl_url().await, host);
        }
    }

    #[tokio::test]
    async fn an_url_is_taken_as_it_always_was() {
        for connection_string in [
            "http://127.0.0.1:5123",
            "host=http://127.0.0.1:5123;ns=alpha",
        ] {
            let (_, host) = factory_of(connection_string, None)
                .get_fl_url()
                .await
                .unwrap();

            assert_eq!("http://127.0.0.1:5123", host);
        }

        // ...and so is every other shape FlUrl has a host for: without a scheme, with a path,
        // a unix socket, an ip next to the name
        for connection_string in [
            "127.0.0.1:5123",
            "localhost",
            "http://localhost:5123/some/path/",
            "http://localhost:5123?x=1",
            "/var/run/my-no-sql.sock",
            "~/unix-sockets/my-no-sql.sock",
            "http+unix://var/run/my-no-sql.sock",
            "unix:///var/run/my-no-sql.sock",
            "http://my-no-sql@10.0.0.5:5123",
            "[::1]:5123",
            // the host of a unix socket is a path, a blank one included
            "http+unix:/ ",
        ] {
            let (_, host) = factory_of(connection_string, None)
                .get_fl_url()
                .await
                .unwrap();

            assert_eq!(connection_string, host);
        }
    }
}
