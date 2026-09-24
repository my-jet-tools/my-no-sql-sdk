#[async_trait::async_trait]
pub trait MyNoSqlTcpConnectionSettings {
    async fn get_host_port(&self) -> String;
}
