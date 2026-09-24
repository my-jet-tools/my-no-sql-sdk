#[async_trait::async_trait]
pub trait MyNoSqlWriterSettings {
    async fn get_url(&self) -> String;
    fn get_app_name(&self) -> &'static str;
    fn get_app_version(&self) -> &'static str;
}
