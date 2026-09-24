
use my_no_sql_tcp_shared::DeleteRowTcpContract;


pub trait UpdateEvent {
    fn init_table(&self, data: Vec<u8>);
    fn init_partition(&self, partition_key: &str, data: Vec<u8>);
    fn update_rows(&self, data: Vec<u8>);
    fn delete_rows(&self, rows_to_delete: Vec<DeleteRowTcpContract>);
}
