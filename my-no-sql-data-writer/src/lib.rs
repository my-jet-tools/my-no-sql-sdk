mod my_no_sql_data_writer;
pub use my_no_sql_data_writer::*;

mod ping_pool;
pub use ping_pool::*;

lazy_static::lazy_static! {
     static ref PING_POOL: crate::PingPool =  crate::PingPool::new();
}
