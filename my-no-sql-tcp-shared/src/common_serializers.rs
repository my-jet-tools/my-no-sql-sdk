use my_tcp_sockets::TcpWriteBuffer;
use rust_extensions::date_time::DateTimeAsMicroseconds;

pub fn serialize_date_time_opt(
    write_buffer: &mut impl TcpWriteBuffer,
    v: Option<DateTimeAsMicroseconds>,
) {
    if let Some(v) = v {
        write_buffer.write_i64(v.unix_microseconds);
    } else {
        write_buffer.write_i64(0);
    }
}
