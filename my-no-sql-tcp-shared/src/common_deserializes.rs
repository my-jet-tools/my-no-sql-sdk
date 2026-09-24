use my_tcp_sockets::socket_reader::{ReadingTcpContractFail, SocketReader};
use rust_extensions::date_time::DateTimeAsMicroseconds;

pub async fn read_pascal_string(
    reader: &mut impl SocketReader,
) -> Result<String, ReadingTcpContractFail> {
    let size = reader.read_byte().await? as usize;

    let mut result: Vec<u8> = Vec::with_capacity(size);
    unsafe { result.set_len(size) }

    reader.read_buf(&mut result).await?;

    Ok(String::from_utf8(result)?)
}

pub async fn read_list_of_pascal_strings<TSocketReader: SocketReader + Send + Sync + 'static>(
    reader: &mut TSocketReader,
) -> Result<Vec<String>, ReadingTcpContractFail> {
    let amount = reader.read_i32().await? as usize;

    let mut result = Vec::with_capacity(amount);

    for _ in 0..amount {
        result.push(read_pascal_string(reader).await?);
    }

    Ok(result)
}

pub async fn read_date_time_opt<TSocketReader: SocketReader + Send + Sync + 'static>(
    reader: &mut TSocketReader,
) -> Result<Option<DateTimeAsMicroseconds>, ReadingTcpContractFail> {
    let unix_microseconds = reader.read_i64().await?;

    let result = if unix_microseconds == 0 {
        None
    } else {
        Some(DateTimeAsMicroseconds::new(unix_microseconds))
    };

    Ok(result)
}
