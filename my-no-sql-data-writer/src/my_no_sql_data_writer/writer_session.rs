use std::sync::LazyLock;

use rust_extensions::SortableId;

// The session id this writer process reports to the server. It is generated
// once on first access, kept globally (not tied to any single writer/table/url)
// and replayed in the `session` header of every request the process issues —
// the Ping handshake and all data requests alike — so the server can attribute
// all of this process's traffic to a single writer. It only changes when the
// application itself restarts.
static WRITER_SESSION_ID: LazyLock<String> = LazyLock::new(|| SortableId::generate().into());

pub fn get_writer_session_id() -> &'static str {
    WRITER_SESSION_ID.as_str()
}
