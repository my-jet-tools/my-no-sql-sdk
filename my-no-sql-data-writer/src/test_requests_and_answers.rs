//! What the writer puts on the wire and what it makes of the answer, checked against a stub of
//! the server: a listener on a port of its own which writes down every request it gets and
//! answers with whatever the test told it to. The routes, the parameters and the answers the
//! tests script are those of the server's `src/http_server/controllers`.
//!
//! Nothing looked at the wire before, and the wire is where the writer was wrong:
//!
//! * `delete_partitions` went to a route the server does not have, with a parameter name the
//!   server does not read, and took the answer it got there for a success;
//! * `delete_row` - the enum-case deletes with it - and `delete_partitions` did not carry the
//!   `syncPeriod` of the writer;
//! * a status no call had a meaning for - 503 while the server is still loading its tables, a
//!   5xx, a 404 of a route nobody serves - was "there is nothing there" to the reads and to the
//!   row deletes, "the table has no partitions" to `get_partition_keys` and "done" to the
//!   clean-and-insert calls;
//! * a 404 was "there is no such table" to `get_partition_keys` whoever sent it;
//! * a 2xx whose body is not what the call reads - a 204 where a row is expected, the page of
//!   a proxy, a row which does not fit the entity - took the calling task down with a panic;
//! * `delete_partitions` put a list of any length into one request, which the server refuses
//!   once the keys outgrow a request head;
//! * `get_partition_keys` was the one call the `with_retries` wrapper sent without its retries;
//! * the writes, the conditional bulk delete and the chunked uploads reported an answer they
//!   had no meaning for by its body alone - a 502 without a body was an error with an empty
//!   message - and most of them handed a missing table back as the raw json of the contract;
//! * `replace_entity` took any 404 for "there is no such row", and `insert_or_update` spent
//!   its attempts on it as on a race it had lost;
//! * a `200 text/html` - the page the server answers a route it does not have with - was
//!   "done" to every call which does not read the body;
//! * `create_table` sent the auto-creation of the writer first and was then told that the
//!   table already exists.

use std::{collections::BTreeMap, sync::Arc};

use my_no_sql_abstractions::{
    DataSynchronizationPeriod, GetMyNoSqlEntitiesByPartitionKey, GetMyNoSqlEntity, MyNoSqlEntity,
    MyNoSqlEntitySerializer, Timestamp,
};
use parking_lot::Mutex;
use rust_extensions::date_time::DateTimeAsMicroseconds;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{
    CreateTableParams, DataWriterError, MyNoSqlDataWriter, MyNoSqlWriterSettings, RowToDeleteIf,
};

/// What the server answers - with a 503 - while it is still loading its tables.
const NOT_READY: &str = "Application is not initialized yet";

/// What the HTTP framework of the server answers - with a 404 - to a request nothing serves.
const NO_ROUTE: &str = "404 - Not Found";

/// What the server answers - with a 404 - when the row, or its whole partition, is not there.
const RECORD_NOT_FOUND: &str = "Record not found";

/// What the server answers - with a 400 - when the table is not there.
const TABLE_NOT_FOUND: &str =
    r#"{"reason":"TableNotFound","message":"Table 'test-table' not found"}"#;

/// What the deployed server answers - with a 200 and as `text/html` - to a request nothing
/// serves: the page of its UI.
const UI_PAGE: &str = "<!DOCTYPE html><html><body>MyNoSqlServer</body></html>";

/// The row [`row`] returns, the way the server sends it.
const ROW: &str = r#"{"PartitionKey":"pk","RowKey":"rk","TimeStamp":"2026-08-12T18:19:36.776352"}"#;

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct TestEntity {
    #[serde(rename = "PartitionKey")]
    partition_key: String,
    #[serde(rename = "RowKey")]
    row_key: String,
    #[serde(rename = "TimeStamp")]
    time_stamp: Timestamp,
}

impl MyNoSqlEntity for TestEntity {
    const TABLE_NAME: &'static str = "test-table";
    const LAZY_DESERIALIZATION: bool = false;

    fn get_partition_key(&self) -> &str {
        self.partition_key.as_str()
    }

    fn get_row_key(&self) -> &str {
        self.row_key.as_str()
    }

    fn get_time_stamp(&self) -> Timestamp {
        self.time_stamp
    }
}

impl MyNoSqlEntitySerializer for TestEntity {
    fn serialize_entity(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }

    fn deserialize_entity(src: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(src).map_err(|err| format!("{}", err))
    }
}

/// It carries a real `TimeStamp`: the `*_with_own_timestamp` calls and `DeleteIf` refuse a
/// default one.
fn row() -> TestEntity {
    TestEntity {
        partition_key: "pk".to_string(),
        row_key: "rk".to_string(),
        time_stamp: time_stamp(),
    }
}

fn time_stamp() -> Timestamp {
    DateTimeAsMicroseconds::from_str("2026-08-12T18:19:36.776352")
        .unwrap()
        .into()
}

/// The row [`row`] returns, the way `bulk_delete` is asked for it.
fn rows_to_delete() -> BTreeMap<String, Vec<String>> {
    BTreeMap::from([("pk".to_string(), vec!["rk".to_string()])])
}

/// Parameters of a table which are not those a writer auto-creates its table with, so the two
/// can be told apart on the wire.
fn table_params() -> CreateTableParams {
    CreateTableParams {
        persist: false,
        max_partitions_amount: Some(3),
        max_rows_per_partition_amount: None,
    }
}

/// A case of an enum of entities, the way `#[enum_model]` makes one out of an entity: its keys
/// are constants of the type.
#[derive(Debug, PartialEq)]
struct TestCase(TestEntity);

impl From<TestEntity> for TestCase {
    fn from(src: TestEntity) -> Self {
        Self(src)
    }
}

impl MyNoSqlEntity for TestCase {
    const TABLE_NAME: &'static str = TestEntity::TABLE_NAME;
    const LAZY_DESERIALIZATION: bool = false;

    fn get_partition_key(&self) -> &str {
        self.0.get_partition_key()
    }

    fn get_row_key(&self) -> &str {
        self.0.get_row_key()
    }

    fn get_time_stamp(&self) -> Timestamp {
        self.0.get_time_stamp()
    }
}

impl GetMyNoSqlEntity for TestCase {
    const PARTITION_KEY: &'static str = "case-pk";
    const ROW_KEY: &'static str = "case-rk";
}

impl GetMyNoSqlEntitiesByPartitionKey for TestCase {
    const PARTITION_KEY: &'static str = "case-pk";
}

struct TestSettings {
    url: String,
}

#[async_trait::async_trait]
impl MyNoSqlWriterSettings for TestSettings {
    async fn get_url(&self) -> String {
        self.url.clone()
    }

    fn get_app_name(&self) -> &'static str {
        "test-app"
    }

    fn get_app_version(&self) -> &'static str {
        "0.0.0"
    }
}

struct StubState {
    /// The request lines without the protocol version, in the order they arrived:
    /// `DELETE /api/Row?rowKey=rk`.
    requests: Vec<String>,
    /// How many of the next requests get no answer at all.
    requests_to_leave_unanswered: usize,
    status_code: u16,
    /// The `Content-Type` of the answer. There is none unless a test asks for one.
    content_type: Option<String>,
    body: String,
    /// The head announces the whole body, half of it comes and the rest never does.
    body_stalls: bool,
    /// The answer - a status and a body - every request gets once so many more requests have
    /// been answered with the current one.
    next_answer: Option<(usize, u16, String)>,
    /// The answers - a status and a body - of the requests which start with the given text,
    /// whatever the other requests are answered with.
    answers_to: Vec<(String, u16, String)>,
}

impl StubState {
    /// The answer to a request - and the switch to the next answer once its turn has come.
    fn answer(&mut self, request_line: &str) -> String {
        let answer_to = self
            .answers_to
            .iter()
            .find(|(request, _, _)| request_line.starts_with(request.as_str()));

        if let Some((_, status_code, body)) = answer_to {
            return compile_response(*status_code, None, body.as_str());
        }

        let mut response = compile_response(
            self.status_code,
            self.content_type.as_deref(),
            self.body.as_str(),
        );

        if self.body_stalls {
            response.truncate(response.len() - self.body.len() / 2);
        }

        if let Some((answers_left, status_code, body)) = self.next_answer.take() {
            if answers_left <= 1 {
                self.status_code = status_code;
                self.content_type = None;
                self.body = body;
                self.body_stalls = false;
            } else {
                self.next_answer = Some((answers_left - 1, status_code, body));
            }
        }

        response
    }
}

/// The server as the writer sees it: HTTP/1.1 on a port the system has picked.
struct StubServer {
    url: String,
    state: Arc<Mutex<StubState>>,
}

impl StubServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());

        let state = Arc::new(Mutex::new(StubState {
            requests: Vec::new(),
            requests_to_leave_unanswered: 0,
            status_code: 204,
            content_type: None,
            body: String::new(),
            body_stalls: false,
            next_answer: None,
            answers_to: Vec::new(),
        }));

        let state_of_connections = state.clone();

        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                tokio::spawn(serve_connection(socket, state_of_connections.clone()));
            }
        });

        Self { url, state }
    }

    /// A writer of the stub. Its sync period is neither the default of the builder nor the one
    /// the server falls back to (both are `Sec5`), so a `syncPeriod=15` on the wire can only be
    /// the writer's own. The table is not auto-created: a test scripts the answer of the call
    /// it makes, not of a `CreateIfNotExists` in front of it.
    fn writer(&self) -> MyNoSqlDataWriter<TestEntity> {
        MyNoSqlDataWriter::create_with_builder(Arc::new(TestSettings {
            url: self.url.clone(),
        }))
        .set_sync_period(DataSynchronizationPeriod::Sec15)
        .do_not_auto_create_table()
        .use_h1()
        .build()
    }

    /// A writer of the stub built the way an application builds one: it creates its table -
    /// with the parameters of the builder - in front of the first request it makes.
    fn writer_which_creates_its_table(&self) -> MyNoSqlDataWriter<TestEntity> {
        MyNoSqlDataWriter::create_with_builder(Arc::new(TestSettings {
            url: self.url.clone(),
        }))
        .set_sync_period(DataSynchronizationPeriod::Sec15)
        .use_h1()
        .build()
    }

    /// Every request from now on is answered with this status and this body.
    fn answers(&self, status_code: u16, body: &str) {
        let mut state = self.state.lock();
        state.status_code = status_code;
        state.content_type = None;
        state.body = body.to_string();
        state.body_stalls = false;
        state.next_answer = None;
        state.answers_to.clear();
    }

    /// [`Self::answers`] - and the answer says what it is by a `Content-Type`.
    fn answers_as(&self, status_code: u16, content_type: &str, body: &str) {
        self.answers(status_code, body);
        self.state.lock().content_type = Some(content_type.to_string());
    }

    /// [`Self::answers`] - but the body stalls: its head announces all of it, the first half
    /// comes, the rest never does and the connection stays open.
    fn answers_with_a_body_which_stalls(&self, status_code: u16, body: &str) {
        // An answer which has a body to begin with.
        assert!(status_code != 204 && body.len() > 1);
        self.answers(status_code, body);
        self.state.lock().body_stalls = true;
    }

    /// The requests which start with `request` - `PUT /Row/Replace` - are answered with this
    /// status and this body from now on, whatever the other requests are answered with.
    fn answers_to(&self, request: &str, status_code: u16, body: &str) {
        self.state
            .lock()
            .answers_to
            .push((request.to_string(), status_code, body.to_string()));
    }

    /// The next `amount` requests are answered the way they are answered now, and every
    /// request after them with this status and this body.
    fn answers_after(&self, amount: usize, status_code: u16, body: &str) {
        assert!(amount > 0);
        self.state.lock().next_answer = Some((amount, status_code, body.to_string()));
    }

    /// The next `amount` requests are read and get no answer - the connection is closed
    /// instead, which is what a request "which got no response" is to the HTTP client.
    fn leaves_unanswered(&self, amount: usize) {
        self.state.lock().requests_to_leave_unanswered = amount;
    }

    /// The requests which have arrived since the last call.
    fn take_requests(&self) -> Vec<String> {
        std::mem::take(&mut self.state.lock().requests)
    }

    /// The one request which has arrived since the last call.
    fn take_request(&self) -> String {
        let mut requests = self.take_requests();
        assert_eq!(requests.len(), 1, "requests: {:?}", requests);
        requests.remove(0)
    }
}

async fn serve_connection(mut socket: TcpStream, state: Arc<Mutex<StubState>>) {
    let mut received = Vec::new();

    while let Some(request_line) = read_request(&mut socket, &mut received).await {
        let response = {
            let mut state = state.lock();

            let response = if state.requests_to_leave_unanswered > 0 {
                state.requests_to_leave_unanswered -= 1;
                None
            } else {
                Some(state.answer(request_line.as_str()))
            };

            state.requests.push(request_line);

            response
        };

        let Some(response) = response else {
            return;
        };

        if socket.write_all(response.as_bytes()).await.is_err() {
            return;
        }
    }
}

/// Reads one request - its head, and the body the `content-length` of the head announces - and
/// returns the request line without the protocol version. `None` once the client has closed
/// the connection.
async fn read_request(socket: &mut TcpStream, received: &mut Vec<u8>) -> Option<String> {
    const END_OF_HEAD: &[u8] = b"\r\n\r\n";

    let head_len = loop {
        let end_of_head = received
            .windows(END_OF_HEAD.len())
            .position(|window| window == END_OF_HEAD);

        if let Some(end_of_head) = end_of_head {
            break end_of_head + END_OF_HEAD.len();
        }

        if socket.read_buf(received).await.ok()? == 0 {
            return None;
        }
    };

    let head = String::from_utf8(received[..head_len].to_vec()).unwrap();

    let body_len = head
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map(|(_, value)| value.trim().parse::<usize>().unwrap())
        .unwrap_or(0);

    while received.len() < head_len + body_len {
        if socket.read_buf(received).await.ok()? == 0 {
            return None;
        }
    }

    received.drain(..head_len + body_len);

    let request_line = head.lines().next().unwrap();
    Some(request_line.trim_end_matches(" HTTP/1.1").to_string())
}

fn compile_response(status_code: u16, content_type: Option<&str>, body: &str) -> String {
    // The status of an answer which has no body - and so no content-length either.
    if status_code == 204 {
        return "HTTP/1.1 204 No Content\r\n\r\n".to_string();
    }

    let content_type = match content_type {
        Some(content_type) => format!("Content-Type: {}\r\n", content_type),
        None => String::new(),
    };

    format!(
        "HTTP/1.1 {} Stub\r\n{}Content-Length: {}\r\n\r\n{}",
        status_code,
        content_type,
        body.len(),
        body
    )
}

/// The error of a call which got an answer it has no meaning for: it names the call, the
/// table and the status, and carries the body.
fn assert_unexpected_status<T>(
    result: Result<T, DataWriterError>,
    operation: &str,
    status_code: u16,
    body: &str,
) {
    match result {
        Err(DataWriterError::Error(message)) => assert_eq!(
            message,
            format!(
                "{} of table test-table returned status code {}. Body: {}",
                operation, status_code, body
            )
        ),
        Err(err) => panic!(
            "{}: status {} came back as {:?}",
            operation, status_code, err
        ),
        Ok(_) => panic!(
            "{}: status {} was read as an answer",
            operation, status_code
        ),
    }
}

/// Makes the calls of the writer - and the calls built on them - against whatever the stub
/// answers now. All of them but three: the two which create the table have
/// [`for_each_create_call`], and `get_rows_count` reports itself under a name of its own and
/// has an answer of its own to a missing table. Each call has a result type of its own, hence
/// a macro; `$expect` is one too, and is given the result of a call and the name the call
/// reports itself under.
macro_rules! for_each_call {
    ($writer:expr, $expect:ident) => {{
        let writer = &$writer;
        let rows = [row()];

        $expect!(writer.insert_entity(&rows[0]).await, "insert_entity");
        $expect!(
            writer.insert_or_replace_entity(&rows[0]).await,
            "insert_or_replace_entity"
        );
        $expect!(writer.replace_entity(&rows[0]).await, "replace_entity");
        $expect!(
            writer.bulk_insert_or_replace(&rows).await,
            "bulk_insert_or_replace"
        );
        $expect!(
            writer.bulk_insert_or_update_with_own_timestamp(&rows).await,
            "bulk_insert_or_update_with_own_timestamp"
        );
        $expect!(writer.bulk_delete(&rows_to_delete()).await, "bulk_delete");
        $expect!(writer.bulk_delete_if(&[&rows[0]]).await, "bulk_delete_if");
        $expect!(
            writer
                .bulk_delete_if_rows(&[RowToDeleteIf::from_entity(&rows[0])])
                .await,
            "bulk_delete_if_rows"
        );
        $expect!(
            writer.insert_or_replace_entity_if_new(&rows[0]).await,
            "insert_or_replace_entity_if_new"
        );
        $expect!(
            writer.bulk_insert_or_replace_if_new(&rows).await,
            "bulk_insert_or_replace_if_new"
        );
        $expect!(
            writer.insert_or_replace_if_new_by_chunks_start(&rows).await,
            "insert_or_replace_if_new_by_chunks_start"
        );
        $expect!(
            writer
                .insert_or_replace_if_new_by_chunks_append("process", &rows)
                .await,
            "insert_or_replace_if_new_by_chunks_append"
        );
        $expect!(
            writer
                .insert_or_replace_if_new_by_chunks_commit("process")
                .await,
            "insert_or_replace_if_new_by_chunks_commit"
        );
        $expect!(
            writer
                .insert_or_replace_if_new_by_chunks_cancel("process")
                .await,
            "insert_or_replace_if_new_by_chunks_cancel"
        );
        $expect!(
            writer
                .clean_and_bulk_insert_by_chunks_with_own_timestamp_start(None, &rows)
                .await,
            "clean_and_bulk_insert_by_chunks_with_own_timestamp_start"
        );
        $expect!(
            writer
                .clean_and_bulk_insert_by_chunks_with_own_timestamp_append("process", &rows)
                .await,
            "clean_and_bulk_insert_by_chunks_with_own_timestamp_append"
        );
        $expect!(
            writer
                .clean_and_bulk_insert_by_chunks_with_own_timestamp_commit("process")
                .await,
            "clean_and_bulk_insert_by_chunks_with_own_timestamp_commit"
        );
        $expect!(
            writer
                .clean_and_bulk_insert_by_chunks_with_own_timestamp_cancel("process")
                .await,
            "clean_and_bulk_insert_by_chunks_with_own_timestamp_cancel"
        );

        $expect!(writer.get_entity("pk", "rk", None).await, "get_entity");
        $expect!(
            writer.get_by_partition_key("pk", None).await,
            "get_by_partition_key"
        );
        $expect!(writer.get_by_row_key("rk").await, "get_by_row_key");
        $expect!(writer.get_all().await, "get_all");
        $expect!(
            writer.get_partition_keys(None, None).await,
            "get_partition_keys"
        );
        $expect!(writer.delete_row("pk", "rk").await, "delete_row");
        $expect!(
            writer.delete_row_if("pk", "rk", time_stamp()).await,
            "delete_row_if"
        );
        $expect!(writer.delete_partitions(&["pk"]).await, "delete_partitions");
        $expect!(
            writer.clean_table_and_bulk_insert(&rows).await,
            "clean_table_and_bulk_insert"
        );
        $expect!(
            writer.clean_partition_and_bulk_insert("pk", &rows).await,
            "clean_partition_and_bulk_insert"
        );
        $expect!(
            writer
                .clean_table_and_bulk_insert_with_own_timestamp(&rows)
                .await,
            "clean_table_and_bulk_insert_with_own_timestamp"
        );
        $expect!(
            writer
                .clean_partition_and_bulk_insert_with_own_timestamp("pk", &rows)
                .await,
            "clean_partition_and_bulk_insert_with_own_timestamp"
        );

        $expect!(
            writer.get_enum_case_model::<TestCase>(None).await,
            "get_entity"
        );
        $expect!(
            writer
                .get_enum_case_models_by_partition_key::<TestCase>(None)
                .await,
            "get_by_partition_key"
        );
        $expect!(writer.delete_enum_case::<TestCase>().await, "delete_row");
        $expect!(
            writer.delete_enum_case_with_row_key::<TestCase>("rk").await,
            "delete_row"
        );
        $expect!(writer.delete_entity_if(&rows[0]).await, "delete_row_if");
        $expect!(writer.update_entity("pk", "rk", |_| {}).await, "get_entity");
        $expect!(
            writer
                .insert_or_update("pk", "rk", row, |_: &mut TestEntity| true)
                .await,
            "get_entity"
        );
        $expect!(
            writer
                .bulk_insert_or_replace_if_new_by_chunks(&rows, 10)
                .await,
            "insert_or_replace_if_new_by_chunks_start"
        );
        $expect!(
            writer
                .clean_and_bulk_insert_by_chunks_with_own_timestamp(None, &rows, 10)
                .await,
            "clean_and_bulk_insert_by_chunks_with_own_timestamp_start"
        );
    }};
}

/// The two calls which create the table, made the way [`for_each_call`] makes the others. They
/// are kept apart for one answer: the API never gives them a 409, and they do not read one as
/// the conflict of an optimistic-concurrency write.
macro_rules! for_each_create_call {
    ($writer:expr, $expect:ident) => {{
        let writer = &$writer;

        $expect!(writer.create_table(table_params()).await, "create_table");
        $expect!(
            writer.create_table_if_not_exists(&table_params()).await,
            "create_table_if_not_exists"
        );
    }};
}

#[tokio::test]
async fn delete_partitions_asks_for_the_route_the_server_has() {
    let stub = StubServer::start().await;
    let writer = stub.writer();

    writer.delete_partitions(&["a", "b"]).await.unwrap();

    assert_eq!(
        stub.take_request(),
        "DELETE /api/Rows/DeletePartitions?syncPeriod=15&tableName=test-table&partitionKeys=a&partitionKeys=b"
    );

    // A parameter per key, each escaped on its own - never one value with the keys joined.
    writer
        .with_retries(2)
        .delete_partitions(&["x&y=z", "a,b"])
        .await
        .unwrap();

    assert_eq!(
        stub.take_request(),
        "DELETE /api/Rows/DeletePartitions?syncPeriod=15&tableName=test-table&partitionKeys=x%26y%3Dz&partitionKeys=a%2Cb"
    );

    // Nothing to delete - nothing to ask for.
    writer.delete_partitions(&[]).await.unwrap();
    writer.with_retries(2).delete_partitions(&[]).await.unwrap();

    assert!(stub.take_requests().is_empty());
}

#[tokio::test]
async fn nothing_to_delete_is_still_a_call_of_the_writer() {
    const AUTO_CREATE: &str = "POST /Tables/CreateIfNotExists?syncPeriod=1&tableName=test-table";

    // An empty list sends no delete request. In everything else the call is made the way
    // `bulk_delete` is made with nothing to delete: the first call of a writer creates its
    // table...
    let stub = StubServer::start().await;

    let writer = stub.writer_which_creates_its_table();
    writer.delete_partitions(&[]).await.unwrap();
    assert_eq!(stub.take_requests(), vec![AUTO_CREATE]);

    let writer = stub.writer_which_creates_its_table();
    writer.with_retries(2).delete_partitions(&[]).await.unwrap();
    assert_eq!(stub.take_requests(), vec![AUTO_CREATE]);

    let writer = stub.writer_which_creates_its_table();
    writer.bulk_delete(&BTreeMap::new()).await.unwrap();
    assert_eq!(stub.take_requests(), vec![AUTO_CREATE]);

    // ...and a connection string which can not be parsed is an error, never a "done".
    let writer: MyNoSqlDataWriter<TestEntity> =
        MyNoSqlDataWriter::create_with_builder(Arc::new(TestSettings {
            url: format!("host={};namespace=alpha", stub.url),
        }))
        .use_h1()
        .build();

    for result in [
        writer.delete_partitions(&[]).await,
        writer.with_retries(2).delete_partitions(&[]).await,
        writer.bulk_delete(&BTreeMap::new()).await,
    ] {
        match result {
            Err(DataWriterError::InvalidConnectionString(_)) => {}
            other => panic!("{:?}", other),
        }
    }

    assert!(stub.take_requests().is_empty());
}

#[tokio::test]
async fn deletes_carry_the_sync_period_of_the_writer() {
    let stub = StubServer::start().await;
    let writer = stub.writer();
    let with_retries = writer.with_retries(2);

    // The writer and its wrapper send one and the same request.
    let from_both = |request: &str| vec![request.to_string(); 2];

    writer.delete_row("pk", "rk").await.unwrap();
    with_retries.delete_row("pk", "rk").await.unwrap();

    assert_eq!(
        stub.take_requests(),
        from_both("DELETE /api/Row?syncPeriod=15&partitionKey=pk&rowKey=rk&tableName=test-table")
    );

    writer.delete_enum_case::<TestCase>().await.unwrap();
    with_retries.delete_enum_case::<TestCase>().await.unwrap();

    assert_eq!(
        stub.take_requests(),
        from_both(
            "DELETE /api/Row?syncPeriod=15&partitionKey=case-pk&rowKey=case-rk&tableName=test-table"
        )
    );

    writer
        .delete_enum_case_with_row_key::<TestCase>("rk")
        .await
        .unwrap();
    with_retries
        .delete_enum_case_with_row_key::<TestCase>("rk")
        .await
        .unwrap();

    assert_eq!(
        stub.take_requests(),
        from_both(
            "DELETE /api/Row?syncPeriod=15&partitionKey=case-pk&rowKey=rk&tableName=test-table"
        )
    );

    writer.delete_partitions(&["pk"]).await.unwrap();
    with_retries.delete_partitions(&["pk"]).await.unwrap();

    assert_eq!(
        stub.take_requests(),
        from_both(
            "DELETE /api/Rows/DeletePartitions?syncPeriod=15&tableName=test-table&partitionKeys=pk"
        )
    );
}

#[tokio::test]
async fn a_status_a_call_has_no_meaning_for_is_an_error_and_never_an_answer() {
    let stub = StubServer::start().await;
    let writer = stub.writer();

    // The server is there, its tables are not loaded yet.
    stub.answers(503, NOT_READY);

    macro_rules! expect_unexpected_status {
        ($result:expr, $operation:expr) => {
            assert_unexpected_status($result, $operation, 503, NOT_READY)
        };
    }

    for_each_call!(writer, expect_unexpected_status);
    for_each_create_call!(writer, expect_unexpected_status);

    // The call the others follow: the counter has always answered this way.
    assert_unexpected_status(
        writer.get_rows_count(Some("pk")).await,
        "Rows count",
        503,
        NOT_READY,
    );

    // A server which has failed to serve the call is read by every call the same way.
    stub.answers(500, "Internal server error");

    macro_rules! expect_internal_server_error {
        ($result:expr, $operation:expr) => {
            assert_unexpected_status($result, $operation, 500, "Internal server error")
        };
    }

    for_each_call!(writer, expect_internal_server_error);
    for_each_create_call!(writer, expect_internal_server_error);

    assert_unexpected_status(
        writer.get_rows_count(Some("pk")).await,
        "Rows count",
        500,
        "Internal server error",
    );

    // It is not about the 503: whatever status a call has no meaning for ends the same way.
    // A body which is empty still leaves an error which says what was asked and what came back.
    for (status_code, body) in [
        (500, "Internal server error"),
        (502, "Application is shutting down"),
        (401, "Unauthorized"),
        (431, ""),
    ] {
        stub.answers(status_code, body);

        assert_unexpected_status(
            writer.get_by_partition_key("pk", None).await,
            "get_by_partition_key",
            status_code,
            body,
        );
        assert_unexpected_status(
            writer.clean_table_and_bulk_insert(&[row()]).await,
            "clean_table_and_bulk_insert",
            status_code,
            body,
        );
        assert_unexpected_status(
            writer.bulk_insert_or_replace(&[row()]).await,
            "bulk_insert_or_replace",
            status_code,
            body,
        );
    }

    // Nor is it about failures only. DeleteIf hands the deleted row back with a 200 and says
    // "no such row" with a 404: a 2xx without the row is neither of them.
    stub.answers(204, "");

    assert_unexpected_status(
        writer.delete_row_if("pk", "rk", time_stamp()).await,
        "delete_row_if",
        204,
        "",
    );
}

#[tokio::test]
async fn a_404_is_nothing_there_only_when_it_is_the_one_of_the_api() {
    let stub = StubServer::start().await;
    let writer = stub.writer();

    // What a read by both keys and DeleteIf are answered with when the row - or its whole
    // partition - is not in the table.
    stub.answers(404, RECORD_NOT_FOUND);

    assert_eq!(writer.get_entity("pk", "rk", None).await.unwrap(), None);
    assert_eq!(
        writer
            .delete_row_if("pk", "rk", time_stamp())
            .await
            .unwrap(),
        None
    );
    assert_eq!(writer.delete_entity_if(&row()).await.unwrap(), None);
    assert_eq!(
        writer.get_enum_case_model::<TestCase>(None).await.unwrap(),
        None
    );
    assert_eq!(
        writer.update_entity("pk", "rk", |_| {}).await.unwrap(),
        None
    );

    // What a server which does not serve the route answers - or something in front of it. It
    // says nothing about the row.
    stub.answers(404, NO_ROUTE);

    assert_unexpected_status(
        writer.get_entity("pk", "rk", None).await,
        "get_entity",
        404,
        NO_ROUTE,
    );
    assert_unexpected_status(
        writer.delete_row_if("pk", "rk", time_stamp()).await,
        "delete_row_if",
        404,
        NO_ROUTE,
    );

    // Nor about anything else: to every call it is an answer the call has no meaning for.
    macro_rules! expect_no_route {
        ($result:expr, $operation:expr) => {
            assert_unexpected_status($result, $operation, 404, NO_ROUTE)
        };
    }

    for_each_call!(writer, expect_no_route);
    for_each_create_call!(writer, expect_no_route);

    // The API never answers the other calls with a 404, so there it can only be the second
    // kind - whatever it says.
    for body in [RECORD_NOT_FOUND, NO_ROUTE] {
        stub.answers(404, body);

        assert_unexpected_status(
            writer.get_by_partition_key("pk", None).await,
            "get_by_partition_key",
            404,
            body,
        );
        assert_unexpected_status(
            writer.get_by_row_key("rk").await,
            "get_by_row_key",
            404,
            body,
        );
        assert_unexpected_status(writer.get_all().await, "get_all", 404, body);
        assert_unexpected_status(writer.delete_row("pk", "rk").await, "delete_row", 404, body);
        assert_unexpected_status(
            writer.delete_partitions(&["pk"]).await,
            "delete_partitions",
            404,
            body,
        );
        assert_unexpected_status(
            writer.clean_table_and_bulk_insert(&[row()]).await,
            "clean_table_and_bulk_insert",
            404,
            body,
        );
        assert_unexpected_status(
            writer.insert_entity(&row()).await,
            "insert_entity",
            404,
            body,
        );
        assert_unexpected_status(
            writer.bulk_insert_or_replace(&[row()]).await,
            "bulk_insert_or_replace",
            404,
            body,
        );
        assert_unexpected_status(
            writer.bulk_delete_if(&[&row()]).await,
            "bulk_delete_if",
            404,
            body,
        );

        assert_unexpected_status(
            writer.get_partition_keys(None, None).await,
            "get_partition_keys",
            404,
            body,
        );
    }

    // The chunked uploads have a 404 of the API of their own: the process is not known to the
    // server (any more). There is no error of its own for it, so it comes back naming the call
    // and carrying what the server said.
    stub.answers(404, "Bulk process process not found");

    assert_unexpected_status(
        writer
            .insert_or_replace_if_new_by_chunks_commit("process")
            .await,
        "insert_or_replace_if_new_by_chunks_commit",
        404,
        "Bulk process process not found",
    );
}

#[tokio::test]
async fn no_such_table_is_what_the_contract_of_the_server_says() {
    let stub = StubServer::start().await;
    let writer = stub.writer();

    // The server sends the contract with a 400. Should it ever move to a 404, it is read the
    // same way - it is the body which says it, not the status.
    for status_code in [400, 404] {
        stub.answers(status_code, TABLE_NOT_FOUND);

        match writer.get_partition_keys(None, None).await {
            Err(DataWriterError::TableNotFound(_)) => {}
            other => panic!("get_partition_keys, {}: {:?}", status_code, other),
        }

        assert_eq!(writer.get_rows_count(None).await.unwrap(), None);
    }

    // Every call names a missing table by the error which stands for it - never by the json
    // of the contract inside some other error.
    stub.answers(400, TABLE_NOT_FOUND);

    macro_rules! expect_table_not_found {
        ($result:expr, $operation:expr) => {
            match $result {
                Err(DataWriterError::TableNotFound(message)) => {
                    assert_eq!(message, "Table 'test-table' not found", "{}", $operation)
                }
                Err(err) => panic!("{}: {:?}", $operation, err),
                Ok(_) => panic!("{}: a missing table was read as an answer", $operation),
            }
        };
    }

    for_each_call!(writer, expect_table_not_found);
    for_each_create_call!(writer, expect_table_not_found);

    // A 404 without the contract is nobody answering about the table.
    stub.answers(404, NO_ROUTE);

    assert_unexpected_status(
        writer.get_partition_keys(None, None).await,
        "get_partition_keys",
        404,
        NO_ROUTE,
    );
    assert_unexpected_status(
        writer.get_rows_count(None).await,
        "Rows count",
        404,
        NO_ROUTE,
    );
}

#[tokio::test]
async fn a_2xx_which_does_not_carry_what_the_call_reads_is_an_error_and_not_a_panic() {
    let stub = StubServer::start().await;
    let writer = stub.writer();

    /// The error names the call and the table; the rest of it says what was wrong with the
    /// body.
    fn assert_unreadable<T>(result: Result<T, DataWriterError>, starts_with: &str) {
        match result {
            Err(DataWriterError::Error(message)) => assert!(
                message.starts_with(starts_with),
                "'{}' does not start with '{}'",
                message,
                starts_with
            ),
            Err(err) => panic!("{}: {:?}", starts_with, err),
            Ok(_) => panic!("{}: read as an answer", starts_with),
        }
    }

    // No body where the call reads one, and a body which is somebody else's: the page of a
    // proxy, of a UI which answers every unknown route.
    for (status_code, body) in [(204, ""), (200, ""), (200, "<html><body>UI</body></html>")] {
        stub.answers(status_code, body);

        assert_unreadable(
            writer.get_entity("pk", "rk", None).await,
            format!(
                "get_entity of table test-table returned status code {} with a body which is not the row: ",
                status_code
            )
            .as_str(),
        );
        assert_unreadable(
            writer.get_enum_case_model::<TestCase>(None).await,
            "get_entity of table test-table returned status code ",
        );
        assert_unreadable(
            writer.update_entity("pk", "rk", |_| {}).await,
            "get_entity of table test-table returned status code ",
        );

        for result in [
            writer.get_by_partition_key("pk", None).await,
            writer.get_by_row_key("rk").await,
            writer.get_all().await,
        ] {
            assert_unreadable(result, "Can not deserialize entities for table: test-table. ");
        }
    }

    // The deletes hand the deleted row back with a 200 - and only with a 200.
    for body in ["", "<html><body>UI</body></html>"] {
        stub.answers(200, body);

        assert_unreadable(
            writer.delete_row("pk", "rk").await,
            "delete_row of table test-table returned status code 200 with a body which is not the row: ",
        );
        assert_unreadable(
            writer.delete_row_if("pk", "rk", time_stamp()).await,
            "delete_row_if of table test-table returned status code 200 with a body which is not the row: ",
        );
        assert_unreadable(
            writer.delete_enum_case::<TestCase>().await,
            "delete_row of table test-table returned status code 200 with a body which is not the row: ",
        );
    }

    // Rows of the table which do not fit the entity: the read fails as a whole, it neither
    // skips the row nor hands out a part of the list.
    stub.answers(
        200,
        format!(r#"[{},{{"PartitionKey":"pk","RowKey":5}}]"#, ROW).as_str(),
    );

    for result in [
        writer.get_by_partition_key("pk", None).await,
        writer.get_by_row_key("rk").await,
        writer.get_all().await,
    ] {
        assert_unreadable(
            result,
            "Table: 'test-table'. Can not deserialize entity: ",
        );
    }

    stub.answers(200, r#"{"PartitionKey":"pk","RowKey":5}"#);

    assert_unreadable(
        writer.get_entity("pk", "rk", None).await,
        "get_entity of table test-table returned status code 200 with a body which is not the row: ",
    );

    // An array which breaks off.
    stub.answers(200, format!("[{},", ROW).as_str());

    assert!(writer.get_all().await.is_err());
}

#[tokio::test]
async fn a_long_list_of_partitions_is_deleted_by_several_requests() {
    const ROUTE: &str = "DELETE /api/Rows/DeletePartitions?syncPeriod=15&tableName=test-table";

    /// The keys a request carries, the way they are written on the wire.
    fn keys_of(request: &str) -> Vec<&str> {
        let query = request.strip_prefix(ROUTE).expect(request);

        query
            .split("&partitionKeys=")
            .skip(1)
            .collect()
    }

    let stub = StubServer::start().await;
    let writer = stub.writer();

    // 36 characters each, like a guid: 51 bytes of a query string with the name of the parameter.
    let keys: Vec<String> = (0..200)
        .map(|no| format!("{:08}-0000-0000-0000-000000000000", no))
        .collect();
    let keys: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();

    writer.delete_partitions(&keys).await.unwrap();

    let requests = stub.take_requests();
    assert_eq!(requests.len(), 3);

    // Every key goes out once, in the order it came in.
    let sent: Vec<&str> = requests
        .iter()
        .flat_map(|request| keys_of(request))
        .collect();
    assert_eq!(sent, keys);

    // A request stays far below the 16 KB the server takes as a request head.
    for request in &requests {
        assert!(request.len() < 4 * 1024 + 256, "{} bytes", request.len());
    }

    // The wrapper sends the very same requests.
    writer.with_retries(2).delete_partitions(&keys).await.unwrap();
    assert_eq!(stub.take_requests(), requests);

    // A key is counted as what it takes on the wire - here six bytes for each of its letters.
    let escaped_keys: Vec<String> = (0..200).map(|no| format!("ключ-{:03}", no)).collect();
    let escaped_keys: Vec<&str> = escaped_keys.iter().map(|key| key.as_str()).collect();

    writer.delete_partitions(&escaped_keys).await.unwrap();

    let requests = stub.take_requests();
    assert!(requests.len() > 1);
    assert_eq!(
        requests
            .iter()
            .map(|request| keys_of(request).len())
            .sum::<usize>(),
        escaped_keys.len()
    );

    for request in &requests {
        assert!(request.len() < 4 * 1024 + 256, "{} bytes", request.len());
    }

    // A request which fails ends the call with its error: the requests before it have been
    // made - their partitions are gone - and the ones after it are not made.
    stub.answers_after(1, 500, "Internal server error");

    assert_unexpected_status(
        writer.delete_partitions(&keys).await,
        "delete_partitions",
        500,
        "Internal server error",
    );
    assert_eq!(stub.take_requests().len(), 2);
}

#[tokio::test]
async fn the_answers_of_the_api_are_read_the_way_they_were() {
    let stub = StubServer::start().await;
    let writer = stub.writer();

    // A row.
    stub.answers(200, ROW);

    assert_eq!(
        writer.get_entity("pk", "rk", None).await.unwrap(),
        Some(row())
    );
    assert_eq!(writer.delete_row("pk", "rk").await.unwrap(), Some(row()));
    assert_eq!(
        writer
            .delete_row_if("pk", "rk", time_stamp())
            .await
            .unwrap(),
        Some(row())
    );
    assert_eq!(
        writer.delete_enum_case::<TestCase>().await.unwrap(),
        Some(TestCase(row()))
    );

    // Rows.
    stub.answers(200, format!("[{}]", ROW).as_str());

    assert_eq!(
        writer.get_by_partition_key("pk", None).await.unwrap(),
        Some(vec![row()])
    );
    assert_eq!(
        writer.get_by_row_key("rk").await.unwrap(),
        Some(vec![row()])
    );
    assert_eq!(writer.get_all().await.unwrap(), Some(vec![row()]));
    assert_eq!(
        writer
            .get_enum_case_models_by_partition_key::<TestCase>(None)
            .await
            .unwrap(),
        Some(vec![TestCase(row())])
    );

    // No rows: a partition - or a row key - which is not in the table is an empty array, not
    // a 404 and not a `None`.
    stub.answers(200, "[]");

    assert_eq!(
        writer.get_by_partition_key("pk", None).await.unwrap(),
        Some(vec![])
    );
    assert_eq!(writer.get_by_row_key("rk").await.unwrap(), Some(vec![]));
    assert_eq!(writer.get_all().await.unwrap(), Some(vec![]));

    // Partition keys - of a table which holds none as well.
    stub.answers(200, r#"{"amount":2,"data":["a","b"]}"#);

    assert_eq!(
        writer.get_partition_keys(None, None).await.unwrap(),
        vec!["a", "b"]
    );

    stub.answers(200, r#"{"amount":0,"data":[]}"#);

    assert!(writer
        .get_partition_keys(None, None)
        .await
        .unwrap()
        .is_empty());

    // Done, and nothing to hand back. To the delete of a row it says there was no such row or
    // no such partition.
    stub.answers(204, "");

    assert_eq!(writer.delete_row("pk", "rk").await.unwrap(), None);
    assert_eq!(writer.delete_enum_case::<TestCase>().await.unwrap(), None);
    writer.delete_partitions(&["pk"]).await.unwrap();
    writer.clean_table_and_bulk_insert(&[row()]).await.unwrap();
    writer
        .clean_partition_and_bulk_insert("pk", &[row()])
        .await
        .unwrap();
    writer
        .clean_table_and_bulk_insert_with_own_timestamp(&[row()])
        .await
        .unwrap();
    writer
        .clean_partition_and_bulk_insert_with_own_timestamp("pk", &[row()])
        .await
        .unwrap();
}

/// FlUrl reads no more than 10 MB of an answer unless it is told otherwise, and a table - a
/// partition even - is easily bigger than that. The writer reads answers of up to 100 MB.
#[tokio::test]
async fn an_answer_over_the_default_limit_of_fl_url_is_read() {
    let stub = StubServer::start().await;
    let writer = stub.writer();

    // A row with a field the entity does not have, which takes the answer past the limit.
    let big_row = format!(
        r#"{{"PartitionKey":"pk","RowKey":"rk","TimeStamp":"2026-08-12T18:19:36.776352","Padding":"{}"}}"#,
        "x".repeat(flurl::DEFAULT_MAX_RESPONSE_BODY_SIZE)
    );

    stub.answers(200, big_row.as_str());

    assert_eq!(
        writer.get_entity("pk", "rk", None).await.unwrap(),
        Some(row())
    );

    stub.answers(200, format!("[{}]", big_row).as_str());

    assert_eq!(writer.get_all().await.unwrap(), Some(vec![row()]));
}

/// The limit is the writer's to set - on the writer or on its builder - and the writer of
/// `with_retries` reads with it too: an answer of exactly the limit is read, a bigger one is
/// refused.
#[tokio::test]
async fn the_limit_of_an_answer_is_set_by_the_writer() {
    fn assert_too_large<T: std::fmt::Debug>(result: Result<T, DataWriterError>, limit: usize) {
        match result {
            Err(DataWriterError::FlUrlError(flurl::FlUrlError::ResponseBodyTooLarge {
                limit: reported,
            })) => assert_eq!(reported, limit),
            other => panic!("the answer over the limit was not refused: {:?}", other),
        }
    }

    let stub = StubServer::start().await;
    stub.answers(200, ROW);

    let mut writer = stub.writer();

    writer.set_body_size_limit(ROW.len());

    assert_eq!(
        writer.get_entity("pk", "rk", None).await.unwrap(),
        Some(row())
    );

    writer.set_body_size_limit(ROW.len() - 1);

    assert_too_large(writer.get_entity("pk", "rk", None).await, ROW.len() - 1);
    assert_too_large(
        writer
            .with_retries(1)
            .get_entity("pk", "rk", None)
            .await,
        ROW.len() - 1,
    );

    let writer = MyNoSqlDataWriter::<TestEntity>::create_with_builder(Arc::new(TestSettings {
        url: stub.url.clone(),
    }))
    .do_not_auto_create_table()
    .use_h1()
    .set_body_size_limit(ROW.len() - 1)
    .build();

    assert_too_large(writer.get_entity("pk", "rk", None).await, ROW.len() - 1);
}

#[tokio::test]
async fn the_errors_the_api_names_stay_typed() {
    let stub = StubServer::start().await;

    // The conflict of an optimistic-concurrency write. The API answers only DeleteIf and
    // Replace with it; it stands here for both statuses `check_error` makes typed errors of -
    // a 400 is the other - because it is the one which never leaves a line in the log.
    stub.answers(409, "Record is changed");

    macro_rules! expect_record_is_changed {
        ($result:expr, $operation:expr) => {
            match $result {
                // The routes of a chunked flow have a 409 of their own - see below.
                result if $operation.contains("_by_chunks") => {
                    assert_unexpected_status(result, $operation, 409, "Record is changed")
                }
                Err(DataWriterError::RecordIsChanged(message)) => {
                    assert_eq!(message, "Record is changed", "{}", $operation)
                }
                Err(err) => panic!("{}: {:?}", $operation, err),
                Ok(_) => panic!("{}: a conflict was read as an answer", $operation),
            }
        };
    }

    let writer = stub.writer();

    for_each_call!(writer, expect_record_is_changed);

    // The 409 of a chunked flow says that the process belongs to another writer session,
    // another table or another namespace. That is not a row somebody has rewritten: the error
    // names the call and carries what the server said.
    const DOES_NOT_MATCH: &str = "Bulk process process does not match: other session";

    stub.answers(409, DOES_NOT_MATCH);

    assert_unexpected_status(
        writer
            .insert_or_replace_if_new_by_chunks_append("process", &[row()])
            .await,
        "insert_or_replace_if_new_by_chunks_append",
        409,
        DOES_NOT_MATCH,
    );
    assert_unexpected_status(
        writer
            .insert_or_replace_if_new_by_chunks_commit("process")
            .await,
        "insert_or_replace_if_new_by_chunks_commit",
        409,
        DOES_NOT_MATCH,
    );
    assert_unexpected_status(
        writer
            .clean_and_bulk_insert_by_chunks_with_own_timestamp_cancel("process")
            .await,
        "clean_and_bulk_insert_by_chunks_with_own_timestamp_cancel",
        409,
        DOES_NOT_MATCH,
    );
}

#[tokio::test]
async fn get_partition_keys_is_sent_again_like_any_other_read_of_the_wrapper() {
    const PARTITION_KEYS: &str = r#"{"amount":1,"data":["pk"]}"#;

    // Every step has a stub of its own, so that it starts with no connection to it: one left
    // in the pool by the step before would be a send more of the first attempt.

    // How many times a single attempt knocks before it gives up is the business of the HTTP
    // client - it reconnects and replays a GET on its own - so it is measured, not assumed.
    let stub = StubServer::start().await;
    stub.leaves_unanswered(usize::MAX);

    assert!(stub.writer().get_partition_keys(None, None).await.is_err());

    let sends_of_an_attempt = stub.take_requests().len();
    assert!(sends_of_an_attempt > 0);

    // An attempt's worth of requests gets no answer and the next request does. The writer on
    // its own gives up before that one...
    let stub = StubServer::start().await;
    stub.answers(200, PARTITION_KEYS);
    stub.leaves_unanswered(sends_of_an_attempt);

    assert!(stub.writer().get_partition_keys(None, None).await.is_err());
    assert_eq!(stub.take_requests().len(), sends_of_an_attempt);

    // ...and one more attempt gets there.
    let stub = StubServer::start().await;
    stub.answers(200, PARTITION_KEYS);
    stub.leaves_unanswered(sends_of_an_attempt);

    assert_eq!(
        stub.writer()
            .with_retries(1)
            .get_partition_keys(None, None)
            .await
            .unwrap(),
        vec!["pk"]
    );
    assert_eq!(stub.take_requests().len(), sends_of_an_attempt + 1);

    // Which is what a read by keys has always done.
    let stub = StubServer::start().await;
    stub.answers(200, ROW);
    stub.leaves_unanswered(sends_of_an_attempt);

    assert_eq!(
        stub.writer()
            .with_retries(1)
            .get_entity("pk", "rk", None)
            .await
            .unwrap(),
        Some(row())
    );
    assert_eq!(stub.take_requests().len(), sends_of_an_attempt + 1);
}

#[tokio::test]
async fn a_2xx_which_is_a_page_of_html_is_not_a_success_of_any_call() {
    let stub = StubServer::start().await;
    let writer = stub.writer();

    // The answer of the deployed server to a route it does not have.
    stub.answers_as(200, "text/html", UI_PAGE);

    macro_rules! expect_unexpected_status {
        ($result:expr, $operation:expr) => {
            assert_unexpected_status($result, $operation, 200, UI_PAGE)
        };
    }

    for_each_call!(writer, expect_unexpected_status);
    for_each_create_call!(writer, expect_unexpected_status);

    assert_unexpected_status(
        writer.get_rows_count(Some("pk")).await,
        "Rows count",
        200,
        UI_PAGE,
    );

    // It is the type of the answer which says it - in any case and with any parameters - and
    // any 2xx at all.
    for (status_code, content_type) in [
        (200, "text/html; charset=utf-8"),
        (200, "Text/HTML"),
        (202, "text/html"),
    ] {
        stub.answers_as(status_code, content_type, UI_PAGE);

        assert_unexpected_status(
            writer.bulk_insert_or_replace(&[row()]).await,
            "bulk_insert_or_replace",
            status_code,
            UI_PAGE,
        );
        assert_unexpected_status(
            writer.delete_partitions(&["pk"]).await,
            "delete_partitions",
            status_code,
            UI_PAGE,
        );
    }

    // What the API sends is read the way it was, with the type it has there.
    stub.answers_as(200, "application/json", ROW);

    assert_eq!(
        writer.get_entity("pk", "rk", None).await.unwrap(),
        Some(row())
    );
    writer.replace_entity(&row()).await.unwrap();

    stub.answers_as(200, "text/plain; charset=utf-8", "5");

    assert_eq!(writer.get_rows_count(Some("pk")).await.unwrap(), Some(5));

    // A page is of any size, the error is not: it carries the head of the page and says how
    // much there was.
    let page = format!("<html>{}</html>", "x".repeat(5000));
    stub.answers_as(200, "text/html", page.as_str());

    match writer.bulk_insert_or_replace(&[row()]).await {
        Err(DataWriterError::Error(message)) => assert_eq!(
            message,
            format!(
                "bulk_insert_or_replace of table test-table returned status code 200. Body of 5013 bytes, cut to the first 1024: <html>{}",
                "x".repeat(1018)
            )
        ),
        other => panic!("{:?}", other),
    }

    // Nor is the table of a writer created by a page: the creation in front of its first
    // request fails that request - under its own name - and is asked for again by the next.
    let writer = stub.writer_which_creates_its_table();
    stub.answers_as(200, "text/html", UI_PAGE);

    assert_unexpected_status(
        writer.insert_or_replace_entity(&row()).await,
        "create_table_if_not_exists",
        200,
        UI_PAGE,
    );

    stub.answers(204, "");
    stub.take_requests();

    writer.insert_or_replace_entity(&row()).await.unwrap();

    assert_eq!(
        stub.take_requests(),
        vec![
            "POST /Tables/CreateIfNotExists?syncPeriod=1&tableName=test-table",
            "POST /Row/InsertOrReplace?syncPeriod=15&tableName=test-table",
        ]
    );
}

#[tokio::test]
async fn replace_takes_only_the_404_of_the_api_for_a_missing_row() {
    let stub = StubServer::start().await;
    let writer = stub.writer();

    // The row is not there - deleted between the read and the replace.
    stub.answers(404, RECORD_NOT_FOUND);

    match writer.replace_entity(&row()).await {
        Err(DataWriterError::RecordNotFound(message)) => assert_eq!(message, RECORD_NOT_FOUND),
        other => panic!("{:?}", other),
    }

    // Nobody serves the route: it says nothing about the row, so it is not a race
    // insert_or_update has lost. One read, one replace, and the error of the replace comes
    // back - not an attempt after another spent on reading the row again.
    stub.answers(200, ROW);
    stub.answers_to("PUT /Row/Replace", 404, NO_ROUTE);
    stub.take_requests();

    let result = writer
        .insert_or_update("pk", "rk", row, |_: &mut TestEntity| true)
        .await;

    assert_eq!(
        stub.take_requests(),
        vec![
            "GET /Row?partitionKey=pk&rowKey=rk&tableName=test-table",
            "PUT /Row/Replace?syncPeriod=15&tableName=test-table",
        ]
    );
    assert_unexpected_status(result, "replace_entity", 404, NO_ROUTE);

    assert_unexpected_status(
        writer.replace_entity(&row()).await,
        "replace_entity",
        404,
        NO_ROUTE,
    );

    // A row which keeps being deleted under the call is a race - lost once per attempt.
    stub.answers(200, ROW);
    stub.answers_to("PUT /Row/Replace", 404, RECORD_NOT_FOUND);
    stub.take_requests();

    match writer
        .insert_or_update_with_max_attempts("pk", "rk", 3, row, |_: &mut TestEntity| true)
        .await
    {
        Err(DataWriterError::RecordNotFound(message)) => assert_eq!(message, RECORD_NOT_FOUND),
        other => panic!("{:?}", other),
    }

    assert_eq!(stub.take_requests().len(), 6);
}

#[tokio::test]
async fn the_table_is_created_by_the_call_which_asks_for_it() {
    const TABLE_ALREADY_EXISTS: &str =
        r#"{"reason":"TableAlreadyExists","message":"Table already exists"}"#;

    const AUTO_CREATE: &str = "POST /Tables/CreateIfNotExists?syncPeriod=1&tableName=test-table";
    const INSERT_OR_REPLACE: &str = "POST /Row/InsertOrReplace?syncPeriod=15&tableName=test-table";

    // One request, with the parameters asked for - and no auto-creation in front of it, after
    // which the server could only answer that the table already exists.
    let stub = StubServer::start().await;
    let writer = stub.writer_which_creates_its_table();
    let with_retries = writer.with_retries(2);

    writer.create_table(table_params()).await.unwrap();

    assert_eq!(
        stub.take_requests(),
        vec!["POST /Tables/Create?tableName=test-table&syncPeriod=15&maxPartitionsAmount=3&persist=false"]
    );

    // Nor behind it: the server would put the auto-create parameters over the ones the table
    // has just been created with. That goes for the wrapper of the writer as well - the one
    // which was made before the table was created included.
    writer.insert_or_replace_entity(&row()).await.unwrap();
    with_retries.insert_or_replace_entity(&row()).await.unwrap();

    assert_eq!(stub.take_requests(), vec![INSERT_OR_REPLACE; 2]);

    // The same goes for the call which is content with a table which is already there.
    let stub = StubServer::start().await;
    let writer = stub.writer_which_creates_its_table();

    writer
        .create_table_if_not_exists(&table_params())
        .await
        .unwrap();
    writer.insert_or_replace_entity(&row()).await.unwrap();

    assert_eq!(
        stub.take_requests(),
        vec![
            "POST /Tables/CreateIfNotExists?syncPeriod=15&tableName=test-table&maxPartitionsAmount=3&persist=false",
            INSERT_OR_REPLACE
        ]
    );

    // A call which did not create the table leaves the auto-creation to the first request, as
    // it was.
    let stub = StubServer::start().await;
    let writer = stub.writer_which_creates_its_table();

    stub.answers(400, TABLE_ALREADY_EXISTS);

    match writer.create_table(table_params()).await {
        Err(DataWriterError::TableAlreadyExists(message)) => {
            assert_eq!(message, "Table already exists")
        }
        other => panic!("{:?}", other),
    }

    stub.answers(204, "");
    stub.take_requests();

    writer.insert_or_replace_entity(&row()).await.unwrap();
    writer.insert_or_replace_entity(&row()).await.unwrap();

    assert_eq!(
        stub.take_requests(),
        vec![AUTO_CREATE, INSERT_OR_REPLACE, INSERT_OR_REPLACE]
    );
}

#[tokio::test]
async fn a_body_which_stalls_does_not_hold_up_a_call_which_does_not_read_it() {
    let stub = StubServer::start().await;
    let writer = stub.writer();

    // Replace is done - the status says so - and the row it hands back stops half way. The
    // call has nothing to read there, so it does not wait for it: it returns what the status
    // said. A call which waited for a body it has no use for would hang here for good - the
    // writer sets no limit on the reading of a body.
    stub.answers_with_a_body_which_stalls(200, ROW);

    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        writer.replace_entity(&row()),
    )
    .await
    .expect("the call is still waiting for a body it does not read")
    .unwrap();

    // ...and the next call is not held up by it either.
    stub.answers(204, "");

    writer.replace_entity(&row()).await.unwrap();
}

#[tokio::test]
async fn the_writes_are_sent_and_read_the_way_they_were() {
    const SKIPPED: &str =
        r#"{"deleted":1,"skipped":[{"PartitionKey":"pk","RowKey":"rk","Reason":"NotFound"}]}"#;

    let stub = StubServer::start().await;
    let writer = stub.writer();
    let rows = [row(), row(), row()];

    // Done, and nothing to hand back.
    writer.insert_entity(&rows[0]).await.unwrap();
    writer.insert_or_replace_entity(&rows[0]).await.unwrap();
    writer.replace_entity(&rows[0]).await.unwrap();
    writer.bulk_insert_or_replace(&rows).await.unwrap();
    writer
        .bulk_insert_or_update_with_own_timestamp(&rows)
        .await
        .unwrap();
    writer.bulk_delete(&rows_to_delete()).await.unwrap();
    writer
        .insert_or_replace_entity_if_new(&rows[0])
        .await
        .unwrap();
    writer.bulk_insert_or_replace_if_new(&rows).await.unwrap();

    assert_eq!(
        stub.take_requests(),
        vec![
            "POST /Row/Insert?syncPeriod=15&tableName=test-table",
            "POST /Row/InsertOrReplace?syncPeriod=15&tableName=test-table",
            "PUT /Row/Replace?syncPeriod=15&tableName=test-table",
            "POST /Bulk/InsertOrReplace?syncPeriod=15&tableName=test-table",
            "POST /Bulk/InsertOrReplace?syncPeriod=15&tableName=test-table&useTimestamp=true",
            "POST /api/Bulk/Delete?syncPeriod=15&tableName=test-table",
            "POST /api/Row/InsertOrReplaceIfNew?syncPeriod=15&tableName=test-table",
            "POST /api/Bulk/InsertOrReplaceIfNew?syncPeriod=15&tableName=test-table",
        ]
    );

    // The key is taken - the answer Insert has an error of its own for.
    stub.answers(
        400,
        r#"{"reason":"RecordAlreadyExists","message":"Record already exists"}"#,
    );

    match writer.insert_entity(&rows[0]).await {
        Err(DataWriterError::RecordAlreadyExists(message)) => {
            assert_eq!(message, "Record already exists")
        }
        other => panic!("{:?}", other),
    }

    // A 400 which is not the contract of the server is carried as it came.
    stub.answers(400, "Bad request");

    match writer.bulk_insert_or_replace(&rows).await {
        Err(DataWriterError::Error(message)) => assert_eq!(message, "Bad request"),
        other => panic!("{:?}", other),
    }

    // The conditional bulk delete hands back what it deleted and what it left.
    stub.answers(200, SKIPPED);
    stub.take_requests();

    for result in [
        writer.bulk_delete_if(&[&rows[0]]).await.unwrap(),
        writer
            .bulk_delete_if_rows(&[RowToDeleteIf::from_entity(&rows[0])])
            .await
            .unwrap(),
    ] {
        assert_eq!(result.deleted, 1);
        assert_eq!(result.not_found().count(), 1);
    }

    assert_eq!(
        stub.take_requests(),
        vec!["POST /api/Bulk/DeleteIf?syncPeriod=15&tableName=test-table"; 2]
    );

    // A chunked upload: the first chunk starts the process, the others carry its id, and so
    // does the commit.
    stub.answers(200, r#"{"processId":"p1"}"#);

    writer
        .bulk_insert_or_replace_if_new_by_chunks(&rows, 2)
        .await
        .unwrap();

    assert_eq!(
        stub.take_requests(),
        vec![
            "POST /api/Bulk/InsertOrReplaceIfNewByChunks?tableName=test-table",
            "POST /api/Bulk/InsertOrReplaceIfNewByChunks?tableName=test-table&processId=p1",
            "POST /api/Bulk/InsertOrReplaceIfNewByChunksCommit?processId=p1&syncPeriod=15",
        ]
    );

    writer
        .clean_and_bulk_insert_by_chunks_with_own_timestamp(Some("pk"), &rows, 2)
        .await
        .unwrap();

    assert_eq!(
        stub.take_requests(),
        vec![
            "POST /api/Bulk/CleanAndBulkInsertByChunks?tableName=test-table&useTimestamp=true&partitionKey=pk",
            "POST /api/Bulk/CleanAndBulkInsertByChunks?tableName=test-table&useTimestamp=true&processId=p1",
            "POST /api/Bulk/CleanAndBulkInsertByChunksCommit?processId=p1&syncPeriod=15",
        ]
    );
}

#[tokio::test]
async fn a_chunked_upload_which_fails_names_the_step_it_failed_at() {
    let stub = StubServer::start().await;
    let writer = stub.writer();
    let rows = [row(), row(), row()];

    // The chunks are taken, the commit is not: the upload is cancelled, and what comes back
    // is the failure of the commit - under the name of the commit.
    stub.answers(200, r#"{"processId":"p1"}"#);
    stub.answers_to(
        "POST /api/Bulk/InsertOrReplaceIfNewByChunksCommit",
        503,
        NOT_READY,
    );

    assert_unexpected_status(
        writer
            .bulk_insert_or_replace_if_new_by_chunks(&rows, 2)
            .await,
        "insert_or_replace_if_new_by_chunks_commit",
        503,
        NOT_READY,
    );

    assert_eq!(
        stub.take_requests(),
        vec![
            "POST /api/Bulk/InsertOrReplaceIfNewByChunks?tableName=test-table",
            "POST /api/Bulk/InsertOrReplaceIfNewByChunks?tableName=test-table&processId=p1",
            "POST /api/Bulk/InsertOrReplaceIfNewByChunksCommit?processId=p1&syncPeriod=15",
            "POST /api/Bulk/InsertOrReplaceIfNewByChunksCancel?processId=p1",
        ]
    );

    // The same with a chunk which is not taken, in the upload which replaces the whole table.
    stub.answers(200, r#"{"processId":"p1"}"#);
    stub.answers_to(
        "POST /api/Bulk/CleanAndBulkInsertByChunks?tableName=test-table&useTimestamp=true&processId=",
        503,
        NOT_READY,
    );

    assert_unexpected_status(
        writer
            .clean_and_bulk_insert_by_chunks_with_own_timestamp(None, &rows, 2)
            .await,
        "clean_and_bulk_insert_by_chunks_with_own_timestamp_append",
        503,
        NOT_READY,
    );

    assert_eq!(
        stub.take_requests(),
        vec![
            "POST /api/Bulk/CleanAndBulkInsertByChunks?tableName=test-table&useTimestamp=true",
            "POST /api/Bulk/CleanAndBulkInsertByChunks?tableName=test-table&useTimestamp=true&processId=p1",
            "POST /api/Bulk/CleanAndBulkInsertByChunksCancel?processId=p1",
        ]
    );
}
