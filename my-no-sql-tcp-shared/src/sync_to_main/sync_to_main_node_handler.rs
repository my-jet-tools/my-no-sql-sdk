use std::sync::Arc;

use rust_extensions::{events_loop::EventsLoop, AppStates, Logger};

use super::{
    sync_to_main_node_handler_inner::SyncToMainNodeHandlerInner, DataReaderTcpConnection,
    SyncToMainNodeEvent, UpdateEntityStatisticsData,
};

pub struct SyncToMainNodeHandler {
    pub inner: Arc<SyncToMainNodeHandlerInner>,
    events_loop: EventsLoop<SyncToMainNodeEvent>,
}

impl SyncToMainNodeHandler {
    pub fn new(logger: Arc<dyn Logger + Send + Sync + 'static>) -> Self {
        // The loop is started by `start` - there is nothing to wait for, so it gets
        // application states which are initialized from the very beginning.
        let events_loop = EventsLoop::new(
            "SyncToMainNodeQueues".to_string(),
            Arc::new(AppStates::create_initialized()),
            logger,
        );

        let events_publisher = events_loop.get_publisher();

        let inner = Arc::new(SyncToMainNodeHandlerInner::new(events_publisher));

        Self {
            inner,
            events_loop: events_loop,
        }
    }

    pub fn start(&self) {
        self.events_loop.register_event_loop(self.inner.clone());
        self.events_loop.start();
    }

    pub fn tcp_events_pusher_new_connection_established(
        &self,
        connection: Arc<DataReaderTcpConnection>,
    ) {
        self.inner
            .events_publisher
            .send(SyncToMainNodeEvent::Connected(connection));
    }

    pub fn tcp_events_pusher_connection_disconnected(
        &self,
        connection: Arc<DataReaderTcpConnection>,
    ) {
        self.inner
            .events_publisher
            .send(SyncToMainNodeEvent::Disconnected(connection));
    }

    pub fn tcp_events_pusher_got_confirmation(&self, confirmation_id: i64) {
        self.inner
            .events_publisher
            .send(SyncToMainNodeEvent::Delivered(confirmation_id));
    }

    /// Queues what a read reports back - a reader calls it on every read which reports, a node
    /// for whatever its readers report.
    ///
    /// The events loop is pinged only when the ping has something to do: the read has queued
    /// something, there is a connection to the main node and nothing is on delivery. A read
    /// which matched no row and reports rows only queues nothing - it has nothing to send.
    /// While a report is on delivery the next one leaves with its confirmation, and while
    /// there is no connection - with the reconnect; a ping would be taken off the loop, find
    /// that out and be dropped. Posted on every read, such pings are a message of the loop
    /// per read: reads which come faster than the loop serves its messages pile them up
    /// without a limit and leave the confirmations of the main node waiting behind them, the
    /// reports get later and later, and the main node expires rows which are being read.
    pub fn update<'s, TRowKeys: Iterator<Item = &'s str>>(
        &self,
        table_name: &str,
        partition_key: &str,
        row_keys: impl Fn() -> TRowKeys,
        data: &UpdateEntityStatisticsData,
    ) {
        if !data.has_data_to_update() {
            return;
        }

        // A read which matched no row has no row to report. When rows are all it reports, the
        // queues take nothing from it: there is nothing to queue and nothing to send
        let reports_the_partition =
            data.partition_last_read_moment || data.partition_expiration_moment.is_some();

        if !reports_the_partition && row_keys().next().is_none() {
            return;
        }

        let mut inner = self.inner.queues.lock();

        if data.partition_last_read_moment {
            inner
                .update_partitions_last_read_time_queue
                .add_partition(table_name, partition_key);
        }

        if let Some(partition_expiration) = data.partition_expiration_moment {
            inner.update_partition_expiration_time_update.add(
                table_name,
                partition_key,
                partition_expiration,
            );
        }

        if data.row_last_read_moment {
            inner
                .update_rows_last_read_time_queue
                .add(table_name, partition_key, row_keys());
        }

        if let Some(row_expiration) = data.row_expiration_moment {
            inner.update_rows_expiration_time_queue.add(
                table_name,
                partition_key,
                row_keys(),
                row_expiration,
            );
        }

        if inner.on_delivery.is_none() && inner.connection.is_some() {
            self.inner
                .events_publisher
                .send(SyncToMainNodeEvent::PingToDeliver);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use my_tcp_sockets::ThreadsStatistics;
    use rust_extensions::{
        events_loop::{EventsLoopTick, RepeatIteration},
        Logger,
    };

    use crate::{sync_to_main::DeliverToMainNodeEvent, MyNoSqlReaderTcpSerializer};

    use super::{
        DataReaderTcpConnection, SyncToMainNodeEvent, SyncToMainNodeHandler,
        UpdateEntityStatisticsData,
    };

    struct TestLogger;

    impl Logger for TestLogger {
        fn write_info(&self, _: String, _: String, _: Option<HashMap<String, String>>) {}
        fn write_warning(&self, _: String, _: String, _: Option<HashMap<String, String>>) {}
        fn write_error(&self, _: String, _: String, _: Option<HashMap<String, String>>) {}
        fn write_fatal_error(&self, _: String, _: String, _: Option<HashMap<String, String>>) {}
        fn write_debug_info(&self, _: String, _: String, _: Option<HashMap<String, String>>) {}
    }

    /// `update` is what fills the queues: a reader calls it on every read which reports back, a
    /// node - for whatever its readers report. While there is no connection to the main node
    /// nothing leaves the queues, so the reads of the same row have to pile up as one event per
    /// queue - not as an event, or a row key, per read.
    #[test]
    fn reads_of_the_same_row_do_not_grow_the_queues_while_there_is_no_connection() {
        let handler = SyncToMainNodeHandler::new(Arc::new(TestLogger));

        let data = UpdateEntityStatisticsData {
            partition_last_read_moment: true,
            row_last_read_moment: true,
            ..Default::default()
        };

        for _ in 0..100 {
            handler.update("table", "pk", || ["rk"].into_iter(), &data);
        }

        let mut queues = handler.inner.queues.lock();

        assert!(queues.get_next_event_to_deliver(None).is_none());

        let event = queues
            .update_partitions_last_read_time_queue
            .dequeue()
            .unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!(
            vec!["pk"],
            event
                .partitions
                .keys()
                .map(|itm| itm.as_str())
                .collect::<Vec<_>>()
        );
        assert!(queues
            .update_partitions_last_read_time_queue
            .dequeue()
            .is_none());

        let event = queues.update_rows_last_read_time_queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!("pk", event.partition_key);
        assert_eq!(event.row_keys, ["rk"]);
        assert!(queues.update_rows_last_read_time_queue.dequeue().is_none());
    }

    /// A read through a filter which matched no row calls `update` without a single row key, and
    /// a node passes on whatever row keys its readers report. The partition is read all the
    /// same, but there is no row to report - an event queued for the rows would go to the main
    /// node as a packet without row keys.
    #[test]
    fn read_which_matched_no_row_queues_nothing_for_the_rows() {
        let handler = SyncToMainNodeHandler::new(Arc::new(TestLogger));

        let data = UpdateEntityStatisticsData {
            partition_last_read_moment: true,
            row_last_read_moment: true,
            row_expiration_moment: Some(None),
            ..Default::default()
        };

        handler.update("table", "pk", || [].into_iter(), &data);

        let mut queues = handler.inner.queues.lock();

        let event = queues
            .update_partitions_last_read_time_queue
            .dequeue()
            .unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!(
            vec!["pk"],
            event
                .partitions
                .keys()
                .map(|itm| itm.as_str())
                .collect::<Vec<_>>()
        );

        assert!(queues.update_rows_last_read_time_queue.dequeue().is_none());
        assert!(queues.update_rows_expiration_time_queue.dequeue().is_none());
    }

    /// Stands for the events loop of the handler: counts the pings `update` posts. A
    /// confirmation is used as a marker - the events are served in the order they were posted,
    /// so once the marker is seen, everything posted before it has been counted.
    #[derive(Default)]
    struct PostedEvents {
        pings: AtomicUsize,
        markers: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl EventsLoopTick<SyncToMainNodeEvent> for PostedEvents {
        async fn started(&self) {}

        async fn tick(&self, event: SyncToMainNodeEvent) -> RepeatIteration<SyncToMainNodeEvent> {
            match event {
                SyncToMainNodeEvent::PingToDeliver => {
                    self.pings.fetch_add(1, Ordering::SeqCst);
                }
                SyncToMainNodeEvent::Delivered(_) => {
                    self.markers.fetch_add(1, Ordering::SeqCst);
                }
                _ => {}
            }

            RepeatIteration::No
        }

        async fn finished(&self) {}
    }

    impl PostedEvents {
        /// How many pings were posted since the previous call.
        async fn pings_since_the_last_look(&self, handler: &SyncToMainNodeHandler) -> usize {
            let markers_before = self.markers.load(Ordering::SeqCst);
            handler.tcp_events_pusher_got_confirmation(0);

            for _ in 0..5000 {
                if self.markers.load(Ordering::SeqCst) > markers_before {
                    return self.pings.swap(0, Ordering::SeqCst);
                }

                tokio::time::sleep(Duration::from_millis(1)).await;
            }

            panic!("the events loop has not served the marker");
        }
    }

    /// A connection with no socket behind it - the queues send nothing themselves, they only
    /// hand the connection out together with the event.
    async fn connection() -> Arc<DataReaderTcpConnection> {
        let connection = DataReaderTcpConnection::new(
            Arc::new("test".to_string()),
            None,
            1,
            None,
            Arc::new(TestLogger),
            1024 * 1024,
            Duration::from_secs(3),
            Duration::from_secs(3),
            Arc::new(ThreadsStatistics::default()),
            MyNoSqlReaderTcpSerializer::new(),
            (),
        )
        .await;

        Arc::new(connection)
    }

    /// A read which queued nothing has nothing to send, whatever the state of the connection:
    /// a read through a filter which matched no row and which reports rows only. On a
    /// connection which has nothing on delivery - an idle one - its ping would be a message of
    /// the loop per read again: reads in a tight loop pile them up faster than the loop takes
    /// them off, and the memory of the process grows without a limit.
    #[tokio::test]
    async fn read_which_queued_nothing_does_not_ping_the_events_loop() {
        let handler = SyncToMainNodeHandler::new(Arc::new(TestLogger));

        let posted = Arc::new(PostedEvents::default());
        handler.events_loop.register_event_loop(posted.clone());
        handler.events_loop.start();

        handler.inner.queues.lock().new_connection(connection().await);

        let rows_only = UpdateEntityStatisticsData {
            row_last_read_moment: true,
            row_expiration_moment: Some(None),
            ..Default::default()
        };

        // Connected, nothing on delivery - and nothing to deliver
        for _ in 0..100 {
            handler.update("table", "pk", || [].into_iter(), &rows_only);
        }
        assert_eq!(0, posted.pings_since_the_last_look(&handler).await);

        // The same read with a row has a report to send
        handler.update("table", "pk", || ["rk"].into_iter(), &rows_only);
        assert_eq!(1, posted.pings_since_the_last_look(&handler).await);

        // ...and while that report has not left, a read which matched no row still has nothing
        // of its own to send
        for _ in 0..100 {
            handler.update("table", "pk", || [].into_iter(), &rows_only);
        }
        assert_eq!(0, posted.pings_since_the_last_look(&handler).await);

        // A read which matched no row and reports its partition as well has the partition to
        // report
        let rows_and_the_partition = UpdateEntityStatisticsData {
            partition_last_read_moment: true,
            row_last_read_moment: true,
            row_expiration_moment: Some(None),
            ..Default::default()
        };

        handler.update("table", "pk", || [].into_iter(), &rows_and_the_partition);
        assert_eq!(1, posted.pings_since_the_last_look(&handler).await);

        // What was queued: the row, twice, and the partition - nothing for the reads which
        // matched no row
        let mut queues = handler.inner.queues.lock();

        assert!(queues
            .update_partitions_last_read_time_queue
            .dequeue()
            .is_some());
        assert_eq!(
            vec!["rk"],
            queues
                .update_rows_last_read_time_queue
                .dequeue()
                .unwrap()
                .row_keys
        );
        assert_eq!(
            1,
            queues
                .update_rows_expiration_time_queue
                .dequeue()
                .unwrap()
                .row_keys
                .len()
        );

        assert!(queues.get_next_event_to_deliver(None).is_none());
    }

    /// A ping makes the events loop look for a report to send. It finds one to send only when
    /// there is a connection and nothing is on delivery - a report which is on delivery takes
    /// the next one out with its confirmation, a lost connection with the reconnect. `update`
    /// used to post a ping per queue it filled, on every read: reads which came as fast as the
    /// loop served its messages kept the confirmations waiting behind their pings, and the
    /// reports fell behind the reads without a limit.
    #[tokio::test]
    async fn read_pings_the_events_loop_only_when_a_report_can_leave() {
        let handler = SyncToMainNodeHandler::new(Arc::new(TestLogger));

        let posted = Arc::new(PostedEvents::default());
        handler.events_loop.register_event_loop(posted.clone());
        handler.events_loop.start();

        // Every queue is filled by a read
        let data = UpdateEntityStatisticsData {
            partition_last_read_moment: true,
            row_last_read_moment: true,
            partition_expiration_moment: Some(None),
            row_expiration_moment: Some(None),
        };

        let read = |row_key: &'static str| {
            handler.update("table", "pk", || [row_key].into_iter(), &data);
        };

        // No connection to the main node - nothing can leave, whatever is read
        for _ in 0..100 {
            read("rk1");
        }
        assert_eq!(0, posted.pings_since_the_last_look(&handler).await);

        // Connected, nothing on delivery - a read has something to send: one ping, not one for
        // every queue it filled
        handler.inner.queues.lock().new_connection(connection().await);

        read("rk1");
        assert_eq!(1, posted.pings_since_the_last_look(&handler).await);

        // What the events loop does with the ping: the first report goes on delivery
        let (_, on_delivery) = handler
            .inner
            .queues
            .lock()
            .get_next_event_to_deliver(None)
            .unwrap();

        // Reads keep coming while it is on delivery - not a single ping
        for _ in 0..100 {
            read("rk2");
        }
        assert_eq!(0, posted.pings_since_the_last_look(&handler).await);

        // ...and nothing waits for a ping which was not posted: every confirmation takes the
        // next report out, until the queues are empty
        let mut confirmed = on_delivery.get_confirmation_id();
        let mut reports = 1;

        loop {
            let next = handler
                .inner
                .queues
                .lock()
                .get_next_event_to_deliver(Some(confirmed));

            let Some((_, on_delivery)) = next else {
                break;
            };

            confirmed = on_delivery.get_confirmation_id();
            reports += 1;
        }

        // The four queues of the first read - what the reads behind it queued left with the
        // three which were still waiting - and the partition expiration once more: that one had
        // left before those reads came
        assert_eq!(5, reports);

        // Everything is delivered and confirmed - the next read pings again
        read("rk3");
        assert_eq!(1, posted.pings_since_the_last_look(&handler).await);

        // The connection is lost with a report on delivery - the reconnect takes it out
        assert!(handler
            .inner
            .queues
            .lock()
            .get_next_event_to_deliver(None)
            .is_some());
        handler.inner.queues.lock().disconnected();

        for _ in 0..100 {
            read("rk4");
        }
        assert_eq!(0, posted.pings_since_the_last_look(&handler).await);
    }

    /// Looks until there is something to see - the events loop of the handler works on its
    /// own, a millisecond at a time is waited for it.
    async fn seen<T>(what: &str, look: impl Fn() -> Option<T>) -> T {
        for _ in 0..5000 {
            if let Some(result) = look() {
                return result;
            }

            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        panic!("{}", what);
    }

    /// The confirmation id of the report which is on delivery.
    fn on_delivery(handler: &SyncToMainNodeHandler) -> Option<i64> {
        handler
            .inner
            .queues
            .lock()
            .on_delivery
            .as_ref()
            .map(|itm| itm.get_confirmation_id())
    }

    /// The other half of what `update` relies on, with the events loop of the handler itself
    /// and what it does on its ticks. A read does not ping while there is no connection, and
    /// not while a report is on delivery: what it queued then has to leave with the tick which
    /// makes it able to leave - the new connection, the confirmation of the report before it -
    /// and no further read is to be needed for it.
    #[tokio::test]
    async fn report_a_read_did_not_ping_for_leaves_with_the_connection_and_the_confirmations() {
        let handler = SyncToMainNodeHandler::new(Arc::new(TestLogger));
        handler.start();

        // Every queue is filled by a read
        let data = UpdateEntityStatisticsData {
            partition_last_read_moment: true,
            row_last_read_moment: true,
            partition_expiration_moment: Some(None),
            row_expiration_moment: Some(None),
        };

        // No connection to the main node: the read queues its reports and pings nothing
        handler.update("table", "pk", || ["rk1"].into_iter(), &data);

        // The connection is there: the first report leaves, and it is sent with that connection
        let connection = connection().await;
        handler.tcp_events_pusher_new_connection_established(connection.clone());

        let first = seen("the new connection has not taken a report out", || {
            on_delivery(&handler)
        })
        .await;

        seen("the report was not sent with the connection", || {
            let sent = connection.statistics().total_sent.load(Ordering::SeqCst);
            (sent > 0).then_some(())
        })
        .await;

        // A read while that report is on delivery pings nothing either
        handler.update("table", "pk", || ["rk2"].into_iter(), &data);

        // Every confirmation takes the next report out, until nothing is left
        let mut confirmed = first;
        let mut reports = 1;

        loop {
            handler.tcp_events_pusher_got_confirmation(confirmed);

            // The loop has served the confirmation once that report is off delivery
            let next = seen("the confirmation has not taken the report off delivery", || {
                let now = on_delivery(&handler);
                (now != Some(confirmed)).then_some(now)
            })
            .await;

            let Some(next) = next else {
                break;
            };

            confirmed = next;
            reports += 1;
        }

        // The four queues of the first read - what the second one queued left with the three
        // which were still waiting - and the partition expiration once more
        assert_eq!(5, reports);

        {
            let mut queues = handler.inner.queues.lock();
            assert!(queues.on_delivery.is_none());
            assert!(queues.get_next_event_to_deliver(None).is_none());
        }

        // Nothing on delivery now: the ping of the next read takes its report out
        let rows_expiration = UpdateEntityStatisticsData {
            row_expiration_moment: Some(None),
            ..Default::default()
        };

        handler.update("table", "pk", || ["rk3"].into_iter(), &rows_expiration);

        let on_its_way = seen("the ping of a read has not taken its report out", || {
            on_delivery(&handler)
        })
        .await;

        // The connection is lost with that report on delivery: it goes back to its queue, and
        // a read which comes meanwhile pings nothing
        handler.tcp_events_pusher_connection_disconnected(connection);

        seen("the lost connection is still there", || {
            handler.inner.queues.lock().connection.is_none().then_some(())
        })
        .await;

        assert_eq!(None, on_delivery(&handler));

        handler.update("table", "pk", || ["rk4"].into_iter(), &rows_expiration);

        // The next connection takes it out again - together with what was read meanwhile
        handler.tcp_events_pusher_new_connection_established(self::connection().await);

        let again = seen("the reconnect has not taken the returned report out", || {
            on_delivery(&handler).filter(|itm| *itm != on_its_way)
        })
        .await;

        match handler.inner.queues.lock().on_delivery.clone() {
            Some(DeliverToMainNodeEvent::UpdateRowsExpirationTime { event, .. }) => {
                assert_eq!(
                    vec!["rk3", "rk4"],
                    event
                        .row_keys
                        .keys()
                        .map(|itm| itm.as_str())
                        .collect::<Vec<_>>()
                );
            }
            other => panic!("Unexpected report on delivery: {:?}", other),
        }

        handler.tcp_events_pusher_got_confirmation(again);

        seen("the last confirmation was not served", || {
            on_delivery(&handler).is_none().then_some(())
        })
        .await;

        assert!(handler
            .inner
            .queues
            .lock()
            .get_next_event_to_deliver(None)
            .is_none());
    }
}
