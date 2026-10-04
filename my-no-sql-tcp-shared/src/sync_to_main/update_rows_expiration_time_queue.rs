use std::collections::BTreeMap;

use rust_extensions::auto_shrink::VecDequeAutoShrink;
use rust_extensions::date_time::DateTimeAsMicroseconds;

#[derive(Debug, Clone)]
pub struct UpdateRowsExpirationTimeEvent {
    pub table_name: String,
    pub partition_key: String,
    pub row_keys: BTreeMap<String, ()>,
    pub expiration_time: Option<DateTimeAsMicroseconds>,
}

/// Expiration moments of rows which wait to be delivered to the main node, one item per
/// partition.
///
/// A row is kept with the moment it was given last - [`Self::add`] overwrites, and
/// [`Self::return_event`] fills in only the rows which are not queued. So however the reads,
/// the deliveries and the disconnects interleave, the main node ends up with the last moment
/// set for a row, and reading the same rows over and over does not grow the queue.
///
/// An event carries one moment for all of its rows. The rows of a partition whose moments
/// fall into the same second leave together, with the latest of those moments - so a row may
/// get a moment which is later than the one it was given by less than a second, never an
/// earlier one - and a row which is given no expiration never shares an event with one which
/// is given a moment. That is what keeps a sliding expiration affordable: every read computes
/// a moment of its own (`now + ttl`), one event is on delivery at a time, and an event a row
/// would not keep up with the reads.
///
/// Nothing waits for ever while reads keep coming. A row keeps its place in the line of its
/// partition however many times it is set again while it waits - and when the event it left
/// with is returned, also when it was set again while that event was on delivery: the main
/// node has not got the row since the read which gave it the place. The row which waits
/// longest decides which rows leave with the next event; and a partition which still has rows
/// after an event goes behind the other partitions. So a row which is read all the time does
/// not hold back the rows behind it, and a partition which is read all the time does not hold
/// back the other partitions - also when deliveries get lost in between.
pub struct UpdateRowsExpirationTimeQueue {
    queue: VecDequeAutoShrink<RowsExpirationOfPartition>,
    /// The place of the next row which joins the line of its partition at its end. The places
    /// are counted for all partitions together, so a place is never given twice - also not
    /// when a partition has left the queue and comes back.
    last_place: i64,
    /// The place of the next row which is put in front of the line of its partition.
    first_place: i64,
    /// The event which was handed out last. One event is on delivery at a time, so it is the
    /// one [`Self::return_event`] gets when the delivery is lost.
    handed_out: Option<HandedOut>,
}

/// The rows of the event which was handed out last, with the places they had in the line of
/// their partition.
struct HandedOut {
    table_name: String,
    partition_key: String,
    places: BTreeMap<String, i64>,
}

struct QueuedRow {
    moment: Option<DateTimeAsMicroseconds>,
    /// The place of the row in the line of its partition - see [`RowsExpirationOfPartition::line`].
    place: i64,
}

struct RowsExpirationOfPartition {
    table_name: String,
    partition_key: String,
    rows: BTreeMap<String, QueuedRow>,
    /// The rows in the order they came in: place -> row key. The first one waits longest.
    line: BTreeMap<i64, String>,
}

impl RowsExpirationOfPartition {
    fn new(table_name: &str, partition_key: &str) -> Self {
        Self {
            table_name: table_name.to_string(),
            partition_key: partition_key.to_string(),
            rows: BTreeMap::new(),
            line: BTreeMap::new(),
        }
    }

    /// Sets the moment of the row. A row which is waiting already keeps its place, a row
    /// which is not joins the line at its end - `last_place` is the place it gets.
    fn set(
        &mut self,
        row_key: &str,
        moment: Option<DateTimeAsMicroseconds>,
        last_place: &mut i64,
    ) {
        if let Some(row) = self.rows.get_mut(row_key) {
            row.moment = moment;
            return;
        }

        let place = *last_place;
        *last_place += 1;

        self.put(row_key.to_string(), moment, place);
    }

    /// Puts a row which does not wait into the line, at the given place.
    fn put(&mut self, row_key: String, moment: Option<DateTimeAsMicroseconds>, place: i64) {
        let taken = self.line.insert(place, row_key.clone());
        debug_assert!(taken.is_none(), "The place {} is given twice", place);

        self.rows.insert(row_key, QueuedRow { moment, place });
    }

    /// Gives a row which waits the place it had before it left the line. It keeps its moment.
    fn move_to(&mut self, row_key: &str, place: i64) {
        let Some(row) = self.rows.get_mut(row_key) else {
            return;
        };

        if let Some(row_key) = self.line.remove(&row.place) {
            row.place = place;

            let taken = self.line.insert(place, row_key);
            debug_assert!(taken.is_none(), "The place {} is given twice", place);
        }
    }
}

/// The second a moment falls into - the rows of a partition which share it leave with one
/// event. No expiration is a group of its own.
fn second_of(moment: Option<DateTimeAsMicroseconds>) -> Option<i64> {
    moment.map(|moment| moment.unix_microseconds.div_euclid(1_000_000))
}

impl UpdateRowsExpirationTimeQueue {
    pub fn new() -> Self {
        Self {
            queue: VecDequeAutoShrink::new(32),
            last_place: 0,
            first_place: -1,
            handed_out: None,
        }
    }

    fn get_or_queue<'q>(
        queue: &'q mut VecDequeAutoShrink<RowsExpirationOfPartition>,
        table_name: &str,
        partition_key: &str,
    ) -> &'q mut RowsExpirationOfPartition {
        let index = queue
            .iter()
            .position(|itm| itm.table_name == table_name && itm.partition_key == partition_key);

        let index = match index {
            Some(index) => index,
            None => {
                queue.push_back(RowsExpirationOfPartition::new(table_name, partition_key));
                queue.len() - 1
            }
        };

        queue.get_mut(index).unwrap()
    }

    pub fn add<'s, TRowKeys: Iterator<Item = &'s str>>(
        &mut self,
        table_name: &str,
        partition_key: &str,
        row_keys: TRowKeys,
        date_time: Option<DateTimeAsMicroseconds>,
    ) {
        let mut row_keys = row_keys.peekable();

        // Nothing to deliver - nothing to queue
        if row_keys.peek().is_none() {
            return;
        }

        let item = Self::get_or_queue(&mut self.queue, table_name, partition_key);

        // The moment set last wins
        for row_key in row_keys {
            item.set(row_key, date_time, &mut self.last_place);
        }
    }

    /// Puts back the event which was on delivery when the connection to the main node got
    /// lost, so it is delivered again.
    ///
    /// Whatever is queued for a row by now was set after the returned event had been taken for
    /// delivery, so a row which is in both keeps the queued, newer moment. Queued as an event
    /// of its own, the returned one would be delivered last and bring the older moment back.
    ///
    /// Every row goes back to the place it had in the line of its partition - the one which
    /// is queued again as well: the read which queued it again came while the row was on
    /// delivery, and the main node still has not got the row since the read before it. Left
    /// at the end of the line, where that read put it, a row which is read all the time would
    /// lose its turn with every delivery which gets lost, and rows which came in after it
    /// would be delivered first - for ever, when deliveries keep getting lost.
    ///
    /// The place a row had is in front of every row which came in while it was on delivery -
    /// it has waited longer - and not in front of a row which was waiting before it: put in
    /// front of the whole line, the rows of a lost delivery would get past such a row, and it
    /// is that row which would never have its turn.
    pub fn return_event(&mut self, event: UpdateRowsExpirationTimeEvent) {
        if event.row_keys.is_empty() {
            return;
        }

        // The places the rows had - known when this is the event which was handed out last
        let is_handed_out_last = match &self.handed_out {
            Some(handed_out) => {
                handed_out.table_name == event.table_name
                    && handed_out.partition_key == event.partition_key
            }
            None => false,
        };

        let mut places = match is_handed_out_last {
            true => self.handed_out.take().unwrap().places,
            false => BTreeMap::new(),
        };

        let expiration_time = event.expiration_time;
        let item = Self::get_or_queue(
            &mut self.queue,
            event.table_name.as_str(),
            event.partition_key.as_str(),
        );

        // In reverse, so that rows whose places are not known - the event was not handed out
        // by this queue - end up in front of the line in the order of their keys
        for (row_key, _) in event.row_keys.into_iter().rev() {
            let place = places.remove(row_key.as_str());

            // Waits again already, with a moment which is newer than the one it was on
            // delivery with: the moment stays, the place is the one it had
            if item.rows.contains_key(row_key.as_str()) {
                if let Some(place) = place {
                    item.move_to(row_key.as_str(), place);
                }

                continue;
            }

            let place = match place {
                Some(place) => place,
                None => {
                    let place = self.first_place;
                    self.first_place -= 1;
                    place
                }
            };

            item.put(row_key, expiration_time, place);
        }
    }

    pub fn dequeue(&mut self) -> Option<UpdateRowsExpirationTimeEvent> {
        loop {
            let item = self.queue.get_mut(0)?;

            let RowsExpirationOfPartition { rows, line, .. } = item;

            // The row which waits longest decides which rows leave: the ones whose moments
            // fall into the same second as its own
            let second = match line.values().next() {
                Some(row_key) => second_of(rows[row_key].moment),
                None => {
                    self.queue.pop_front();
                    continue;
                }
            };

            let mut row_keys = BTreeMap::new();

            // The places the rows leave - they go back to them if the delivery gets lost
            let mut places = BTreeMap::new();

            // An event carries one moment: the latest of the ones its rows were given
            let mut expiration_time: Option<DateTimeAsMicroseconds> = None;

            rows.retain(|row_key, row| {
                if second_of(row.moment) != second {
                    return true;
                }

                if let Some(moment) = row.moment {
                    let is_the_latest = match expiration_time {
                        Some(latest) => moment.unix_microseconds > latest.unix_microseconds,
                        None => true,
                    };

                    if is_the_latest {
                        expiration_time = Some(moment);
                    }
                }

                line.remove(&row.place);
                places.insert(row_key.clone(), row.place);
                row_keys.insert(row_key.clone(), ());
                false
            });

            let result = UpdateRowsExpirationTimeEvent {
                table_name: item.table_name.clone(),
                partition_key: item.partition_key.clone(),
                row_keys,
                expiration_time,
            };

            self.handed_out = Some(HandedOut {
                table_name: result.table_name.clone(),
                partition_key: result.partition_key.clone(),
                places,
            });

            // The partition has had its turn. If it still has rows it goes behind the other
            // partitions - left in front, a partition which is read all the time would never
            // let them through.
            if let Some(item) = self.queue.pop_front() {
                if !item.rows.is_empty() {
                    self.queue.push_back(item);
                }
            }

            return Some(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use super::{UpdateRowsExpirationTimeEvent, UpdateRowsExpirationTimeQueue};

    const SECOND: i64 = 1_000_000;

    /// A moment at the given second. The moments of these tests are a second apart at least:
    /// rows whose moments fall into one second leave together, which has tests of its own.
    fn moment(second: i64) -> Option<DateTimeAsMicroseconds> {
        Some(DateTimeAsMicroseconds::new(second * SECOND))
    }

    fn row_keys(event: &UpdateRowsExpirationTimeEvent) -> Vec<&str> {
        event.row_keys.keys().map(|itm| itm.as_str()).collect()
    }

    /// What the main node ends up with: events are delivered in the order they leave the queue,
    /// and the moment delivered last for a row is the one it keeps.
    fn delivered(
        queue: &mut UpdateRowsExpirationTimeQueue,
    ) -> BTreeMap<String, Option<DateTimeAsMicroseconds>> {
        let mut result = BTreeMap::new();

        while let Some(event) = queue.dequeue() {
            assert!(!event.row_keys.is_empty());

            for row_key in event.row_keys.keys() {
                result.insert(row_key.to_string(), event.expiration_time);
            }
        }

        result
    }

    #[test]
    fn add_of_the_same_row_does_not_grow_the_queue() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        for _ in 0..100 {
            queue.add("table", "pk", ["rk"].into_iter(), moment(1));
        }

        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!("pk", event.partition_key);
        assert_eq!(vec!["rk"], row_keys(&event));
        assert_eq!(moment(1), event.expiration_time);

        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn add_of_the_same_row_with_a_new_moment_does_not_grow_the_queue_and_keeps_the_last_moment() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        // A sliding expiration: every read gives the row a moment of its own
        for second in 1..=100 {
            queue.add("table", "pk", ["rk"].into_iter(), moment(second));
        }

        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!("pk", event.partition_key);
        assert_eq!(vec!["rk"], row_keys(&event));
        assert_eq!(moment(100), event.expiration_time);

        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn add_merges_the_rows_of_a_partition_into_one_event() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        queue.add("table", "pk", ["rk3", "rk1"].into_iter(), None);
        queue.add("table", "pk", ["rk2", "rk3", "rk0"].into_iter(), None);

        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!("pk", event.partition_key);
        assert_eq!(vec!["rk0", "rk1", "rk2", "rk3"], row_keys(&event));
        assert_eq!(None, event.expiration_time);

        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn every_row_gets_the_moment_of_its_own_add() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        queue.add("table", "pk", ["rk1"].into_iter(), moment(1));
        queue.add("table", "pk", ["rk2"].into_iter(), moment(2));
        // None removes the expiration - it is a value to deliver as any other
        queue.add("table", "pk", ["rk3"].into_iter(), None);

        let delivered = delivered(&mut queue);

        assert_eq!(3, delivered.len());
        assert_eq!(moment(1), delivered["rk1"]);
        assert_eq!(moment(2), delivered["rk2"]);
        assert_eq!(None, delivered["rk3"]);
    }

    #[test]
    fn rows_of_a_partition_are_handed_out_moment_by_moment() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        queue.add("table", "pk", ["rk1", "rk4"].into_iter(), moment(1));
        queue.add("table", "pk", ["rk2"].into_iter(), moment(2));
        queue.add("table", "pk", ["rk3"].into_iter(), moment(1));
        // Set again before the delivery - rk4 leaves the rows of moment 1
        queue.add("table", "pk", ["rk4"].into_iter(), None);

        // An event carries one moment - the rows which share it leave together. The moment is
        // the one of the row which waits longest: rk1
        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!("pk", event.partition_key);
        assert_eq!(vec!["rk1", "rk3"], row_keys(&event));
        assert_eq!(moment(1), event.expiration_time);

        // rk4 came in before rk2 - and has kept its place when it was set again
        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!("pk", event.partition_key);
        assert_eq!(vec!["rk4"], row_keys(&event));
        assert_eq!(None, event.expiration_time);

        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!("pk", event.partition_key);
        assert_eq!(vec!["rk2"], row_keys(&event));
        assert_eq!(moment(2), event.expiration_time);

        assert!(queue.dequeue().is_none());
    }

    /// Moments which fall into one second are one event: every read of a sliding expiration
    /// computes a moment of its own, and an event a row would not keep up with the reads. The
    /// event carries the latest of them - nobody expires earlier than asked.
    #[test]
    fn rows_whose_moments_fall_into_one_second_leave_together_with_the_latest_of_them() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        let at = |microseconds: i64| Some(DateTimeAsMicroseconds::new(microseconds));

        queue.add("table", "pk", ["rk1"].into_iter(), at(5 * SECOND + 10));
        queue.add("table", "pk", ["rk2"].into_iter(), at(5 * SECOND + 999_999));
        queue.add("table", "pk", ["rk3"].into_iter(), at(5 * SECOND));
        // The next second begins here
        queue.add("table", "pk", ["rk4"].into_iter(), at(6 * SECOND));
        queue.add("table", "pk", ["rk5"].into_iter(), at(6 * SECOND + 1));

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk1", "rk2", "rk3"], row_keys(&event));
        assert_eq!(at(5 * SECOND + 999_999), event.expiration_time);

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk4", "rk5"], row_keys(&event));
        assert_eq!(at(6 * SECOND + 1), event.expiration_time);

        assert!(queue.dequeue().is_none());

        // The same before the epoch: the second is the one the moment falls into, not the
        // number of microseconds cut towards zero
        queue.add("table", "pk", ["rk1"].into_iter(), at(-1));
        queue.add("table", "pk", ["rk2"].into_iter(), at(1));
        queue.add("table", "pk", ["rk3"].into_iter(), at(-SECOND));

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk1", "rk3"], row_keys(&event));
        assert_eq!(at(-1), event.expiration_time);

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk2"], row_keys(&event));
        assert_eq!(at(1), event.expiration_time);

        assert!(queue.dequeue().is_none());
    }

    /// A thousand reads of a thousand rows, each with its own `now + ttl`, are a handful of
    /// events - one for every second the moments fall into - and not a thousand.
    #[test]
    fn a_sliding_expiration_does_not_cost_an_event_a_row() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        let row_keys: Vec<String> = (0..1000).map(|no| format!("rk{:04}", no)).collect();

        // The reads take two and a half seconds; every one of them asks for 30 seconds more
        for (no, row_key) in row_keys.iter().enumerate() {
            let now = 1_700_000_000 * SECOND + no as i64 * 2_500;
            let expires = DateTimeAsMicroseconds::new(now + 30 * SECOND);

            queue.add("table", "pk", [row_key.as_str()].into_iter(), Some(expires));
        }

        let mut events = 0;
        let mut delivered = BTreeMap::new();

        while let Some(event) = queue.dequeue() {
            events += 1;

            for row_key in event.row_keys.keys() {
                delivered.insert(row_key.to_string(), event.expiration_time.unwrap());
            }
        }

        assert_eq!(3, events);
        assert_eq!(1000, delivered.len());

        // Nobody got less than asked for, and nobody a second more
        for (no, row_key) in row_keys.iter().enumerate() {
            let asked = 1_700_000_000 * SECOND + no as i64 * 2_500 + 30 * SECOND;
            let got = delivered[row_key.as_str()].unix_microseconds;

            assert!(got >= asked && got - asked < SECOND, "{}", row_key);
        }
    }

    /// No expiration is not a moment to round: a row which is given none never leaves with a
    /// row which is given one, whatever second that one falls into.
    #[test]
    fn a_row_without_an_expiration_never_shares_an_event_with_one_which_has() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        queue.add("table", "pk", ["rk1"].into_iter(), moment(0));
        queue.add("table", "pk", ["rk2"].into_iter(), None);
        queue.add("table", "pk", ["rk3"].into_iter(), moment(0));
        queue.add("table", "pk", ["rk4"].into_iter(), None);

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk1", "rk3"], row_keys(&event));
        assert_eq!(moment(0), event.expiration_time);

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk2", "rk4"], row_keys(&event));
        assert_eq!(None, event.expiration_time);

        assert!(queue.dequeue().is_none());

        // ...and it is no second among the seconds - not the one before the epoch, not the
        // first and not the last one there is
        for second in [-1, 1, i64::MIN / SECOND, i64::MAX / SECOND] {
            queue.add("table", "pk", ["rk1"].into_iter(), None);
            queue.add("table", "pk", ["rk2"].into_iter(), moment(second));

            let event = queue.dequeue().unwrap();
            assert_eq!(vec!["rk1"], row_keys(&event), "second {}", second);
            assert_eq!(None, event.expiration_time, "second {}", second);

            let event = queue.dequeue().unwrap();
            assert_eq!(vec!["rk2"], row_keys(&event), "second {}", second);
            assert_eq!(moment(second), event.expiration_time, "second {}", second);

            assert!(queue.dequeue().is_none());
        }
    }

    #[test]
    fn add_without_rows_queues_nothing() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        queue.add("table", "pk", std::iter::empty(), moment(1));

        // Not an event - and not an item which lies in the queue until its turn comes either:
        // every add would have to look past it
        assert!(queue.queue.is_empty());
        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn returned_event_without_rows_queues_nothing() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        queue.return_event(UpdateRowsExpirationTimeEvent {
            table_name: "table".to_string(),
            partition_key: "pk".to_string(),
            row_keys: BTreeMap::new(),
            expiration_time: moment(1),
        });

        assert!(queue.queue.is_empty());
        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn every_partition_has_an_event_of_its_own() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        queue.add("table1", "pk1", ["rk1"].into_iter(), moment(1));
        queue.add("table1", "pk2", ["rk1"].into_iter(), moment(2));
        queue.add("table2", "pk1", ["rk1"].into_iter(), moment(3));
        queue.add("table1", "pk1", ["rk2"].into_iter(), moment(1));

        // Events leave the queue in the order their partitions came in
        let event = queue.dequeue().unwrap();
        assert_eq!("table1", event.table_name);
        assert_eq!("pk1", event.partition_key);
        assert_eq!(vec!["rk1", "rk2"], row_keys(&event));
        assert_eq!(moment(1), event.expiration_time);

        let event = queue.dequeue().unwrap();
        assert_eq!("table1", event.table_name);
        assert_eq!("pk2", event.partition_key);
        assert_eq!(vec!["rk1"], row_keys(&event));
        assert_eq!(moment(2), event.expiration_time);

        let event = queue.dequeue().unwrap();
        assert_eq!("table2", event.table_name);
        assert_eq!("pk1", event.partition_key);
        assert_eq!(vec!["rk1"], row_keys(&event));
        assert_eq!(moment(3), event.expiration_time);

        assert!(queue.dequeue().is_none());
    }

    /// A sliding expiration: every read gives its row a moment of its own - here a second
    /// apart, so every row is an event of its own - and the reads keep coming. A row which was
    /// handed out every time it was there - which is every time when it is read between two
    /// deliveries - would let none of the rows behind it through.
    #[test]
    fn row_which_is_set_again_all_the_time_does_not_hold_back_the_rows_behind_it() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        let mut next_moment = 0;
        let mut read = |queue: &mut UpdateRowsExpirationTimeQueue, row_key: &str| {
            next_moment += 1;
            queue.add("table", "pk", [row_key].into_iter(), moment(next_moment));
        };

        for row_key in ["rk1", "rk2", "rk3", "rk4", "rk5"] {
            read(&mut queue, row_key);
        }

        let mut handed_out = Vec::new();

        for _ in 0..5 {
            let event = queue.dequeue().unwrap();
            handed_out.extend(event.row_keys.keys().cloned());

            // rk1 and rk2 are read again before the next delivery - every time
            read(&mut queue, "rk1");
            read(&mut queue, "rk2");
        }

        // Every row has been handed out within as many events as there were rows waiting
        assert_eq!(vec!["rk1", "rk2", "rk3", "rk4", "rk5"], handed_out);

        // ...and the two which are read all the time are still served, with their last moment
        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk1"], row_keys(&event));
        assert_eq!(moment(next_moment - 1), event.expiration_time);

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk2"], row_keys(&event));
        assert_eq!(moment(next_moment), event.expiration_time);

        assert!(queue.dequeue().is_none());
    }

    /// The same between partitions: one which always has rows waiting must not stay the first
    /// one for ever - nothing of the partitions behind it would be delivered.
    #[test]
    fn partition_which_is_read_all_the_time_does_not_hold_back_the_other_partitions() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        let mut next_moment = 0;
        let mut read_hot = |queue: &mut UpdateRowsExpirationTimeQueue| {
            for row_key in ["rk1", "rk2", "rk3"] {
                next_moment += 1;
                queue.add("table", "hot", [row_key].into_iter(), moment(next_moment));
            }
        };

        read_hot(&mut queue);
        queue.add("table", "cold", ["rk"].into_iter(), moment(1_000));
        queue.add("other-table", "cold", ["rk"].into_iter(), moment(2_000));

        let mut partitions = Vec::new();

        for _ in 0..6 {
            let event = queue.dequeue().unwrap();
            partitions.push(format!("{}/{}", event.table_name, event.partition_key));

            // the hot partition is read again before the next delivery - every time
            read_hot(&mut queue);
        }

        // The partitions take turns: the two cold ones are out after three events, and then the
        // hot one has the queue to itself
        assert_eq!(
            vec![
                "table/hot",
                "table/cold",
                "other-table/cold",
                "table/hot",
                "table/hot",
                "table/hot"
            ],
            partitions
        );
    }

    #[test]
    fn returned_event_is_dequeued_again() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        queue.add("table", "pk", ["rk1", "rk2"].into_iter(), moment(1));

        // Taken for delivery - and the connection is lost before the confirmation
        let on_delivery = queue.dequeue().unwrap();
        assert!(queue.dequeue().is_none());

        queue.return_event(on_delivery);

        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!("pk", event.partition_key);
        assert_eq!(vec!["rk1", "rk2"], row_keys(&event));
        assert_eq!(moment(1), event.expiration_time);

        assert!(queue.dequeue().is_none());
    }

    /// The rows of a returned event are in the line in the order they had: one which is set
    /// again after the return has not lost its place among them.
    #[test]
    fn returned_rows_keep_their_order() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        queue.add("table", "pk", ["rk1"].into_iter(), moment(1));
        queue.add("table", "pk", ["rk2"].into_iter(), moment(1));

        let on_delivery = queue.dequeue().unwrap();
        assert_eq!(vec!["rk1", "rk2"], row_keys(&on_delivery));
        queue.return_event(on_delivery);

        // rk1 is set again and no longer shares the second of rk2 - it is still the first one
        queue.add("table", "pk", ["rk1"].into_iter(), moment(2));

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk1"], row_keys(&event));
        assert_eq!(moment(2), event.expiration_time);

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk2"], row_keys(&event));
        assert_eq!(moment(1), event.expiration_time);

        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn returned_event_does_not_override_what_was_queued_after_it() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        queue.add("table", "pk", ["rk1", "rk2", "rk3"].into_iter(), moment(1));
        let on_delivery = queue.dequeue().unwrap();

        // Set again while the event is on delivery - these are the moments the main node has
        // to end up with
        queue.add("table", "pk", ["rk1"].into_iter(), moment(2));
        queue.add("table", "pk", ["rk3"].into_iter(), None);

        queue.return_event(on_delivery);

        // And once more after the return
        queue.add("table", "pk", ["rk4"].into_iter(), moment(3));

        let delivered = delivered(&mut queue);

        assert_eq!(4, delivered.len());
        assert_eq!(moment(2), delivered["rk1"]);
        assert_eq!(moment(1), delivered["rk2"]);
        assert_eq!(None, delivered["rk3"]);
        assert_eq!(moment(3), delivered["rk4"]);
    }

    #[test]
    fn returned_event_whose_rows_were_all_set_again_delivers_nothing_of_its_own() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        queue.add("table", "pk", ["rk"].into_iter(), moment(1));
        let on_delivery = queue.dequeue().unwrap();

        queue.add("table", "pk", ["rk"].into_iter(), moment(2));
        queue.return_event(on_delivery);

        // The newer moment only - nothing is left to be delivered after it and to bring the
        // older one back
        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!("pk", event.partition_key);
        assert_eq!(vec!["rk"], row_keys(&event));
        assert_eq!(moment(2), event.expiration_time);

        assert!(queue.dequeue().is_none());
    }

    /// The rows of an event which was on delivery when the connection got lost have waited
    /// longest: they go back in front of the rows which came in while it was on delivery.
    #[test]
    fn returned_rows_go_back_in_front_of_the_line() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        queue.add("table", "pk", ["rk1", "rk2"].into_iter(), moment(1));
        let on_delivery = queue.dequeue().unwrap();

        queue.add("table", "pk", ["rk3"].into_iter(), moment(2));
        queue.add("table", "pk", ["rk4"].into_iter(), moment(3));

        queue.return_event(on_delivery);

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk1", "rk2"], row_keys(&event));
        assert_eq!(moment(1), event.expiration_time);

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk3"], row_keys(&event));
        assert_eq!(moment(2), event.expiration_time);

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk4"], row_keys(&event));
        assert_eq!(moment(3), event.expiration_time);

        assert!(queue.dequeue().is_none());
    }

    /// The rows of a lost delivery go back to the places they had - not in front of a row which
    /// was waiting before them. Here h and x leave together and r waits between them. In front
    /// of the whole line x would be past r, and r would wait for one more event with every
    /// delivery lost that way - for ever, when they keep getting lost.
    #[test]
    fn returned_rows_do_not_get_in_front_of_a_row_which_waited_before_them() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        queue.add("table", "pk", ["h"].into_iter(), moment(100));
        queue.add("table", "pk", ["r"].into_iter(), moment(1));
        queue.add("table", "pk", ["x"].into_iter(), moment(100));

        // h decides, x shares its second - and the connection is lost
        let on_delivery = queue.dequeue().unwrap();
        assert_eq!(vec!["h", "x"], row_keys(&on_delivery));
        queue.return_event(on_delivery);

        // x is read again after the reconnect and no longer shares the second of h
        queue.add("table", "pk", ["x"].into_iter(), moment(101));

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["h"], row_keys(&event));

        // ...and h is read again, in the second of x
        queue.add("table", "pk", ["h"].into_iter(), moment(101));

        // r was waiting before x - it is r which waits longest now
        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["r"], row_keys(&event));
        assert_eq!(moment(1), event.expiration_time);

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["h", "x"], row_keys(&event));
        assert_eq!(moment(101), event.expiration_time);

        assert!(queue.dequeue().is_none());
    }

    /// ...and among themselves the returned rows are in the order they came in, which is not
    /// the order of their keys.
    #[test]
    fn returned_rows_keep_the_order_they_had() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        // rk2 comes in before rk1
        queue.add("table", "pk", ["rk2"].into_iter(), moment(1));
        queue.add("table", "pk", ["rk1"].into_iter(), moment(1));

        let on_delivery = queue.dequeue().unwrap();
        assert_eq!(vec!["rk1", "rk2"], row_keys(&on_delivery));
        queue.return_event(on_delivery);

        // Set again after the return, each to a second of its own
        queue.add("table", "pk", ["rk1"].into_iter(), moment(2));
        queue.add("table", "pk", ["rk2"].into_iter(), moment(3));

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk2"], row_keys(&event));
        assert_eq!(moment(3), event.expiration_time);

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk1"], row_keys(&event));
        assert_eq!(moment(2), event.expiration_time);

        assert!(queue.dequeue().is_none());
    }

    /// An event the queue has not handed out (the places of its rows are not known) goes in
    /// front of the line, its rows in the order of their keys. A row of it which waits already
    /// has no place to get back: it stays where it is, with the moment it waits with.
    #[test]
    fn rows_of_an_event_which_was_not_handed_out_go_in_front_of_the_line() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        queue.add("table", "pk", ["rk2"].into_iter(), moment(2));

        queue.return_event(UpdateRowsExpirationTimeEvent {
            table_name: "table".to_string(),
            partition_key: "pk".to_string(),
            row_keys: [
                ("rk3".to_string(), ()),
                ("rk1".to_string(), ()),
                ("rk2".to_string(), ()),
            ]
            .into_iter()
            .collect(),
            expiration_time: moment(1),
        });

        // rk3 is set again: it keeps the place behind rk1 and in front of rk2
        queue.add("table", "pk", ["rk3"].into_iter(), moment(3));

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk1"], row_keys(&event));
        assert_eq!(moment(1), event.expiration_time);

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk3"], row_keys(&event));
        assert_eq!(moment(3), event.expiration_time);

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk2"], row_keys(&event));
        assert_eq!(moment(2), event.expiration_time);

        assert!(queue.dequeue().is_none());
    }

    /// The places the queue keeps are the ones the rows of the event it handed out last had in
    /// the line of their own partition. An event which is returned into another partition - of
    /// the same table, or with the same key in another table - gets none of them, also when
    /// its row keys are the same.
    #[test]
    fn places_of_the_event_handed_out_last_are_not_used_for_another_partition() {
        for (other_table, other_partition) in [("table", "other"), ("other-table", "pk")] {
            let mut queue = UpdateRowsExpirationTimeQueue::new();

            queue.add(other_table, other_partition, ["rk1"].into_iter(), moment(1));
            queue.add(other_table, other_partition, ["rk2"].into_iter(), moment(2));
            queue.add("table", "pk", ["rk"].into_iter(), moment(3));

            let event = queue.dequeue().unwrap();
            assert_eq!(other_table, event.table_name);
            assert_eq!(other_partition, event.partition_key);
            assert_eq!(vec!["rk1"], row_keys(&event));

            // Handed out last: the row rk of table / pk - which came in after rk2
            let event = queue.dequeue().unwrap();
            assert_eq!("table", event.table_name);
            assert_eq!("pk", event.partition_key);

            queue.return_event(UpdateRowsExpirationTimeEvent {
                table_name: other_table.to_string(),
                partition_key: other_partition.to_string(),
                row_keys: [("rk".to_string(), ())].into_iter().collect(),
                expiration_time: moment(4),
            });

            // In front of the line, as the rows of any event the queue has not handed out
            let event = queue.dequeue().unwrap();
            assert_eq!(other_table, event.table_name);
            assert_eq!(other_partition, event.partition_key);
            assert_eq!(vec!["rk"], row_keys(&event));
            assert_eq!(moment(4), event.expiration_time);

            let event = queue.dequeue().unwrap();
            assert_eq!(vec!["rk2"], row_keys(&event));
            assert_eq!(moment(2), event.expiration_time);

            assert!(queue.dequeue().is_none());
        }
    }

    /// A row which is read while it is on delivery has a newer moment to report - and has
    /// still not been reported since the read before it. When the delivery is lost it keeps
    /// the newer moment and gets back the place it had: left where the read made on delivery
    /// put it, at the end of the line, it would be behind rows which came in after it.
    #[test]
    fn row_which_was_set_again_while_on_delivery_gets_its_place_back_when_the_delivery_is_lost() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        queue.add("table", "pk", ["rk1", "rk2", "rk3"].into_iter(), moment(1));
        queue.add("table", "pk", ["rk4"].into_iter(), moment(2));

        let on_delivery = queue.dequeue().unwrap();
        assert_eq!(vec!["rk1", "rk2", "rk3"], row_keys(&on_delivery));

        // rk2 is read again while the event is on delivery - and the connection is lost
        queue.add("table", "pk", ["rk2"].into_iter(), moment(5));
        queue.return_event(on_delivery);

        // rk1 decides, rk3 shares its second
        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk1", "rk3"], row_keys(&event));
        assert_eq!(moment(1), event.expiration_time);

        // rk2 came in before rk4: it is the next one - with the moment of its last read
        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk2"], row_keys(&event));
        assert_eq!(moment(5), event.expiration_time);

        let event = queue.dequeue().unwrap();
        assert_eq!(vec!["rk4"], row_keys(&event));
        assert_eq!(moment(2), event.expiration_time);

        assert!(queue.dequeue().is_none());
    }

    /// Two rows which are read all the time - each of them while it is on delivery - and a
    /// connection which breaks on every second delivery. The row whose delivery was lost
    /// has to be the next one: sent behind the other row with every loss, it would be handed
    /// out every time and never delivered, while the other row is delivered every time.
    #[test]
    fn row_which_is_read_while_on_delivery_is_delivered_when_every_second_delivery_is_lost() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        let mut next_moment = 0;
        let mut read = |queue: &mut UpdateRowsExpirationTimeQueue, row_key: &str| {
            next_moment += 1;
            queue.add("table", "pk", [row_key].into_iter(), moment(next_moment));
        };

        read(&mut queue, "rk1");
        read(&mut queue, "rk2");

        let mut confirmed: BTreeMap<String, usize> = BTreeMap::new();

        for delivery in 0..100 {
            let on_delivery = queue.dequeue().unwrap();
            assert_eq!(1, on_delivery.row_keys.len());

            let row_key = on_delivery.row_keys.keys().next().unwrap().to_string();

            // Read again while it is on delivery
            read(&mut queue, row_key.as_str());

            if delivery % 2 == 0 {
                queue.return_event(on_delivery);
            } else {
                *confirmed.entry(row_key).or_default() += 1;
            }
        }

        assert_eq!(25, confirmed["rk1"]);
        assert_eq!(25, confirmed["rk2"]);
    }

    #[test]
    fn returned_event_is_not_merged_into_another_partition() {
        let mut queue = UpdateRowsExpirationTimeQueue::new();

        queue.add("table1", "pk", ["rk"].into_iter(), moment(1));
        let on_delivery = queue.dequeue().unwrap();

        queue.add("table2", "pk", ["rk"].into_iter(), moment(2));
        queue.add("table1", "pk2", ["rk"].into_iter(), moment(3));
        queue.return_event(on_delivery);

        // Nothing is queued for table1 / pk - the returned event is queued as it is
        let event = queue.dequeue().unwrap();
        assert_eq!("table2", event.table_name);
        assert_eq!("pk", event.partition_key);
        assert_eq!(vec!["rk"], row_keys(&event));
        assert_eq!(moment(2), event.expiration_time);

        let event = queue.dequeue().unwrap();
        assert_eq!("table1", event.table_name);
        assert_eq!("pk2", event.partition_key);
        assert_eq!(vec!["rk"], row_keys(&event));
        assert_eq!(moment(3), event.expiration_time);

        let event = queue.dequeue().unwrap();
        assert_eq!("table1", event.table_name);
        assert_eq!("pk", event.partition_key);
        assert_eq!(vec!["rk"], row_keys(&event));
        assert_eq!(moment(1), event.expiration_time);

        assert!(queue.dequeue().is_none());
    }

    // ------------------------------------------------------------------------------------------
    // A model check: the queue is driven the way SyncToMainNodeQueue drives it - one event on
    // delivery at a time, which is confirmed or lost and returned - by a generated sequence, and
    // compared with a model.
    // ------------------------------------------------------------------------------------------

    /// The same sequence on every run.
    struct Generated(u64);

    impl Generated {
        fn below(&mut self, below: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 33) % below
        }
    }

    const TABLES: [&str; 2] = ["t1", "t2"];
    const PARTITIONS: [&str; 3] = ["p1", "p2", "p3"];
    const ROWS: [&str; 6] = ["r1", "r2", "r3", "r4", "r5", "r6"];

    type RowId = (String, String, String);

    struct Model {
        queue: UpdateRowsExpirationTimeQueue,
        /// The moment which was set last for a row, in microseconds.
        set_last: BTreeMap<RowId, Option<i64>>,
        /// What the main node has: the confirmed deliveries, applied in the order they left.
        main_node: BTreeMap<RowId, Option<i64>>,
        /// The rows which have something to deliver - set, or returned, and not handed out
        /// since - with the number of events which had been handed out when they began to wait.
        waiting: BTreeMap<RowId, usize>,
        on_delivery: Option<UpdateRowsExpirationTimeEvent>,
        handed_out: usize,
        longest_wait: usize,
        waited_longest: Option<RowId>,
    }

    impl Model {
        fn new() -> Self {
            Self {
                queue: UpdateRowsExpirationTimeQueue::new(),
                set_last: BTreeMap::new(),
                main_node: BTreeMap::new(),
                waiting: BTreeMap::new(),
                on_delivery: None,
                handed_out: 0,
                longest_wait: 0,
                waited_longest: None,
            }
        }

        fn add(&mut self, table: &str, partition: &str, rows: &[&str], moment: Option<i64>) {
            self.queue.add(
                table,
                partition,
                rows.iter().copied(),
                moment.map(DateTimeAsMicroseconds::new),
            );

            for row in rows {
                let id = (table.to_string(), partition.to_string(), row.to_string());
                self.set_last.insert(id.clone(), moment);
                self.waiting.entry(id).or_insert(self.handed_out);
            }
        }

        /// Takes the next event for delivery. False when the queue has nothing.
        fn dequeue(&mut self) -> bool {
            assert!(self.on_delivery.is_none());

            let Some(event) = self.queue.dequeue() else {
                assert!(
                    self.waiting.is_empty(),
                    "the queue has nothing, and these rows wait: {:?}",
                    self.waiting
                );
                return false;
            };

            self.handed_out += 1;

            assert!(!event.row_keys.is_empty(), "an event without rows");

            for row in event.row_keys.keys() {
                let id = (
                    event.table_name.clone(),
                    event.partition_key.clone(),
                    row.clone(),
                );

                let Some(since) = self.waiting.remove(&id) else {
                    panic!("{:?} is handed out, and it has nothing to deliver", id);
                };

                if self.handed_out - since > self.longest_wait {
                    self.longest_wait = self.handed_out - since;
                    self.waited_longest = Some(id);
                }
            }

            self.on_delivery = Some(event);
            true
        }

        fn confirm(&mut self) {
            let event = self.on_delivery.take().unwrap();

            for row in event.row_keys.keys() {
                let id = (
                    event.table_name.clone(),
                    event.partition_key.clone(),
                    row.clone(),
                );
                self.main_node
                    .insert(id, event.expiration_time.map(|itm| itm.unix_microseconds));
            }
        }

        /// The connection is lost while the event is on delivery: the main node has not got it.
        fn lose(&mut self) {
            let event = self.on_delivery.take().unwrap();

            for row in event.row_keys.keys() {
                let id = (
                    event.table_name.clone(),
                    event.partition_key.clone(),
                    row.clone(),
                );
                self.waiting.entry(id).or_insert(self.handed_out);
            }

            self.queue.return_event(event);
        }

        fn check_one_item_per_partition(&self) {
            let mut seen = BTreeMap::new();

            for item in self.queue.queue.iter() {
                let before = seen.insert(
                    (item.table_name.to_string(), item.partition_key.to_string()),
                    (),
                );
                assert!(
                    before.is_none(),
                    "two items for {}/{}",
                    item.table_name,
                    item.partition_key
                );

                assert_eq!(item.rows.len(), item.line.len());
            }
        }

        /// No more reads: everything which waits is delivered and confirmed.
        fn deliver_the_rest(&mut self) {
            if self.on_delivery.is_some() {
                self.confirm();
            }

            while self.dequeue() {
                self.confirm();
            }
        }

        /// The main node has, for every row, what was set last: no expiration where none was
        /// asked for, and otherwise the moment asked for or one of the same second which is
        /// later - the moment of the rows it has left the queue together with.
        fn check_the_main_node_has_what_was_set_last(&self, seed: u64) {
            assert_eq!(self.set_last.len(), self.main_node.len(), "seed {}", seed);

            for (id, set_last) in &self.set_last {
                let delivered = self.main_node[id];

                let is_what_was_asked_for = match (*set_last, delivered) {
                    (None, None) => true,
                    (Some(set_last), Some(delivered)) => {
                        delivered >= set_last
                            && delivered.div_euclid(SECOND) == set_last.div_euclid(SECOND)
                    }
                    _ => false,
                };

                assert!(
                    is_what_was_asked_for,
                    "seed {}: {:?} was set to {:?} and the main node has {:?}",
                    seed, id, set_last, delivered
                );
            }
        }
    }

    /// Reads, deliveries, confirmations and lost connections in a generated order. Whatever the
    /// order: the main node ends up with the moment set last for every row; a row is handed out
    /// only when it has something to deliver; the queue is empty only when nothing waits; and
    /// there is one item per table and partition.
    #[test]
    fn model_the_moment_set_last_wins_and_nothing_is_lost() {
        for seed in 1..=20u64 {
            let mut generated = Generated(seed);
            let mut model = Model::new();
            let mut own_second = 1000;
            let mut lost = 0;

            for _ in 0..5000 {
                match generated.below(100) {
                    0..=54 => {
                        let table = TABLES[generated.below(2) as usize];
                        let partition = PARTITIONS[generated.below(3) as usize];

                        let first = generated.below(6) as usize;
                        let amount = 1 + generated.below(3) as usize;
                        let rows: Vec<&str> =
                            (0..amount).map(|no| ROWS[(first + no * 2) % 6]).collect();

                        // No expiration, one of a few seconds many rows share - at several
                        // places inside the second - or a second of its own
                        let moment = match generated.below(10) {
                            0 => None,
                            1..=4 => Some(
                                (1 + generated.below(3) as i64) * SECOND
                                    + [0, 1, 500_000, 999_999][generated.below(4) as usize],
                            ),
                            _ => {
                                own_second += 1;
                                Some(own_second * SECOND)
                            }
                        };

                        model.add(table, partition, rows.as_slice(), moment);
                    }
                    55..=84 => {
                        if model.on_delivery.is_some() {
                            model.confirm();
                        } else {
                            model.dequeue();
                        }
                    }
                    _ => {
                        if model.on_delivery.is_some() {
                            model.lose();
                            lost += 1;
                        } else {
                            model.dequeue();
                        }
                    }
                }

                model.check_one_item_per_partition();
            }

            model.deliver_the_rest();

            assert!(lost > 100, "seed {}: only {} lost deliveries", seed, lost);
            assert_eq!(36, model.set_last.len(), "seed {}", seed);
            model.check_the_main_node_has_what_was_set_last(seed);
            assert!(model.queue.dequeue().is_none());
        }
    }

    /// A sliding expiration under load, at its worst: every read gives its row a second of its
    /// own, so every row is an event of its own, and the reads come faster than the main node
    /// confirms - three reads of the partition t1/p1 and one read of some other partition for
    /// every delivery. A row which waits has, at the worst, the other five rows of its
    /// partition in front of it, and its partition the other five partitions: it is handed out
    /// with the 36th event at the latest.
    #[test]
    fn model_a_row_waits_for_its_turn_and_no_longer_while_the_reads_keep_coming() {
        let mut generated = Generated(7);
        let mut model = Model::new();
        let mut own_second = 1000;

        let mut read = |model: &mut Model, table: &str, partition: &str, row: &str| {
            own_second += 1;
            model.add(table, partition, &[row], Some(own_second * SECOND));
        };

        for _ in 0..6000 {
            for _ in 0..3 {
                let row = ROWS[generated.below(6) as usize];
                read(&mut model, "t1", "p1", row);
            }

            let other = 1 + generated.below(5) as usize;
            let row = ROWS[generated.below(6) as usize];
            read(&mut model, TABLES[other / 3], PARTITIONS[other % 3], row);

            assert!(model.dequeue());
            model.confirm();

            model.check_one_item_per_partition();
        }

        model.deliver_the_rest();

        model.check_the_main_node_has_what_was_set_last(7);

        assert!(
            model.longest_wait <= TABLES.len() * PARTITIONS.len() * ROWS.len(),
            "{:?} was handed out with the event number {} after it began to wait; {} events in all",
            model.waited_longest,
            model.longest_wait,
            model.handed_out
        );
    }

    /// The turns hold when deliveries get lost: whatever is lost and returned in between, an
    /// event has the row of its partition which has waited longest - a row which is returned
    /// has waited since the read it left with, also when it was set again while it was on
    /// delivery: the main node has not got it since. So a row is not passed by rows which
    /// came in after it, and every delivery of its partition which is confirmed brings its
    /// turn nearer.
    #[test]
    fn model_the_row_which_waits_longest_leaves_first_also_when_deliveries_get_lost() {
        for seed in 1..=20u64 {
            let mut generated = Generated(seed);
            let mut queue = UpdateRowsExpirationTimeQueue::new();

            // The rows which wait, with the number of the read they wait since
            let mut waiting: BTreeMap<RowId, usize> = BTreeMap::new();
            let mut on_delivery: Option<(UpdateRowsExpirationTimeEvent, BTreeMap<RowId, usize>)> =
                None;

            let mut reads = 0;
            let mut own_second = 1000;
            let mut lost = 0;

            for _ in 0..5000 {
                if generated.below(100) < 60 {
                    let table = TABLES[generated.below(2) as usize];
                    let partition = PARTITIONS[generated.below(3) as usize];
                    let row = ROWS[generated.below(6) as usize];

                    // One of a few seconds many rows share, or a second of its own
                    let second = match generated.below(2) {
                        0 => 1 + generated.below(3) as i64,
                        _ => {
                            own_second += 1;
                            own_second
                        }
                    };

                    queue.add(table, partition, [row].into_iter(), moment(second));

                    reads += 1;
                    waiting
                        .entry((table.to_string(), partition.to_string(), row.to_string()))
                        .or_insert(reads);

                    continue;
                }

                if let Some((event, since)) = on_delivery.take() {
                    // Confirmed - or lost, and returned
                    if generated.below(3) == 0 {
                        for (id, since) in since {
                            waiting.insert(id, since);
                        }

                        queue.return_event(event);
                        lost += 1;
                    }

                    continue;
                }

                let Some(event) = queue.dequeue() else {
                    assert!(waiting.is_empty(), "seed {}", seed);
                    continue;
                };

                let mut since = BTreeMap::new();

                for row in event.row_keys.keys() {
                    let id = (
                        event.table_name.clone(),
                        event.partition_key.clone(),
                        row.clone(),
                    );
                    let waits_since = waiting.remove(&id).unwrap();
                    since.insert(id, waits_since);
                }

                let longest_in_the_event = *since.values().min().unwrap();

                for (id, waits_since) in &waiting {
                    if id.0 == event.table_name && id.1 == event.partition_key {
                        assert!(
                            longest_in_the_event < *waits_since,
                            "seed {}: {:?} waits since the read {} and was left behind by {:?}",
                            seed,
                            id,
                            waits_since,
                            since
                        );
                    }
                }

                on_delivery = Some((event, since));
            }

            assert!(lost > 100, "seed {}: only {} lost deliveries", seed, lost);
        }
    }
}
