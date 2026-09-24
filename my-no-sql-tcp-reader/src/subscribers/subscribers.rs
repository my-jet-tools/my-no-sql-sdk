use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;
use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};
use my_no_sql_tcp_shared::sync_to_main::SyncToMainNodeHandler;
use rust_extensions::ApplicationStates;

use super::{MyNoSqlDataReaderTcp, UpdateEvent};

type SubscribersMap = BTreeMap<String, Arc<dyn UpdateEvent + Send + Sync + 'static>>;

struct SubscribersInner {
    map: SubscribersMap,
    table_names: Arc<Vec<String>>,
}

impl SubscribersInner {
    fn empty() -> Self {
        Self {
            map: BTreeMap::new(),
            table_names: Arc::new(Vec::new()),
        }
    }

    fn from_map(map: SubscribersMap) -> Self {
        let table_names: Vec<String> = map.keys().cloned().collect();
        Self {
            map,
            table_names: Arc::new(table_names),
        }
    }
}

struct SubscribersState {
    inner: ArcSwap<SubscribersInner>,
    write_lock: Mutex<()>,
}

#[derive(Clone)]
pub struct Subscribers {
    state: Arc<SubscribersState>,
}

impl Subscribers {
    pub fn new() -> Self {
        Self {
            state: Arc::new(SubscribersState {
                inner: ArcSwap::from_pointee(SubscribersInner::empty()),
                write_lock: Mutex::new(()),
            }),
        }
    }

    pub fn create_subscriber<TMyNoSqlEntity>(
        &self,
        app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>,
        sync_handler: Arc<SyncToMainNodeHandler>,
    ) -> Arc<MyNoSqlDataReaderTcp<TMyNoSqlEntity>>
    where
        TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
    {
        let new_reader = MyNoSqlDataReaderTcp::new(app_states, sync_handler);
        let new_reader = Arc::new(new_reader);

        let _guard = self.state.write_lock.lock().unwrap();
        let current = self.state.inner.load_full();

        if current.map.contains_key(TMyNoSqlEntity::TABLE_NAME) {
            panic!(
                "You already subscribed for the table {}",
                TMyNoSqlEntity::TABLE_NAME
            );
        }

        let mut new_map = current.map.clone();
        new_map.insert(TMyNoSqlEntity::TABLE_NAME.to_string(), new_reader.clone());
        self.state
            .inner
            .store(Arc::new(SubscribersInner::from_map(new_map)));

        new_reader
    }

    pub fn get(
        &self,
        table_name: &str,
    ) -> Option<Arc<dyn UpdateEvent + Send + Sync + 'static>> {
        self.state.inner.load().map.get(table_name).cloned()
    }

    pub fn get_tables_to_subscribe(&self) -> Arc<Vec<String>> {
        self.state.inner.load().table_names.clone()
    }
}
