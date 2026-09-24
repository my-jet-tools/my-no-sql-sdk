use std::sync::Arc;

use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};
use rust_extensions::{
    events_loop::{EventsLoop, EventsLoopTick, RepeatIteration},
    ApplicationStates,
};

use super::{LazyMyNoSqlEntity, MyNoSqlDataReaderCallBacks};

pub enum PusherEvents<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
> {
    InsertedOrReplaced(String, Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>),
    Deleted(String, Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>),
}

pub struct MyNoSqlDataReaderCallBacksPusher<TMyNoSqlEntity>
where
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
{
    events_loop: EventsLoop<PusherEvents<TMyNoSqlEntity>>,
}

impl<TMyNoSqlEntity> MyNoSqlDataReaderCallBacksPusher<TMyNoSqlEntity>
where
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
{
    pub fn new<
        TMyNoSqlDataReaderCallBacks: MyNoSqlDataReaderCallBacks<TMyNoSqlEntity> + Send + Sync + 'static,
    >(
        callbacks: Arc<TMyNoSqlDataReaderCallBacks>,
        app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>,
    ) -> Self {
        let events_loop_reader = MyNoSqlDataReaderCallBacksSender::new(callbacks, None);
        let  events_loop = EventsLoop::new(
            "MyNoSqlDataReaderCallBacksPusher".to_string(),
  
        );
        events_loop.register_event_loop(Arc::new(events_loop_reader));
        events_loop.start(app_states,  my_logger::LOGGER.clone(),);
        Self { events_loop }
    }

    pub fn inserted_or_replaced(
        &self,
        partition_key: &str,
        entities: Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>,
    ) {
        self.events_loop.send(PusherEvents::InsertedOrReplaced(
            partition_key.to_string(),
            entities,
        ));
    }

    pub fn deleted(
        &self,
        partition_key: &str,
        entities: Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>,
    ) {
        self.events_loop
            .send(PusherEvents::Deleted(partition_key.to_string(), entities));
    }
}

#[async_trait::async_trait]
impl<TMyNoSqlEntity> MyNoSqlDataReaderCallBacks<TMyNoSqlEntity>
    for MyNoSqlDataReaderCallBacksPusher<TMyNoSqlEntity>
where
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
{
    fn inserted_or_replaced(
        &self,
        partition_key: &str,
        entities: Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>,
    ) {
        self.events_loop.send(PusherEvents::InsertedOrReplaced(
            partition_key.to_string(),
            entities,
        ));
    }

    fn deleted(
        &self,
        partition_key: &str,
        entities: Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>,
    ) {
        self.events_loop
            .send(PusherEvents::Deleted(partition_key.to_string(), entities));
    }
}

pub struct MyNoSqlDataReaderCallBacksSender<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
    TMyNoSqlDataReaderCallBacks: MyNoSqlDataReaderCallBacks<TMyNoSqlEntity>,
> {
    callbacks: Arc<TMyNoSqlDataReaderCallBacks>,
    item: Option<TMyNoSqlEntity>,
}

impl<
        TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
        TMyNoSqlDataReaderCallBacks: MyNoSqlDataReaderCallBacks<TMyNoSqlEntity> + Send + Sync + 'static,
    > MyNoSqlDataReaderCallBacksSender<TMyNoSqlEntity, TMyNoSqlDataReaderCallBacks>
{
    pub fn new(callbacks: Arc<TMyNoSqlDataReaderCallBacks>, item: Option<TMyNoSqlEntity>) -> Self {
        Self { callbacks, item }
    }
}

#[async_trait::async_trait]
impl<
        TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
        TMyNoSqlDataReaderCallBacks: MyNoSqlDataReaderCallBacks<TMyNoSqlEntity> + Send + Sync + 'static,
    > EventsLoopTick<PusherEvents<TMyNoSqlEntity>>
    for MyNoSqlDataReaderCallBacksSender<TMyNoSqlEntity, TMyNoSqlDataReaderCallBacks>
{
    async fn started(&self) {}
    async fn tick(
        &self,
        model: PusherEvents<TMyNoSqlEntity>,
    ) -> RepeatIteration<PusherEvents<TMyNoSqlEntity>> {
        match model {
            PusherEvents::InsertedOrReplaced(partition_key, entities) => {
                self.callbacks
                    .inserted_or_replaced(partition_key.as_str(), entities);
            }
            PusherEvents::Deleted(partition_key, entities) => {
                self.callbacks
                    .deleted(partition_key.as_str(), entities);
            }
        }
        if self.item.is_some() {}

        RepeatIteration::No
    }

    async fn finished(&self) {}
}
