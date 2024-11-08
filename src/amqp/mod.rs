pub use lapin;
mod context;

use fnv::FnvHashMap;
use lapin::{BasicProperties, Channel};
use std::any::{Any, TypeId};
use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;

use lapin::acker::Acker;
use lapin::message::Delivery;
use lapin::types::{DeliveryTag, ShortString};

use crate::event::{Encode, Event};

pub trait AMQPEvent: Event {}

pub struct Context {
    /// The global lapin channel to interact with the broker
    channel: Channel,
    /// A generic data storage for shared instances of types
    data: FnvHashMap<TypeId, &'static (dyn Any + Send + Sync)>,
}

pub struct Publish<E>(E);

pub struct Exchange(ShortString);

pub struct ReplyTo<E>(E)
where
    E: Encode;

#[derive(Clone)]
pub struct Streameroo {
    context: Arc<Context>,
}

impl Streameroo {
    pub fn new(channel: Channel) -> Self {
        let context = Context {
            channel,
            data: FnvHashMap::default(),
        };
        Self {
            context: Arc::new(context),
        }
    }

    pub async fn spawn_handler<P, R>(&self, handler: impl AMQPHandler<P, R>)
    where
        P: Send,
        R: Send,
    {
        let c: Arc<Context> = self.context.clone();
        tokio::spawn(async move {});
        let d: Delivery = todo!();
        tokio::spawn(handler.call(d, c));
    }
}

impl Context {
    pub fn insert<D: Any + Send + Sync>(&mut self, data: D) {
        let data = Box::new(data);
        self.data.insert(TypeId::of::<D>(), Box::leak(data));
    }

    pub fn data_unchecked<D: Any + Send + Sync>(&self) -> &'static D {
        self.data_opt::<D>().unwrap()
    }

    pub fn data_opt<D: Any + Send + Sync>(&self) -> Option<&'static D> {
        self.data
            .get(&TypeId::of::<D>())
            .and_then(|x| x.downcast_ref::<D>())
    }
}

/// The context of a Delivery. All values derivable from the derivable and global context can be accessed here.
pub struct DeliveryContext {
    /// Reference to the global context
    global: Arc<Context>,
    delivery_tag: DeliveryTag,
    exchange: ShortString,
    routing_key: ShortString,
    redelivered: bool,
    properties: BasicProperties,
    acker: Acker,
}

pub struct State<T: 'static>(&'static T);

impl<T> Deref for State<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<T> State<T> {
    pub fn into_inner(self) -> &'static T {
        self.0
    }
}

pub struct StateOwned<T>(pub T);

impl<T> FromDeliveryContext for StateOwned<T>
where
    T: Any + Send + Sync + Clone,
{
    fn from_delivery_context(context: &DeliveryContext) -> Self {
        let value = context.global.data_unchecked::<T>().clone();
        StateOwned(value)
    }
}

impl<T> FromDeliveryContext for State<T>
where
    T: Any + Send + Sync + 'static,
{
    fn from_delivery_context(context: &DeliveryContext) -> Self {
        State(context.global.data_unchecked::<T>())
    }
}

impl FromDeliveryContext for Channel {
    fn from_delivery_context(context: &DeliveryContext) -> Self {
        context.global.channel.clone()
    }
}

pub trait FromDeliveryContext {
    fn from_delivery_context(context: &DeliveryContext) -> Self;
}

fn create_handler_context(delivery: Delivery, context: Arc<Context>) -> (DeliveryContext, Vec<u8>) {
    (
        DeliveryContext {
            global: context,
            delivery_tag: delivery.delivery_tag,
            exchange: delivery.exchange,
            routing_key: delivery.routing_key,
            redelivered: delivery.redelivered,
            properties: delivery.properties,
            acker: delivery.acker,
        },
        delivery.data,
    )
}

impl<T1, E, F, Fut, R> AMQPHandler<(T1, E), R> for F
where
    F: Fn(T1, E) -> Fut + Send + 'static,
    Fut: Future<Output = R> + Send,
    E: AMQPEvent + Send + 'static,
    T1: FromDeliveryContext + Send + 'static,
    R: Send + 'static,
{
    async fn call(self, delivery: Delivery, context: Arc<Context>) {
        let (delivery_context, payload) = create_handler_context(delivery, context);
        let event = E::decode(payload).unwrap();
        let t1 = T1::from_delivery_context(&delivery_context);
        let r = self(t1, event).await;
        todo!()
    }
}

pub trait AMQPHandler<P, R>
where
    P: Send,
    R: Send,
{
    fn call(self, delivery: Delivery, context: Arc<Context>) -> impl Future<Output = ()> + Send;
}

async fn spawn_handler<P, R>(h: impl AMQPHandler<P, R>)
where
    P: Send,
    R: Send,
{
    let c: Arc<Context> = todo!();
    let d: Delivery = todo!();
    tokio::spawn(h.call(d, c));
}

macro_rules! impl_handler {
    (
        [$($ty:ident),*]
    ) => {};
}

impl_handler!([T1]);
impl_handler!([T1, T2]);
impl_handler!([T1, T2, T3]);
impl_handler!([T1, T2, T3, T4]);
impl_handler!([T1, T2, T3, T4, T5]);
impl_handler!([T1, T2, T3, T4, T5, T6]);
impl_handler!([T1, T2, T3, T4, T5, T6, T7]);
impl_handler!([T1, T2, T3, T4, T5, T6, T7, T8]);
impl_handler!([T1, T2, T3, T4, T5, T6, T7, T8, T9]);
impl_handler!([T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]);
impl_handler!([T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11]);
impl_handler!([T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12]);
impl_handler!([T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13]);
impl_handler!([T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14]);

#[cfg(test)]
mod test {
    use std::convert::Infallible;

    use crate::event::Decode;

    use super::*;

    #[derive(Debug)]
    struct TestEvent;

    impl Decode for TestEvent {
        type Error = Infallible;

        fn decode(_: Vec<u8>) -> Result<Self, Infallible> {
            todo!()
        }
    }

    async fn test_handler(d: State<String>, event: TestEvent) {
        todo!()
    }

    impl Event for TestEvent {}

    impl AMQPEvent for TestEvent {}

    #[tokio::test]
    async fn test_context() {
        let app = Streameroo::new(todo!());
        app.spawn_handler(test_handler).await;
    }
}
