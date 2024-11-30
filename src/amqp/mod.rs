pub use lapin;
mod context;

use fnv::FnvHashMap;
use lapin::options::BasicConsumeOptions;
use lapin::{BasicProperties, Channel};
use std::any::{Any, TypeId};
use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;
use tokio_stream::StreamExt;

use lapin::acker::Acker;
use lapin::message::Delivery;
use lapin::types::{DeliveryTag, FieldTable, ShortString};

use crate::event::{Decode, Encode};

pub struct Context {
    /// The global lapin channel to interact with the broker
    channel: Channel,
    /// A generic data storage for shared instances of types
    data: FnvHashMap<TypeId, &'static (dyn Any + Send + Sync)>,
}

pub struct Publish<E> {
    payload: E,
    exchange: String,
    routing_key: String,
}

pub trait AMQPResult: Send {
    fn handle_result(
        self,
        context: &DeliveryContext,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

impl AMQPResult for () {
    async fn handle_result(self, _: &DeliveryContext) -> Result<(), Error> {
        Ok(())
    }
}

impl<E> AMQPResult for Publish<E>
where
    E: Encode + Send,
{
    async fn handle_result(self, context: &DeliveryContext) -> Result<(), Error> {
        Ok(())
    }
}

pub struct Exchange(String);

impl Deref for Exchange {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct ReplyTo<E>(E)
where
    E: Encode;

#[derive(Clone)]
pub struct Streameroo {
    context: Arc<Context>,
    consumer_tag: String,
}

impl Streameroo {
    pub fn new(channel: Channel, consumer_tag: impl Into<String>) -> Self {
        let context = Context {
            channel,
            data: FnvHashMap::default(),
        };
        Self {
            context: Arc::new(context),
            consumer_tag: consumer_tag.into(),
        }
    }

    pub fn spawn_handler<P, T, E>(
        &self,
        handler: impl AMQPHandler<P, T, E>,
        queue: impl Into<String>,
        options: BasicConsumeOptions,
        arguments: FieldTable,
    ) where
        T: AMQPResult,
    {
        let context: Arc<Context> = self.context.clone();
        let consumer_tag = self.consumer_tag.clone();
        let queue = queue.into();
        tokio::spawn(async move {
            let Ok(mut consumer) = context
                .channel
                .basic_consume(&queue, &consumer_tag, options, arguments)
                .await
            else {
                todo!()
            };
            while let Some(attempted_delivery) = consumer.next().await {
                match attempted_delivery {
                    Ok(delivery) => {
                        let context = context.clone();
                        let handler = handler.clone();
                        tokio::spawn(async move {
                            let (delivery_context, payload) =
                                create_delivery_context(delivery, context);
                            match handler.call(payload, &delivery_context).await {
                                Ok(ret) => match ret.handle_result(&delivery_context).await {
                                    Ok(_) => {
                                        if let Err(e) =
                                            delivery_context.acker.ack(Default::default()).await
                                        {
                                            tracing::error!(?e, "Error acking delivery");
                                        }
                                    }
                                    Err(_) => todo!(),
                                },
                                Err(e) => {}
                            }
                        });
                    }
                    Err(_) => todo!(),
                }
            }
            unreachable!()
        });
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

impl<T> FromDeliveryContext<'_> for StateOwned<T>
where
    T: Any + Send + Sync + Clone,
{
    fn from_delivery_context(context: &DeliveryContext) -> Self {
        let value = context.global.data_unchecked::<T>().clone();
        StateOwned(value)
    }
}

impl FromDeliveryContext<'_> for Exchange {
    fn from_delivery_context(context: &DeliveryContext) -> Self {
        Exchange(context.exchange.to_string())
    }
}

impl<T> FromDeliveryContext<'_> for State<T>
where
    T: Any + Send + Sync + 'static,
{
    fn from_delivery_context(context: &DeliveryContext) -> Self {
        State(context.global.data_unchecked::<T>())
    }
}

impl FromDeliveryContext<'_> for Channel {
    fn from_delivery_context(context: &DeliveryContext) -> Self {
        context.global.channel.clone()
    }
}

pub trait FromDeliveryContext<'a> {
    fn from_delivery_context(context: &'a DeliveryContext) -> Self;
}

#[inline]
fn create_delivery_context(
    delivery: Delivery,
    context: Arc<Context>,
) -> (DeliveryContext, Vec<u8>) {
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

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

// P: Type for the parameters of the future generating closure
// T: Type for the return value of the future generated by the closure
// E: Error type of the handler
pub trait AMQPHandler<P, T, Err>: Clone + Send + 'static
where
    T: AMQPResult,
{
    fn call(
        &self,
        payload: Vec<u8>,
        delivery_context: &DeliveryContext,
    ) -> impl Future<Output = Result<T, Error>> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Handler error: {0}")]
    Handler(BoxError),
    #[error("Event error: {0}")]
    EventDecode(BoxError),
    #[error("Lapin error: {0}")]
    Lapin(#[from] lapin::Error),
}

macro_rules! impl_handler {
    (
        [$($ty:ident),*]
    ) => {
        #[allow(non_snake_case, unused_variables, unused_parens)]
        impl<F, Fut, T, Err, $($ty,)* E> AMQPHandler<($($ty,)* E), T, Err> for F
        where
            F: Fn($($ty,)* E) -> Fut + Send + Sync + 'static + Clone,
            Err: Into<BoxError>,
            Fut: Future<Output = Result<T, Err>> + Send,
            $( $ty: for<'a> FromDeliveryContext<'a>, )*
            E: Decode,
            T: AMQPResult,
        {
            async fn call(&self, payload: Vec<u8>, delivery_context: &DeliveryContext) -> Result<T, Error> {
                let event = E::decode(payload).map_err(|e| Error::EventDecode(e.into()))?;
                $(
                    let $ty = $ty::from_delivery_context(&delivery_context);
                )*
                self($($ty,)* event).await.map_err(|e| Error::Handler(e.into()))
            }
        }
    };
}

impl_handler!([]);
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

    async fn test_handler(exchange: Exchange, event: TestEvent) -> anyhow::Result<()> {
        todo!()
    }

    #[tokio::test]
    async fn test_context() {
        let app = Streameroo::new(todo!(), "deez");
        app.spawn_handler(test_handler, "nuts", Default::default(), Default::default());
    }
}
