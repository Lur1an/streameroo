mod auto;
mod channel;
mod context;
mod extensions;
mod result;

pub use auto::*;
pub use channel::*;
pub use context::*;
pub use extensions::*;
pub use lapin;
pub use result::*;

use crate::event::Decode;
use lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions};
use lapin::types::FieldTable;
use lapin::Channel;
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tracing::{Instrument, Level};

pub struct Streameroo {
    context: Arc<Context>,
    consumer_tag: String,
    shutdown: Arc<AtomicBool>,
    tasks: Vec<JoinHandle<Result<(), lapin::Error>>>,
}

impl From<Channel> for Context {
    fn from(channel: Channel) -> Self {
        Self::new(channel)
    }
}

impl Streameroo {
    pub fn new(context: impl Into<Context>, consumer_tag: impl Into<String>) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        tokio::spawn({
            let shutdown = shutdown.clone();
            async move {
                tokio::signal::ctrl_c()
                    .await
                    .expect("Failed to listen for shutdown signal");
                shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        });
        Self {
            shutdown,
            context: Arc::new(context.into()),
            consumer_tag: consumer_tag.into(),
            tasks: Vec::new(),
        }
    }

    pub fn channel(&self) -> &Channel {
        &self.context.channel
    }

    pub async fn join(self) -> Result<(), lapin::Error> {
        for task in self.tasks {
            // Tasks should never have a JoinError as we don't interfere with the handles
            task.await.expect("JoinError")?;
        }
        Ok(())
    }

    /// Runs a consumer on the given queue with default options
    /// - acks all successful deliveries with `multiple: false`
    /// - nacks all failed deliveries with `requeue: true` and `multiple: false`
    /// - Default options & fieldtable
    pub async fn consume<P, T, E>(
        &mut self,
        handler: impl AMQPHandler<P, T, E>,
        queue: impl AsRef<str>,
    ) -> lapin::Result<()>
    where
        T: AMQPResult,
    {
        self.consume_with_options(
            handler,
            queue,
            Default::default(),
            Default::default(),
            BasicAckOptions { multiple: false },
            BasicNackOptions {
                requeue: true,
                multiple: false,
            },
            None::<String>,
        )
        .await
    }

    /// Fully configurable queue consuming
    #[allow(clippy::too_many_arguments)]
    pub async fn consume_with_options<P, T, E>(
        &mut self,
        handler: impl AMQPHandler<P, T, E>,
        queue: impl AsRef<str>,
        options: BasicConsumeOptions,
        arguments: FieldTable,
        ack_options: BasicAckOptions,
        nack_options: BasicNackOptions,
        consumer_tag_override: Option<impl Into<String>>,
    ) -> lapin::Result<()>
    where
        T: AMQPResult,
    {
        let context: Arc<Context> = self.context.clone();
        let queue = queue.as_ref();
        // If the return type is manual or no_ack is set we skip ack/nack'ing the delivery
        let skip_ack = T::manual() || options.no_ack;
        let consumer_tag = consumer_tag_override
            .map(Into::into)
            .unwrap_or(format!("{}-{}", self.consumer_tag, queue));
        tracing::info!(
            queue,
            consumer_tag,
            skip_ack,
            ?options,
            ?arguments,
            ?ack_options,
            ?nack_options,
            "Starting consumer"
        );
        let mut consumer = context
            .channel
            .basic_consume(queue, &consumer_tag, options, arguments)
            .await?;
        let shutdown = self.shutdown.clone();
        let fut = async move {
            loop {
                if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                    tracing::info!("Shutting down consumer");
                    break;
                }
                if let Some(attempted_delivery) = consumer.next().await {
                    let delivery = attempted_delivery?;
                    let context = context.clone();
                    let handler = handler.clone();
                    let delivery_tag = delivery.delivery_tag;
                    let fut = async move {
                        let (delivery_context, payload) =
                            context::create_delivery_context(delivery, context);
                        match handler.call(payload, &delivery_context).await {
                            Ok(ret) => match ret.handle_result(&delivery_context).await {
                                Ok(_) => {
                                    // On manual AMQPResult don't ack the result handling
                                    if skip_ack {
                                        return;
                                    }
                                    tracing::info!(?ack_options, "Acking delivery");
                                    if let Err(e) = delivery_context.acker.ack(ack_options).await {
                                        tracing::error!(?e, "Error acking delivery");
                                    }
                                }
                                Err(e) => {
                                    tracing::error!(?e, "Error processing AMQPResult");
                                    if skip_ack {
                                        return;
                                    }
                                    tracing::info!(?nack_options, "Nacking delivery");
                                    if let Err(e) = delivery_context.acker.nack(nack_options).await
                                    {
                                        tracing::error!(?e, "Error nacking delivery");
                                    }
                                }
                            },
                            // Decoding an event failed, this would fail again if we nacked the
                            // delivery, so the framework automatically nacks without requeue.
                            Err(Error::Event(e)) => {
                                tracing::error!(?e, "Error decoding event");
                                let nack_options = BasicNackOptions {
                                    requeue: false,
                                    multiple: nack_options.multiple,
                                };
                                tracing::info!(?nack_options, "Nacking delivery");
                                if let Err(e) = delivery_context.acker.nack(nack_options).await {
                                    tracing::error!(?e, "Error acking delivery");
                                }
                            }
                            Err(e) => {
                                tracing::error!(?e, "Error calling AMQP handler");
                                if skip_ack {
                                    return;
                                }
                                tracing::info!(?nack_options, "Nacking delivery");
                                if let Err(e) = delivery_context.acker.nack(nack_options).await {
                                    tracing::error!(?e, "Error nacking delivery");
                                }
                            }
                        }
                    };
                    tokio::spawn(fut.instrument(tracing::span!(
                        Level::INFO,
                        "streameroo::amqp",
                        delivery_tag
                    )));
                }
            }
            Ok::<_, lapin::Error>(())
        };
        let task = tokio::spawn(fut);
        self.tasks.push(task);
        Ok(())
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// - `P`: Type for the parameters of the future generating closure
/// - `T`: Type for the return value of the future generated by the closure, implements `AMQPResult`
/// - `Err`: Error type of the handler
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
    #[error("Event Data error: {0}")]
    Event(BoxError),
    #[error("Consumer stream closed")]
    StreamClosed,
    #[error(transparent)]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("Lapin error: {0}")]
    Lapin(#[from] lapin::Error),
}

impl Error {
    pub(crate) fn event(e: impl Into<BoxError>) -> Self {
        Self::Event(e.into())
    }
}

pub trait AMQPDecode: Sized {
    fn decode(payload: Vec<u8>, context: &DeliveryContext) -> Result<Self, Error>;
}

impl<E> AMQPDecode for E
where
    E: Decode,
{
    fn decode(payload: Vec<u8>, _: &DeliveryContext) -> Result<Self, Error> {
        E::decode(payload).map_err(Error::event)
    }
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
            E: AMQPDecode,
            T: AMQPResult,
        {
            async fn call(&self, payload: Vec<u8>, delivery_context: &DeliveryContext) -> Result<T, Error> {
                let event = E::decode(payload, delivery_context)?;
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
mod amqp_test {
    use std::time::Duration;

    use lapin::{Channel, Connection};
    use test_context::AsyncTestContext;

    pub struct AMQPTest {
        pub channel: Channel,
        pub connection: Connection,
    }

    impl AsyncTestContext for AMQPTest {
        async fn setup() -> Self {
            tracing_subscriber::fmt().init();
            let url = "amqp://user:password@localhost:5672";
            let connection = Connection::connect(url, Default::default())
                .await
                .expect("Failed to connect to broker");
            tokio::time::sleep(Duration::from_millis(100)).await;
            let channel = connection.create_channel().await.unwrap();
            Self {
                channel,
                connection,
            }
        }
        async fn teardown(self) {
            self.channel.close(0, "bye").await.unwrap();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::event::Json;
    use amqp_test::AMQPTest;
    use lapin::options::QueueDeclareOptions;
    use serde::{Deserialize, Serialize};
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::time::{Duration, Instant};
    use test_context::test_context;
    use uuid::Uuid;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestEvent(String);

    async fn reply_to_handler(
        event: Json<TestEvent>,
    ) -> anyhow::Result<PublishReply<Json<TestEvent>>> {
        let event = event.into_inner();
        assert_eq!(event.0, "hello");
        Ok(PublishReply(Json(TestEvent("world".into()))))
    }

    async fn event_handler(
        counter: StateOwned<Arc<AtomicU8>>,
        exchange: Exchange,
        redelivered: Redelivered,
        event: Json<TestEvent>,
    ) -> anyhow::Result<()> {
        let event = event.into_inner();

        assert_eq!(exchange.into_inner(), "");
        assert_eq!(event.0, "hello");
        let count = counter.load(Ordering::Relaxed);
        tracing::info!(?count);
        if count == 0 {
            assert!(!redelivered.into_inner());
        } else {
            assert!(redelivered.into_inner());
        }
        if count < 3 {
            counter.fetch_add(1, Ordering::Relaxed);
            anyhow::bail!("Go again");
        }
        Ok(())
    }

    async fn manual_ack_handler(
        counter: StateOwned<Arc<AtomicU8>>,
        event: Json<TestEvent>,
    ) -> Result<DeliveryAction, Infallible> {
        let count = counter.load(Ordering::Relaxed);
        let event = event.into_inner();
        assert_eq!(event.0, "hello");
        tracing::info!(?count);
        if count < 5 {
            counter.fetch_add(1, Ordering::Relaxed);
            Ok(DeliveryAction::Nack {
                requeue: true,
                multiple: false,
            })
        } else {
            Ok(DeliveryAction::Nack {
                multiple: false,
                requeue: false,
            })
        }
    }

    #[test_context(AMQPTest)]
    #[tokio::test]
    async fn test_reply_to_handler(ctx: &mut AMQPTest) -> anyhow::Result<()> {
        let queue = Uuid::new_v4().to_string();
        ctx.channel
            .queue_declare(
                &queue,
                QueueDeclareOptions {
                    auto_delete: true,
                    ..Default::default()
                },
                Default::default(),
            )
            .await?;
        let mut app = Streameroo::new(ctx.channel.clone(), "test-consumer");
        app.consume(reply_to_handler, &queue).await?;
        let mut channel = ctx.connection.create_channel().await?;
        let result: Json<TestEvent> = channel
            .direct_rpc(
                "",
                &queue,
                Duration::from_secs(5),
                Json(TestEvent("hello".into())),
            )
            .await?;
        assert_eq!(result.into_inner().0, "world");

        Ok(())
    }

    #[test_context(AMQPTest)]
    #[tokio::test]
    async fn test_manual_ack_handler(ctx: &mut AMQPTest) -> anyhow::Result<()> {
        let queue = Uuid::new_v4().to_string();
        ctx.channel
            .queue_declare(
                &queue,
                QueueDeclareOptions {
                    auto_delete: true,
                    ..Default::default()
                },
                Default::default(),
            )
            .await?;
        let counter = Arc::new(AtomicU8::new(0));
        let mut context = Context::new(ctx.channel.clone());
        context.data(counter.clone());

        let mut app = Streameroo::new(context, "test-consumer");
        app.consume(manual_ack_handler, &queue).await?;
        ctx.channel
            .publish("", &queue, Json(TestEvent("hello".into())))
            .await?;
        let t = Instant::now();
        loop {
            if counter.load(Ordering::Relaxed) == 5 {
                break;
            }
            if t.elapsed().as_secs() > 5 {
                panic!("Test timed out");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok(())
    }

    #[test_context(AMQPTest)]
    #[tokio::test]
    async fn test_simple_handler(ctx: &mut AMQPTest) -> anyhow::Result<()> {
        let queue = Uuid::new_v4().to_string();
        ctx.channel
            .queue_declare(
                &queue,
                QueueDeclareOptions {
                    auto_delete: true,
                    ..Default::default()
                },
                Default::default(),
            )
            .await?;
        let counter = Arc::new(AtomicU8::new(0));
        let mut context = Context::new(ctx.channel.clone());
        context.data(counter.clone());

        let mut app = Streameroo::new(context, "test-consumer");
        app.consume(event_handler, &queue).await?;
        ctx.channel
            .publish("", &queue, Json(TestEvent("hello".into())))
            .await?;
        let t = Instant::now();
        loop {
            if counter.load(Ordering::Relaxed) == 3 {
                break;
            }
            if t.elapsed().as_secs() > 5 {
                panic!("Test timed out");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }
}
