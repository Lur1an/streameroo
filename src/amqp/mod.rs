mod auto;
mod channel;
mod connection;
mod context;
mod handler;
mod result;

pub use amqprs;
use amqprs::channel::BasicConsumeArguments;
use amqprs::BasicProperties;
pub use auto::*;
pub use channel::*;
pub use context::*;
pub use handler::*;
pub use result::*;

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_stream::StreamExt;
use tracing::{Instrument, Level};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Handler error: {0}")]
    Handler(BoxError),
    #[error("Event Data error: {0}")]
    Event(BoxError),
    #[error(transparent)]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("AMQP error: {0}")]
    AMQP(#[from] amqprs::error::Error),
}

impl Error {
    pub(crate) fn event(e: impl Into<BoxError>) -> Self {
        Self::Event(e.into())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Streameroo {
    context: Arc<Context>,
    consumer_tag: String,
    shutdown: Arc<AtomicBool>,
    tasks: Vec<JoinHandle<Result<()>>>,
}

impl Streameroo {
    pub fn new(context: impl Into<Context>, consumer_tag: impl Into<String>) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        Self {
            shutdown,
            context: Arc::new(context.into()),
            consumer_tag: consumer_tag.into(),
            tasks: Vec::new(),
        }
    }

    pub async fn join(self, graceful: bool) -> Result<()> {
        if graceful {
            tokio::spawn({
                async move {
                    tokio::signal::ctrl_c()
                        .await
                        .expect("Failed to listen for shutdown signal");
                    tracing::info!("Shutdown intercepted, disabling consumers");
                    self.shutdown.store(true, Relaxed);
                }
            });
        }
        for task in self.tasks {
            // Tasks should never have a JoinError as we don't interfere with the handles
            task.await.expect("JoinError")?;
        }
        Ok(())
    }

    ///// Runs a consumer on the given queue with default options
    ///// - acks all successful deliveries with `multiple: false`
    ///// - nacks all failed deliveries with `requeue: true` and `multiple: false`
    ///// - Default options & fieldtable
    //pub async fn consume<P, T, E>(
    //    &mut self,
    //    handler: impl AMQPHandler<P, T, E>,
    //    queue: impl AsRef<str>,
    //) -> Result<()>
    //where
    //    T: AMQPResult,
    //{
    //    self.consume_with_options(handler, queue, Default::default(), None::<String>)
    //        .await
    //}
    //
    ///// Fully configurable queue consuming
    //#[allow(clippy::too_many_arguments)]
    //pub async fn consume_with_options<P, T, E>(
    //    &mut self,
    //    handler: impl AMQPHandler<P, T, E>,
    //    queue: impl AsRef<str>,
    //    options: BasicConsumeArguments,
    //    consumer_tag_override: Option<impl Into<String>>,
    //) -> Result<()>
    //where
    //    T: AMQPResult,
    //{
    //    let context: Arc<Context> = self.context.clone();
    //    let queue = queue.as_ref();
    //    // If the return type is manual or no_ack is set we skip ack/nack'ing the delivery
    //    let skip_ack = T::manual() || options.no_ack;
    //    let consumer_tag = consumer_tag_override
    //        .map(Into::into)
    //        .unwrap_or(format!("{}-{}", self.consumer_tag, queue));
    //    tracing::info!(
    //        queue,
    //        consumer_tag,
    //        skip_ack,
    //        ?options,
    //        ?arguments,
    //        ?ack_options,
    //        ?nack_options,
    //        "Starting consumer"
    //    );
    //    let mut consumer = context
    //        .channel
    //        .basic_consume(queue, &consumer_tag, options, arguments)
    //        .await?;
    //    let shutdown = self.shutdown.clone();
    //    let fut = async move {
    //        let mut tasks = JoinSet::new();
    //        loop {
    //            if shutdown.load(Relaxed) {
    //                tracing::info!("Shutting down consumer");
    //                break;
    //            }
    //            if let Some(attempted_delivery) = consumer.next().await {
    //                let delivery = attempted_delivery?;
    //                let context = context.clone();
    //                let handler = handler.clone();
    //                let delivery_tag = delivery.delivery_tag;
    //                let fut = async move {
    //                    let (delivery_context, payload) =
    //                        context::create_delivery_context(delivery, context);
    //                    match handler.call(payload, &delivery_context).await {
    //                        Ok(ret) => match ret.handle_result(&delivery_context).await {
    //                            Ok(_) => {
    //                                // On manual AMQPResult don't ack the result handling
    //                                if skip_ack {
    //                                    return;
    //                                }
    //                                tracing::info!(?ack_options, "Acking delivery");
    //                                if let Err(e) = delivery_context.acker.ack(ack_options).await {
    //                                    tracing::error!(?e, "Error acking delivery");
    //                                }
    //                            }
    //                            Err(e) => {
    //                                tracing::error!(?e, "Error processing AMQPResult");
    //                                if skip_ack {
    //                                    return;
    //                                }
    //                                tracing::info!(?nack_options, "Nacking delivery");
    //                                if let Err(e) = delivery_context.acker.nack(nack_options).await
    //                                {
    //                                    tracing::error!(?e, "Error nacking delivery");
    //                                }
    //                            }
    //                        },
    //                        // Decoding an event failed, this would fail again if we nacked the
    //                        // delivery, so the framework automatically nacks without requeue.
    //                        Err(Error::Event(e)) => {
    //                            tracing::error!(?e, "Error decoding event");
    //                            let nack_options = BasicNackOptions {
    //                                requeue: false,
    //                                multiple: nack_options.multiple,
    //                            };
    //                            tracing::info!(?nack_options, "Nacking delivery");
    //                            if let Err(e) = delivery_context.acker.nack(nack_options).await {
    //                                tracing::error!(?e, "Error acking delivery");
    //                            }
    //                        }
    //                        Err(e) => {
    //                            tracing::error!(?e, "Error calling AMQP handler");
    //                            if skip_ack {
    //                                return;
    //                            }
    //                            tracing::info!(?nack_options, "Nacking delivery");
    //                            if let Err(e) = delivery_context.acker.nack(nack_options).await {
    //                                tracing::error!(?e, "Error nacking delivery");
    //                            }
    //                        }
    //                    }
    //                };
    //                tasks.spawn(fut.instrument(tracing::span!(
    //                    Level::INFO,
    //                    "streameroo",
    //                    delivery_tag
    //                )));
    //            }
    //            // Drain the taskset as it might become full otherwise
    //            while tasks.try_join_next().is_some() {}
    //        }
    //        tasks.join_all().await;
    //        Ok(())
    //    };
    //    let task = tokio::spawn(fut);
    //    self.tasks.push(task);
    //    Ok(())
    //}
}

//#[cfg(test)]
//mod amqp_test {
//    use std::time::Duration;
//
//    use lapin::{Channel, Connection};
//    use test_context::AsyncTestContext;
//    use testcontainers_modules::rabbitmq::RabbitMq;
//    use testcontainers_modules::testcontainers::runners::AsyncRunner;
//    use testcontainers_modules::testcontainers::ContainerAsync;
//
//    pub struct AMQPTest {
//        pub channel: Channel,
//        pub connection: Connection,
//        container: ContainerAsync<RabbitMq>,
//    }
//
//    impl AsyncTestContext for AMQPTest {
//        async fn setup() -> Self {
//            tracing_subscriber::fmt().init();
//            let container = RabbitMq::default().start().await.unwrap();
//            let host_ip = container.get_host().await.unwrap();
//            let host_port = container.get_host_port_ipv4(5672).await.unwrap();
//
//            let url = format!("amqp://guest:guest@{host_ip}:{host_port}");
//            let connection = Connection::connect(&url, Default::default())
//                .await
//                .expect("Failed to connect to broker");
//            tokio::time::sleep(Duration::from_millis(100)).await;
//            let channel = connection.create_channel().await.unwrap();
//            Self {
//                channel,
//                connection,
//                container,
//            }
//        }
//        async fn teardown(self) {
//            self.channel.close(0, "bye").await.unwrap();
//            self.container.rm().await.unwrap();
//        }
//    }
//}

//#[cfg(test)]
//mod test {
//    use super::*;
//    use crate::event::Json;
//    use amqp_test::AMQPTest;
//    use lapin::options::QueueDeclareOptions;
//    use nix::sys::signal::Signal;
//    use nix::unistd::Pid;
//    use serde::{Deserialize, Serialize};
//    use std::convert::Infallible;
//    use std::sync::atomic::{AtomicU8, Ordering};
//    use std::time::{Duration, Instant};
//    use test_context::test_context;
//    use uuid::Uuid;
//
//    #[derive(Debug, Serialize, Deserialize)]
//    struct TestEvent(String);
//
//    async fn reply_to_handler(
//        event: Json<TestEvent>,
//    ) -> anyhow::Result<PublishReply<Json<TestEvent>>> {
//        let event = event.into_inner();
//        assert_eq!(event.0, "hello");
//        Ok(PublishReply(Json(TestEvent("world".into()))))
//    }
//
//    /// This event handler "nacks" messages whilst the count is < 3 after incrementing.
//    /// This lets us assume that it is redelivered when the count is not 0, and fresh otherwise.
//    /// The content of the event needs to be equal to "hello".
//    async fn event_handler(
//        counter: StateOwned<Arc<AtomicU8>>,
//        exchange: Exchange,
//        redelivered: Redelivered,
//        event: Json<TestEvent>,
//    ) -> anyhow::Result<()> {
//        let event = event.into_inner();
//
//        assert_eq!(exchange.into_inner(), "");
//        assert_eq!(event.0, "hello");
//        let count = counter.load(Ordering::Relaxed);
//        tracing::info!(?count);
//        if count == 0 {
//            assert!(!*redelivered);
//        } else {
//            assert!(*redelivered);
//        }
//        if count < 3 {
//            counter.fetch_add(1, Ordering::Relaxed);
//            anyhow::bail!("Go again");
//        }
//        Ok(())
//    }
//
//    async fn manual_ack_handler(
//        counter: StateOwned<Arc<AtomicU8>>,
//        event: Json<TestEvent>,
//    ) -> Result<DeliveryAction, Infallible> {
//        let count = counter.load(Ordering::Relaxed);
//        let event = event.into_inner();
//        assert_eq!(event.0, "hello");
//        tracing::info!(?count);
//        if count < 5 {
//            counter.fetch_add(1, Ordering::Relaxed);
//            Ok(DeliveryAction::Nack {
//                requeue: true,
//                multiple: false,
//            })
//        } else {
//            Ok(DeliveryAction::Nack {
//                multiple: false,
//                requeue: false,
//            })
//        }
//    }
//
//    #[test_context(AMQPTest)]
//    #[tokio::test]
//    async fn test_reply_to_handler(ctx: &mut AMQPTest) -> anyhow::Result<()> {
//        let queue = Uuid::new_v4().to_string();
//        ctx.channel
//            .queue_declare(
//                &queue,
//                QueueDeclareOptions {
//                    auto_delete: true,
//                    ..Default::default()
//                },
//                Default::default(),
//            )
//            .await?;
//        let mut app = Streameroo::new(ctx.channel.clone(), "test-consumer");
//        app.consume(reply_to_handler, &queue).await?;
//        let mut channel = ctx.connection.create_channel().await?;
//        let result: Json<TestEvent> = channel
//            .direct_rpc(
//                "",
//                &queue,
//                Duration::from_secs(5),
//                Json(TestEvent("hello".into())),
//            )
//            .await?;
//        assert_eq!(result.into_inner().0, "world");
//
//        Ok(())
//    }
//
//    #[test_context(AMQPTest)]
//    #[tokio::test]
//    async fn test_manual_ack_handler(ctx: &mut AMQPTest) -> anyhow::Result<()> {
//        let queue = Uuid::new_v4().to_string();
//        ctx.channel
//            .queue_declare(
//                &queue,
//                QueueDeclareOptions {
//                    auto_delete: true,
//                    ..Default::default()
//                },
//                Default::default(),
//            )
//            .await?;
//        let counter = Arc::new(AtomicU8::new(0));
//        let mut context = Context::new(ctx.channel.clone());
//        context.data(counter.clone());
//
//        let mut app = Streameroo::new(context, "test-consumer");
//        app.consume(manual_ack_handler, &queue).await?;
//        ctx.channel
//            .publish("", &queue, Json(TestEvent("hello".into())))
//            .await?;
//        let t = Instant::now();
//        loop {
//            if counter.load(Ordering::Relaxed) == 5 {
//                break;
//            }
//            if t.elapsed().as_secs() > 5 {
//                panic!("Test timed out");
//            }
//            tokio::time::sleep(Duration::from_millis(500)).await;
//        }
//        Ok(())
//    }
//
//    #[test_context(AMQPTest)]
//    #[tokio::test]
//    async fn test_simple_handler(ctx: &mut AMQPTest) -> anyhow::Result<()> {
//        let queue = Uuid::new_v4().to_string();
//        ctx.channel
//            .queue_declare(
//                &queue,
//                QueueDeclareOptions {
//                    auto_delete: true,
//                    ..Default::default()
//                },
//                Default::default(),
//            )
//            .await?;
//        let counter = Arc::new(AtomicU8::new(0));
//        let mut context = Context::new(ctx.channel.clone());
//        context.data(counter.clone());
//
//        let mut app = Streameroo::new(context, "test-consumer");
//        app.consume(event_handler, &queue).await?;
//        ctx.channel
//            .publish("", &queue, Json(TestEvent("hello".into())))
//            .await?;
//        let t = Instant::now();
//        loop {
//            if counter.load(Ordering::Relaxed) == 3 {
//                break;
//            }
//            if t.elapsed().as_secs() > 5 {
//                panic!("Test timed out");
//            }
//            tokio::time::sleep(Duration::from_millis(100)).await;
//        }
//        Ok(())
//    }
//
//    #[test_context(AMQPTest)]
//    #[tokio::test]
//    async fn test_simple_handler_graceful_shutdown(ctx: &mut AMQPTest) -> anyhow::Result<()> {
//        let queue = Uuid::new_v4().to_string();
//        ctx.channel
//            .queue_declare(
//                &queue,
//                QueueDeclareOptions {
//                    auto_delete: true,
//                    ..Default::default()
//                },
//                Default::default(),
//            )
//            .await?;
//        let counter = Arc::new(AtomicU8::new(0));
//        let mut context = Context::new(ctx.channel.clone());
//        context.data(counter.clone());
//
//        let mut app = Streameroo::new(context, "test-consumer");
//        app.consume(event_handler, &queue).await?;
//        let join = tokio::spawn(app.join(true));
//        tokio::time::sleep(Duration::from_millis(100)).await;
//
//        nix::sys::signal::kill(Pid::this(), Signal::SIGINT).unwrap();
//        ctx.channel
//            .publish("", &queue, Json(TestEvent("hello".into())))
//            .await?;
//        tokio::time::sleep(Duration::from_millis(100)).await;
//        // Use timeout as if the signal handling fails `app.join` will never complete
//        tokio::time::timeout(Duration::from_secs(2), join).await???;
//        // We killed the process, however the handler has been spawned once since we don't abort
//        // the delivery consumption future.
//        assert_eq!(counter.load(Ordering::Relaxed), 1);
//        Ok(())
//    }
//}
