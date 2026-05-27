mod auto;
mod channel;
mod connection;
mod consumer;
mod context;
mod extensions;
mod handler;
mod result;
#[cfg(feature = "telemetry")]
pub mod telemetry;

pub use amqprs;
pub use auto::*;
pub use channel::*;
pub use connection::*;
pub use context::*;
pub use extensions::*;
pub use handler::*;
pub use result::*;

use self::consumer::Consumer;
use amqprs::channel::{BasicConsumeArguments, BasicQosArguments};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Event Data error: {0}")]
    Event(BoxError),
    #[error(transparent)]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("Connection error: {0}")]
    Connection(#[from] ConnectionError),
    #[error("AMQP error: {0}")]
    AMQP(#[from] amqprs::error::Error),
}

impl Error {
    pub(crate) fn event(e: impl Into<BoxError>) -> Self {
        Self::Event(e.into())
    }
}

pub type StreamerooResult<T> = std::result::Result<T, Error>;

pub struct Streameroo {
    consumer_tag: String,
    shutdown: Arc<Notify>,
    connection: AMQPConnection,
    tasks: Vec<JoinHandle<()>>,
}

impl Streameroo {
    pub fn new(connection: AMQPConnection, consumer_tag: impl Into<String>) -> Self {
        let shutdown = Arc::new(Notify::new());
        Self {
            connection,
            shutdown,
            consumer_tag: consumer_tag.into(),
            tasks: Vec::new(),
        }
    }

    pub fn connection(&self) -> &AMQPConnection {
        &self.connection
    }

    /// Listens for the given signal and disables all consumers when it is received.
    /// Return type of the signal is ignored, so it can be a `Result<T, E>`, an `Option<T>`
    /// or just `()`.
    /// # Example
    /// How to block until the signal is received and wait for all consumers to finish gracefully:
    /// ```ignore
    /// app.with_graceful_shutdown(tokio::signal::ctrl_c()).join().await;
    /// ```
    pub fn with_graceful_shutdown<F, T>(&mut self, signal: F) -> &mut Self
    where
        F: Future<Output = T> + Send + 'static,
    {
        let notify = self.shutdown.clone();
        tokio::spawn({
            async move {
                signal.await;
                tracing::info!("Shutdown intercepted, disabling consumers");
                notify.notify_waiters();
            }
        });
        self
    }

    /// Returns a clone of the `Notify` used to shutdown all active consumers
    pub fn shutdown_handle(&self) -> Arc<Notify> {
        self.shutdown.clone()
    }

    /// Joins all active consumer tasks
    /// This will block until all tasks have finished, which will only happen if the shutdown
    /// handle is called or we registered a ctrl-c handler.
    pub async fn join(&mut self) {
        for task in self.tasks.drain(..) {
            match task.await {
                Ok(result) => {
                    tracing::info!("Consumer task finished with result: {:?}", result);
                }
                Err(e) => {
                    tracing::error!("JoinError on consumer task: {:?}", e);
                }
            }
        }
    }

    /// Runs a consumer on the given queue with default options
    /// - acks all successful deliveries with `multiple: false`
    /// - nacks all failed deliveries with `requeue: true` and `multiple: false`
    /// - Default options & fieldtable
    pub async fn consume<H: Handler>(
        &mut self,
        handler: H,
        queue: impl Into<String>,
        consumers: u16,
    ) -> StreamerooResult<&mut Self> {
        let options = BasicConsumeArguments {
            queue: queue.into(),
            consumer_tag: self.consumer_tag.clone(),
            ..Default::default()
        };
        let qos_args = BasicQosArguments {
            prefetch_count: consumers,
            ..Default::default()
        };
        self.consume_with_options(handler, options, qos_args)
            .await?;
        Ok(self)
    }

    pub async fn consume_with_options<H: Handler>(
        &mut self,
        handler: H,
        options: BasicConsumeArguments,
        qos_args: BasicQosArguments,
    ) -> StreamerooResult<&mut Self> {
        let consumer = Consumer::new(
            self.connection.clone(),
            options,
            qos_args,
            handler,
            self.shutdown.clone(),
        );
        let task = tokio::spawn(consumer.consume());
        self.tasks.push(task);
        Ok(self)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::event::Json;
    use crate::field_table;
    use amqprs::FieldValue;
    use amqprs::channel::{ExchangeDeclareArguments, QueueBindArguments, QueueDeclareArguments};
    use connection::amqp_test::AMQPTest;
    use nix::sys::signal::Signal;
    use nix::unistd::Pid;
    use result::Publish;
    use serde::{Deserialize, Serialize};
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
    use std::time::{Duration, Instant};
    use test_context::test_context;
    use uuid::Uuid;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestEvent(String);

    // --- Handlers ---

    #[derive(Clone)]
    struct SimpleHandler {
        counter: Arc<AtomicU8>,
    }

    impl Handler for SimpleHandler {
        type Event = Json<TestEvent>;
        type Result = ();
        type Error = anyhow::Error;

        async fn handle(
            &self,
            ctx: &DeliveryContext,
            event: Json<TestEvent>,
        ) -> anyhow::Result<()> {
            let event = event.into_inner();
            assert_eq!(ctx.exchange, "");
            assert_eq!(event.0, "hello");
            let count = self.counter.load(Ordering::Relaxed);
            tracing::info!(?count);
            if count == 0 {
                assert!(!ctx.redelivered);
            } else {
                assert!(ctx.redelivered);
            }
            if count < 3 {
                self.counter.fetch_add(1, Ordering::Relaxed);
                anyhow::bail!("Go again");
            }
            Ok(())
        }
    }

    #[test_context(AMQPTest)]
    #[tokio::test]
    async fn test_simple_handler(ctx: &mut AMQPTest) -> anyhow::Result<()> {
        let queue = Uuid::new_v4().to_string();
        let channel = ctx.connection.open_channel().await?;
        channel
            .queue_declare(QueueDeclareArguments::new(&queue))
            .await?;
        let counter = Arc::new(AtomicU8::new(0));
        let handler = SimpleHandler {
            counter: counter.clone(),
        };

        let mut app = Streameroo::new(ctx.connection.clone(), "test-consumer");
        ctx.connection
            .publish("", &queue, Json(TestEvent("hello".into())))
            .await?;
        app.consume(handler, &queue, 3).await?;
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

    #[derive(Clone)]
    struct ReplyToHandler;

    impl Handler for ReplyToHandler {
        type Event = Json<TestEvent>;
        type Result = PublishReply<Json<TestEvent>>;
        type Error = anyhow::Error;

        async fn handle(
            &self,
            ctx: &DeliveryContext,
            event: Json<TestEvent>,
        ) -> anyhow::Result<PublishReply<Json<TestEvent>>> {
            let event = event.into_inner();
            assert_eq!(event.0, "hello");
            tracing::info!(properties = ?ctx.properties);
            Ok(PublishReply(Json(TestEvent("world".into()))))
        }
    }

    #[test_context(AMQPTest)]
    #[tokio::test]
    async fn test_reply_to_handler(ctx: &mut AMQPTest) -> anyhow::Result<()> {
        let queue = Uuid::new_v4().to_string();
        let channel = ctx.connection.open_channel().await?;
        channel
            .queue_declare(QueueDeclareArguments::new(&queue))
            .await?;
        let mut app = Streameroo::new(ctx.connection.clone(), "test-consumer");
        app.consume(ReplyToHandler, &queue, 1).await?;
        let mut channel = ctx.connection.open_channel().await?;
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

    #[derive(Clone)]
    struct DeliveryLimitHandler {
        counter: Arc<AtomicU8>,
    }

    impl Handler for DeliveryLimitHandler {
        type Event = Json<TestEvent>;
        type Result = ();
        type Error = anyhow::Error;

        async fn handle(
            &self,
            _ctx: &DeliveryContext,
            event: Json<TestEvent>,
        ) -> anyhow::Result<()> {
            let event = event.into_inner();
            assert_eq!(event.0, "hello");
            let deliveries = self.counter.fetch_add(1, Ordering::Relaxed);
            if deliveries > 6 {
                panic!("Too many deliveries: {deliveries}")
            }
            anyhow::bail!("Go again");
        }
    }

    #[test_context(AMQPTest)]
    #[tokio::test]
    async fn test_delivery_limit(ctx: &mut AMQPTest) -> anyhow::Result<()> {
        let queue = Uuid::new_v4().to_string();
        let channel = ctx.connection.open_channel().await?;
        channel
            .queue_declare(
                QueueDeclareArguments::new(&queue)
                    .durable(true)
                    .arguments(field_table!(
                        ("x-queue-type", XQueueType::Quorum),
                        ("x-delivery-limit", FieldValue::u(5))
                    ))
                    .finish(),
            )
            .await
            .ok();
        ctx.connection
            .publish("", &queue, Json(TestEvent("hello".into())))
            .await?;
        let counter = Arc::new(AtomicU8::new(0));
        let handler = DeliveryLimitHandler {
            counter: counter.clone(),
        };

        let mut app = Streameroo::new(ctx.connection.clone(), "test-consumer");
        app.consume(handler, &queue, 1).await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(counter.load(Ordering::Relaxed), 6);
        Ok(())
    }

    #[derive(Clone)]
    struct ManualAckHandler {
        counter: Arc<AtomicU8>,
    }

    impl Handler for ManualAckHandler {
        type Event = Json<TestEvent>;
        type Result = DeliveryAction;
        type Error = anyhow::Error;

        async fn handle(
            &self,
            _ctx: &DeliveryContext,
            event: Json<TestEvent>,
        ) -> anyhow::Result<DeliveryAction> {
            let count = self.counter.load(Ordering::Relaxed);
            let event = event.into_inner();
            assert_eq!(event.0, "hello");
            tracing::info!(?count);
            if count < 5 {
                self.counter.fetch_add(1, Ordering::Relaxed);
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
    }

    #[test_context(AMQPTest)]
    #[tokio::test]
    async fn test_manual_ack_handler(ctx: &mut AMQPTest) -> anyhow::Result<()> {
        let queue = Uuid::new_v4().to_string();
        let channel = ctx.connection.open_channel().await?;
        channel
            .queue_declare(QueueDeclareArguments::new(&queue))
            .await?;
        let counter = Arc::new(AtomicU8::new(0));
        let handler = ManualAckHandler {
            counter: counter.clone(),
        };

        let mut app = Streameroo::new(ctx.connection.clone(), "test-consumer");
        app.consume(handler, &queue, 1).await?;
        ctx.connection
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

    #[derive(Clone)]
    struct GracefulShutdownHandler {
        counter: Arc<AtomicU8>,
    }

    impl Handler for GracefulShutdownHandler {
        type Event = Json<TestEvent>;
        type Result = ();
        type Error = anyhow::Error;

        async fn handle(
            &self,
            _ctx: &DeliveryContext,
            event: Json<TestEvent>,
        ) -> anyhow::Result<()> {
            let event = event.into_inner();
            assert_eq!(event.0, "hello");
            self.counter.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test_context(AMQPTest)]
    #[tokio::test]
    async fn test_simple_handler_graceful_shutdown(ctx: &mut AMQPTest) -> anyhow::Result<()> {
        let queue = Uuid::new_v4().to_string();
        let channel = ctx.connection.open_channel().await?;
        channel
            .queue_declare(QueueDeclareArguments::new(&queue))
            .await?;
        let counter = Arc::new(AtomicU8::new(0));
        let handler = GracefulShutdownHandler {
            counter: counter.clone(),
        };

        let mut app = Streameroo::new(ctx.connection.clone(), "test-consumer");
        app.consume(handler, &queue, 1).await?;
        let join = tokio::spawn(async move {
            app.with_graceful_shutdown(tokio::signal::ctrl_c())
                .join()
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        ctx.connection
            .publish("", &queue, Json(TestEvent("hello".into())))
            .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        nix::sys::signal::kill(Pid::this(), Signal::SIGINT).unwrap();
        ctx.connection
            .publish("", &queue, Json(TestEvent("hello".into())))
            .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Use timeout as if the signal handling fails `app.join` will never complete
        tokio::time::timeout(Duration::from_secs(2), join).await??;
        // We killed the process, however the handler has been spawned once since we don't abort
        // the delivery consumption future.
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        Ok(())
    }

    #[derive(Clone)]
    struct AllExtractorsHandler {
        success: Arc<AtomicBool>,
    }

    impl Handler for AllExtractorsHandler {
        type Event = Json<TestEvent>;
        type Result = ();
        type Error = anyhow::Error;

        async fn handle(
            &self,
            ctx: &DeliveryContext,
            event: Json<TestEvent>,
        ) -> anyhow::Result<()> {
            let event = event.into_inner();
            assert_eq!(event.0, "hello");
            assert_eq!(ctx.exchange, "test-exchange");
            assert_eq!(ctx.routing_key, "test.routing.key");
            assert_eq!(ctx.properties.reply_to().as_ref(), None);
            assert_eq!(ctx.delivery_tag, 1);
            assert!(!ctx.redelivered);
            self.success.store(true, Ordering::Relaxed);

            Ok(())
        }
    }

    #[test_context(AMQPTest)]
    #[tokio::test]
    async fn test_delivery_context_fields(ctx: &mut AMQPTest) -> anyhow::Result<()> {
        let queue = Uuid::new_v4().to_string();
        let channel = ctx.connection.open_channel().await?;

        channel
            .queue_declare(QueueDeclareArguments::new(&queue).durable(true).finish())
            .await?;
        channel
            .exchange_declare(ExchangeDeclareArguments::new("test-exchange", "direct"))
            .await?;
        channel
            .queue_bind(QueueBindArguments::new(
                &queue,
                "test-exchange",
                "test.routing.key",
            ))
            .await?;
        let success = Arc::new(AtomicBool::new(false));
        let handler = AllExtractorsHandler {
            success: success.clone(),
        };
        let mut app = Streameroo::new(ctx.connection.clone(), "test-consumer");
        app.consume(handler, &queue, 1).await?;

        // Publish message to the exchange with specific routing key
        channel
            .publish(
                "test-exchange",
                "test.routing.key",
                Json(TestEvent("hello".into())),
            )
            .await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(success.load(Ordering::Relaxed));
        Ok(())
    }

    #[derive(Clone)]
    struct PublishForwardHandler;

    impl Handler for PublishForwardHandler {
        type Event = Json<TestEvent>;
        type Result = Publish<Json<TestEvent>>;
        type Error = anyhow::Error;

        async fn handle(
            &self,
            _ctx: &DeliveryContext,
            event: Json<TestEvent>,
        ) -> anyhow::Result<Publish<Json<TestEvent>>> {
            let event = event.into_inner();
            assert_eq!(event.0, "initial");
            Ok(Publish::new(
                Json(TestEvent("forwarded".into())),
                "",
                "forward-queue",
            ))
        }
    }

    #[test_context(AMQPTest)]
    #[tokio::test]
    async fn test_publish_action_with_consume_next(ctx: &mut AMQPTest) -> anyhow::Result<()> {
        let initial_queue = Uuid::new_v4().to_string();
        let forward_queue = "forward-queue";
        let channel = ctx.connection.open_channel().await?;

        channel
            .queue_declare(QueueDeclareArguments::new(&initial_queue))
            .await?;
        channel
            .queue_declare(QueueDeclareArguments::new(forward_queue))
            .await?;

        let mut app = Streameroo::new(ctx.connection.clone(), "test-consumer");
        app.consume(PublishForwardHandler, &initial_queue, 1)
            .await?;

        ctx.connection
            .publish("", &initial_queue, Json(TestEvent("initial".into())))
            .await?;

        let forwarded_event: Json<TestEvent> = ctx.connection.consume_next(forward_queue).await;
        assert_eq!(forwarded_event.into_inner().0, "forwarded");

        Ok(())
    }

    #[derive(Clone)]
    struct DecodeErrorHandler {
        invoked: Arc<AtomicBool>,
    }

    impl Handler for DecodeErrorHandler {
        type Event = Json<TestEvent>;
        type Result = ();
        type Error = Infallible;

        async fn handle(
            &self,
            _ctx: &DeliveryContext,
            _event: Json<TestEvent>,
        ) -> Result<(), Infallible> {
            self.invoked.store(true, Ordering::Relaxed);
            Ok(())
        }
    }

    /// Verifies that a message which fails to decode (poison pill) is nacked WITHOUT requeue,
    /// preventing an infinite redelivery loop.
    #[test_context(AMQPTest)]
    #[tokio::test]
    async fn test_decode_error_nacks_without_requeue(ctx: &mut AMQPTest) -> anyhow::Result<()> {
        let queue = Uuid::new_v4().to_string();
        let channel = ctx.connection.open_channel().await?;
        channel
            .queue_declare(QueueDeclareArguments::new(&queue))
            .await?;

        let invoked = Arc::new(AtomicBool::new(false));
        let handler = DecodeErrorHandler {
            invoked: invoked.clone(),
        };

        let mut app = Streameroo::new(ctx.connection.clone(), "test-consumer");
        app.consume(handler, &queue, 1).await?;

        // Publish invalid payload — raw bytes that are not valid JSON
        ctx.connection
            .publish("", &queue, b"not valid json".to_vec())
            .await?;

        // Give consumer time to process and nack the message
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Handler should never have been called
        assert!(
            !invoked.load(Ordering::Relaxed),
            "Handler was called despite decode failure"
        );

        // Queue should be empty — message was nacked without requeue, not stuck in a loop.
        // Passive declare returns (name, message_count, consumer_count).
        let (_, message_count, _) = channel
            .queue_declare(QueueDeclareArguments::new(&queue).passive(true).finish())
            .await?
            .unwrap();
        assert_eq!(
            message_count, 0,
            "Poison pill was requeued — expected nack without requeue"
        );

        Ok(())
    }

    #[derive(Clone)]
    struct ReconnectHandler {
        counter: Arc<AtomicU8>,
    }

    impl Handler for ReconnectHandler {
        type Event = Json<TestEvent>;
        type Result = ();
        type Error = anyhow::Error;

        async fn handle(
            &self,
            _ctx: &DeliveryContext,
            event: Json<TestEvent>,
        ) -> anyhow::Result<()> {
            let event = event.into_inner();
            assert_eq!(event.0, "hello");
            self.counter.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_consumer_reconnect() -> anyhow::Result<()> {
        tracing_subscriber::fmt().init();
        let (container, args) = connection::amqp_test::start_rabbitmq_with_port(Some(5672)).await;
        let connection = AMQPConnection::connect(args).await?;

        let queue = Uuid::new_v4().to_string();
        let channel = connection.open_channel().await?;
        channel
            .queue_declare(QueueDeclareArguments::new(&queue).durable(true).finish())
            .await?;

        let counter = Arc::new(AtomicU8::new(0));
        let handler = ReconnectHandler {
            counter: counter.clone(),
        };

        let mut app = Streameroo::new(connection.clone(), "test-consumer");
        app.consume(handler, &queue, 1).await?;

        // Publish first message
        connection
            .publish("", &queue, Json(TestEvent("hello".into())))
            .await?;

        // Wait for first message to be processed
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(counter.load(Ordering::Relaxed), 1);

        // Stop the container
        container.stop().await?;
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Start the container again
        container.start().await?;
        tokio::time::sleep(Duration::from_secs(4)).await;

        // Publish second message after reconnection
        connection
            .publish("", &queue, Json(TestEvent("hello".into())))
            .await?;

        // Wait 5 seconds as requested
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(counter.load(Ordering::Relaxed), 2);

        container.rm().await?;
        Ok(())
    }
}
