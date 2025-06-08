mod auto;
mod channel;
mod connection;
mod consumer;
mod context;
mod handler;
mod result;

pub use amqprs;
pub use auto::*;
pub use channel::*;
pub use context::*;
pub use handler::*;
pub use result::*;

use self::connection::{AMQPConnection, ConnectionError};
use self::consumer::Consumer;
use amqprs::channel::{BasicConsumeArguments, BasicQosArguments};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Handler error: {0}")]
    Handler(BoxError),
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

pub type Result<T> = std::result::Result<T, Error>;

pub struct Streameroo {
    context: Arc<Context>,
    consumer_tag: String,
    shutdown: Arc<Notify>,
    connection: AMQPConnection,
    tasks: Vec<JoinHandle<()>>,
}

impl Streameroo {
    pub fn new(
        connection: AMQPConnection,
        context: Context,
        consumer_tag: impl Into<String>,
    ) -> Self {
        let shutdown = Arc::new(Notify::new());
        Self {
            connection,
            shutdown,
            context: Arc::new(context),
            consumer_tag: consumer_tag.into(),
            tasks: Vec::new(),
        }
    }

    pub fn connection(&self) -> &AMQPConnection {
        &self.connection
    }

    //pub async fn join(self, graceful: bool) -> Result<()> {
    //    if graceful {
    //        tokio::spawn({
    //            async move {
    //                tokio::signal::ctrl_c()
    //                    .await
    //                    .expect("Failed to listen for shutdown signal");
    //                tracing::info!("Shutdown intercepted, disabling consumers");
    //                self.shutdown.store(true, Relaxed);
    //            }
    //        });
    //    }
    //    for task in self.tasks {
    //        // Tasks should never have a JoinError as we don't interfere with the handles
    //        task.await.expect("JoinError")?;
    //    }
    //    Ok(())
    //}

    /// Runs a consumer on the given queue with default options
    /// - acks all successful deliveries with `multiple: false`
    /// - nacks all failed deliveries with `requeue: true` and `multiple: false`
    /// - Default options & fieldtable
    pub async fn consume<P, T, E>(
        &mut self,
        handler: impl AMQPHandler<P, T, E>,
        queue: impl Into<String>,
        consumers: u16,
    ) -> Result<()>
    where
        T: AMQPResult + 'static,
        E: Send + 'static,
        P: Send + 'static,
    {
        let options = BasicConsumeArguments {
            queue: queue.into(),
            consumer_tag: self.consumer_tag.clone(),
            ..Default::default()
        };
        let qos_args = BasicQosArguments {
            prefetch_count: consumers,
            ..Default::default()
        };
        self.consume_with_options(handler, options, qos_args).await
    }

    pub async fn consume_with_options<P, T, E>(
        &mut self,
        handler: impl AMQPHandler<P, T, E>,
        options: BasicConsumeArguments,
        qos_args: BasicQosArguments,
    ) -> Result<()>
    where
        T: AMQPResult + 'static,
        E: Send + 'static,
        P: Send + 'static,
    {
        let consumer = Consumer::new(
            self.context.clone(),
            self.connection.clone(),
            options,
            qos_args,
            handler,
            self.shutdown.clone(),
        );
        let task = tokio::spawn(consumer.consume());
        self.tasks.push(task);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::event::Json;
    use amqprs::channel::QueueDeclareArguments;
    use connection::amqp_test::AMQPTest;
    use nix::sys::signal::Signal;
    use nix::unistd::Pid;
    use serde::{Deserialize, Serialize};
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::time::{Duration, Instant};
    use test_context::test_context;
    use uuid::Uuid;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestEvent(String);

    #[test_context(AMQPTest)]
    #[tokio::test]
    async fn test_reply_to_handler(ctx: &mut AMQPTest) -> anyhow::Result<()> {
        async fn reply_to_handler(
            event: Json<TestEvent>,
        ) -> anyhow::Result<PublishReply<Json<TestEvent>>> {
            let event = event.into_inner();
            assert_eq!(event.0, "hello");
            Ok(PublishReply(Json(TestEvent("world".into()))))
        }
        let queue = Uuid::new_v4().to_string();
        let channel = ctx.connection.open_channel().await?;
        channel
            .queue_declare(QueueDeclareArguments::new(&queue))
            .await?;
        let context = Context::new();
        let mut app = Streameroo::new(ctx.connection.clone(), context, "test-consumer");
        app.consume(reply_to_handler, &queue, 1).await?;
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
}
//
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
