mod concurrent;
mod consumer;
mod dlq;
mod error;
mod extensions;
mod handler;
#[cfg(any(test, feature = "nats-test"))]
pub mod nats_test;
mod sequential;
#[cfg(feature = "telemetry")]
mod telemetry;

pub use dlq::{
    DLQ_DEAD_LETTERED_AT, DLQ_DELIVERED, DLQ_ERROR, DLQ_RETRIABLE, DLQ_SOURCE_SUBJECT,
    DLQ_STREAM_SEQUENCE, DlqConfig,
};
pub use error::{Error, NatsResult};
pub use extensions::{ClientExt, JetStreamExt};
pub use handler::{Handler, HandlerError, MessageContext};

use crate::nats::concurrent::QueueProcessor;
use crate::nats::sequential::SequentialProcessor;
use async_nats::jetstream::consumer::Consumer;
use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
use async_nats::jetstream::stream::Config as StreamConfig;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// The entrypoint for building NATS JetStream consumers and managing their
/// lifecycle.
pub struct Streameroo {
    /// Notified when a graceful shutdown is requested; all running consumers
    /// observe this and stop pulling new messages.
    shutdown: Arc<Notify>,
    /// The underlying NATS client.
    client: async_nats::Client,
    /// The JetStream context derived from `client`.
    js: async_nats::jetstream::Context,
    /// Handles to spawned consumer tasks, awaited by [`Streameroo::join`].
    tasks: Vec<JoinHandle<()>>,
}

impl Streameroo {
    pub fn new(client: async_nats::Client, domain: Option<&str>) -> Self {
        let js = if let Some(domain) = domain {
            async_nats::jetstream::with_domain(client.clone(), domain)
        } else {
            async_nats::jetstream::new(client.clone())
        };
        Self {
            shutdown: Arc::new(Notify::new()),
            client,
            js,
            tasks: Vec::new(),
        }
    }

    /// The underlying core NATS client.
    pub fn client(&self) -> &async_nats::Client {
        &self.client
    }

    /// The JetStream context.
    pub fn jetstream(&self) -> &async_nats::jetstream::Context {
        &self.js
    }

    /// Returns a clone of the [`Notify`] used to shut down all active consumers.
    pub fn shutdown_handle(&self) -> Arc<Notify> {
        self.shutdown.clone()
    }

    /// Listens for the given signal and gracefully shuts down all consumers when
    /// it resolves. The signal's output is ignored, so it may be a
    /// `Result<T, E>`, an `Option<T>`, or `()`.
    ///
    /// # Example
    /// ```ignore
    /// app.with_graceful_shutdown(tokio::signal::ctrl_c()).join().await;
    /// ```
    pub fn with_graceful_shutdown<F, T>(&mut self, signal: F) -> &mut Self
    where
        F: Future<Output = T> + Send + 'static,
    {
        let notify = self.shutdown.clone();
        tokio::spawn(async move {
            signal.await;
            tracing::info!("Shutdown intercepted, disabling consumers");
            notify.notify_waiters();
        });
        self
    }

    /// Joins all active consumer tasks. Blocks until every consumer has stopped,
    /// which only happens after a graceful shutdown is requested.
    pub async fn join(&mut self) {
        for task in self.tasks.drain(..) {
            if let Err(e) = task.await {
                tracing::error!(?e, "JoinError on consumer task");
            }
        }
    }

    /// Registers a durable JetStream consumer that processes messages
    /// **sequentially**, one at a time and in order.
    ///
    /// The handler is invoked inline on the consumer's task, so it does not need
    /// to be `Clone`. Use this for ordered workloads (event sourcing,
    /// projections, partitioned consumers).
    pub async fn consume_sequential<H: Handler + Send + 'static>(
        &mut self,
        stream_config: StreamConfig,
        consumer_config: ConsumerConfig,
        handler: H,
    ) -> NatsResult<&mut Self> {
        let consumer = self.setup_consumer(stream_config, &consumer_config).await?;
        let processor = SequentialProcessor {
            consumer,
            handler,
            js: self.js.clone(),
            dlq: consumer_config.dlq,
            shutdown: self.shutdown.clone(),
        };
        self.tasks.push(tokio::spawn(processor.run()));
        Ok(self)
    }

    /// Registers a durable JetStream consumer that processes messages
    /// **concurrently**, spawning a task per message.
    ///
    /// Concurrency is bounded by the consumer's `max_ack_pending` setting (the
    /// server will not deliver more in-flight messages than that). The handler
    /// is cloned per task and must therefore be `Clone`. Ordering is not
    /// guaranteed. Use this for work-queue style consumers.
    pub async fn consume_queue<H: Handler + Clone + Send + 'static>(
        &mut self,
        stream_config: StreamConfig,
        consumer_config: ConsumerConfig,
        handler: H,
    ) -> NatsResult<&mut Self> {
        let consumer = self.setup_consumer(stream_config, &consumer_config).await?;
        let processor = QueueProcessor {
            consumer,
            handler,
            js: self.js.clone(),
            dlq: consumer_config.dlq,
            shutdown: self.shutdown.clone(),
        };
        self.tasks.push(tokio::spawn(processor.run()));
        Ok(self)
    }

    /// Ensures the main stream (and the DLQ stream, if configured) exist, then
    /// creates / fetches the durable pull consumer.
    async fn setup_consumer(
        &self,
        stream_config: StreamConfig,
        consumer_config: &ConsumerConfig,
    ) -> NatsResult<Consumer<PullConsumerConfig>> {
        // Resolve the durable name up-front so we fail before touching the server
        // if the config is invalid.
        let consumer_name = consumer_config
            .config
            .durable_name
            .clone()
            .ok_or(Error::Config(
                "`durable_name` is required for a durable consumer",
            ))?;

        // Ensure the main stream exists.
        let stream = self.js.get_or_create_stream(stream_config).await?;

        // Ensure the DLQ stream exists, if a DLQ is configured.
        if let Some(dlq) = &consumer_config.dlq {
            let mut dlq_stream = dlq.stream.clone();
            if let Some(window) = dlq.duplicate_window {
                dlq_stream.duplicate_window = window;
            }
            let stream = self.js.get_or_create_stream(dlq_stream.clone()).await?;

            // If we manage the dedup window and a pre-existing stream's window
            // differs, reconcile it. (`get_or_create_stream` returns the existing
            // config unchanged when the stream already exists.)
            if let Some(window) = dlq.duplicate_window
                && stream.cached_info().config.duplicate_window != window
            {
                self.js.update_stream(dlq_stream).await?;
            }
        }

        // Create / fetch the durable pull consumer.
        let consumer = stream
            .get_or_create_consumer(&consumer_name, consumer_config.config.clone())
            .await?;
        Ok(consumer)
    }
}

/// Configuration for a durable JetStream consumer.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Optional dead-letter configuration. When `None`, non-retriable failures
    /// are terminated (`AckKind::Term`) and dropped.
    pub dlq: Option<DlqConfig>,
    /// The underlying pull consumer configuration. `durable_name` must be set.
    pub config: PullConsumerConfig,
}

#[cfg(all(test, feature = "json"))]
mod test {
    use super::*;
    use crate::event::Json;
    use crate::nats::nats_test::NatsTest;
    use async_nats::jetstream::consumer::AckPolicy;
    use async_nats::jetstream::consumer::pull::Config as PullConfig;
    use async_nats::jetstream::stream::{Config as StreamCfg, RetentionPolicy};
    use futures::StreamExt;
    use serde::{Deserialize, Serialize};
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU8, AtomicU16, Ordering};
    use std::time::{Duration, Instant};
    use test_context::test_context;
    use uuid::Uuid;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestEvent(String);

    // --- Error types ---

    #[derive(Debug, thiserror::Error)]
    #[error("retriable test error")]
    struct RetriableError;
    impl HandlerError for RetriableError {
        fn is_retriable(&self) -> bool {
            true
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("permanent test error")]
    struct PermanentError;
    impl HandlerError for PermanentError {
        fn is_retriable(&self) -> bool {
            false
        }
    }

    impl HandlerError for Infallible {
        fn is_retriable(&self) -> bool {
            unreachable!()
        }
    }

    // --- Test helpers ---

    /// Builds a unique stream + durable pull consumer config for a subject.
    fn stream_and_consumer(
        subject: &str,
        max_deliver: i64,
        dlq: Option<DlqConfig>,
    ) -> (StreamCfg, ConsumerConfig) {
        stream_and_consumer_with_ack_pending(subject, max_deliver, dlq, 0)
    }

    fn stream_and_consumer_with_ack_pending(
        subject: &str,
        max_deliver: i64,
        dlq: Option<DlqConfig>,
        max_ack_pending: i64,
    ) -> (StreamCfg, ConsumerConfig) {
        let suffix = Uuid::new_v4().simple().to_string();
        let stream = StreamCfg {
            name: format!("S_{suffix}"),
            subjects: vec![subject.to_owned()],
            retention: RetentionPolicy::Limits,
            ..Default::default()
        };
        let consumer = ConsumerConfig {
            dlq,
            config: PullConfig {
                durable_name: Some(format!("C_{suffix}")),
                ack_policy: AckPolicy::Explicit,
                max_deliver,
                max_ack_pending,
                ack_wait: Duration::from_secs(2),
                ..Default::default()
            },
        };
        (stream, consumer)
    }

    fn dlq_config(subject: &str) -> DlqConfig {
        let suffix = Uuid::new_v4().simple().to_string();
        DlqConfig {
            subject: subject.to_owned(),
            stream: StreamCfg {
                name: format!("DLQ_{suffix}"),
                subjects: vec![subject.to_owned()],
                retention: RetentionPolicy::Limits,
                ..Default::default()
            },
            duplicate_window: Some(Duration::from_secs(120)),
        }
    }

    async fn wait_for(counter: &AtomicU8, expected: u8, timeout: Duration) {
        let start = Instant::now();
        loop {
            if counter.load(Ordering::Relaxed) >= expected {
                return;
            }
            if start.elapsed() > timeout {
                panic!(
                    "timed out waiting for counter to reach {expected}, got {}",
                    counter.load(Ordering::Relaxed)
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn wait_for_u16(counter: &AtomicU16, expected: u16, timeout: Duration) {
        let start = Instant::now();
        loop {
            if counter.load(Ordering::Relaxed) >= expected {
                return;
            }
            if start.elapsed() > timeout {
                panic!(
                    "timed out waiting for counter to reach {expected}, got {}",
                    counter.load(Ordering::Relaxed)
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // --- Tests ---

    #[derive(Clone)]
    struct SuccessHandler {
        counter: Arc<AtomicU8>,
    }
    impl Handler for SuccessHandler {
        type Event = Json<TestEvent>;
        type Error = Infallible;
        async fn handle(
            &self,
            ctx: &MessageContext<'_>,
            event: Json<TestEvent>,
        ) -> Result<(), Infallible> {
            assert_eq!(event.into_inner().0, "hello");
            assert_eq!(ctx.delivered, 1);
            self.counter.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn test_consume_success(ctx: &mut NatsTest) -> anyhow::Result<()> {
        let subject = format!("test.{}", Uuid::new_v4().simple());
        let (stream, consumer) = stream_and_consumer(&subject, 5, None);
        let counter = Arc::new(AtomicU8::new(0));
        ctx.app
            .consume_sequential(
                stream,
                consumer,
                SuccessHandler {
                    counter: counter.clone(),
                },
            )
            .await?;

        ctx.js
            .publish(
                subject,
                serde_json::to_vec(&TestEvent("hello".into()))?.into(),
            )
            .await?
            .await?;

        wait_for(&counter, 1, Duration::from_secs(10)).await;
        Ok(())
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn test_jetstream_xpublish(ctx: &mut NatsTest) -> anyhow::Result<()> {
        let subject = format!("test.{}", Uuid::new_v4().simple());
        let (stream, consumer) = stream_and_consumer(&subject, 5, None);
        let counter = Arc::new(AtomicU8::new(0));
        ctx.app
            .consume_sequential(
                stream,
                consumer,
                SuccessHandler {
                    counter: counter.clone(),
                },
            )
            .await?;

        // `xpublish` encodes the event, awaits the JetStream ack, and returns it.
        let ack = ctx.js.xpublish(&subject, Json(TestEvent("hello".into()))).await?;
        assert_eq!(ack.sequence, 1);

        wait_for(&counter, 1, Duration::from_secs(10)).await;
        Ok(())
    }

    #[derive(Clone)]
    struct RetryHandler {
        counter: Arc<AtomicU8>,
    }
    impl Handler for RetryHandler {
        type Event = Json<TestEvent>;
        type Error = RetriableError;
        async fn handle(
            &self,
            _ctx: &MessageContext<'_>,
            _event: Json<TestEvent>,
        ) -> Result<(), RetriableError> {
            let n = self.counter.fetch_add(1, Ordering::Relaxed);
            // Succeed on the 3rd delivery (n == 2).
            if n < 2 { Err(RetriableError) } else { Ok(()) }
        }
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn test_retriable_error_redelivers(ctx: &mut NatsTest) -> anyhow::Result<()> {
        let subject = format!("test.{}", Uuid::new_v4().simple());
        let (stream, consumer) = stream_and_consumer(&subject, 10, None);
        let counter = Arc::new(AtomicU8::new(0));
        ctx.app
            .consume_sequential(
                stream,
                consumer,
                RetryHandler {
                    counter: counter.clone(),
                },
            )
            .await?;

        ctx.js
            .publish(
                subject,
                serde_json::to_vec(&TestEvent("retry".into()))?.into(),
            )
            .await?
            .await?;

        // Handler is invoked at least 3 times (2 fails + 1 success).
        wait_for(&counter, 3, Duration::from_secs(15)).await;
        Ok(())
    }

    #[derive(Clone)]
    struct PermanentFailHandler;
    impl Handler for PermanentFailHandler {
        type Event = Json<TestEvent>;
        type Error = PermanentError;
        async fn handle(
            &self,
            _ctx: &MessageContext<'_>,
            _event: Json<TestEvent>,
        ) -> Result<(), PermanentError> {
            Err(PermanentError)
        }
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn test_non_retriable_error_dead_letters(ctx: &mut NatsTest) -> anyhow::Result<()> {
        let subject = format!("test.{}", Uuid::new_v4().simple());
        let dlq_subject = format!("dlq.{}", Uuid::new_v4().simple());
        let dlq = dlq_config(&dlq_subject);
        let dlq_stream_name = dlq.stream.name.clone();
        let (stream, consumer) = stream_and_consumer(&subject, 5, Some(dlq));

        ctx.app
            .consume_sequential(stream, consumer, PermanentFailHandler)
            .await?;

        let original = TestEvent("poison".into());
        ctx.js
            .publish(subject.clone(), serde_json::to_vec(&original)?.into())
            .await?
            .await?;

        // The message should land in the DLQ stream with original payload intact
        // and metadata headers attached.
        let dlq_stream = ctx.js.get_stream(&dlq_stream_name).await?;
        let dlq_consumer = dlq_stream
            .get_or_create_consumer(
                "dlq-reader",
                PullConfig {
                    durable_name: Some("dlq-reader".into()),
                    ack_policy: AckPolicy::Explicit,
                    ..Default::default()
                },
            )
            .await?;

        let mut messages = dlq_consumer.messages().await?;
        let msg = tokio::time::timeout(Duration::from_secs(10), messages.next())
            .await?
            .expect("dlq stream closed")?;

        // Original payload preserved byte-for-byte.
        let decoded: TestEvent = serde_json::from_slice(&msg.payload)?;
        assert_eq!(decoded.0, "poison");

        // DLQ metadata headers present.
        let headers = msg.headers.clone().expect("dlq message has headers");
        assert_eq!(
            headers.get(DLQ_SOURCE_SUBJECT).map(|v| v.as_str()),
            Some(subject.as_str())
        );
        assert_eq!(
            headers.get(DLQ_RETRIABLE).map(|v| v.as_str()),
            Some("false")
        );
        assert!(headers.get(DLQ_ERROR).is_some());
        msg.ack().await.ok();
        Ok(())
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn test_dlq_dedup_window_prevents_double_publish(
        ctx: &mut NatsTest,
    ) -> anyhow::Result<()> {
        let subject = format!("test.{}", Uuid::new_v4().simple());
        let dlq_subject = format!("dlq.{}", Uuid::new_v4().simple());
        let dlq = dlq_config(&dlq_subject);
        let dlq_stream_name = dlq.stream.name.clone();
        let (stream, consumer) = stream_and_consumer(&subject, 5, Some(dlq));
        // The dedup id is "{source_stream}-{stream_sequence}"; the first message
        // published to the source stream has sequence 1.
        let source_stream_name = stream.name.clone();

        ctx.app
            .consume_sequential(stream, consumer, PermanentFailHandler)
            .await?;

        ctx.js
            .publish(
                subject.clone(),
                serde_json::to_vec(&TestEvent("poison".into()))?.into(),
            )
            .await?
            .await?;

        // Wait for the message to be dead-lettered.
        let dlq_stream = ctx.js.get_stream(&dlq_stream_name).await?;
        let dlq_consumer = dlq_stream
            .get_or_create_consumer(
                "dlq-reader",
                PullConfig {
                    durable_name: Some("dlq-reader".into()),
                    ack_policy: AckPolicy::Explicit,
                    ..Default::default()
                },
            )
            .await?;
        let mut messages = dlq_consumer.messages().await?;
        let msg = tokio::time::timeout(Duration::from_secs(10), messages.next())
            .await?
            .expect("dlq stream closed")?;
        msg.ack().await.ok();

        // The DLQ stream must carry the configured dedup window so the server
        // deduplicates re-publishes of the same dead-lettered message.
        let mut dlq_stream = ctx.js.get_stream(&dlq_stream_name).await?;
        let info = dlq_stream.info().await?;
        assert_eq!(info.config.duplicate_window, Duration::from_secs(120));
        assert_eq!(info.state.messages, 1);

        // Simulate a redelivery re-publishing the same dead-lettered message:
        // a publish carrying the same `Nats-Msg-Id` must be deduplicated.
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", format!("{source_stream_name}-1").as_str());
        let ack = ctx
            .js
            .publish_with_headers(dlq_subject.clone(), headers, b"poison".to_vec().into())
            .await?
            .await?;
        assert!(ack.duplicate, "re-publish should be deduplicated by server");

        // The DLQ stream still holds exactly one message.
        let info = dlq_stream.info().await?;
        assert_eq!(info.state.messages, 1);
        Ok(())
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn test_dlq_dedup_window_reconciled_on_mismatch(
        ctx: &mut NatsTest,
    ) -> anyhow::Result<()> {
        let subject = format!("test.{}", Uuid::new_v4().simple());
        let dlq_subject = format!("dlq.{}", Uuid::new_v4().simple());
        let dlq = dlq_config(&dlq_subject);
        let dlq_stream_name = dlq.stream.name.clone();

        // Pre-create the DLQ stream with a different dedup window than configured.
        let mut existing = dlq.stream.clone();
        existing.duplicate_window = Duration::from_secs(30);
        ctx.js.create_stream(existing).await?;

        // Setting up the consumer must reconcile the window to the configured value.
        let (stream, consumer) = stream_and_consumer(&subject, 5, Some(dlq));
        ctx.app
            .consume_sequential(stream, consumer, PermanentFailHandler)
            .await?;

        let mut dlq_stream = ctx.js.get_stream(&dlq_stream_name).await?;
        let info = dlq_stream.info().await?;
        assert_eq!(info.config.duplicate_window, Duration::from_secs(120));
        Ok(())
    }

    #[derive(Clone)]
    struct NeverCalledHandler {
        counter: Arc<AtomicU8>,
    }
    impl Handler for NeverCalledHandler {
        type Event = Json<TestEvent>;
        type Error = Infallible;
        async fn handle(
            &self,
            _ctx: &MessageContext<'_>,
            _event: Json<TestEvent>,
        ) -> Result<(), Infallible> {
            self.counter.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn test_decode_error_dead_letters(ctx: &mut NatsTest) -> anyhow::Result<()> {
        let subject = format!("test.{}", Uuid::new_v4().simple());
        let dlq_subject = format!("dlq.{}", Uuid::new_v4().simple());
        let dlq = dlq_config(&dlq_subject);
        let dlq_stream_name = dlq.stream.name.clone();
        let (stream, consumer) = stream_and_consumer(&subject, 5, Some(dlq));

        let counter = Arc::new(AtomicU8::new(0));
        ctx.app
            .consume_sequential(
                stream,
                consumer,
                NeverCalledHandler {
                    counter: counter.clone(),
                },
            )
            .await?;

        // Publish invalid JSON — handler must never be called, message DLQ'd.
        ctx.js
            .publish(subject.clone(), b"not valid json".to_vec().into())
            .await?
            .await?;

        let dlq_stream = ctx.js.get_stream(&dlq_stream_name).await?;
        let dlq_consumer = dlq_stream
            .get_or_create_consumer(
                "dlq-reader",
                PullConfig {
                    durable_name: Some("dlq-reader".into()),
                    ack_policy: AckPolicy::Explicit,
                    ..Default::default()
                },
            )
            .await?;

        let mut messages = dlq_consumer.messages().await?;
        let msg = tokio::time::timeout(Duration::from_secs(10), messages.next())
            .await?
            .expect("dlq stream closed")?;
        assert_eq!(&msg.payload[..], b"not valid json");
        msg.ack().await.ok();

        // Handler should never have been invoked for a poison-pill payload.
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        Ok(())
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn test_graceful_shutdown(ctx: &mut NatsTest) -> anyhow::Result<()> {
        let subject = format!("test.{}", Uuid::new_v4().simple());
        let (stream, consumer) = stream_and_consumer(&subject, 5, None);
        let counter = Arc::new(AtomicU8::new(0));
        ctx.app
            .consume_sequential(
                stream,
                consumer,
                SuccessHandler {
                    counter: counter.clone(),
                },
            )
            .await?;

        ctx.js
            .publish(
                subject,
                serde_json::to_vec(&TestEvent("hello".into()))?.into(),
            )
            .await?
            .await?;
        wait_for(&counter, 1, Duration::from_secs(10)).await;

        // Trigger shutdown and ensure join returns promptly.
        ctx.app.shutdown_handle().notify_waiters();
        tokio::time::timeout(Duration::from_secs(5), ctx.app.join()).await?;
        Ok(())
    }

    // --- Queue (concurrent) processor tests ---

    #[derive(Clone)]
    struct QueueHandler {
        counter: Arc<AtomicU16>,
    }
    impl Handler for QueueHandler {
        type Event = Json<TestEvent>;
        type Error = Infallible;
        async fn handle(
            &self,
            _ctx: &MessageContext<'_>,
            _event: Json<TestEvent>,
        ) -> Result<(), Infallible> {
            // Small delay to encourage overlap between concurrent tasks.
            tokio::time::sleep(Duration::from_millis(20)).await;
            self.counter.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn test_consume_queue_processes_all(ctx: &mut NatsTest) -> anyhow::Result<()> {
        let subject = format!("test.{}", Uuid::new_v4().simple());
        // Allow several in-flight messages so the queue processor runs them
        // concurrently.
        let (stream, consumer) =
            stream_and_consumer_with_ack_pending(&subject, 5, None, 10);
        let counter = Arc::new(AtomicU16::new(0));
        ctx.app
            .consume_queue(
                stream,
                consumer,
                QueueHandler {
                    counter: counter.clone(),
                },
            )
            .await?;

        // Publish a batch of messages.
        const N: u16 = 25;
        for i in 0..N {
            ctx.js
                .publish(
                    subject.clone(),
                    serde_json::to_vec(&TestEvent(format!("msg-{i}")))?.into(),
                )
                .await?
                .await?;
        }

        wait_for_u16(&counter, N, Duration::from_secs(15)).await;
        Ok(())
    }
}
