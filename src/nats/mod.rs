mod concurrent;
mod dlq;
mod error;
mod extensions;
mod handler;
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
