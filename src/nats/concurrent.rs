//! Concurrent message processing for work-queue style consumers.
//!
//! Each message is handled on its own spawned task. Concurrency is bounded by
//! the consumer's `max_ack_pending` setting on the server side — the server will
//! not deliver more in-flight (unacked) messages than that limit, so there is no
//! need for a client-side semaphore. The handler is cloned per task and must
//! therefore be `Clone`. Ordering is **not** guaranteed.

use crate::nats::consumer;
use crate::nats::dlq::DlqConfig;
use crate::nats::handler::Handler;
use async_nats::jetstream::consumer::Consumer;
use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::task::JoinSet;

/// Drives a JetStream pull consumer, processing messages concurrently by
/// spawning a task per message.
pub(crate) struct QueueProcessor<H> {
    pub consumer: Consumer<PullConsumerConfig>,
    pub handler: H,
    pub js: async_nats::jetstream::Context,
    pub dlq: Option<DlqConfig>,
    pub shutdown: Arc<Notify>,
}

impl<H: Handler + Clone + Send + 'static> QueueProcessor<H> {
    pub(crate) async fn run(self) {
        let QueueProcessor {
            consumer,
            handler,
            js,
            dlq,
            shutdown,
        } = self;

        let mut messages = match consumer.messages().await {
            Ok(messages) => messages,
            Err(e) => {
                tracing::error!(%e, "Failed to open consumer message stream");
                return;
            }
        };

        let notified = shutdown.notified();
        tokio::pin!(notified);

        let mut tasks = JoinSet::new();

        loop {
            tokio::select! {
                biased;
                _ = &mut notified => {
                    tracing::info!("Shutdown signal received, stopping queue consumer");
                    break;
                }
                msg = messages.next() => {
                    let Some(msg) = msg else {
                        tracing::warn!("Consumer message stream ended unexpectedly");
                        break;
                    };
                    match msg {
                        Ok(msg) => {
                            let handler = handler.clone();
                            let js = js.clone();
                            let dlq = dlq.clone();
                            tasks.spawn(async move {
                                consumer::process(handler, &js, dlq.as_ref(), msg).await;
                            });
                            // Reap completed tasks eagerly so the set does not
                            // grow unbounded.
                            while tasks.try_join_next().is_some() {}
                        }
                        Err(e) => {
                            tracing::error!(%e, "Error receiving message from consumer");
                        }
                    }
                }
            }
        }

        // Drain in-flight handlers so we don't drop work mid-flight on shutdown.
        tasks.join_all().await;
    }
}
