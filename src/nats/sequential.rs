//! Sequential, in-order message processing.
//!
//! Messages are handled one at a time on the consumer's own task. Because the
//! handler is borrowed inline (never cloned, never moved into a sub-task) it
//! does **not** need to be `Clone`. Ordering is guaranteed.

use crate::nats::consumer;
use crate::nats::dlq::DlqConfig;
use crate::nats::handler::Handler;
use async_nats::jetstream::consumer::Consumer;
use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::Notify;

/// Drives a JetStream pull consumer, processing each message sequentially.
pub(crate) struct SequentialProcessor<H> {
    pub consumer: Consumer<PullConsumerConfig>,
    pub handler: H,
    pub js: async_nats::jetstream::Context,
    pub dlq: Option<DlqConfig>,
    pub shutdown: Arc<Notify>,
}

impl<H: Handler + Send> SequentialProcessor<H> {
    pub(crate) async fn run(self) {
        // Destructure into owned fields so the future never holds `&self`,
        // which would otherwise force a `Sync` bound on `H`.
        let SequentialProcessor {
            consumer,
            mut handler,
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

        loop {
            tokio::select! {
                biased;
                _ = &mut notified => {
                    tracing::info!("Shutdown signal received, stopping sequential consumer");
                    break;
                }
                msg = messages.next() => {
                    let Some(msg) = msg else {
                        tracing::warn!("Consumer message stream ended unexpectedly");
                        break;
                    };
                    match msg {
                        Ok(msg) => {
                            // Thread the handler through so it can be reused for
                            // the next message without requiring `Clone`.
                            handler = consumer::process(handler, &js, dlq.as_ref(), msg).await;
                        }
                        Err(e) => {
                            tracing::error!(%e, "Error receiving message from consumer");
                        }
                    }
                }
            }
        }
    }
}
