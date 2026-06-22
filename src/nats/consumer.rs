//! Durable JetStream consumers and the machinery that drives them.
//!
//! A [`Consumer`] pairs a [`Handler`] with the stream/consumer configuration and
//! exposes two ways to run it:
//! - [`Consumer::run_sequential`] processes one message at a time, in order. The
//!   handler is borrowed inline (never cloned), so it does **not** need `Clone`.
//! - [`Consumer::run_concurrent`] spawns a task per message. The handler is
//!   cloned per task and must therefore be `Clone`; ordering is not guaranteed.
//!
//! Both racing a caller-supplied `shutdown` future against each pull so the
//! consumer stops promptly when shutdown is requested.

use crate::event::Decode;
use crate::nats::dlq::{self, DlqConfig};
use crate::nats::handler::{Handler, HandlerError, MessageContext};
use async_nats::jetstream::consumer::Consumer as JsConsumer;
use async_nats::jetstream::consumer::pull::{
    Config as PullConsumerConfig, MessagesError, MessagesErrorKind,
};
use async_nats::jetstream::stream::Config as StreamConfig;
use async_nats::jetstream::{self, AckKind};
use futures::StreamExt;
use std::future::Future;
use tokio::task::JoinSet;
use tracing::Instrument;

use super::Error;

/// Configuration for a durable JetStream consumer.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Optional dead-letter configuration. When `None`, non-retriable failures
    /// are terminated (`AckKind::Term`) and dropped.
    pub dlq: Option<DlqConfig>,
    /// The underlying pull consumer configuration. `durable_name` must be set.
    pub config: PullConsumerConfig,
    /// The stream configuration we're consuming from.
    pub stream: StreamConfig,
}

/// A durable JetStream consumer bound to a [`Handler`].
pub struct Consumer<H> {
    pub config: ConsumerConfig,
    pub handler: H,
    pub js: async_nats::jetstream::Context,
}

impl<H> Consumer<H>
where
    H: Handler + Send + 'static,
{
    /// Processes one message at a time on the consumer task, in order.
    /// Ensure that `config.max_ack_pending` is set to 1 or else
    /// the consumer will not start with a config validation error,
    /// as on NACK's the server does not redeliver the nacked message in order.
    /// Use this consumer when ordering is important for kafka-like message processing.
    ///
    /// `shutdown` is a future that resolves when the consumer should stop pulling
    /// new messages. Callers that never want to shut down can pass
    /// [`std::future::pending`].
    ///
    /// # Errors
    /// Errors are returned if the consumer encounters non recoverable errors or is unable to be
    /// created/start consuming.
    pub async fn run_sequential(
        self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), Error> {
        let Consumer {
            config:
                ConsumerConfig {
                    config,
                    dlq,
                    stream,
                },
            mut handler,
            js,
        } = self;

        if config.max_ack_pending != 1 {
            return Err(Error::Config(
                "max_ack_pending must be 1 for sequential consumers",
            ));
        }

        // Ensure the stream(s) exist and create / fetch the durable consumer,
        // propagating any setup failure to the caller.
        let consumer = setup_consumer(&js, stream, dlq.as_ref(), config).await?;
        let mut messages = consumer.messages().await?;

        // Race the shutdown future against each pull so we stop promptly when
        // shutdown is requested.
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    tracing::info!("Shutdown signal received, stopping sequential consumer");
                    break;
                }
                Some(msg) = messages.next() => {
                    process_message_result_sequential(msg, &mut handler, &js, dlq.as_ref()).await?;
                }
                else => {
                    tracing::warn!("Consumer message stream ended unexpectedly");
                    break;
                }
            }
        }

        Ok(())
    }
}

impl<H> Consumer<H>
where
    H: Handler + Clone + Send + 'static,
{
    /// Processes messages concurrently, spawning a task per message which is why `H` must be
    /// `Clone`.
    /// Use this consumer when work-queue style consumers are desired.
    ///
    /// `shutdown` is a future that resolves when the consumer should stop pulling
    /// new messages; in-flight handlers are drained before returning. Callers
    /// that never want to shut down can pass [`std::future::pending`].
    ///
    /// # Errors
    /// Errors are returned if the consumer encounters non recoverable errors or is unable to be
    /// created/start consuming.
    pub async fn run_concurrent(
        self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), Error> {
        let Consumer {
            config:
                ConsumerConfig {
                    config,
                    dlq,
                    stream,
                },
            handler,
            js,
        } = self;

        // Ensure the stream(s) exist and create / fetch the durable consumer,
        // propagating any setup failure to the caller.
        let consumer = setup_consumer(&js, stream, dlq.as_ref(), config).await?;
        let mut messages = consumer.messages().await?;

        // Each message is handled on its own task. Concurrency is bounded by the
        // consumer's `max_ack_pending` (the server will not deliver more
        // in-flight messages than that), so no client-side semaphore is needed.
        let mut tasks = JoinSet::new();
        let mut result = Ok(());

        // Race the shutdown future against each pull so we stop promptly when
        // shutdown is requested.
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    tracing::info!("Shutdown signal received, stopping concurrent consumer");
                    break;
                }
                Some(msg) = messages.next() => {
                    match msg {
                        Ok(msg) => {
                            let mut handler = handler.clone();
                            let js = js.clone();
                            let dlq = dlq.clone();
                            tasks.spawn(async move {
                                process(&mut handler, &js, dlq.as_ref(), msg).await;
                            });
                            // Reap completed tasks eagerly so the set does not
                            // grow unbounded.
                            while tasks.try_join_next().is_some() {}
                        }
                        Err(e) => {
                            if let Err(e) = classify_pull_error(e) {
                                result = Err(e);
                                break;
                            }
                        }
                    }
                }
                else => {
                    tracing::warn!("Consumer message stream ended unexpectedly");
                    break;
                }
            }
        }

        // Drain in-flight handlers so we don't drop work mid-flight.
        tasks.join_all().await;

        result
    }
}

/// Ensures the main stream (and the DLQ stream, if configured) exist, then
/// creates / fetches the durable pull consumer.
async fn setup_consumer(
    js: &jetstream::Context,
    stream: StreamConfig,
    dlq: Option<&DlqConfig>,
    config: PullConsumerConfig,
) -> Result<JsConsumer<PullConsumerConfig>, Error> {
    // Resolve the durable name up-front so we fail before touching the server
    // if the config is invalid.
    let consumer_name = config.durable_name.clone().ok_or(Error::Config(
        "`durable_name` is required for a durable consumer",
    ))?;

    // Ensure the main stream exists.
    let stream = js.get_or_create_stream(stream).await?;

    // Ensure the DLQ stream exists, if a DLQ is configured.
    if let Some(dlq) = dlq {
        let mut dlq_stream = dlq.stream.clone();
        if let Some(window) = dlq.duplicate_window {
            dlq_stream.duplicate_window = window;
        }
        let dlq_stream_handle = js.get_or_create_stream(dlq_stream.clone()).await?;

        // If we manage the dedup window and a pre-existing stream's window
        // differs, reconcile it. (`get_or_create_stream` returns the existing
        // config unchanged when the stream already exists.)
        if let Some(window) = dlq.duplicate_window
            && dlq_stream_handle.cached_info().config.duplicate_window != window
        {
            js.update_stream(dlq_stream).await?;
        }
    }

    // Create / fetch the durable pull consumer.
    let consumer = stream
        .get_or_create_consumer(&consumer_name, config)
        .await?;
    Ok(consumer)
}

/// Handles one item yielded by the consumer's message stream, borrowing the
/// handler mutably so a sequential caller can reuse it without `Clone`.
async fn process_message_result_sequential<H: Handler + Send>(
    msg: Result<jetstream::Message, MessagesError>,
    handler: &mut H,
    js: &jetstream::Context,
    dlq: Option<&DlqConfig>,
) -> Result<(), Error> {
    match msg {
        Ok(msg) => {
            process(handler, js, dlq, msg).await;
            Ok(())
        }
        Err(e) => classify_pull_error(e),
    }
}

/// Classifies an error yielded by the consumer's message stream. Recoverable
/// errors are logged and `Ok(())` is returned so the caller keeps consuming;
/// non-recoverable errors are returned for the caller to stop on.
fn classify_pull_error(e: MessagesError) -> Result<(), Error> {
    match e.kind() {
        MessagesErrorKind::MissingHeartbeat
        | MessagesErrorKind::Pull
        | MessagesErrorKind::NoResponders => {
            tracing::error!(%e, "Recoverable error on consumer");
            Ok(())
        }
        MessagesErrorKind::PushBasedConsumer
        | MessagesErrorKind::ConsumerDeleted
        | MessagesErrorKind::Other => Err(e.into()),
    }
}

/// Handles a single message end-to-end: parse metadata, build a tracing span,
/// then decode, dispatch, and acknowledge within it.
///
/// Borrows the handler mutably so a sequential caller can reuse the same handler
/// for the next message without requiring `Clone`.
async fn process<H: Handler>(
    handler: &mut H,
    js: &jetstream::Context,
    dlq: Option<&DlqConfig>,
    msg: jetstream::Message,
) {
    let info = match msg.info() {
        Ok(info) => info,
        Err(e) => {
            tracing::error!(%e, "Failed to parse JetStream message info, terminating");
            ack(&msg, AckKind::Term).await;
            return;
        }
    };

    let subject = msg.subject.as_str().to_owned();
    let source_stream = info.stream.to_owned();
    let delivered = info.delivered;
    let stream_sequence = info.stream_sequence;
    let consumer_sequence = info.consumer_sequence;

    #[cfg(feature = "telemetry")]
    let span = {
        use crate::nats::telemetry;
        use opentelemetry::trace::SpanKind;
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        let span = telemetry::make_span_for_subject(&subject, SpanKind::Consumer);
        if let Some(headers) = &msg.headers {
            let parent = telemetry::extract_context(headers);
            if let Err(e) = span.set_parent(parent) {
                tracing::warn!("Failed to set parent context for consumer span: {e}");
            }
        }
        span
    };
    #[cfg(not(feature = "telemetry"))]
    let span = tracing::info_span!("streameroo::nats::consume", %subject, delivered);

    let ctx = MessageContext {
        subject: &subject,
        source_stream: &source_stream,
        headers: msg.headers.as_ref(),
        delivered,
        stream_sequence,
        consumer_sequence,
    };

    dispatch(handler, js, dlq, &msg, &ctx)
        .instrument(span)
        .await
}

/// Decode an event into the handler's event type, run the handler and ack the
/// message.
/// If the handler returns a non-retriable error, the message is dead-lettered
async fn dispatch<H: Handler>(
    handler: &mut H,
    js: &jetstream::Context,
    dlq: Option<&DlqConfig>,
    msg: &jetstream::Message,
    ctx: &MessageContext<'_>,
) {
    let event = match H::Event::decode(msg.payload.to_vec()) {
        Ok(event) => event,
        Err(e) => {
            tracing::error!(%e, "Failed to decode message, dead-lettering");
            if let Some(dlq) = dlq {
                let dlq_ctx = dlq::DlqContext {
                    message: ctx,
                    error: e.to_string(),
                    retriable: false,
                };
                if let Err(e) =
                    dlq::publish_to_dlq(js, &dlq.subject, msg.payload.clone(), dlq_ctx).await
                {
                    tracing::error!(%e, "Failed to publish to DLQ, leaving message un-acked");
                    return;
                }
            }
            ack(msg, AckKind::Term).await;
            return;
        }
    };

    match handler.handle(ctx, event).await {
        Ok(()) => ack(msg, AckKind::Ack).await,
        Err(e) if e.is_retriable() => {
            tracing::warn!(%e, delivered = ctx.delivered, "Retriable handler error, NAK'ing for redelivery");
            ack(msg, AckKind::Nak(None)).await;
        }
        Err(e) => {
            tracing::error!(%e, "Non-retriable handler error, dead-lettering");
            if let Some(dlq) = dlq {
                let dlq_ctx = dlq::DlqContext {
                    message: ctx,
                    error: e.to_string(),
                    retriable: false,
                };
                if let Err(e) =
                    dlq::publish_to_dlq(js, &dlq.subject, msg.payload.clone(), dlq_ctx).await
                {
                    tracing::error!(%e, "Failed to publish to DLQ, leaving message un-acked");
                    return;
                }
            }
            ack(msg, AckKind::Term).await;
        }
    }
}

/// Acknowledges a message, logging any errors.
async fn ack(msg: &jetstream::Message, kind: AckKind) {
    if let Err(e) = msg.ack_with(kind).await {
        tracing::error!(%e, ?kind, "Failed to acknowledge message");
    }
}
