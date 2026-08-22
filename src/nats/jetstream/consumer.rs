//! Durable JetStream consumers and the machinery that drives them.
//!
//! A [`Consumer`] pairs a [`Handler`] with the stream/consumer configuration and
//! exposes two ways to run it:
//! - [`Consumer::run_sequential`] processes one message at a time, in order. The
//!   handler is borrowed inline (never cloned), so it does **not** need `Clone`.
//! - [`Consumer::run_concurrent`] spawns a task per message. The handler is
//!   cloned per task and must therefore be `Clone`; ordering is not guaranteed.
//!
//! Both race a caller-supplied `shutdown` future against each pull so the
//! consumer stops promptly when shutdown is requested.

use crate::event::Decode;
use crate::nats::Error;
use crate::nats::jetstream::dlq::{self, DlqConfig};
use crate::nats::jetstream::handler::{
    BackoffPolicy, ErrorAction, Handler, HandlerError, MessageContext,
};
use async_nats::jetstream::consumer::Consumer as JsConsumer;
use async_nats::jetstream::consumer::pull::{
    Config as PullConsumerConfig, MessagesError, MessagesErrorKind,
};
use async_nats::jetstream::stream::Config as StreamConfig;
use async_nats::jetstream::{self, AckKind};
use futures::StreamExt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::Instrument;

/// Configuration for a durable JetStream consumer.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Optional dead-letter configuration. When `None`, non-retriable failures
    /// are terminated (`AckKind::Term`) and dropped.
    pub dlq: Option<DlqConfig>,
    /// Backoff policy used to compute the NAK delay when a handler returns an
    /// [`ErrorAction::Retry`] error. Defaults to [`BackoffPolicy::None`].
    pub backoff: BackoffPolicy,
    /// The underlying pull consumer configuration. `durable_name` must be set.
    pub config: PullConsumerConfig,
    /// The stream configuration we're consuming from.
    pub stream: StreamConfig,
}

/// A durable JetStream consumer bound to a [`Handler`].
pub struct Consumer<H> {
    config: ConsumerConfig,
    handler: H,
    js: async_nats::jetstream::Context,
}

impl<H> Consumer<H> {
    /// Creates a consumer from a JetStream context, configuration and handler.
    ///
    /// The stream and consumer are not touched until one of the `run_*` methods
    /// is called.
    pub fn new(js: async_nats::jetstream::Context, config: ConsumerConfig, handler: H) -> Self {
        Self {
            config,
            handler,
            js,
        }
    }
}

/// Computes how often to send a working-ack (`AckKind::Progress`) while a
/// handler is in flight, given the consumer's effective `ack_wait`.
///
/// Heartbeating at half of `ack_wait` keeps the delivery comfortably ahead of
/// the server's redelivery deadline. A zero `ack_wait` (the protocol's "use the
/// server default", 30s) is treated as 30s so we never busy-loop.
fn heartbeat_interval(ack_wait: Duration) -> Duration {
    if ack_wait.is_zero() {
        Duration::from_secs(30)
    } else {
        ack_wait / 2
    }
}

impl<H> Consumer<H>
where
    H: Handler + Send + 'static,
{
    /// Processes one message at a time on the consumer task, in order.
    /// The consumer's *server-side* `max_ack_pending` must be 1 or this returns
    /// a config validation error. This ensures only one message is globally in
    /// flight when multiple sequential workers share the durable. The check is
    /// against the value reported by the server (authoritative even when the
    /// consumer is provisioned externally via Kubernetes or the NATS CLI), not
    /// the locally-supplied config.
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
                    backoff,
                    stream,
                },
            mut handler,
            js,
        } = self;

        // Ensure the stream(s) exist and create / fetch the durable consumer,
        // propagating any setup failure to the caller.
        let consumer = setup_consumer(&js, stream, dlq.as_ref(), config).await?;

        // Validate against the consumer's *server-side* config, which is
        // authoritative even when the consumer is provisioned externally (e.g.
        // Kubernetes or the NATS CLI) and the locally-supplied config differs.
        let server_config = &consumer.cached_info().config;
        if server_config.max_ack_pending != 1 {
            return Err(Error::Config(
                "max_ack_pending must be 1 for sequential consumers",
            ));
        }
        // Derive the heartbeat from the server-side `ack_wait` for the same
        // reason: it is correct even when `ack_wait` is set externally and left
        // unset in code.
        let handler_heartbeat_duration = heartbeat_interval(server_config.ack_wait);
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
                    match msg {
                        Ok(msg) => {
                            process(&mut handler, &js, dlq.as_ref(), backoff, msg, handler_heartbeat_duration).await;
                        }
                        Err(e) => classify_pull_error(e)?,
                    }
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
                    backoff,
                    stream,
                },
            handler,
            js,
        } = self;

        // Ensure the stream(s) exist and create / fetch the durable consumer,
        // propagating any setup failure to the caller.
        let consumer = setup_consumer(&js, stream, dlq.as_ref(), config).await?;
        // Derive the heartbeat from the consumer's *server-side* `ack_wait`. This
        // is authoritative even when the stream/consumer was provisioned
        // externally (e.g. Kubernetes or the NATS CLI) and the locally-supplied
        // config left `ack_wait` unset.
        let handler_heartbeat_duration = heartbeat_interval(consumer.cached_info().config.ack_wait);
        let mut messages = consumer.messages().await?;
        let dlq = dlq.map(Arc::new);

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
                                process(&mut handler, &js, dlq.as_deref(), backoff, msg, handler_heartbeat_duration).await;
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
        while let Some(result) = tasks.join_next().await {
            if let Err(e) = result {
                tracing::error!(?e, "Error joining task");
            }
        }

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

/// Classifies an error yielded by the consumer's message stream. Recoverable
/// errors are logged and `Ok(())` is returned so the caller keeps consuming;
/// non-recoverable errors are returned for the caller to stop on.
fn classify_pull_error(e: MessagesError) -> Result<(), Error> {
    match e.kind() {
        MessagesErrorKind::MissingHeartbeat
        | MessagesErrorKind::Pull
        | MessagesErrorKind::NoResponders => {
            tracing::debug!(%e, "Recoverable error on consumer");
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
    backoff: BackoffPolicy,
    msg: jetstream::Message,
    handler_heartbeat_duration: Duration,
) {
    let info = match msg.info() {
        Ok(info) => info,
        Err(e) => {
            tracing::error!(%e, "Failed to parse JetStream message info, terminating");
            ack(&msg, AckKind::Term).await;
            return;
        }
    };

    let subject = msg.subject.as_str();
    let delivered = info.delivered;
    let stream_sequence = info.stream_sequence;
    let consumer_sequence = info.consumer_sequence;

    #[cfg(feature = "telemetry")]
    let span = {
        use crate::nats::telemetry;
        use opentelemetry::trace::SpanKind;
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        let span = telemetry::make_span_for_subject(subject, SpanKind::Consumer);
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
        subject,
        source_stream: info.stream,
        headers: msg.headers.as_ref(),
        delivered,
        stream_sequence,
        consumer_sequence,
        published: info.published,
    };

    let dispatch_fut = dispatch(handler, js, dlq, backoff, &msg, &ctx).instrument(span);
    tokio::pin!(dispatch_fut);

    loop {
        tokio::select! {
            _ = tokio::time::sleep(handler_heartbeat_duration) => {
                ack(&msg, AckKind::Progress).await;
            }
            result = &mut dispatch_fut => {
                return result;
            }
        }
    }
}

/// Decode an event into the handler's event type, run the handler and ack the
/// message according to the [`ErrorAction`] of any returned error.
///
/// - [`ErrorAction::Retry`] NAKs with a delay computed from `backoff`.
/// - [`ErrorAction::Dlq`] dead-letters (if a DLQ is configured) then terminates.
/// - [`ErrorAction::Term`] terminates without redelivery.
///
/// A decode failure is always treated as a dead-letter (it can never succeed on
/// redelivery).
async fn dispatch<H: Handler>(
    handler: &mut H,
    js: &jetstream::Context,
    dlq: Option<&DlqConfig>,
    backoff: BackoffPolicy,
    msg: &jetstream::Message,
    ctx: &MessageContext<'_>,
) {
    let event = match H::Event::decode(msg.payload.to_vec()) {
        Ok(event) => event,
        Err(e) => {
            tracing::error!(%e, "Failed to decode message, dead-lettering");
            dead_letter(js, dlq, msg, ctx, e.to_string()).await;
            return;
        }
    };

    let error = match handler.handle(ctx, event).await {
        Ok(()) => {
            ack(msg, AckKind::Ack).await;
            return;
        }
        Err(e) => e,
    };

    match error.action() {
        ErrorAction::Retry => {
            let delay = backoff.nak_delay(ctx.delivered);
            tracing::warn!(
                %error,
                delivered = ctx.delivered,
                ?delay,
                "Retriable handler error, NAK'ing for redelivery"
            );
            ack(msg, AckKind::Nak(delay)).await;
        }
        ErrorAction::Dlq => {
            tracing::error!(%error, "Handler error, dead-lettering");
            dead_letter(js, dlq, msg, ctx, error.to_string()).await;
        }
        ErrorAction::Term => {
            tracing::error!(%error, "Handler error, terminating without redelivery");
            ack(msg, AckKind::Term).await;
        }
    }
}

/// Publishes the message to the DLQ (if configured) and then terminates it.
///
/// If the DLQ publish fails the message is left un-acked so it can be retried
/// later rather than silently dropped.
async fn dead_letter(
    js: &jetstream::Context,
    dlq: Option<&DlqConfig>,
    msg: &jetstream::Message,
    ctx: &MessageContext<'_>,
    error: String,
) {
    if let Some(dlq) = dlq {
        let dlq_ctx = dlq::DlqContext {
            message: ctx,
            error,
            retriable: false,
        };
        if let Err(e) = dlq::publish_to_dlq(js, &dlq.subject, msg.payload.clone(), dlq_ctx).await {
            tracing::error!(%e, "Failed to publish to DLQ, leaving message un-acked");
            return;
        }
    }
    ack(msg, AckKind::Term).await;
}

/// Acknowledges a message, logging any errors.
async fn ack(msg: &jetstream::Message, kind: AckKind) {
    if let Err(e) = msg.ack_with(kind).await {
        tracing::error!(%e, ?kind, "Failed to acknowledge message");
    }
}
