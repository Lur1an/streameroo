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

use crate::nats::Error;

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

#[cfg(test)]
mod test {
    use super::*;
    use crate::event::{Encode, Json};
    use crate::nats::test_util::{Mode, NatsTest, TestError, TestEvent, TestHandler, wait_for};
    use assert_matches::assert_matches;
    use std::sync::Mutex;
    use std::time::Duration;
    use test_context::test_context;
    use time::OffsetDateTime;
    use tokio::sync::oneshot;

    /// Publishes a `TestEvent` to a subject and awaits the JetStream ack.
    async fn publish(ctx: &NatsTest, subject: &str, msg: &str) {
        use crate::nats::jetstream::Producer;
        ctx.js
            .produce(subject, Json(TestEvent::new(msg)))
            .await
            .expect("publish failed");
    }

    /// `(message_id, published)` captured from each handled message's context.
    type SeenContexts = Arc<Mutex<Vec<(Option<String>, OffsetDateTime)>>>;

    /// A handler that records the `message_id()` and `published` time it observes
    /// on each `MessageContext`, for asserting that metadata is surfaced.
    #[derive(Clone, Default)]
    struct CapturingHandler {
        seen: SeenContexts,
    }

    impl Handler for CapturingHandler {
        type Event = Json<TestEvent>;
        type Error = TestError;

        async fn handle(
            &mut self,
            ctx: &MessageContext<'_>,
            _event: Json<TestEvent>,
        ) -> Result<(), TestError> {
            self.seen
                .lock()
                .unwrap()
                .push((ctx.message_id().map(String::from), ctx.published));
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct OrderingRetryHandler {
        attempts: Arc<Mutex<Vec<(String, i64, std::time::Instant)>>>,
    }

    impl Handler for OrderingRetryHandler {
        type Event = Json<TestEvent>;
        type Error = TestError;

        async fn handle(
            &mut self,
            ctx: &MessageContext<'_>,
            event: Json<TestEvent>,
        ) -> Result<(), TestError> {
            let value = event.into_inner().0;
            self.attempts.lock().unwrap().push((
                value.clone(),
                ctx.delivered,
                std::time::Instant::now(),
            ));

            if value == "a" && ctx.delivered == 1 {
                Err(TestError {
                    message: "retry once".to_string(),
                    action: ErrorAction::Retry,
                })
            } else {
                Ok(())
            }
        }
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn sequential_processes_in_order(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;
        for m in ["a", "b", "c"] {
            publish(ctx, &names.subject, m).await;
        }

        let handler = TestHandler::new(Mode::Succeed);
        let consumer = Consumer {
            config: ctx.consumer_config(&names, false),
            handler: handler.clone(),
            js: ctx.js.clone(),
        };

        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(consumer.run_sequential(async move {
            let _ = rx.await;
        }));

        wait_for(Duration::from_secs(5), || handler.handled().len() == 3).await;
        let _ = tx.send(());
        task.await.unwrap().expect("consumer returned an error");

        assert_eq!(handler.handled(), vec!["a", "b", "c"]);
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn sequential_rejects_bad_max_ack_pending(ctx: &mut NatsTest) {
        let names = ctx.names();
        let mut config = ctx.consumer_config(&names, false);
        // The consumer is created from this config, so the server ends up with
        // max_ack_pending = 5; the server-side check then rejects it.
        config.config.max_ack_pending = 5;

        let consumer = Consumer {
            config,
            handler: TestHandler::new(Mode::Succeed),
            js: ctx.js.clone(),
        };

        let result = consumer.run_sequential(std::future::pending()).await;
        assert_matches!(result, Err(Error::Config(_)));
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn concurrent_processes_all(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;
        let expected = ["m1", "m2", "m3", "m4", "m5"];
        for m in expected {
            publish(ctx, &names.subject, m).await;
        }

        let mut config = ctx.consumer_config(&names, false);
        config.config.max_ack_pending = 10;

        let handler = TestHandler::new(Mode::Succeed);
        let consumer = Consumer {
            config,
            handler: handler.clone(),
            js: ctx.js.clone(),
        };

        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(consumer.run_concurrent(async move {
            let _ = rx.await;
        }));

        wait_for(Duration::from_secs(5), || handler.handled().len() == 5).await;
        let _ = tx.send(());
        task.await.unwrap().expect("consumer returned an error");

        let mut handled = handler.handled();
        handled.sort();
        let mut want: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
        want.sort();
        assert_eq!(handled, want);
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn retriable_error_redelivers(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;
        publish(ctx, &names.subject, "x").await;

        // Fail retriably on the first delivery, succeed on the second.
        let handler = TestHandler::new(Mode::RetryUntil(2));
        let consumer = Consumer {
            config: ctx.consumer_config(&names, false),
            handler: handler.clone(),
            js: ctx.js.clone(),
        };

        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(consumer.run_sequential(async move {
            let _ = rx.await;
        }));

        wait_for(Duration::from_secs(5), || handler.handled() == ["x"]).await;
        let _ = tx.send(());
        task.await.unwrap().expect("consumer returned an error");

        assert!(
            handler.call_count() >= 2,
            "expected at least one redelivery, got {} calls",
            handler.call_count()
        );
    }

    /// A `Linear` backoff policy must delay the redelivery: the gap between the
    /// first (failing) delivery and the second (succeeding) delivery should be
    /// at least the policy's base delay. Without backoff the redelivery would be
    /// nearly immediate.
    #[test_context(NatsTest)]
    #[tokio::test]
    async fn backoff_delays_redelivery(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;
        publish(ctx, &names.subject, "x").await;

        // NAK on delivery 1, succeed on delivery 2, with a 2s linear backoff so
        // the redelivery is delayed by ~2s rather than the server default.
        let mut config = ctx.consumer_config(&names, false);
        config.backoff = BackoffPolicy::Linear {
            base: Duration::from_secs(2),
            max_backoff: Duration::from_secs(10),
        };
        // ack_wait must exceed the backoff so the delayed NAK, not an ack-wait
        // timeout, drives the redelivery.
        config.config.ack_wait = Duration::from_secs(10);

        let handler = TestHandler::new(Mode::RetryUntil(2));
        let consumer = Consumer {
            config,
            handler: handler.clone(),
            js: ctx.js.clone(),
        };

        let started = std::time::Instant::now();
        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(consumer.run_sequential(async move {
            let _ = rx.await;
        }));

        wait_for(Duration::from_secs(15), || handler.handled() == ["x"]).await;
        let elapsed = started.elapsed();
        let _ = tx.send(());
        task.await.unwrap().expect("consumer returned an error");

        assert!(
            elapsed >= Duration::from_millis(1800),
            "redelivery happened too quickly ({elapsed:?}); backoff delay was not applied"
        );
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn always_retriable_caps_at_max_deliver(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;
        publish(ctx, &names.subject, "loop").await;

        // max_deliver defaults to 3 in the harness; a perpetually-retriable
        // handler should be invoked exactly that many times, then give up.
        let handler = TestHandler::new(Mode::AlwaysRetriable);
        let consumer = Consumer {
            config: ctx.consumer_config(&names, false),
            handler: handler.clone(),
            js: ctx.js.clone(),
        };

        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(consumer.run_sequential(async move {
            let _ = rx.await;
        }));

        wait_for(Duration::from_secs(5), || handler.call_count() >= 3).await;
        // Give the server a chance to (not) redeliver beyond max_deliver.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _ = tx.send(());
        task.await.unwrap().expect("consumer returned an error");

        assert_eq!(handler.call_count(), 3);
        assert!(handler.handled().is_empty());
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn non_retriable_error_dead_letters(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;
        publish(ctx, &names.subject, "bad").await;

        let handler = TestHandler::new(Mode::NonRetriable);
        let consumer = Consumer {
            config: ctx.consumer_config(&names, true),
            handler: handler.clone(),
            js: ctx.js.clone(),
        };

        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(consumer.run_sequential(async move {
            let _ = rx.await;
        }));

        let dlq = ctx
            .drain_stream(&names.dlq_stream, 1, Duration::from_secs(5))
            .await;
        let _ = tx.send(());
        task.await.unwrap().expect("consumer returned an error");

        assert_eq!(dlq.len(), 1, "expected one dead-lettered message");
        let msg = &dlq[0];
        let headers = msg.headers.as_ref().expect("DLQ message must have headers");
        assert_eq!(
            headers
                .get(crate::nats::jetstream::DLQ_RETRIABLE)
                .unwrap()
                .as_str(),
            "false"
        );
        assert_eq!(
            headers
                .get(crate::nats::jetstream::DLQ_SOURCE_SUBJECT)
                .unwrap()
                .as_str(),
            names.subject
        );
        // Payload is preserved byte-for-byte.
        let original = Json(TestEvent::new("bad")).encode().unwrap();
        assert_eq!(msg.payload.to_vec(), original);
        // Handler ran exactly once (terminated, never redelivered).
        assert_eq!(handler.call_count(), 1);
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn decode_failure_dead_letters(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;
        // Publish a payload that is not valid JSON for TestEvent.
        let garbage = b"this is not json".to_vec();
        ctx.js
            .publish(names.subject.clone(), garbage.clone().into())
            .await
            .unwrap()
            .await
            .unwrap();

        let handler = TestHandler::new(Mode::Succeed);
        let consumer = Consumer {
            config: ctx.consumer_config(&names, true),
            handler: handler.clone(),
            js: ctx.js.clone(),
        };

        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(consumer.run_sequential(async move {
            let _ = rx.await;
        }));

        let dlq = ctx
            .drain_stream(&names.dlq_stream, 1, Duration::from_secs(5))
            .await;
        let _ = tx.send(());
        task.await.unwrap().expect("consumer returned an error");

        assert_eq!(dlq.len(), 1);
        let msg = &dlq[0];
        let headers = msg.headers.as_ref().unwrap();
        assert_eq!(
            headers
                .get(crate::nats::jetstream::DLQ_RETRIABLE)
                .unwrap()
                .as_str(),
            "false"
        );
        assert_eq!(msg.payload.to_vec(), garbage);
        // The handler was never invoked because decoding failed first.
        assert_eq!(handler.call_count(), 0);
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn non_retriable_without_dlq_terminates(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;
        publish(ctx, &names.subject, "drop-me").await;

        let handler = TestHandler::new(Mode::NonRetriable);
        let consumer = Consumer {
            config: ctx.consumer_config(&names, false),
            handler: handler.clone(),
            js: ctx.js.clone(),
        };

        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(consumer.run_sequential(async move {
            let _ = rx.await;
        }));

        wait_for(Duration::from_secs(5), || handler.call_count() == 1).await;
        // Ensure it is terminated, not redelivered.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _ = tx.send(());
        task.await.unwrap().expect("consumer returned an error");

        assert_eq!(handler.call_count(), 1);
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn shutdown_stops_consumer(ctx: &mut NatsTest) {
        let names = ctx.names();
        let consumer = Consumer {
            config: ctx.consumer_config(&names, false),
            handler: TestHandler::new(Mode::Succeed),
            js: ctx.js.clone(),
        };

        // An already-ready shutdown future should stop the consumer immediately.
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            consumer.run_concurrent(std::future::ready(())),
        )
        .await
        .expect("consumer did not honor shutdown in time");

        result.expect("consumer returned an error");
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn dlq_duplicate_window_is_reconciled(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;

        // Pre-create the DLQ stream with a 1s dedup window.
        let pre = StreamConfig {
            name: names.dlq_stream.clone(),
            subjects: vec![names.dlq_subject.clone()],
            duplicate_window: Duration::from_secs(1),
            ..Default::default()
        };
        ctx.js.create_stream(pre).await.unwrap();

        // Configure the consumer's DLQ to manage a 5s window; setup should
        // reconcile the existing stream via update_stream.
        let mut config = ctx.consumer_config(&names, true);
        config.dlq.as_mut().unwrap().duplicate_window = Some(Duration::from_secs(5));

        let consumer = Consumer {
            config,
            handler: TestHandler::new(Mode::Succeed),
            js: ctx.js.clone(),
        };
        // Running with a ready shutdown still performs setup (stream creation +
        // reconcile) before stopping.
        consumer
            .run_concurrent(std::future::ready(()))
            .await
            .expect("consumer returned an error");

        let info = ctx
            .js
            .get_stream(&names.dlq_stream)
            .await
            .unwrap()
            .info()
            .await
            .unwrap()
            .config
            .duplicate_window;
        assert_eq!(info, Duration::from_secs(5));
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn consumer_deleted_returns_error(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;

        let mut config = ctx.consumer_config(&names, false);
        config.config.max_ack_pending = 10;

        let consumer = Consumer {
            config,
            handler: TestHandler::new(Mode::Succeed),
            js: ctx.js.clone(),
        };
        let task = tokio::spawn(consumer.run_concurrent(std::future::pending::<()>()));

        // Let the consumer get created and start pulling, then delete it.
        tokio::time::sleep(Duration::from_millis(500)).await;
        ctx.js
            .get_stream(&names.stream)
            .await
            .unwrap()
            .delete_consumer(&names.durable)
            .await
            .unwrap();

        let result = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("consumer did not stop after deletion")
            .unwrap();
        assert!(
            result.is_err(),
            "expected a non-recoverable error after consumer deletion"
        );
    }

    /// With `max_ack_pending == 1`, a delayed NAK remains acknowledgment-pending
    /// and continues occupying the only delivery slot. The failed message must
    /// therefore be redelivered before the next stream message.
    #[test_context(NatsTest)]
    #[tokio::test]
    async fn sequential_preserves_order_with_delayed_nak(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;
        for m in ["a", "b"] {
            publish(ctx, &names.subject, m).await;
        }

        let mut config = ctx.consumer_config(&names, false);
        config.backoff = BackoffPolicy::Linear {
            base: Duration::from_secs(1),
            max_backoff: Duration::from_secs(1),
        };

        let handler = OrderingRetryHandler::default();
        let consumer = Consumer::new(ctx.js.clone(), config, handler.clone());

        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(consumer.run_sequential(async move {
            let _ = rx.await;
        }));

        wait_for(Duration::from_secs(10), || {
            handler.attempts.lock().unwrap().len() == 3
        })
        .await;
        let _ = tx.send(());
        task.await.unwrap().expect("consumer returned an error");

        let attempts = handler.attempts.lock().unwrap();
        assert_eq!(attempts[0].0, "a");
        assert_eq!(attempts[0].1, 1);
        assert_eq!(attempts[1].0, "a");
        assert_eq!(attempts[1].1, 2);
        assert_eq!(attempts[2].0, "b");
        assert_eq!(attempts[2].1, 1);
        assert!(
            attempts[1].2.duration_since(attempts[0].2) >= Duration::from_millis(900),
            "redelivery ignored the configured delay"
        );
    }

    /// Two sequential consumers bound to the *same* durable form a competing
    /// pull group. Because the shared durable enforces `max_ack_pending == 1`,
    /// only one message is ever in flight across both consumers, so the globally
    /// observed processing order still matches the stream order. The two
    /// consumers share one handler (its `handled` buffer is `Arc`-backed and
    /// shared across clones), so the resulting snapshot is the interleaved global
    /// order.
    #[test_context(NatsTest)]
    #[tokio::test]
    async fn two_sequential_consumers_preserve_global_order(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;

        let count = 12;
        let expected: Vec<String> = (0..count).map(|i| i.to_string()).collect();
        for m in &expected {
            publish(ctx, &names.subject, m).await;
        }

        // Cloning shares the `handled`/`calls` Arcs, so both consumers append to
        // a single ordered buffer.
        let handler = TestHandler::new(Mode::Succeed);
        let consumer_a = Consumer::new(
            ctx.js.clone(),
            ctx.consumer_config(&names, false),
            handler.clone(),
        );
        let consumer_b = Consumer::new(
            ctx.js.clone(),
            ctx.consumer_config(&names, false),
            handler.clone(),
        );

        let (tx_a, rx_a) = oneshot::channel();
        let (tx_b, rx_b) = oneshot::channel();
        let task_a = tokio::spawn(consumer_a.run_sequential(async move {
            let _ = rx_a.await;
        }));
        let task_b = tokio::spawn(consumer_b.run_sequential(async move {
            let _ = rx_b.await;
        }));

        wait_for(Duration::from_secs(15), || {
            handler.handled().len() == count as usize
        })
        .await;
        let _ = tx_a.send(());
        let _ = tx_b.send(());
        task_a.await.unwrap().expect("consumer A returned an error");
        task_b.await.unwrap().expect("consumer B returned an error");

        // No duplicates and strict global ordering across both consumers.
        assert_eq!(handler.handled(), expected);
    }

    /// A handler that runs ~4x longer than `ack_wait` must be kept alive by the
    /// working-ack heartbeat (`AckKind::Progress` every `ack_wait / 2`) and
    /// therefore handled exactly once on the concurrent path. With a broken
    /// heartbeat the server would redeliver mid-handle, spawning a second task
    /// and pushing `call_count` past 1.
    #[test_context(NatsTest)]
    #[tokio::test]
    async fn slow_handler_is_not_redelivered_concurrent(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;
        publish(ctx, &names.subject, "slow").await;

        let mut config = ctx.consumer_config(&names, false);
        // ack_wait of 4s => heartbeat at 2s, leaving a wide margin that stays
        // reliable even under heavy parallel test load.
        config.config.ack_wait = Duration::from_secs(4);
        config.config.max_ack_pending = 10; // concurrent path

        // Handler runs well past ack_wait. The heartbeat must hold the delivery
        // open for the full duration; otherwise the server redelivers at ~4s.
        let handler = TestHandler::new(Mode::SlowSucceed(Duration::from_secs(10)));
        let consumer = Consumer::new(ctx.js.clone(), config, handler.clone());

        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(consumer.run_concurrent(async move {
            let _ = rx.await;
        }));

        wait_for(Duration::from_secs(25), || handler.handled().len() == 1).await;
        let _ = tx.send(());
        task.await.unwrap().expect("consumer returned an error");

        assert_eq!(
            handler.call_count(),
            1,
            "slow handler was redelivered mid-flight"
        );
        assert_eq!(handler.handled(), ["slow"]);
    }

    /// The sequential path makes the same guarantee: a handler slower than
    /// `ack_wait` is heartbeated and handled exactly once. If the heartbeat
    /// regressed, the redelivered copy would be processed after the first
    /// (inline) handle returns, pushing `call_count` past 1.
    #[test_context(NatsTest)]
    #[tokio::test]
    async fn slow_handler_is_not_redelivered_sequential(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;
        publish(ctx, &names.subject, "slow").await;

        // max_ack_pending stays 1 (the harness default) for the sequential path.
        let mut config = ctx.consumer_config(&names, false);
        // ack_wait of 4s => heartbeat at 2s, leaving a wide margin that stays
        // reliable even under heavy parallel test load.
        config.config.ack_wait = Duration::from_secs(4);

        let handler = TestHandler::new(Mode::SlowSucceed(Duration::from_secs(10)));
        let consumer = Consumer::new(ctx.js.clone(), config, handler.clone());

        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(consumer.run_sequential(async move {
            let _ = rx.await;
        }));

        wait_for(Duration::from_secs(25), || handler.handled().len() == 1).await;
        let _ = tx.send(());
        task.await.unwrap().expect("consumer returned an error");

        assert_eq!(
            handler.call_count(),
            1,
            "slow handler was redelivered mid-flight"
        );
        assert_eq!(handler.handled(), ["slow"]);
    }

    /// The heartbeat interval is derived from the consumer's *server-side*
    /// `ack_wait`, not the locally-supplied config — important when the
    /// stream/consumer is provisioned externally (Kubernetes, NATS CLI) and the
    /// caller leaves `ack_wait` unset.
    ///
    /// Here the consumer is pre-created on the server with `ack_wait = 4s`, but
    /// the `ConsumerConfig` passed to `run_*` leaves `ack_wait` at zero. If the
    /// heartbeat used the local value it would resolve to the 30s default and the
    /// 10s handler would be redelivered at ~4s. Reading the server value yields a
    /// 2s heartbeat, so the slow handler is processed exactly once.
    #[test_context(NatsTest)]
    #[tokio::test]
    async fn heartbeat_uses_server_side_ack_wait(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;
        publish(ctx, &names.subject, "slow").await;

        // Provision the durable consumer out-of-band with a 2s ack_wait, as an
        // external orchestrator (k8s/CLI) would.
        ctx.js
            .get_stream(&names.stream)
            .await
            .unwrap()
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                durable_name: Some(names.durable.clone()),
                filter_subject: names.subject.clone(),
                ack_wait: Duration::from_secs(4),
                max_deliver: 3,
                max_ack_pending: 10,
                ..Default::default()
            })
            .await
            .expect("failed to pre-create consumer");

        // Local config leaves ack_wait unset; setup_consumer will fetch the
        // existing (server) config, which is what the heartbeat must use.
        let mut config = ctx.consumer_config(&names, false);
        config.config.ack_wait = Duration::ZERO;
        config.config.max_ack_pending = 10;

        let handler = TestHandler::new(Mode::SlowSucceed(Duration::from_secs(10)));
        let consumer = Consumer::new(ctx.js.clone(), config, handler.clone());

        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(consumer.run_concurrent(async move {
            let _ = rx.await;
        }));

        wait_for(Duration::from_secs(25), || handler.handled().len() == 1).await;
        let _ = tx.send(());
        task.await.unwrap().expect("consumer returned an error");

        assert_eq!(
            handler.call_count(),
            1,
            "heartbeat did not use the server-side ack_wait; handler was redelivered"
        );
        assert_eq!(handler.handled(), ["slow"]);
    }

    /// A sequential consumer must reject a consumer whose *server-side*
    /// `max_ack_pending` is not 1, even when the local config claims a valid
    /// value. This is the dangerous drift case: an externally-provisioned
    /// consumer that allows >1 in-flight message would silently break ordering.
    #[test_context(NatsTest)]
    #[tokio::test]
    async fn sequential_rejects_server_side_bad_max_ack_pending(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;

        // Provision the consumer out-of-band with max_ack_pending = 5.
        ctx.js
            .get_stream(&names.stream)
            .await
            .unwrap()
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                durable_name: Some(names.durable.clone()),
                filter_subject: names.subject.clone(),
                ack_wait: Duration::from_secs(2),
                max_deliver: 3,
                max_ack_pending: 5,
                ..Default::default()
            })
            .await
            .expect("failed to pre-create consumer");

        // Local config claims the valid value of 1, but the server says 5.
        let mut config = ctx.consumer_config(&names, false);
        config.config.max_ack_pending = 1;

        let consumer = Consumer::new(ctx.js.clone(), config, TestHandler::new(Mode::Succeed));
        let result = consumer.run_sequential(std::future::pending()).await;
        assert_matches!(result, Err(Error::Config(_)));
    }

    /// Conversely, a sequential consumer must *accept* a consumer whose
    /// server-side `max_ack_pending` is 1 even when the local config carries a
    /// different (would-be-invalid) value — proving the validation reads the
    /// server, not the caller. The old client-side pre-check rejected this
    /// valid, externally-provisioned setup.
    #[test_context(NatsTest)]
    #[tokio::test]
    async fn sequential_accepts_server_side_max_ack_pending(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;
        publish(ctx, &names.subject, "x").await;

        // Provision the consumer out-of-band, correctly, with max_ack_pending = 1.
        ctx.js
            .get_stream(&names.stream)
            .await
            .unwrap()
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                durable_name: Some(names.durable.clone()),
                filter_subject: names.subject.clone(),
                ack_wait: Duration::from_secs(2),
                max_deliver: 3,
                max_ack_pending: 1,
                ..Default::default()
            })
            .await
            .expect("failed to pre-create consumer");

        // Local config carries a value the old pre-check would have rejected.
        let mut config = ctx.consumer_config(&names, false);
        config.config.max_ack_pending = 5;

        let handler = TestHandler::new(Mode::Succeed);
        let consumer = Consumer::new(ctx.js.clone(), config, handler.clone());

        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(consumer.run_sequential(async move {
            let _ = rx.await;
        }));

        wait_for(Duration::from_secs(5), || handler.handled() == ["x"]).await;
        let _ = tx.send(());
        task.await.unwrap().expect("consumer returned an error");

        assert_eq!(handler.handled(), ["x"]);
    }

    /// "Never lose a message": if the DLQ publish fails, the original message
    /// must be left un-acked so the server redelivers it rather than dropping
    /// it. Here the DLQ publish subject is captured by no stream, so every
    /// dead-letter publish fails and the message must keep coming back.
    #[test_context(NatsTest)]
    #[tokio::test]
    async fn dlq_publish_failure_leaves_message_unacked(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;
        publish(ctx, &names.subject, "bad").await;

        // The DLQ stream exists but only listens on `dlq_subject`; we publish to
        // an unrouted subject, so `publish_to_dlq` always fails (no responders).
        let mut config = ctx.consumer_config(&names, true);
        config.dlq = Some(DlqConfig {
            subject: format!("unrouted.{}", names.dlq_stream),
            stream: StreamConfig {
                name: names.dlq_stream.clone(),
                subjects: vec![names.dlq_subject.clone()],
                ..Default::default()
            },
            duplicate_window: None,
        });

        let handler = TestHandler::new(Mode::NonRetriable);
        let consumer = Consumer::new(ctx.js.clone(), config, handler.clone());

        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(consumer.run_sequential(async move {
            let _ = rx.await;
        }));

        // The message is never acked, so the server redelivers it (up to the
        // harness's max_deliver of 3). Seeing >1 delivery proves it wasn't
        // dropped on the publish failure.
        wait_for(Duration::from_secs(15), || handler.call_count() >= 2).await;
        let _ = tx.send(());
        task.await.unwrap().expect("consumer returned an error");

        assert!(
            handler.call_count() >= 2,
            "message was not redelivered after a DLQ publish failure"
        );
        // Nothing should have been persisted to the DLQ stream.
        let dlq = ctx
            .drain_stream(&names.dlq_stream, 1, Duration::from_secs(2))
            .await;
        assert!(dlq.is_empty(), "no message should have reached the DLQ");
        // The handler never succeeded.
        assert!(handler.handled().is_empty());
    }

    /// The concurrent path must dead-letter handler failures too. All other
    /// `run_concurrent` tests use `Mode::Succeed` or no DLQ, so the
    /// `Arc`-wrapped DLQ dead-letter path is otherwise never exercised.
    #[test_context(NatsTest)]
    #[tokio::test]
    async fn concurrent_dead_letters_on_handler_error(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;
        publish(ctx, &names.subject, "bad").await;

        let mut config = ctx.consumer_config(&names, true);
        config.config.max_ack_pending = 10;

        let handler = TestHandler::new(Mode::NonRetriable);
        let consumer = Consumer::new(ctx.js.clone(), config, handler.clone());

        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(consumer.run_concurrent(async move {
            let _ = rx.await;
        }));

        let dlq = ctx
            .drain_stream(&names.dlq_stream, 1, Duration::from_secs(5))
            .await;
        let _ = tx.send(());
        task.await.unwrap().expect("consumer returned an error");

        assert_eq!(dlq.len(), 1, "expected one dead-lettered message");
        let headers = dlq[0]
            .headers
            .as_ref()
            .expect("DLQ message must have headers");
        assert_eq!(
            headers
                .get(crate::nats::jetstream::DLQ_SOURCE_SUBJECT)
                .unwrap()
                .as_str(),
            names.subject
        );
        assert_eq!(handler.call_count(), 1);
    }

    /// `ErrorAction::Term` terminates the message: no redelivery and — crucially
    /// — no dead-lettering even when a DLQ is configured.
    #[test_context(NatsTest)]
    #[tokio::test]
    async fn term_action_terminates_without_dlq(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;
        publish(ctx, &names.subject, "term-me").await;

        // A DLQ is configured precisely to prove Term does NOT publish to it.
        let handler = TestHandler::new(Mode::Terminate);
        let consumer = Consumer::new(
            ctx.js.clone(),
            ctx.consumer_config(&names, true),
            handler.clone(),
        );

        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(consumer.run_sequential(async move {
            let _ = rx.await;
        }));

        wait_for(Duration::from_secs(5), || handler.call_count() == 1).await;
        // Give the server a chance to (not) redeliver.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _ = tx.send(());
        task.await.unwrap().expect("consumer returned an error");

        // Handled once, never redelivered...
        assert_eq!(handler.call_count(), 1);
        assert!(handler.handled().is_empty());
        // ...and nothing was dead-lettered.
        let dlq = ctx
            .drain_stream(&names.dlq_stream, 1, Duration::from_secs(2))
            .await;
        assert!(dlq.is_empty(), "Term must not publish to the DLQ");
    }

    /// A decode failure with no DLQ configured is terminated (silently dropped):
    /// the handler is never invoked and the consumer keeps going with the next
    /// message. Exercises the `dlq = None` silent-drop branch.
    #[test_context(NatsTest)]
    #[tokio::test]
    async fn decode_failure_without_dlq_drops_silently(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;
        // An un-decodable payload, followed by a valid one.
        ctx.js
            .publish(names.subject.clone(), b"not json".to_vec().into())
            .await
            .unwrap()
            .await
            .unwrap();
        publish(ctx, &names.subject, "good").await;

        let handler = TestHandler::new(Mode::Succeed);
        let consumer = Consumer::new(
            ctx.js.clone(),
            ctx.consumer_config(&names, false),
            handler.clone(),
        );

        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(consumer.run_sequential(async move {
            let _ = rx.await;
        }));

        // The garbage message is dropped without invoking the handler; the valid
        // message that follows is processed, proving the consumer didn't stall.
        wait_for(Duration::from_secs(5), || handler.handled() == ["good"]).await;
        let _ = tx.send(());
        task.await.unwrap().expect("consumer returned an error");

        assert_eq!(handler.handled(), ["good"]);
        // The handler ran exactly once — only for the valid message.
        assert_eq!(handler.call_count(), 1);
    }

    /// `MessageContext` surfaces the publisher's `Nats-Msg-Id` (via
    /// `message_id()`) and the server-side `published` timestamp.
    #[test_context(NatsTest)]
    #[tokio::test]
    async fn context_exposes_message_id_and_published(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;

        // Publish with an explicit Nats-Msg-Id so the handler can read it back.
        let before = OffsetDateTime::now_utc();
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(async_nats::header::NATS_MESSAGE_ID, "idem-123");
        ctx.js
            .publish_with_headers(
                names.subject.clone(),
                headers,
                Json(TestEvent::new("hi")).encode().unwrap().into(),
            )
            .await
            .unwrap()
            .await
            .unwrap();

        let handler = CapturingHandler::default();
        let consumer = Consumer::new(
            ctx.js.clone(),
            ctx.consumer_config(&names, false),
            handler.clone(),
        );

        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(consumer.run_sequential(async move {
            let _ = rx.await;
        }));

        wait_for(Duration::from_secs(5), || {
            handler.seen.lock().unwrap().len() == 1
        })
        .await;
        let _ = tx.send(());
        task.await.unwrap().expect("consumer returned an error");

        let seen = handler.seen.lock().unwrap();
        let (message_id, published) = &seen[0];
        assert_eq!(message_id.as_deref(), Some("idem-123"));
        // The publish timestamp sits between just-before-publish and now.
        assert!(*published >= before - time::Duration::seconds(5));
        assert!(*published <= OffsetDateTime::now_utc() + time::Duration::seconds(5));
    }
}
