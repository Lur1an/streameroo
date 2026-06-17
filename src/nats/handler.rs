//! The [`Handler`] trait and its supporting types.
//!
//! The trait is intentionally free of `Clone`/`Send`/`'static` bounds. Those are
//! requirements of *how* a handler is driven (sequentially or concurrently) and
//! are imposed by the processor structs rather than the contract itself.

use crate::nats::dlq::{self, DlqConfig};
use async_nats::jetstream::{self, AckKind};
use tracing::Instrument;

use crate::event::Decode;
use async_nats::HeaderMap;
use std::fmt::Display;
use std::future::Future;

/// Errors returned by a [`Handler`] must declare whether they are retriable.
///
/// - Retriable errors cause the message to be NAK'd and redelivered by the
///   server (subject to the consumer's `max_deliver` and `backoff`).
/// - Non-retriable errors are dead-lettered (if a DLQ is configured) and then
///   terminated, so they are never redelivered.
pub trait HandlerError: Display + Send + 'static {
    /// Whether the message should be retried. Return `false` for permanent
    /// failures (validation errors, unknown message types, etc.).
    fn is_retriable(&self) -> bool;
}

/// Metadata about a JetStream message passed to a [`Handler`].
///
/// The context borrows the subject and headers from the underlying message,
/// avoiding a clone on every dispatch. It is only valid for the duration of the
/// [`Handler::handle`] call.
pub struct MessageContext<'a> {
    /// The subject the message was published to.
    pub subject: &'a str,
    /// The stream the message was consumed from.
    pub source_stream: &'a str,
    /// The message headers, if any.
    pub headers: Option<&'a HeaderMap>,
    /// Number of times this message has been delivered (1 on first delivery).
    pub delivered: i64,
    /// The message's sequence number within the stream.
    pub stream_sequence: u64,
    /// The message's sequence number within the consumer.
    pub consumer_sequence: u64,
}

/// Processes messages from a JetStream consumer.
///
/// The trait declares only the message type, error type, and the handling
/// method. Lifecycle bounds are added by the processor:
/// - the sequential processor requires `Handler + Send`,
/// - the queue (concurrent) processor requires `Handler + Clone + Send + 'static`.
pub trait Handler {
    /// The decoded message type.
    type Event: Decode + Send;
    /// The error type, which declares its own retriability.
    type Error: HandlerError;

    fn handle(
        &self,
        ctx: &MessageContext<'_>,
        event: Self::Event,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Handles a single message end-to-end: parse metadata, build a tracing span,
/// then decode, dispatch, and acknowledge within it.
///
/// Takes the handler by value and returns it so a sequential caller can reuse
/// the same handler for the next message without requiring `Clone`.
pub(crate) async fn process<H: Handler>(
    handler: H,
    js: &jetstream::Context,
    dlq: Option<&DlqConfig>,
    msg: jetstream::Message,
) -> H {
    let info = match msg.info() {
        Ok(info) => info,
        Err(e) => {
            tracing::error!(%e, "Failed to parse JetStream message info, terminating");
            ack(&msg, AckKind::Term).await;
            return handler;
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

    dispatch(handler, js, dlq, &msg, &ctx).instrument(span).await
}

/// Decode an event into the handler's event type, run the handler and ack the
/// message.
/// If the handler returns a non-retriable error, the message is dead-lettered
async fn dispatch<H: Handler>(
    handler: H,
    js: &jetstream::Context,
    dlq: Option<&DlqConfig>,
    msg: &jetstream::Message,
    ctx: &MessageContext<'_>,
) -> H {
    let event = match H::Event::decode(msg.payload.to_vec()) {
        Ok(event) => event,
        Err(e) => {
            tracing::error!(%e, "Failed to decode message, dead-lettering");
            dead_letter(js, dlq, msg, ctx, &e.to_string(), false).await;
            return handler;
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
            dead_letter(js, dlq, msg, ctx, &e.to_string(), false).await;
        }
    }

    handler
}

async fn dead_letter(
    js: &jetstream::Context,
    dlq: Option<&DlqConfig>,
    msg: &jetstream::Message,
    ctx: &MessageContext<'_>,
    error: &str,
    retriable: bool,
) {
    if let Some(dlq) = dlq {
        let dlq_ctx = dlq::DlqContext {
            message: ctx,
            error,
            retriable,
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
