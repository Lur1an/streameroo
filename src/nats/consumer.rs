//! Shared message-handling pipeline used by both the sequential and the queue
//! (concurrent) processors.
//!
//! [`process`] takes the handler **by value** and returns it back to the caller.
//! Owning the handler inside the same future that borrows it (for the
//! `handle(&self, ..)` call) keeps the future `Send` without requiring `H: Sync`
//! — a `&H` held across `.await` would otherwise force a `Sync` bound. The
//! sequential processor threads its single handler through each call; the queue
//! processor hands each task its own clone.

use crate::event::Decode;
use crate::nats::dlq::{self, DlqConfig};
use crate::nats::handler::{Handler, HandlerError, MessageContext};
use async_nats::jetstream::{self, AckKind};
use tracing::Instrument;

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
            // Without delivery metadata we cannot make sensible decisions;
            // terminate so we don't spin on a malformed message.
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

    let fut = dispatch(
        handler,
        js,
        dlq,
        &msg,
        &source_stream,
        &subject,
        delivered,
        stream_sequence,
        consumer_sequence,
    );

    fut.instrument(span).await
}

/// Decodes, invokes the handler, and acknowledges. Takes the handler by value
/// and returns it so it can be reused for the next message.
#[allow(clippy::too_many_arguments)]
async fn dispatch<H: Handler>(
    handler: H,
    js: &jetstream::Context,
    dlq: Option<&DlqConfig>,
    msg: &jetstream::Message,
    source_stream: &str,
    subject: &str,
    delivered: i64,
    stream_sequence: u64,
    consumer_sequence: u64,
) -> H {
    // Decode failures are never retriable: the same bytes would fail again.
    let event = match H::Event::decode(msg.payload.to_vec()) {
        Ok(event) => event,
        Err(e) => {
            tracing::error!(%e, "Failed to decode message, dead-lettering");
            dead_letter(
                js,
                dlq,
                msg,
                source_stream,
                subject,
                &e.to_string(),
                false,
                delivered,
                stream_sequence,
            )
            .await;
            return handler;
        }
    };

    let ctx = MessageContext {
        subject,
        headers: msg.headers.as_ref(),
        delivered,
        stream_sequence,
        consumer_sequence,
    };

    match handler.handle(&ctx, event).await {
        Ok(()) => ack(msg, AckKind::Ack).await,
        Err(e) if e.is_retriable() => {
            tracing::warn!(%e, delivered, "Retriable handler error, NAK'ing for redelivery");
            // NAK with no explicit delay: the consumer's `backoff`
            // configuration governs the redelivery schedule.
            ack(msg, AckKind::Nak(None)).await;
        }
        Err(e) => {
            tracing::error!(%e, "Non-retriable handler error, dead-lettering");
            dead_letter(
                js,
                dlq,
                msg,
                source_stream,
                subject,
                &e.to_string(),
                false,
                delivered,
                stream_sequence,
            )
            .await;
        }
    }

    handler
}

/// Dead-letters a message: publishes to the DLQ (if configured, awaiting the
/// JetStream ack first) and only then terminates the original.
#[allow(clippy::too_many_arguments)]
async fn dead_letter(
    js: &jetstream::Context,
    dlq: Option<&DlqConfig>,
    msg: &jetstream::Message,
    source_stream: &str,
    subject: &str,
    error: &str,
    retriable: bool,
    delivered: i64,
    stream_sequence: u64,
) {
    if let Some(dlq) = dlq {
        let ctx = dlq::DlqContext {
            source_stream,
            source_subject: subject,
            error,
            retriable,
            delivered,
            stream_sequence,
        };
        if let Err(e) = dlq::publish_to_dlq(js, &dlq.subject, msg.payload.clone(), ctx).await {
            // The DLQ publish failed: do NOT ack/term the original so it can
            // be retried later rather than silently lost.
            tracing::error!(%e, "Failed to publish to DLQ, leaving message un-acked");
            return;
        }
    }
    // Either the DLQ publish succeeded, or no DLQ is configured: terminate so
    // the server stops redelivering this message.
    ack(msg, AckKind::Term).await;
}

/// Acknowledges a message, logging (but not propagating) ack failures.
async fn ack(msg: &jetstream::Message, kind: AckKind) {
    if let Err(e) = msg.ack_with(kind).await {
        tracing::error!(%e, ?kind, "Failed to acknowledge message");
    }
}
