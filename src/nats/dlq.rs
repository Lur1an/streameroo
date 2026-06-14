//! Dead-letter queue support.
//!
//! Dead-lettered messages keep their original binary payload untouched; all
//! metadata describing *why* the message was dead-lettered is attached as NATS
//! headers. This keeps the payload byte-for-byte identical to the original so
//! that replaying a message is simply a re-publish of `msg.payload` to the
//! subject stored in [`DLQ_SOURCE_SUBJECT`].

use crate::nats::error::NatsResult;
use async_nats::HeaderMap;
use async_nats::jetstream::Context;
use async_nats::jetstream::stream::Config as StreamConfig;
use bytes::Bytes;

/// Configuration for dead-lettering failed messages to a persistent stream.
#[derive(Debug, Clone)]
pub struct DlqConfig {
    /// The subject dead-lettered messages are published to.
    pub subject: String,
    /// Stream that captures the DLQ subject. Created if it does not exist.
    pub stream: StreamConfig,
}

/// The subject the original message was consumed from. Used for replay.
pub const DLQ_SOURCE_SUBJECT: &str = "Dlq-Source-Subject";
/// Human-readable description of the error that caused dead-lettering.
pub const DLQ_ERROR: &str = "Dlq-Error";
/// `"true"`/`"false"` — whether the originating error was retriable.
pub const DLQ_RETRIABLE: &str = "Dlq-Retriable";
/// Number of delivery attempts the message had when dead-lettered.
pub const DLQ_DELIVERED: &str = "Dlq-Delivered";
/// Stream sequence number of the original message.
pub const DLQ_STREAM_SEQUENCE: &str = "Dlq-Stream-Sequence";
/// RFC3339 timestamp of when the message was dead-lettered.
pub const DLQ_DEAD_LETTERED_AT: &str = "Dlq-Dead-Lettered-At";

/// Metadata describing why a message is being dead-lettered.
pub(crate) struct DlqContext<'a> {
    pub source_subject: &'a str,
    pub error: &'a str,
    pub retriable: bool,
    pub delivered: i64,
    pub stream_sequence: u64,
}

/// Publishes a message to the DLQ subject and **awaits the JetStream ack**
/// before returning.
///
/// The caller must only acknowledge the original message *after* this returns
/// `Ok`, guaranteeing the dead-lettered message is durably persisted first and
/// can never be lost.
pub(crate) async fn publish_to_dlq(
    js: &Context,
    dlq_subject: &str,
    payload: Bytes,
    ctx: DlqContext<'_>,
) -> NatsResult<()> {
    let mut headers = HeaderMap::new();
    headers.insert(DLQ_SOURCE_SUBJECT, ctx.source_subject);
    headers.insert(DLQ_ERROR, ctx.error);
    headers.insert(DLQ_RETRIABLE, ctx.retriable.to_string());
    headers.insert(DLQ_DELIVERED, ctx.delivered.to_string());
    headers.insert(DLQ_STREAM_SEQUENCE, ctx.stream_sequence.to_string());
    headers.insert(DLQ_DEAD_LETTERED_AT, chrono::Utc::now().to_rfc3339());

    // Double await: the first resolves once the publish is sent, the second
    // resolves once the server confirms the message was persisted to the DLQ
    // stream.
    js.publish_with_headers(dlq_subject.to_owned(), headers, payload)
        .await?
        .await?;

    Ok(())
}
