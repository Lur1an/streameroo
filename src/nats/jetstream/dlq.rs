//! Dead-letter queue support.
//!
//! Dead-lettered messages keep their original binary payload untouched; all
//! metadata describing *why* the message was dead-lettered is attached as NATS
//! headers. This keeps the payload byte-for-byte identical to the original so
//! that replaying a message is simply a re-publish of `msg.payload` to the
//! subject stored in [`DLQ_SOURCE_SUBJECT`].

use crate::nats::error::NatsResult;
use crate::nats::jetstream::handler::MessageContext;
use async_nats::HeaderMap;
use async_nats::jetstream::Context;
use async_nats::jetstream::publish::PublishAck;
use async_nats::jetstream::stream::Config as StreamConfig;
use bytes::Bytes;
use std::time::Duration;

/// Configuration for dead-lettering failed messages to a persistent stream.
#[derive(Debug, Clone)]
pub struct DlqConfig {
    /// The subject dead-lettered messages are published to.
    pub subject: String,
    /// Stream that captures the DLQ subject. Created if it does not exist.
    pub stream: StreamConfig,
    /// This guards against double-publishing the same dead-lettered message
    /// when a redelivery occurs (e.g. a lost publish/term ack). It must exceed
    /// the worst-case redelivery span (`ack_wait × max_deliver` plus any
    /// `backoff`) for deduplication to be reliable; streameroo does not
    /// validate this.
    pub duplicate_window: Option<Duration>,
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

/// JetStream's deduplication header. Setting it on a publish makes the server
/// discard duplicates seen within the stream's `duplicate_window`.
const NATS_MSG_ID: &str = "Nats-Msg-Id";

/// Metadata describing why a message is being dead-lettered.
pub struct DlqContext<'a> {
    pub message: &'a MessageContext<'a>,
    /// Display message for the last error that caused the message to get dead-lettered
    pub error: String,
    /// Whether a message had a retriable error or not.
    /// Deserialization errors are non-retriable and get immediately dead-lettered.
    pub retriable: bool,
}

fn sanitize_header_value(value: String) -> String {
    value.replace(['\r', '\n'], " ")
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
) -> NatsResult<PublishAck> {
    let mut headers = HeaderMap::new();

    headers.insert(DLQ_SOURCE_SUBJECT, ctx.message.subject);
    headers.insert(DLQ_ERROR, sanitize_header_value(ctx.error));
    headers.insert(DLQ_RETRIABLE, ctx.retriable.to_string());
    headers.insert(DLQ_DELIVERED, ctx.message.delivered.to_string());
    headers.insert(DLQ_STREAM_SEQUENCE, ctx.message.stream_sequence.to_string());
    headers.insert(DLQ_DEAD_LETTERED_AT, chrono::Utc::now().to_rfc3339());

    // Deterministic id derived from the original message's identity. A redelivery
    // of the same message (e.g. after a lost publish/term ack) produces the same
    // id, so the server deduplicates the re-publish within the stream's
    // `duplicate_window` instead of writing a second DLQ entry.
    let msg_id = format!(
        "{}-{}",
        ctx.message.source_stream, ctx.message.stream_sequence
    );
    headers.insert(NATS_MSG_ID, msg_id.as_str());

    // Double await: the first resolves once the publish is sent, the second
    // resolves once the server confirms the message was persisted to the DLQ
    // stream.
    let ack = js
        .publish_with_headers(dlq_subject.to_owned(), headers, payload)
        .await?
        .await?;

    if ack.duplicate {
        tracing::debug!(
            %msg_id,
            "DLQ publish deduplicated by server; message was already dead-lettered"
        );
    }

    Ok(ack)
}

#[cfg(test)]
mod test {
    use super::*;
    use async_nats::jetstream::stream::Config as StreamConfig;
    use uuid::Uuid;

    #[test]
    fn error_header_removes_line_breaks() {
        assert_eq!(
            sanitize_header_value("first\r\nsecond\nthird".to_string()),
            "first  second third"
        );
    }

    #[tokio::test]
    async fn publish_deduplicates_redelivered_message() {
        let id = Uuid::new_v4().simple().to_string();
        let stream_name = format!("dlq-stream-{id}");
        let subject = format!("dlq.{id}");
        let client = async_nats::connect(
            std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string()),
        )
        .await
        .expect("failed to connect to NATS; start it with `docker compose up -d nats`");
        let js = async_nats::jetstream::new(client);
        js.create_stream(StreamConfig {
            name: stream_name.clone(),
            subjects: vec![subject.clone()],
            duplicate_window: Duration::from_secs(30),
            ..Default::default()
        })
        .await
        .expect("failed to create DLQ stream");

        let message = MessageContext {
            subject: "source.subject",
            source_stream: "source-stream",
            headers: None,
            delivered: 1,
            stream_sequence: 7,
            consumer_sequence: 7,
            published: time::OffsetDateTime::UNIX_EPOCH,
        };
        let publish = || DlqContext {
            message: &message,
            error: "boom".to_string(),
            retriable: false,
        };

        let first = publish_to_dlq(&js, &subject, Bytes::from_static(b"payload"), publish())
            .await
            .expect("first DLQ publish failed");
        let second = publish_to_dlq(&js, &subject, Bytes::from_static(b"payload"), publish())
            .await
            .expect("second DLQ publish failed");
        let _ = js.delete_stream(&stream_name).await;

        assert!(!first.duplicate);
        assert!(second.duplicate);
    }
}
