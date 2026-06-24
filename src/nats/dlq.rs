//! Dead-letter queue support.
//!
//! Dead-lettered messages keep their original binary payload untouched; all
//! metadata describing *why* the message was dead-lettered is attached as NATS
//! headers. This keeps the payload byte-for-byte identical to the original so
//! that replaying a message is simply a re-publish of `msg.payload` to the
//! subject stored in [`DLQ_SOURCE_SUBJECT`].

use crate::nats::error::NatsResult;
use crate::nats::handler::MessageContext;
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
    headers.insert(DLQ_ERROR, ctx.error);
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
    use crate::nats::test_util::NatsTest;
    use async_nats::jetstream::stream::Config as StreamConfig;
    use std::time::Duration;
    use test_context::test_context;

    /// Builds a `MessageContext` with the given identity for driving DLQ publishes.
    fn message_ctx<'a>(
        subject: &'a str,
        source_stream: &'a str,
        delivered: i64,
        stream_sequence: u64,
    ) -> MessageContext<'a> {
        MessageContext {
            subject,
            source_stream,
            headers: None,
            delivered,
            stream_sequence,
            consumer_sequence: stream_sequence,
        }
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn publish_sets_all_metadata_headers(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.js
            .create_stream(StreamConfig {
                name: names.dlq_stream.clone(),
                subjects: vec![names.dlq_subject.clone()],
                ..Default::default()
            })
            .await
            .unwrap();

        let message = message_ctx(&names.subject, &names.stream, 4, 42);
        let dlq_ctx = DlqContext {
            message: &message,
            error: "boom".to_string(),
            retriable: true,
        };
        let payload = Bytes::from_static(b"raw-payload");

        publish_to_dlq(&ctx.js, &names.dlq_subject, payload.clone(), dlq_ctx)
            .await
            .expect("publish_to_dlq failed");

        let drained = ctx
            .drain_stream(&names.dlq_stream, 1, Duration::from_secs(5))
            .await;
        assert_eq!(drained.len(), 1);
        let msg = &drained[0];
        let headers = msg.headers.as_ref().expect("DLQ message must have headers");

        assert_eq!(
            headers.get(DLQ_SOURCE_SUBJECT).unwrap().as_str(),
            names.subject
        );
        assert_eq!(headers.get(DLQ_ERROR).unwrap().as_str(), "boom");
        assert_eq!(headers.get(DLQ_RETRIABLE).unwrap().as_str(), "true");
        assert_eq!(headers.get(DLQ_DELIVERED).unwrap().as_str(), "4");
        assert_eq!(headers.get(DLQ_STREAM_SEQUENCE).unwrap().as_str(), "42");
        // Timestamp parses as RFC3339.
        let ts = headers.get(DLQ_DEAD_LETTERED_AT).unwrap().as_str();
        chrono::DateTime::parse_from_rfc3339(ts).expect("dead-lettered-at must be RFC3339");
        // Deterministic dedup id derived from the original message identity.
        assert_eq!(
            headers.get("Nats-Msg-Id").unwrap().as_str(),
            format!("{}-{}", names.stream, 42)
        );
        // Payload is preserved untouched.
        assert_eq!(msg.payload, payload);
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn redelivery_is_deduplicated(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.js
            .create_stream(StreamConfig {
                name: names.dlq_stream.clone(),
                subjects: vec![names.dlq_subject.clone()],
                // Window must exceed the gap between the two publishes below.
                duplicate_window: Duration::from_secs(30),
                ..Default::default()
            })
            .await
            .unwrap();

        let message = message_ctx(&names.subject, &names.stream, 1, 7);
        let payload = Bytes::from_static(b"dup-payload");

        // Publish the "same" dead-lettered message twice (as a redelivery would).
        for _ in 0..2 {
            let dlq_ctx = DlqContext {
                message: &message,
                error: "boom".to_string(),
                retriable: false,
            };
            publish_to_dlq(&ctx.js, &names.dlq_subject, payload.clone(), dlq_ctx)
                .await
                .expect("publish_to_dlq failed");
        }

        // Only one entry should be persisted thanks to server-side dedup.
        let drained = ctx
            .drain_stream(&names.dlq_stream, 2, Duration::from_secs(2))
            .await;
        assert_eq!(
            drained.len(),
            1,
            "duplicate DLQ publish was not deduplicated"
        );
    }
}
