//! Codec-aware, telemetry-propagating publish helpers for the JetStream
//! [`async_nats::jetstream::Context`].

use crate::event::Encode;
use crate::nats::error::{Error, NatsResult};
use async_nats::HeaderMap;
use async_nats::jetstream::Context;
use async_nats::jetstream::publish::PublishAck;

/// Publishes an [`Encode`]-able message to a stream, awaiting the JetStream ack
/// that confirms durable persistence.
/// When the `telemetry` feature is enabled the current OpenTelemetry context
/// is injected into the headers so it propagates to consumers.
pub async fn publish<T: Encode>(js: &Context, subject: &str, message: T) -> NatsResult<PublishAck> {
    publish_with_headers(js, subject, HeaderMap::new(), message).await
}

/// Publishes an [`Encode`]-able message to a stream with the given headers,
/// awaiting the JetStream ack that confirms durable persistence.
///
/// When the `telemetry` feature is enabled the current OpenTelemetry context
/// is injected into the headers so it propagates to consumers.
pub async fn publish_with_headers<T: Encode>(
    js: &Context,
    subject: &str,
    #[cfg_attr(not(feature = "telemetry"), allow(unused_mut))] mut headers: HeaderMap,
    message: T,
) -> NatsResult<PublishAck> {
    let subject = subject.to_owned();
    let payload = message.encode().map_err(Error::event)?;

    #[cfg(feature = "telemetry")]
    {
        use crate::nats::telemetry;
        use opentelemetry::Context as OtelContext;
        use opentelemetry::trace::SpanKind;
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        use tracing_opentelemetry_instrumentation_sdk::find_context_from_tracing;

        let span = telemetry::make_span_for_subject(&subject, SpanKind::Producer);
        if let Err(e) = span.set_parent(OtelContext::current()) {
            tracing::warn!("Failed to set parent context for span: {e}");
        }
        telemetry::inject_context(&find_context_from_tracing(&span), &mut headers);
    }

    // Double await: the first resolves once the publish is sent, the second
    // resolves once the server confirms the message was persisted.
    let ack = js
        .publish_with_headers(subject, headers, payload.into())
        .await?
        .await?;

    Ok(ack)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::event::{Decode, Json};
    use crate::nats::test_util::{NatsTest, TestEvent};
    use assert_matches::assert_matches;
    use async_nats::jetstream::stream::Config as StreamConfig;
    use std::time::Duration;
    use test_context::test_context;

    /// An `Encode` implementation that always fails, to drive the error path.
    struct FailEncode;

    #[derive(Debug, thiserror::Error)]
    #[error("intentional encode failure")]
    struct EncodeFailure;

    impl Encode for FailEncode {
        type Error = EncodeFailure;

        fn encode(&self) -> Result<Vec<u8>, Self::Error> {
            Err(EncodeFailure)
        }
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn jetstream_publish_persists_and_acks(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;

        let ack = publish(&ctx.js, &names.subject, Json(TestEvent::new("stored")))
            .await
            .expect("jetstream publish failed");
        assert_eq!(ack.stream, names.stream);
        assert_eq!(ack.sequence, 1);

        let drained = ctx
            .drain_stream(&names.stream, 1, Duration::from_secs(5))
            .await;
        assert_eq!(drained.len(), 1);
        let decoded = Json::<TestEvent>::decode(drained[0].payload.to_vec()).unwrap();
        assert_eq!(decoded.into_inner(), TestEvent::new("stored"));
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn jetstream_publish_with_headers_preserves_headers(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.js
            .create_stream(StreamConfig {
                name: names.stream.clone(),
                subjects: vec![names.subject.clone()],
                ..Default::default()
            })
            .await
            .unwrap();

        let mut headers = HeaderMap::new();
        headers.insert("X-Js", "js-value");
        publish_with_headers(&ctx.js, &names.subject, headers, Json(TestEvent::new("hh")))
            .await
            .expect("jetstream publish_with_headers failed");

        let drained = ctx
            .drain_stream(&names.stream, 1, Duration::from_secs(5))
            .await;
        assert_eq!(drained.len(), 1);
        let headers = drained[0].headers.as_ref().expect("headers missing");
        assert_eq!(headers.get("X-Js").unwrap().as_str(), "js-value");
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn jetstream_publish_surfaces_encode_errors(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;
        let result = publish(&ctx.js, &names.subject, FailEncode).await;
        assert_matches!(result, Err(Error::Event(_)));
    }
}
