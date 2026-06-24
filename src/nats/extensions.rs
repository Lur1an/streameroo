//! Extension traits adding ergonomic, codec-aware, telemetry-propagating
//! helpers to the `async-nats` client and JetStream context.

use crate::event::Encode;
use crate::nats::error::{Error, NatsResult};
use async_nats::HeaderMap;
use async_nats::jetstream::publish::PublishAck;
use std::future::Future;

/// Extension methods on the core NATS [`async_nats::Client`].
pub trait ClientExt {
    fn xpublish<T: Encode>(
        &self,
        subject: &str,
        message: T,
    ) -> impl Future<Output = NatsResult<()>> {
        self.xpublish_with_headers(subject, HeaderMap::new(), message)
    }

    /// Publishes an [`Encode`]-able message with the given headers.
    ///
    /// When the `telemetry` feature is enabled the current OpenTelemetry context
    /// is injected into the headers so it propagates to consumers.
    fn xpublish_with_headers<T: Encode>(
        &self,
        subject: &str,
        headers: HeaderMap,
        message: T,
    ) -> impl Future<Output = NatsResult<()>>;
}

impl ClientExt for async_nats::Client {
    async fn xpublish_with_headers<T: Encode>(
        &self,
        subject: &str,
        #[cfg_attr(not(feature = "telemetry"), allow(unused_mut))] mut headers: HeaderMap,
        message: T,
    ) -> NatsResult<()> {
        let subject = subject.to_owned();
        let payload = message.encode().map_err(Error::event)?;

        #[cfg(feature = "telemetry")]
        {
            use crate::nats::telemetry;
            use opentelemetry::Context;
            use opentelemetry::trace::SpanKind;
            use tracing_opentelemetry::OpenTelemetrySpanExt;
            use tracing_opentelemetry_instrumentation_sdk::find_context_from_tracing;

            let span = telemetry::make_span_for_subject(&subject, SpanKind::Producer);
            if let Err(e) = span.set_parent(Context::current()) {
                tracing::warn!("Failed to set parent context for span: {e}");
            }
            telemetry::inject_context(&find_context_from_tracing(&span), &mut headers);
        }

        self.publish_with_headers(subject, headers, payload.into())
            .await?;

        Ok(())
    }
}

/// Extension methods on the JetStream [`async_nats::jetstream::Context`].
pub trait JetStreamExt {
    fn xpublish<T: Encode>(
        &self,
        subject: &str,
        message: T,
    ) -> impl Future<Output = NatsResult<PublishAck>> {
        self.xpublish_with_headers(subject, HeaderMap::new(), message)
    }

    /// Publishes an [`Encode`]-able message to a stream with the given headers,
    /// awaiting the JetStream ack that confirms durable persistence.
    ///
    /// When the `telemetry` feature is enabled the current OpenTelemetry context
    /// is injected into the headers so it propagates to consumers.
    fn xpublish_with_headers<T: Encode>(
        &self,
        subject: &str,
        headers: HeaderMap,
        message: T,
    ) -> impl Future<Output = NatsResult<PublishAck>>;
}

impl JetStreamExt for async_nats::jetstream::Context {
    async fn xpublish_with_headers<T: Encode>(
        &self,
        subject: &str,
        #[cfg_attr(not(feature = "telemetry"), allow(unused_mut))] mut headers: HeaderMap,
        message: T,
    ) -> NatsResult<PublishAck> {
        let subject = subject.to_owned();
        let payload = message.encode().map_err(Error::event)?;

        #[cfg(feature = "telemetry")]
        {
            use crate::nats::telemetry;
            use opentelemetry::Context;
            use opentelemetry::trace::SpanKind;
            use tracing_opentelemetry::OpenTelemetrySpanExt;
            use tracing_opentelemetry_instrumentation_sdk::find_context_from_tracing;

            let span = telemetry::make_span_for_subject(&subject, SpanKind::Producer);
            if let Err(e) = span.set_parent(Context::current()) {
                tracing::warn!("Failed to set parent context for span: {e}");
            }
            telemetry::inject_context(&find_context_from_tracing(&span), &mut headers);
        }

        // Double await: the first resolves once the publish is sent, the second
        // resolves once the server confirms the message was persisted.
        let ack = self
            .publish_with_headers(subject, headers, payload.into())
            .await?
            .await?;

        Ok(ack)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::event::{Decode, Json};
    use crate::nats::test_util::{NatsTest, TestEvent};
    use assert_matches::assert_matches;
    use async_nats::jetstream::stream::Config as StreamConfig;
    use futures::StreamExt;
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
    async fn client_xpublish_roundtrips(ctx: &mut NatsTest) {
        let names = ctx.names();
        let mut sub = ctx.client.subscribe(names.subject.clone()).await.unwrap();
        ctx.client.flush().await.unwrap();

        ctx.client
            .xpublish(&names.subject, Json(TestEvent::new("ping")))
            .await
            .expect("xpublish failed");

        let msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .expect("timed out waiting for message")
            .expect("subscription closed");
        let decoded = Json::<TestEvent>::decode(msg.payload.to_vec()).unwrap();
        assert_eq!(decoded.into_inner(), TestEvent::new("ping"));
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn client_xpublish_with_headers_preserves_headers(ctx: &mut NatsTest) {
        let names = ctx.names();
        let mut sub = ctx.client.subscribe(names.subject.clone()).await.unwrap();
        ctx.client.flush().await.unwrap();

        let mut headers = HeaderMap::new();
        headers.insert("X-Test", "value-1");
        ctx.client
            .xpublish_with_headers(&names.subject, headers, Json(TestEvent::new("h")))
            .await
            .expect("xpublish_with_headers failed");

        let msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .expect("timed out")
            .expect("subscription closed");
        let headers = msg.headers.expect("message must carry headers");
        assert_eq!(headers.get("X-Test").unwrap().as_str(), "value-1");
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn client_xpublish_surfaces_encode_errors(ctx: &mut NatsTest) {
        let names = ctx.names();
        let result = ctx.client.xpublish(&names.subject, FailEncode).await;
        assert_matches!(result, Err(Error::Event(_)));
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn jetstream_xpublish_persists_and_acks(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;

        let ack = ctx
            .js
            .xpublish(&names.subject, Json(TestEvent::new("stored")))
            .await
            .expect("jetstream xpublish failed");
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
    async fn jetstream_xpublish_with_headers_preserves_headers(ctx: &mut NatsTest) {
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
        ctx.js
            .xpublish_with_headers(&names.subject, headers, Json(TestEvent::new("hh")))
            .await
            .expect("jetstream xpublish_with_headers failed");

        let drained = ctx
            .drain_stream(&names.stream, 1, Duration::from_secs(5))
            .await;
        assert_eq!(drained.len(), 1);
        let headers = drained[0].headers.as_ref().expect("headers missing");
        assert_eq!(headers.get("X-Js").unwrap().as_str(), "js-value");
    }

    #[test_context(NatsTest)]
    #[tokio::test]
    async fn jetstream_xpublish_surfaces_encode_errors(ctx: &mut NatsTest) {
        let names = ctx.names();
        ctx.ensure_stream(&names).await;
        let result = ctx.js.xpublish(&names.subject, FailEncode).await;
        assert_matches!(result, Err(Error::Event(_)));
    }
}
