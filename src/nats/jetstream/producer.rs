//! Codec-aware, telemetry-propagating publish helpers for the JetStream
//! [`async_nats::jetstream::Context`].

use crate::event::Encode;
use crate::nats::error::{Error, NatsResult};
use async_nats::HeaderMap;
use async_nats::jetstream::Context;
use async_nats::jetstream::publish::PublishAck;
use std::future::Future;

/// Extension methods on the JetStream [`async_nats::jetstream::Context`].
pub trait Producer {
    /// Publishes an [`Encode`]-able message to a stream, awaiting the JetStream
    /// ack that confirms durable persistence.
    ///
    /// When the `telemetry` feature is enabled the current OpenTelemetry context
    /// is injected into the headers so it propagates to consumers.
    fn produce<T: Encode>(
        &self,
        subject: &str,
        message: T,
    ) -> impl Future<Output = NatsResult<PublishAck>> {
        self.produce_with_headers(subject, HeaderMap::new(), message)
    }

    /// Publishes an [`Encode`]-able message to a stream with the given headers,
    /// awaiting the JetStream ack that confirms durable persistence.
    ///
    /// When the `telemetry` feature is enabled the current OpenTelemetry context
    /// is injected into the headers so it propagates to consumers.
    fn produce_with_headers<T: Encode>(
        &self,
        subject: &str,
        headers: HeaderMap,
        message: T,
    ) -> impl Future<Output = NatsResult<PublishAck>>;
}

impl Producer for Context {
    async fn produce_with_headers<T: Encode>(
        &self,
        subject: &str,
        #[cfg_attr(not(feature = "telemetry"), allow(unused_mut))] mut headers: HeaderMap,
        message: T,
    ) -> NatsResult<PublishAck> {
        let subject = subject.to_owned();
        let payload = message.encode().map_err(Error::event)?;

        #[cfg(feature = "telemetry")]
        crate::nats::telemetry::inject_producer_context(&subject, &mut headers);

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

        let ack = ctx
            .js
            .produce(&names.subject, Json(TestEvent::new("stored")))
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
        ctx.js
            .produce_with_headers(&names.subject, headers, Json(TestEvent::new("hh")))
            .await
            .expect("jetstream produce_with_headers failed");

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
        let result = ctx.js.produce(&names.subject, FailEncode).await;
        assert_matches!(result, Err(Error::Event(_)));
    }
}
