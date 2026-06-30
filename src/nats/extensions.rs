//! Extension trait adding ergonomic, codec-aware, telemetry-propagating
//! helpers to the core `async-nats` client.

use crate::event::Encode;
use crate::nats::error::{Error, NatsResult};
use async_nats::HeaderMap;
use std::future::Future;

/// Shared publish preparation used by both the core-client ([`ClientExt`]) and
/// JetStream (`Producer`) publish helpers.
///
/// Takes ownership of the subject, encodes the payload, and — when the
/// `telemetry` feature is enabled — injects the current OpenTelemetry context
/// into the headers so it propagates to consumers. Returns the owned subject,
/// headers and payload ready to hand to a `publish_with_headers` call.
pub(crate) fn prepare_publish<T: Encode>(
    subject: &str,
    #[cfg_attr(not(feature = "telemetry"), allow(unused_mut))] mut headers: HeaderMap,
    message: T,
) -> NatsResult<(String, HeaderMap, Vec<u8>)> {
    let subject = subject.to_owned();
    let payload = message.encode().map_err(Error::event)?;

    #[cfg(feature = "telemetry")]
    crate::nats::telemetry::inject_producer_context(&subject, &mut headers);

    Ok((subject, headers, payload))
}

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
        headers: HeaderMap,
        message: T,
    ) -> NatsResult<()> {
        let (subject, headers, payload) = prepare_publish(subject, headers, message)?;

        self.publish_with_headers(subject, headers, payload.into())
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::event::{Decode, Json};
    use crate::nats::test_util::{NatsTest, TestEvent};
    use assert_matches::assert_matches;
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
}
