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
