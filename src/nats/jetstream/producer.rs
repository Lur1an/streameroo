//! Codec-aware, telemetry-propagating publish helpers for the JetStream
//! [`async_nats::jetstream::Context`].

use crate::event::Encode;
use crate::nats::error::NatsResult;
use crate::nats::extensions::prepare_publish;
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
        headers: HeaderMap,
        message: T,
    ) -> NatsResult<PublishAck> {
        let (subject, headers, payload) = prepare_publish(subject, headers, message)?;

        // Double await: the first resolves once the publish is sent, the second
        // resolves once the server confirms the message was persisted.
        let ack = self
            .publish_with_headers(subject, headers, payload.into())
            .await?
            .await?;

        Ok(ack)
    }
}
