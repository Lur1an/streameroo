//! Extension traits adding ergonomic, codec-aware, telemetry-propagating
//! helpers to the `async-nats` client and JetStream context.

use crate::event::Encode;
use crate::nats::error::{Error, NatsResult};
use async_nats::HeaderMap;
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
pub trait JetStreamExt {}
