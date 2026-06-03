mod error;
#[cfg(feature = "telemetry")]
mod telemetry;

pub use error::{Error, NatsResult};

use crate::event::Encode;
use async_nats::HeaderMap;
use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Notify;

pub struct Streameroo {
    shutdown: Arc<Notify>,
    client: async_nats::Client,
}

pub struct StreamConsumerConfig {
    pub dlq_subject: Option<String>,
    pub pull_consumer_config: PullConsumerConfig,
}

pub enum HandlerResult {}

pub trait Handler {}

pub trait ClientExt {
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
        #[cfg_attr(not(feature = "telemetry"), allow(unused_mut))]
        mut headers: HeaderMap,
        message: T,
    ) -> NatsResult<()> {
        let subject = subject.to_owned();
        let payload = message.encode().map_err(Error::event)?;

        #[cfg(feature = "telemetry")]
        {
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

pub trait JetStreamExt {}
