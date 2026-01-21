use std::future::Future;
use std::time::Duration;

use amqprs::BasicProperties;
use amqprs::channel::{BasicConsumeArguments, BasicPublishArguments};
use uuid::Uuid;

use crate::event::{Decode, Encode};

use super::{AMQPConnection, Error};

const DIRECT_REPLY_TO_QUEUE: &str = "amq.rabbitmq.reply-to";

pub trait ChannelExt {
    fn publish<E>(
        &self,
        exchange: &str,
        routing_key: &str,
        event: E,
    ) -> impl Future<Output = Result<(), Error>>
    where
        E: Encode,
    {
        self.publish_with_options(
            BasicPublishArguments::new(exchange, routing_key),
            Default::default(),
            event,
        )
    }

    /// Follows the direct reply-to RPC pattern specified in the RabbitMQ docs
    /// https://www.rabbitmq.com/docs/direct-reply-to
    fn direct_rpc<E, T>(
        &mut self,
        exchange: &str,
        routing_key: &str,
        timeout: Duration,
        event: E,
    ) -> impl Future<Output = Result<T, Error>>
    where
        E: Encode,
        T: Decode;

    fn publish_with_options<E>(
        &self,
        args: BasicPublishArguments,
        properties: BasicProperties,
        event: E,
    ) -> impl Future<Output = Result<(), Error>>
    where
        E: Encode;
}

impl ChannelExt for amqprs::channel::Channel {
    #[tracing::instrument(name = "streameroo::amqp::direct_rpc", skip(self, timeout, event))]
    async fn direct_rpc<E, T>(
        &mut self,
        exchange: &str,
        routing_key: &str,
        timeout: Duration,
        event: E,
    ) -> Result<T, Error>
    where
        E: Encode,
        T: Decode,
    {
        let consumer_tag = format!("streameroo-rpc-{}", Uuid::new_v4());
        let mut args = BasicConsumeArguments::new(DIRECT_REPLY_TO_QUEUE, &consumer_tag);
        args.manual_ack(false);

        let (_, mut consumer) = self.basic_consume_rx(args).await?;

        let mut properties = BasicProperties::default();
        properties.with_reply_to(DIRECT_REPLY_TO_QUEUE);

        self.publish_with_options(
            BasicPublishArguments::new(exchange, routing_key),
            properties,
            event,
        )
        .await?;

        let fut = async move {
            if let Some(msg) = consumer.recv().await {
                T::decode(
                    msg.content
                        .expect("ConsumerMessage must have content according to amqprs spec"),
                )
                .map_err(|e| Error::Event(e.into()))
            } else {
                todo!()
            }
        };
        tokio::time::timeout(timeout, fut).await?
    }

    #[tracing::instrument(
        name = "streameroo::amqp::publish_with_options",
        skip(self, event),
        fields(
            exchange = args.exchange,
            routing_key = args.routing_key
        )
    )]
    async fn publish_with_options<E>(
        &self,
        args: BasicPublishArguments,
        mut properties: BasicProperties,
        event: E,
    ) -> Result<(), Error>
    where
        E: Encode,
    {
        let payload = event.encode().map_err(|e| Error::Event(e.into()))?;
        if properties.content_type().is_none()
            && let Some(content_type) = E::content_type()
        {
            properties.with_content_type(content_type);
        }

        #[cfg(feature = "telemetry")]
        {
            use super::telemetry::{inject_context, make_span_from_properties};
            use opentelemetry::Context;
            use opentelemetry::trace::SpanKind;
            use tracing_opentelemetry::OpenTelemetrySpanExt;
            use tracing_opentelemetry_instrumentation_sdk::find_context_from_tracing;

            let mut headers = properties.headers().cloned().unwrap_or_default();
            let span = make_span_from_properties(
                &properties,
                SpanKind::Producer,
                &args.exchange,
                &args.routing_key,
            );
            if let Err(e) = span.set_parent(Context::current()) {
                tracing::warn!("Failed to set parent context for span: {e}");
            };
            inject_context(&find_context_from_tracing(&span), &mut headers);
            properties.with_headers(headers);
        }

        self.basic_publish(properties, payload, args).await?;
        Ok(())
    }
}

impl ChannelExt for AMQPConnection {
    async fn direct_rpc<E, T>(
        &mut self,
        exchange: &str,
        routing_key: &str,
        timeout: Duration,
        event: E,
    ) -> Result<T, Error>
    where
        E: Encode,
        T: Decode,
    {
        let mut channel = self.open_channel().await?;
        channel
            .direct_rpc(exchange, routing_key, timeout, event)
            .await
    }

    #[tracing::instrument(
        name = "streameroo::amqp::publish_with_options",
        skip(self, event),
        fields(
            exchange = args.exchange,
            routing_key = args.routing_key
        )
    )]
    async fn publish_with_options<E>(
        &self,
        args: BasicPublishArguments,
        mut properties: BasicProperties,
        event: E,
    ) -> Result<(), Error>
    where
        E: Encode,
    {
        let payload = event.encode().map_err(|e| Error::Event(e.into()))?;
        if properties.content_type().is_none()
            && let Some(content_type) = E::content_type()
        {
            properties.with_content_type(content_type);
        }

        #[cfg(feature = "telemetry")]
        {
            use super::telemetry::{inject_context, make_span_from_properties};
            use opentelemetry::trace::SpanKind;
            use tracing_opentelemetry_instrumentation_sdk::find_context_from_tracing;

            let mut headers = properties.headers().cloned().unwrap_or_default();
            let span = make_span_from_properties(
                &properties,
                SpanKind::Producer,
                &args.exchange,
                &args.routing_key,
            );
            inject_context(&find_context_from_tracing(&span), &mut headers);
            properties.with_headers(headers);
        }

        self.basic_publish(properties, payload, args).await?;
        Ok(())
    }
}
