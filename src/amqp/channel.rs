use std::future::Future;
use std::time::Duration;

use amqprs::channel::{BasicConsumeArguments, BasicPublishArguments};
use amqprs::BasicProperties;
use uuid::Uuid;

use crate::event::{Decode, Encode};

use super::Error;

const DIRECT_REPLY_TO_QUEUE: &str = "amq.rabbitmq.reply-to";

pub trait ChannelExt {
    fn publish<E>(
        &self,
        exchange: impl AsRef<str>,
        routing_key: impl AsRef<str>,
        event: E,
    ) -> impl Future<Output = Result<(), Error>>
    where
        E: Encode;

    /// Follows the direct reply-to RPC pattern specified in the RabbitMQ docs
    /// https://www.rabbitmq.com/docs/direct-reply-to
    fn direct_rpc<E, T>(
        &mut self,
        exchange: impl AsRef<str>,
        routing_key: impl AsRef<str>,
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
    async fn publish<E>(
        &self,
        exchange: impl AsRef<str>,
        routing_key: impl AsRef<str>,
        event: E,
    ) -> Result<(), Error>
    where
        E: Encode,
    {
        self.publish_with_options(
            BasicPublishArguments::new(exchange.as_ref(), routing_key.as_ref()),
            Default::default(),
            event,
        )
        .await
    }

    async fn direct_rpc<E, T>(
        &mut self,
        exchange: impl AsRef<str>,
        routing_key: impl AsRef<str>,
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
            BasicPublishArguments::new(exchange.as_ref(), routing_key.as_ref()),
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
        if properties.content_type().is_none() {
            if let Some(content_type) = E::content_type() {
                properties.with_content_type(content_type);
            }
        }
        self.basic_publish(properties, payload, args).await?;
        Ok(())
    }
}
