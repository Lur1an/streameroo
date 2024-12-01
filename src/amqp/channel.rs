use std::future::Future;
use std::time::Duration;

use lapin::options::{BasicConsumeOptions, BasicPublishOptions};
use lapin::BasicProperties;
use tokio_stream::StreamExt;
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
        exchange: impl AsRef<str>,
        routing_key: impl AsRef<str>,
        options: BasicPublishOptions,
        properties: BasicProperties,
        event: E,
    ) -> impl Future<Output = Result<(), Error>>
    where
        E: Encode;
}

impl ChannelExt for lapin::Channel {
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
            exchange,
            routing_key,
            Default::default(),
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
        let consumer_tag = Uuid::new_v4().to_string();
        let mut consumer = self
            .basic_consume(
                DIRECT_REPLY_TO_QUEUE,
                &consumer_tag,
                BasicConsumeOptions {
                    no_ack: true,
                    ..Default::default()
                },
                Default::default(),
            )
            .await?;
        self.publish_with_options(
            exchange,
            routing_key,
            Default::default(),
            BasicProperties::default().with_reply_to(DIRECT_REPLY_TO_QUEUE.into()),
            event,
        )
        .await?;
        let fut = async move {
            if let Some(delivery) = consumer.next().await {
                let delivery = delivery?;
                let payload = delivery.data;
                T::decode(payload).map_err(|e| Error::Event(e.into()))
            } else {
                Err(Error::Custom("Stream terminated".into()))
            }
        };
        tokio::time::timeout(timeout, fut)
            .await
            .map_err(|e| Error::Custom(e.into()))?
    }

    async fn publish_with_options<E>(
        &self,
        exchange: impl AsRef<str>,
        routing_key: impl AsRef<str>,
        options: BasicPublishOptions,
        properties: BasicProperties,
        event: E,
    ) -> Result<(), Error>
    where
        E: Encode,
    {
        let payload = event.encode().map_err(|e| Error::Event(e.into()))?;
        self.basic_publish(
            exchange.as_ref(),
            routing_key.as_ref(),
            options,
            &payload,
            properties,
        )
        .await?;
        Ok(())
    }
}
