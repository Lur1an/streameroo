use std::future::Future;

use lapin::options::BasicPublishOptions;
use lapin::BasicProperties;

use crate::event::Encode;

use super::Error;

pub trait ChannelExt {
    fn publish<E>(
        &self,
        exchange: impl AsRef<str>,
        routing_key: impl AsRef<str>,
        event: E,
    ) -> impl Future<Output = Result<(), Error>>
    where
        E: Encode;

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
