use std::future::Future;

use lapin::options::BasicPublishOptions;
use lapin::BasicProperties;

use crate::event::Encode;

#[derive(Debug, thiserror::Error)]
pub enum ChannelPublishError<E>
where
    E: Encode,
{
    #[error(transparent)]
    Lapin(#[from] lapin::Error),
    #[error("Error serializing event: {0}")]
    EventEncode(E::Error),
}

pub trait ChannelExt {
    fn publish<E>(
        &self,
        exchange: impl AsRef<str>,
        routing_key: impl AsRef<str>,
        options: BasicPublishOptions,
        properties: BasicProperties,
        event: E,
    ) -> impl Future<Output = Result<(), ChannelPublishError<E>>>
    where
        E: Encode;
}

impl ChannelExt for lapin::Channel {
    async fn publish<E>(
        &self,
        exchange: impl AsRef<str>,
        routing_key: impl AsRef<str>,
        options: BasicPublishOptions,
        properties: BasicProperties,
        event: E,
    ) -> Result<(), ChannelPublishError<E>>
    where
        E: Encode,
    {
        let payload = event
            .encode()
            .map_err(|e| ChannelPublishError::EventEncode(e))?;
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
