use super::{ChannelExt, Error};
use crate::event::Decode;
use lapin::options::{BasicConsumeOptions, BasicQosOptions, QueueDeclareOptions};
use lapin::publisher_confirm::PublisherConfirm;
use lapin::types::FieldTable;
use lapin::Error::InvalidConnectionState;
use lapin::{Channel, Connection, ConnectionProperties, ConnectionState, Consumer};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// A wrapper around a `lapin::Connection`
/// Allows creating channels from the underlying connection. Automatically handles reconnection
#[derive(Clone)]
pub struct AMQPConnection(Arc<Inner>);

struct Inner {
    connection: Mutex<Connection>,
    /// Main channel for publishing. This is used by the `ChannelExt` impl on `AMQPConnection`
    /// Avoids `create_channel` spam calls for normal publishing, allows implementing `ChannelExt`
    /// on `AMQPConnection`.
    channel: RwLock<Channel>,
    url: String,
}

impl AMQPConnection {
    pub async fn connect(url: impl Into<String>) -> Result<Self, Error> {
        let url = url.into();
        let connection = Connection::connect(&url, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;
        Ok(Self(Arc::new(Inner {
            url,
            channel: RwLock::new(channel),
            connection: Mutex::new(connection),
        })))
    }

    pub async fn queue_declare(
        &self,
        queue: impl AsRef<str>,
        options: QueueDeclareOptions,
        arguments: FieldTable,
    ) -> lapin::Result<lapin::Queue> {
        self.0
            .channel
            .read()
            .await
            .queue_declare(queue.as_ref(), options, arguments)
            .await
    }

    /// Create a new channel and a consumer on top of it. Return both the consumer and the channel
    pub async fn create_consumer(
        &self,
        queue: &str,
        consumer_tag: &str,
        options: BasicConsumeOptions,
        arguments: FieldTable,
        prefetch_count: u16,
    ) -> lapin::Result<(Consumer, Channel)> {
        let channel = self.create_channel().await?;
        channel
            .basic_qos(prefetch_count, BasicQosOptions::default())
            .await?;
        Ok((
            channel
                .basic_consume(queue, consumer_tag, options, arguments)
                .await?,
            channel,
        ))
    }

    pub async fn create_channel(&self) -> lapin::Result<Channel> {
        let mut connection = self.0.connection.lock().await;
        match connection.create_channel().await {
            Ok(channel) => Ok(channel),
            Err(e) => match &e {
                InvalidConnectionState(connection_state) => {
                    if let ConnectionState::Closing
                    | ConnectionState::Closed
                    | ConnectionState::Error = connection_state
                    {
                        let new_connection =
                            Connection::connect(&self.0.url, ConnectionProperties::default())
                                .await?;
                        let new_channel = new_connection.create_channel().await?;
                        *connection = new_connection;
                        {
                            let mut guard = self.0.channel.write().await;
                            *guard = new_channel;
                        };
                        Ok(connection.create_channel().await?)
                    } else {
                        Err(e)
                    }
                }
                _ => Err(e),
            },
        }
    }
}

impl ChannelExt for AMQPConnection {
    async fn publish<E>(
        &self,
        exchange: impl AsRef<str>,
        routing_key: impl AsRef<str>,
        event: E,
    ) -> Result<PublisherConfirm, Error>
    where
        E: crate::event::Encode,
    {
        let channel = self.0.channel.read().await;
        channel.publish(exchange, routing_key, event).await
    }

    async fn direct_rpc<E, T>(
        &mut self,
        exchange: impl AsRef<str>,
        routing_key: impl AsRef<str>,
        timeout: std::time::Duration,
        event: E,
    ) -> Result<T, Error>
    where
        E: crate::event::Encode,
        T: Decode,
    {
        let mut channel = self.create_channel().await?;
        channel
            .direct_rpc(exchange, routing_key, timeout, event)
            .await
    }

    async fn publish_with_options<E>(
        &self,
        exchange: impl AsRef<str>,
        routing_key: impl AsRef<str>,
        options: lapin::options::BasicPublishOptions,
        properties: lapin::BasicProperties,
        event: E,
    ) -> Result<PublisherConfirm, Error>
    where
        E: crate::event::Encode,
    {
        let channel = self.0.channel.read().await;
        channel
            .publish_with_options(exchange, routing_key, options, properties, event)
            .await
    }
}
