use super::{ChannelExt, Error};
use crate::amqp::context::DeliveryContext;
use crate::event::Encode;
use amqprs::BasicProperties;
use amqprs::channel::BasicPublishArguments;
use std::future::Future;
use tracing::warn;

pub struct Publish<E> {
    pub payload: E,
    pub exchange: String,
    pub routing_key: String,
    pub options: BasicPublishArguments,
    pub properties: BasicProperties,
}

impl<E> Publish<E> {
    pub fn new(payload: E, exchange: impl Into<String>, routing_key: impl Into<String>) -> Self {
        let exchange = exchange.into();
        let routing_key = routing_key.into();
        Self {
            payload,
            exchange,
            routing_key,
            options: Default::default(),
            properties: Default::default(),
        }
    }
}

pub trait AMQPResult: Send {
    fn handle_result(
        self,
        context: &DeliveryContext,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

impl AMQPResult for () {
    async fn handle_result(self, _: &DeliveryContext) -> Result<(), Error> {
        Ok(())
    }
}

impl<E> AMQPResult for Publish<E>
where
    E: Encode + Send,
{
    async fn handle_result(self, context: &DeliveryContext) -> Result<(), Error> {
        let args = BasicPublishArguments::new(&self.exchange, &self.routing_key);
        context
            .channel
            .publish_with_options(args, self.properties, self.payload)
            .await?;
        Ok(())
    }
}

pub struct PublishReply<E>(pub E);

impl<E> PublishReply<E> {
    pub fn new(payload: E) -> Self {
        Self(payload)
    }
}

impl<E> AMQPResult for PublishReply<E>
where
    E: Encode + Send,
{
    async fn handle_result(self, context: &DeliveryContext) -> Result<(), Error> {
        if let Some(reply_to) = context.properties.reply_to() {
            context
                .channel
                .publish("", reply_to.as_str(), self.0)
                .await?;
        } else {
            warn!("No reply_to queue found for PublishReply");
        }
        Ok(())
    }
}
