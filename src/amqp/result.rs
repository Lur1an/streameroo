use amqprs::channel::{BasicAckArguments, BasicNackArguments, BasicPublishArguments};
use amqprs::BasicProperties;
use std::future::Future;

use crate::amqp::context::DeliveryContext;
use crate::event::Encode;

use super::{ChannelExt, Error};

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
    /// Indicates if this result's `handle_result` implementation takes care of acking/nacking the
    /// delivery.
    fn manual() -> bool {
        false
    }

    fn handle_result(
        self,
        context: &DeliveryContext,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

/// Return this type from your handler to have fine-grained control over what happens to the
/// delivery after the handler has finished.
pub enum DeliveryAction {
    Ack { multiple: bool },
    Nack { requeue: bool, multiple: bool },
}

impl AMQPResult for DeliveryAction {
    fn manual() -> bool {
        true
    }
    async fn handle_result(self, context: &DeliveryContext) -> Result<(), Error> {
        match self {
            DeliveryAction::Ack { multiple } => {
                let args = BasicAckArguments {
                    delivery_tag: context.delivery_tag,
                    multiple,
                };
                context.channel.basic_ack(args).await?
            }
            DeliveryAction::Nack { requeue, multiple } => {
                let args = BasicNackArguments {
                    delivery_tag: context.delivery_tag,
                    multiple,
                    requeue,
                };
                context.channel.basic_nack(args).await?
            }
        };
        Ok(())
    }
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
        }
        Ok(())
    }
}
