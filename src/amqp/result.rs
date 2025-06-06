use lapin::options::{BasicAckOptions, BasicNackOptions, BasicPublishOptions};
use lapin::BasicProperties;
use std::future::Future;

use crate::amqp::context::DeliveryContext;
use crate::event::Encode;

use super::{ChannelExt, Error};

pub struct Publish<E> {
    pub payload: E,
    pub exchange: String,
    pub routing_key: String,
    pub options: BasicPublishOptions,
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
    fn manual() -> bool {
        false
    }

    fn handle_result(
        self,
        context: &DeliveryContext,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

/// Return this type from your handler to have fine-grained control over what happens to the
/// delivery
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
                context.acker.ack(BasicAckOptions { multiple }).await?
            }
            DeliveryAction::Nack { requeue, multiple } => {
                context
                    .acker
                    .nack(BasicNackOptions { requeue, multiple })
                    .await?
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
        context
            .channel
            .publish_with_options(
                self.exchange,
                self.routing_key,
                self.options,
                self.properties,
                self.payload,
            )
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
