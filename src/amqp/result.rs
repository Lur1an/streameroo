use std::future::Future;
use crate::amqp::context::DeliveryContext;
use crate::amqp::Error;
use crate::event::Encode;

pub struct Publish<E> {
    payload: E,
    exchange: String,
    routing_key: String,
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
        Ok(())
    }
}

pub struct ReplyTo<E>(E)
where
    E: Encode;