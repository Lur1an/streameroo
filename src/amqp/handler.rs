use crate::amqp::{AMQPResult, DeliveryContext, Error};
use crate::event::Decode;
use std::fmt::Display;
use std::future::Future;

pub trait Handler: Clone + Send + 'static {
    type Event: AMQPDecode + Send;
    type Result: AMQPResult;
    type Error: Display + Send;

    fn handle(
        &self,
        ctx: &DeliveryContext,
        event: Self::Event,
    ) -> impl Future<Output = Result<Self::Result, Self::Error>> + Send;
}

pub trait AMQPDecode: Sized {
    fn decode(payload: Vec<u8>, context: &DeliveryContext) -> Result<Self, Error>;
}

impl<E> AMQPDecode for E
where
    E: Decode,
{
    fn decode(payload: Vec<u8>, _: &DeliveryContext) -> Result<Self, Error> {
        E::decode(payload).map_err(Error::event)
    }
}
