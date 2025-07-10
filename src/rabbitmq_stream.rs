use std::pin::Pin;

use rabbitmq_stream_client::error::ConsumerCreateError;
use rabbitmq_stream_client::types::OffsetSpecification;
use rabbitmq_stream_client::{Consumer, ConsumerBuilder, Environment};
use tokio_stream::Stream;

use crate::event::Decode;

pub trait StreamerooExt {}

pub struct StreamerooStream<T> {
    consumer: Consumer,
    confirm_offset: Option<()>,
    phantom: std::marker::PhantomData<T>,
}

impl<T> StreamerooStream<T> {
    pub async fn with_offset(
        base: impl Fn() -> ConsumerBuilder,
        stream: &str,
        environment: Environment,
    ) -> Result<Self, ConsumerCreateError> {
        let offset = base()
            .offset(OffsetSpecification::First)
            .build(stream)
            .await?
            .query_offset()
            .await
            .unwrap_or(0);
        let consumer = base()
            .offset(OffsetSpecification::Offset(offset))
            .build(stream)
            .await?;
        Ok(Self {
            consumer,
            confirm_offset: None,
            phantom: std::marker::PhantomData,
        })
    }

    pub fn from_consumer(consumer: Consumer) -> Self {
        Self {
            consumer,
            confirm_offset: None,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<T> Stream for StreamerooStream<T>
where
    T: Decode,
{
    type Item = T;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use bson::Uuid;
    use rabbitmq_stream_client::types::OffsetSpecification::Next;
    use rabbitmq_stream_client::types::RoutingStrategy;
    use rabbitmq_stream_client::Environment;
    use serde::{Deserialize, Serialize};
    use test_context::futures::StreamExt;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestEvent {
        hello: String,
        deez: Option<String>,
    }

    #[tokio::test]
    async fn test_deez() -> anyhow::Result<()> {
        let environment = Environment::builder().build().await?;
        let s_producer = environment
            .super_stream_producer(RoutingStrategy::RoutingKeyStrategy(()))
            .build("test")
            .await?;
        let s_consumer = environment
            .super_stream_consumer()
            .offset(Next)
            .build("test")
            .await?;
        Ok(())
    }
}
