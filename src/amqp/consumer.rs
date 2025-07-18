use super::connection::AMQPConnection;
use super::{AMQPHandler, AMQPResult, Context};
use crate::amqp::{create_delivery_context, Error};
use amqprs::channel::{
    BasicAckArguments, BasicConsumeArguments, BasicNackArguments, BasicQosArguments,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tracing::{Instrument, Level};

pub struct Consumer<H, P, T, Err> {
    context: Arc<Context>,
    connection: AMQPConnection,
    consume_args: BasicConsumeArguments,
    qos_args: BasicQosArguments,
    handler: H,
    notifier: Arc<Notify>,
    _phantom: std::marker::PhantomData<(P, T, Err)>,
}

impl<H, P, T, Err> Consumer<H, P, T, Err>
where
    H: AMQPHandler<P, T, Err>,
    T: AMQPResult,
{
    pub fn new(
        context: Arc<Context>,
        connection: AMQPConnection,
        options: BasicConsumeArguments,
        qos_args: BasicQosArguments,
        handler: H,
        notifier: Arc<Notify>,
    ) -> Self {
        Self {
            context,
            connection,
            consume_args: options,
            handler,
            qos_args,
            notifier,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Consumes the consumer and starts the loop.
    /// The loop will run indefinitely, relying on `AMQPConnection` to restore the connection
    /// if it is closed, until the notifier is triggered.
    pub async fn consume(self) {
        let notified = self.notifier.notified();
        tokio::pin!(notified);

        let mut tasks = JoinSet::new();
        let skip_ack = T::manual() || self.consume_args.no_ack;
        let mut channel;
        'outer: loop {
            tracing::info!("Creating channel for consumer");
            channel = match self.connection.open_channel().await {
                Ok(channel) => channel,
                Err(e) => {
                    tracing::error!(?e, "Failed to create channel for consumer");
                    continue;
                }
            };
            if let Err(e) = channel.basic_qos(self.qos_args.clone()).await {
                tracing::error!(?e, "Failed to set qos for consumer");
                continue;
            }
            let (_, mut consumer_rx) =
                match channel.basic_consume_rx(self.consume_args.clone()).await {
                    Ok(consume) => consume,
                    Err(e) => {
                        tracing::error!(?e, "Failed to start consuming");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };
            loop {
                tokio::select! {
                    biased;
                    _ = &mut notified => {
                        break 'outer;
                    },
                    delivery = consumer_rx.recv() => {
                        if let Some(delivery) = delivery {
                            let (delivery_context, payload) = create_delivery_context(
                                delivery,
                                &self.context,
                                &channel,
                            );
                            let delivery_tag = delivery_context.delivery_tag;
                            let handler = self.handler.clone();
                            let fut = async move {
                                match handler.call(payload, &delivery_context).await {
                                    Ok(ret) => match ret.handle_result(&delivery_context).await {
                                        Ok(_) => {
                                            // On manual AMQPResult don't ack the result handling
                                            if skip_ack {
                                                return;
                                            }
                                            tracing::info!("Acking delivery");
                                            if let Err(e) = delivery_context.channel.basic_ack(BasicAckArguments {
                                                delivery_tag: delivery_context.delivery_tag,
                                                multiple: false
                                            }).await {
                                                tracing::error!(?e, "Error acking delivery");
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!(?e, "Error processing AMQPResult. Nacking delivery");
                                            let nack_args = BasicNackArguments {
                                                delivery_tag: delivery_context.delivery_tag, multiple: false, requeue: true
                                            };
                                            if let Err(e) = delivery_context.channel.basic_nack(nack_args).await {
                                                tracing::error!(?e, "Error nacking delivery");
                                            }
                                        }
                                    },
                                    // Decoding an event failed, this would fail again if we nacked the
                                    // delivery, so the framework automatically nacks without requeue!.
                                    Err(Error::Event(e)) => {
                                        tracing::error!(?e, "Error decoding event");
                                        let nack_args = BasicNackArguments {
                                            delivery_tag: delivery_context.delivery_tag, multiple: false, requeue: false
                                        };
                                        tracing::info!(?nack_args, "Nacking delivery");
                                        if let Err(e) = delivery_context.channel.basic_nack(nack_args).await {
                                            tracing::error!(?e, "Error acking delivery");
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!(?e, "Error calling AMQP handler");
                                        let nack_args = BasicNackArguments {
                                            delivery_tag: delivery_context.delivery_tag, multiple: false, requeue: true
                                        };
                                        tracing::info!(?nack_args, "Nacking delivery");
                                        if let Err(e) = delivery_context.channel.basic_nack(nack_args).await {
                                            tracing::error!(?e, "Error nacking delivery");
                                        }
                                    }
                                }
                            };
                            tasks.spawn(fut.instrument(tracing::span!(
                                Level::INFO,
                                "streameroo",
                                delivery_tag
                            )));
                        } else {
                            tracing::warn!("Consumer closed unexpectedly");
                            break
                        }
                    }
                }
                // Quickly drain the taskset of all completed tasks
                // To avoid filling it up indefinitely.
                while tasks.try_join_next().is_some() {}
            }
        }
        tasks.join_all().await;
        if let Err(e) = channel.close().await {
            tracing::error!(?e, "Failed to close channel in consumer");
        }
    }
}
