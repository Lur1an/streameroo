use super::Handler;
use super::connection::AMQPConnection;
use super::handler::AMQPDecode;
use crate::amqp::AMQPResult;
use crate::amqp::context::create_delivery_context;
use amqprs::channel::{
    BasicAckArguments, BasicConsumeArguments, BasicNackArguments, BasicQosArguments,
    ConsumerMessage,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tracing::Instrument;

pub struct Consumer<H: Handler> {
    connection: AMQPConnection,
    consume_args: BasicConsumeArguments,
    qos_args: BasicQosArguments,
    handler: H,
    notifier: Arc<Notify>,
}

impl<H: Handler> Consumer<H> {
    pub fn new(
        connection: AMQPConnection,
        options: BasicConsumeArguments,
        qos_args: BasicQosArguments,
        handler: H,
        notifier: Arc<Notify>,
    ) -> Self {
        Self {
            connection,
            consume_args: options,
            handler,
            qos_args,
            notifier,
        }
    }

    /// Handles a single delivery by spawning a task to process it
    fn handle_delivery(
        &self,
        delivery: ConsumerMessage,
        channel: &amqprs::channel::Channel,
        skip_ack: bool,
        tasks: &mut JoinSet<()>,
    ) {
        let (ctx, payload) = create_delivery_context(delivery, channel);
        let handler = self.handler.clone();

        #[cfg(feature = "telemetry")]
        let span = super::telemetry::make_span_from_delivery_context(&ctx);
        #[cfg(not(feature = "telemetry"))]
        let span = {
            let delivery_tag = ctx.delivery_tag;
            tracing::span!(tracing::Level::INFO, "streameroo::consumer", delivery_tag)
        };

        let fut = async move {
            // Decode error → nack WITHOUT requeue (would fail again on retry)
            let event = match H::Event::decode(payload, &ctx) {
                Ok(event) => event,
                Err(e) => {
                    tracing::error!(%e, "Failed to decode event, nacking without requeue");
                    let nack_args = BasicNackArguments {
                        delivery_tag: ctx.delivery_tag,
                        multiple: false,
                        requeue: false,
                    };
                    if let Err(e) = ctx.channel.basic_nack(nack_args).await {
                        tracing::error!(?e, "Error nacking delivery");
                    }
                    return;
                }
            };

            // Handler error → nack WITH requeue
            let result = match handler.handle(&ctx, event).await {
                Ok(result) => result,
                Err(e) => {
                    tracing::error!(%e, "Handler error, nacking with requeue");
                    let nack_args = BasicNackArguments {
                        delivery_tag: ctx.delivery_tag,
                        multiple: false,
                        requeue: true,
                    };
                    if let Err(e) = ctx.channel.basic_nack(nack_args).await {
                        tracing::error!(?e, "Error nacking delivery");
                    }
                    return;
                }
            };

            // AMQPResult handling
            match result.handle_result(&ctx).await {
                Ok(_) => {
                    if skip_ack {
                        return;
                    }
                    if let Err(e) = ctx
                        .channel
                        .basic_ack(BasicAckArguments {
                            delivery_tag: ctx.delivery_tag,
                            multiple: false,
                        })
                        .await
                    {
                        tracing::error!(?e, "Error acking delivery");
                    }
                }
                Err(e) => {
                    tracing::error!(?e, "Error processing AMQPResult, nacking delivery");
                    let nack_args = BasicNackArguments {
                        delivery_tag: ctx.delivery_tag,
                        multiple: false,
                        requeue: true,
                    };
                    if let Err(e) = ctx.channel.basic_nack(nack_args).await {
                        tracing::error!(?e, "Error nacking delivery");
                    }
                }
            }
        };

        tasks.spawn(fut.instrument(span));
    }

    /// Consumes the consumer and starts the loop.
    /// The loop will run indefinitely, relying on `AMQPConnection` to restore the connection
    /// if it is closed, until the notifier is triggered.
    pub async fn consume(self) {
        let notified = self.notifier.notified();
        tokio::pin!(notified);

        let mut tasks = JoinSet::new();
        let skip_ack = H::Result::manual() || self.consume_args.no_ack;
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
                            self.handle_delivery(delivery, &channel, skip_ack, &mut tasks);
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
