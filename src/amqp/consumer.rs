use super::Handler;
use super::connection::AMQPConnection;
use super::handler::{AMQPDecode, HandlerError};
use crate::amqp::AMQPResult;
use crate::amqp::context::create_delivery_context;
use amqprs::FieldTable;
use amqprs::channel::{
    BasicAckArguments, BasicConsumeArguments, BasicNackArguments, BasicQosArguments,
    ConsumerMessage,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tracing::Instrument;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerConfig {
    /// Target queue name. Must be provided.
    pub queue: String,
    /// Ignored by modern RabbitMQ releases. Default: `false`.
    #[serde(default)]
    pub no_local: bool,
    /// Should automatic acknowledgements be used? Default: `false`.
    #[serde(default)]
    pub no_ack: bool,
    /// Should this consumer be exclusive (the only one allowed on the target queue)? Default: `false`.
    #[serde(default)]
    pub exclusive: bool,
    /// Default: `false`.
    #[serde(default)]
    pub no_wait: bool,
    /// Default: empty table.
    #[serde(default)]
    pub arguments: FieldTable,
    /// Default: 0.
    #[serde(default)]
    pub prefetch_size: u32,
    /// Default: 0.
    #[serde(default)]
    pub prefetch_count: u16,
}

impl ConsumerConfig {
    /// Turns this configuration into the AMQP types required by `amqprs` functions.
    /// `consumer_tag` is left out of the config type as it is part of the `Streameroo` runner
    /// struct
    pub(crate) fn into_amqp(
        self,
        consumer_tag: String,
    ) -> (BasicConsumeArguments, BasicQosArguments) {
        let consume_args = BasicConsumeArguments {
            queue: self.queue,
            consumer_tag,
            no_local: self.no_local,
            no_ack: self.no_ack,
            exclusive: self.exclusive,
            no_wait: self.no_wait,
            arguments: self.arguments,
        };
        let qos_args = BasicQosArguments {
            prefetch_size: self.prefetch_size,
            prefetch_count: self.prefetch_count,
            global: false,
        };
        (consume_args, qos_args)
    }

    pub fn new(queue: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            no_local: false,
            no_ack: false,
            exclusive: false,
            no_wait: false,
            arguments: FieldTable::default(),
            prefetch_size: 0,
            prefetch_count: 0,
        }
    }
}

pub struct Consumer<H> {
    connection: AMQPConnection,
    consume_args: BasicConsumeArguments,
    qos_args: BasicQosArguments,
    handler: H,
    notifier: Arc<Notify>,
}

impl<H> Consumer<H> {
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
    fn handle_delivery<E>(
        &self,
        delivery: ConsumerMessage,
        channel: &amqprs::channel::Channel,
        skip_ack: bool,
        tasks: &mut JoinSet<()>,
    ) where
        H: Handler<E>,
        E: AMQPDecode + Send,
    {
        let (ctx, payload) = create_delivery_context(delivery, channel);
        let handler = self.handler.clone();

        #[cfg(feature = "telemetry")]
        let span = super::telemetry::make_delivery_span(&ctx, Some(&self.consume_args.queue));
        #[cfg(not(feature = "telemetry"))]
        let span = {
            let delivery_tag = ctx.delivery_tag;
            tracing::span!(tracing::Level::INFO, "streameroo::consumer", delivery_tag)
        };

        let fut = async move {
            // Decode error → nack WITHOUT requeue (would fail again on retry)
            let event = match E::decode(payload, &ctx) {
                Ok(event) => event,
                Err(e) => {
                    #[cfg(feature = "telemetry")]
                    super::telemetry::record_error(&tracing::Span::current(), "decode_error");
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

            // Handler error → nack, requeue decided by the error
            let result = match handler.handle(&ctx, event).await {
                Ok(result) => result,
                Err(e) => {
                    #[cfg(feature = "telemetry")]
                    super::telemetry::record_error(&tracing::Span::current(), "handler_error");
                    let requeue = e.requeue();
                    tracing::error!(%e, requeue, "Handler error, nacking delivery");
                    let nack_args = BasicNackArguments {
                        delivery_tag: ctx.delivery_tag,
                        multiple: false,
                        requeue,
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
                        #[cfg(feature = "telemetry")]
                        super::telemetry::record_error(
                            &tracing::Span::current(),
                            "settlement_error",
                        );
                        tracing::error!(?e, "Error acking delivery");
                    }
                }
                Err(e) => {
                    #[cfg(feature = "telemetry")]
                    super::telemetry::record_error(&tracing::Span::current(), "result_error");
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
    pub async fn consume<E>(self)
    where
        H: Handler<E>,
        E: AMQPDecode + Send,
    {
        let notified = self.notifier.notified();
        tokio::pin!(notified);

        let mut tasks = JoinSet::new();
        let skip_ack = self.consume_args.no_ack;
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
                            self.handle_delivery::<E>(delivery, &channel, skip_ack, &mut tasks);
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
