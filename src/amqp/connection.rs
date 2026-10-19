use amqprs::BasicProperties;
use amqprs::callbacks::{DefaultChannelCallback, DefaultConnectionCallback};
use amqprs::channel::{BasicPublishArguments, Channel};
use amqprs::connection::{Connection, OpenConnectionArguments};
use std::time::Duration;
use tokio::sync::mpsc::{self};
use tokio::sync::oneshot;
use tokio::time::error::Elapsed;

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("AMQP error: {0}")]
    Amqp(#[from] amqprs::error::Error),
    #[error("Timed out waiting for response from loop: {0}")]
    Timeout(#[from] Elapsed),
    #[error("Send error, io loop faulty")]
    LoopTx,
    #[error("Rpc sender dropped: {0}")]
    RpcRecv(#[from] oneshot::error::RecvError),
}

struct Publish {
    tx: oneshot::Sender<Result<(), amqprs::error::Error>>,
    data: Vec<u8>,
    properties: BasicProperties,
    args: BasicPublishArguments,
}

#[allow(dead_code)]
enum AMQPRpc {
    OpenChannel(oneshot::Sender<Result<Channel, amqprs::error::Error>>),
    Publish(Box<Publish>),
    Close,
}

#[derive(Clone)]
pub struct AMQPConnection {
    tx: mpsc::Sender<AMQPRpc>,
    rpc_timeout: Duration,
}

async fn default_connect(
    arguments: &OpenConnectionArguments,
) -> Result<Connection, amqprs::error::Error> {
    let connection = Connection::open(arguments).await?;
    connection
        .register_callback(DefaultConnectionCallback)
        .await?;
    Ok(connection)
}

async fn connection_loop(
    mut rx: mpsc::Receiver<AMQPRpc>,
    mut connection: Connection,
    mut pool: Vec<Channel>,
    arguments: OpenConnectionArguments,
) {
    let pool_size = pool.len();
    let mut pool_idx = 0;
    loop {
        // The bias makes sure the `listen_network_io_failure` future is polled first
        tokio::select! {
            biased;
            server_disconnected = connection.listen_network_io_failure() => {
                tracing::info!(server_disconnected, "Detected network io failure, reconnecting");
                loop {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    match default_connect(&arguments).await {
                        Ok(new_connection) => {
                            tracing::info!("Reconnected successfully. Discarding old connection & channels, filling up pool with new channels");
                            connection = new_connection;
                            for pool_slot in &mut pool {
                                let Ok(channel) = connection.open_channel(None).await else {
                                    tracing::error!("Failed to open channel after acquiring connection");
                                    continue;
                                };
                                *pool_slot = channel;
                            }
                            break;
                        },
                        Err(e) => {
                            tracing::error!(error = ?e, "Failed to reconnect, waiting 3s before retrying");
                        },
                    }
                }
            }
            rpc = rx.recv() => {
                match rpc {
                    Some(AMQPRpc::Publish(rpc)) => {
                        let channel = pool[pool_idx].clone();
                        tokio::spawn(async move {
                            let result = channel.basic_publish(
                                rpc.properties,
                                rpc.data,
                                rpc.args
                            ).await;
                            rpc.tx.send(result).ok();
                        });
                        pool_idx = (pool_idx + 1) % pool_size;
                    }
                    Some(AMQPRpc::OpenChannel(tx)) => {
                        tracing::info!("Opening AMQP channel");
                        let channel = connection.open_channel(None).await;
                        let result = match channel {
                            Ok(channel) => {
                                if let Err(e) = channel.register_callback(DefaultChannelCallback).await {
                                    Err(e)
                                } else {
                                    Ok(channel)
                                }
                            }
                            Err(e) => {
                                Err(e)
                            }
                        };
                        tx.send(result).ok();
                    }
                    // Both on close and drop close the connection
                    Some(AMQPRpc::Close) | None => {
                        if connection.is_open() {
                            tracing::info!("Closing AMQP connection");
                            if let Err(e) = connection.close().await {
                                tracing::error!(error = ?e, "Failed to close AMQP connection");
                            }
                        }
                        break;
                    }
                };
            }
        }
    }
}

impl AMQPConnection {
    pub async fn connect(arguments: OpenConnectionArguments) -> Result<Self, ConnectionError> {
        let (tx, rx) = mpsc::channel(2000);
        let rpc_timeout = Duration::from_secs(10);
        let connection = default_connect(&arguments).await?;
        let mut pool: Vec<Channel> = Vec::with_capacity(10);
        for _ in 0..10 {
            pool.push(connection.open_channel(None).await?);
        }
        tokio::spawn(connection_loop(rx, connection, pool, arguments));
        Ok(Self { tx, rpc_timeout })
    }

    pub async fn open_channel(&self) -> Result<Channel, ConnectionError> {
        let (tx, rx) = oneshot::channel();
        let fut = async {
            self.tx
                .send(AMQPRpc::OpenChannel(tx))
                .await
                .map_err(|_| ConnectionError::LoopTx)?;
            Ok::<_, ConnectionError>(rx.await?)
        };
        Ok(tokio::time::timeout(self.rpc_timeout, fut).await???)
    }

    /// Wrapper for a basic_publish operation over the AMQPRpc channel
    /// Will use a Channel from the pool to perform the operation
    pub async fn basic_publish(
        &self,
        properties: BasicProperties,
        data: Vec<u8>,
        args: BasicPublishArguments,
    ) -> Result<(), ConnectionError> {
        let (tx, rx) = oneshot::channel();
        let fut = async {
            self.tx
                .send(AMQPRpc::Publish(Box::new(Publish {
                    tx,
                    data,
                    properties,
                    args,
                })))
                .await
                .map_err(|_| ConnectionError::LoopTx)?;
            Ok::<_, ConnectionError>(rx.await?)
        };
        Ok(tokio::time::timeout(self.rpc_timeout, fut).await???)
    }
}

#[cfg(all(test, feature = "telemetry"))]
mod telemetry_tests {
    use super::*;
    use crate::amqp::{ChannelExt, telemetry};
    use opentelemetry::trace::{SpanKind, Status, TraceContextExt, TracerProvider};
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    use tracing_subscriber::prelude::*;

    #[tokio::test]
    async fn publish_span_covers_pending_publish_and_records_outcome() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));
        let _subscriber = tracing::subscriber::set_default(subscriber);
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

        for fail in [false, true] {
            exporter.reset();
            let (tx, mut rx) = mpsc::channel(1);
            let connection = AMQPConnection {
                tx,
                rpc_timeout: Duration::from_secs(10),
            };
            let parent = tracing::info_span!("caller");
            let parent_context = parent.context();
            let _entered = parent.enter();
            let publish = connection.publish("orders", "created", vec![1, 2, 3]);
            tokio::pin!(publish);
            let request = tokio::select! {
                result = &mut publish => panic!("publish completed before broker response: {result:?}"),
                request = rx.recv() => request.unwrap(),
            };
            let AMQPRpc::Publish(request) = request else {
                panic!("expected a publish request");
            };
            let context = telemetry::extract_context(request.properties.headers().unwrap());
            let propagated = context.span().span_context().clone();
            assert!(propagated.is_valid());
            assert_eq!(
                propagated.trace_id(),
                parent_context.span().span_context().trace_id()
            );
            assert!(exporter.get_finished_spans().unwrap().is_empty());

            let completed_at = std::time::SystemTime::now();
            if fail {
                drop(request.tx);
            } else {
                request.tx.send(Ok(())).unwrap();
            }
            assert_eq!(publish.await.is_err(), fail);

            let spans = exporter.get_finished_spans().unwrap();
            let producer = spans
                .iter()
                .find(|span| span.span_kind == SpanKind::Producer)
                .unwrap();
            assert_eq!(producer.name, "publish orders:created");
            assert_eq!(producer.span_context.span_id(), propagated.span_id());
            assert!(producer.end_time >= completed_at);
            let publish_call = spans
                .iter()
                .find(|span| span.name == "streameroo::amqp::publish_with_options")
                .unwrap();
            assert_eq!(producer.parent_span_id, publish_call.span_context.span_id());
            let error_type = producer
                .attributes
                .iter()
                .find(|attr| attr.key.as_str() == "error.type");
            if fail {
                assert!(matches!(producer.status, Status::Error { .. }));
                assert_eq!(
                    error_type.unwrap().value.as_str(),
                    std::any::type_name::<ConnectionError>()
                );
            } else {
                assert_eq!(producer.status, Status::Unset);
                assert!(error_type.is_none());
            }
        }
    }
}

#[cfg(any(test, feature = "amqp-test"))]
pub mod amqp_test {
    use super::*;
    use crate::event::Decode;
    use amqprs::channel::BasicConsumeArguments;
    use amqprs::connection::OpenConnectionArguments;
    use test_context::AsyncTestContext;
    use testcontainers_modules::rabbitmq::RabbitMq;
    use testcontainers_modules::testcontainers::core::IntoContainerPort;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tokio_stream::{Stream, StreamExt};
    use uuid::Uuid;

    pub struct AMQPTest {
        pub connection: AMQPConnection,
        pub container: ContainerAsync<RabbitMq>,
        #[cfg(test)]
        _guard: init_tracing_opentelemetry::Guard,
    }

    pub async fn start_rabbitmq() -> (ContainerAsync<RabbitMq>, OpenConnectionArguments) {
        start_rabbitmq_with_port(None).await
    }

    impl AMQPConnection {
        /// Consumes all messages left in the queue with a set timeout for each message
        /// whilst decoding it into the given type
        pub async fn drain_queue<E>(&self, queue: &str, timeout: Duration) -> impl Stream<Item = E>
        where
            E: Decode,
        {
            let channel = self.open_channel().await.unwrap();
            let (_, rx) = channel
                .basic_consume_rx(
                    BasicConsumeArguments::new(queue, &Uuid::new_v4().to_string())
                        .manual_ack(false)
                        .finish(),
                )
                .await
                .unwrap();
            UnboundedReceiverStream::new(rx)
                .timeout(timeout)
                .map_while(|result| {
                    result
                        .map(|msg| E::decode(msg.content.unwrap_or_default()).unwrap())
                        .ok()
                })
        }
        /// Consumes the next message in line from the given queue and acks it
        pub async fn consume_next<E>(&self, queue: &str) -> E
        where
            E: Decode,
        {
            let rx = self.drain_queue(queue, Duration::from_secs(2)).await;
            tokio::pin!(rx);
            rx.next().await.unwrap()
        }
    }

    pub async fn start_rabbitmq_with_port(
        static_port: Option<u16>,
    ) -> (ContainerAsync<RabbitMq>, OpenConnectionArguments) {
        let container = if let Some(port) = static_port {
            RabbitMq::default()
                .with_mapped_port(port, 5672.tcp())
                .start()
                .await
                .unwrap()
        } else {
            RabbitMq::default().start().await.unwrap()
        };
        let host_ip = container.get_host().await.unwrap();
        let host_port = container.get_host_port_ipv4(5672).await.unwrap();
        let args = OpenConnectionArguments::new(&host_ip.to_string(), host_port, "guest", "guest");
        (container, args)
    }

    impl AsyncTestContext for AMQPTest {
        async fn setup() -> Self {
            #[cfg(test)]
            let _guard = init_tracing_opentelemetry::TracingConfig::production()
                .init_subscriber()
                .unwrap();

            let (container, args) = start_rabbitmq().await;
            let connection = AMQPConnection::connect(args).await.unwrap();
            AMQPTest {
                connection,
                container,
                #[cfg(test)]
                _guard,
            }
        }
        async fn teardown(self) {
            self.container.rm().await.unwrap();
            #[cfg(test)]
            {
                drop(self._guard);
                // Give time for otel to flush
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_reconnect() -> anyhow::Result<()> {
        tracing_subscriber::fmt().init();
        let (container, args) = amqp_test::start_rabbitmq_with_port(Some(42068)).await;
        let connection = AMQPConnection::connect(args).await?;

        let channel = connection.open_channel().await?;
        assert!(channel.is_open());

        container.stop().await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!channel.is_open());

        container.start().await?;
        tokio::time::sleep(Duration::from_secs(4)).await;
        let channel = connection.open_channel().await?;
        assert!(channel.is_open());

        container.rm().await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!channel.is_open());
        Ok(())
    }
}
