use amqprs::callbacks::{DefaultChannelCallback, DefaultConnectionCallback};
use amqprs::channel::{BasicPublishArguments, Channel};
use amqprs::connection::{Connection, OpenConnectionArguments};
use amqprs::BasicProperties;
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

#[cfg(any(test, feature = "amqp-test"))]
pub mod amqp_test {
    use super::*;
    use amqprs::connection::OpenConnectionArguments;
    use test_context::AsyncTestContext;
    use testcontainers_modules::rabbitmq::RabbitMq;
    use testcontainers_modules::testcontainers::core::IntoContainerPort;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

    pub struct AMQPTest {
        pub connection: AMQPConnection,
        pub container: ContainerAsync<RabbitMq>,
    }
    pub async fn start_rabbitmq() -> (ContainerAsync<RabbitMq>, OpenConnectionArguments) {
        start_rabbitmq_with_port(None).await
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
            tracing_subscriber::fmt().init();

            let (container, args) = start_rabbitmq().await;
            let connection = AMQPConnection::connect(args).await.unwrap();
            AMQPTest {
                connection,
                container,
            }
        }
        async fn teardown(self) {
            self.container.rm().await.unwrap();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_reconnect() -> anyhow::Result<()> {
        tracing_subscriber::fmt().init();
        let (container, args) = amqp_test::start_rabbitmq_with_port(Some(42069)).await;
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
