use std::future::Future;
use std::time::Duration;

use amqprs::callbacks::{DefaultChannelCallback, DefaultConnectionCallback};
use amqprs::channel::{BasicPublishArguments, Channel};
use amqprs::connection::{Connection, OpenConnectionArguments};
use amqprs::BasicProperties;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::oneshot;
use tokio::time::error::Elapsed;

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("AMQP error: {0}")]
    Amqp(#[from] amqprs::error::Error),
    #[error("Timed out waiting for response from loop: {0}")]
    Timeout(#[from] Elapsed),
    #[error("Send error, io loop faulty: {0}")]
    LoopTx(#[from] mpsc::error::SendError<AMQPRpc>),
    #[error("Rpc sender dropped: {0}")]
    RpcRecv(#[from] oneshot::error::RecvError),
}

pub enum AMQPRpc {
    OpenChannel(oneshot::Sender<Result<Channel, amqprs::error::Error>>),
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
    arguments: OpenConnectionArguments,
) {
    loop {
        // The bias makes sure the `listen_network_io_failure` future is polled first
        tokio::select! {
            biased;
            server_disconnected = connection.listen_network_io_failure() => {
                tracing::info!(server_disconnected, "Detected network io failure, reconnecting");
                loop {
                    match default_connect(&arguments).await {
                        Ok(new_connection) => {
                            tracing::info!("Reconnected successfully");
                            connection = new_connection;
                            break;
                        },
                        Err(e) => {
                            tracing::error!(error = ?e, "Failed to reconnect, waiting 3s before retrying");
                            tokio::time::sleep(Duration::from_secs(3)).await;
                        },
                    }
                }
            }
            rpc = rx.recv() => {
                match rpc {
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
                        if tx.send(result).is_err() {
                            tracing::error!("Error sending response over rpc channel");
                        }
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
        tokio::spawn(connection_loop(rx, connection, arguments));
        Ok(Self { tx, rpc_timeout })
    }

    pub async fn open_channel(&self) -> Result<Channel, ConnectionError> {
        let (tx, rx) = oneshot::channel();
        let fut = async {
            self.tx.send(AMQPRpc::OpenChannel(tx)).await?;
            Ok::<_, ConnectionError>(rx.await?)
        };
        Ok(tokio::time::timeout(self.rpc_timeout, fut).await???)
    }

    async fn rpc(&self, req: AMQPRpc) {}
}

#[cfg(test)]
mod amqp_test {
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

    pub async fn start_rabbitmq(
        static_port: bool,
    ) -> (ContainerAsync<RabbitMq>, OpenConnectionArguments) {
        let container = if static_port {
            RabbitMq::default()
                .with_mapped_port(5672, 5672.tcp())
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
            tracing_subscriber::fmt().init();
            let (container, args) = start_rabbitmq(false).await;
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
    use amqp_test::AMQPTest;
    use test_context::test_context;
    use testcontainers_modules::rabbitmq::RabbitMq;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::ContainerAsync;

    #[tokio::test]
    async fn test_reconnect() -> anyhow::Result<()> {
        tracing_subscriber::fmt().init();
        let (container, args) = amqp_test::start_rabbitmq(true).await;
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

        let (_container, _) = amqp_test::start_rabbitmq(true).await;
        tokio::time::sleep(Duration::from_secs(6)).await;
        let channel = connection.open_channel().await?;
        assert!(channel.is_open());

        Ok(())
    }

    #[tokio::test]
    async fn test_playground() {
        tracing_subscriber::fmt().init();
        let container = RabbitMq::default().start().await.unwrap();
        let host_ip = container.get_host().await.unwrap();
        let host_port = container.get_host_port_ipv4(5672).await.unwrap();
        let args = OpenConnectionArguments::new(&host_ip.to_string(), host_port, "guest", "guest");
        let connection = Connection::open(&args).await.unwrap();
        let channel = connection.open_channel(None).await.unwrap();

        tokio::spawn({
            let connection = connection.clone();
            async move {
                if connection.listen_network_io_failure().await {
                    tracing::info!(
                        open = connection.is_open(),
                        "network io failure. connectoin dropped"
                    );
                } else {
                    tracing::info!(
                        open = connection.is_open(),
                        "no network io failure. connection closed by server"
                    );
                }
            }
        });
        connection.close().await.unwrap();
        tracing::info!("Connection closed");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        container.rm().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        assert!(!channel.is_open());
    }
}
