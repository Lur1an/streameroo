//! Test fixtures for NATS + JetStream backed by a testcontainer.

use crate::nats::Streameroo;
#[cfg(test)]
use std::time::Duration;
use test_context::AsyncTestContext;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::core::IntoContainerPort;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

/// A running NATS container with JetStream enabled, a connected client and a
/// ready-to-use [`Streameroo`] instance.
pub struct NatsTest {
    pub client: async_nats::Client,
    pub js: async_nats::jetstream::Context,
    pub app: Streameroo,
    pub container: ContainerAsync<Nats>,
    #[cfg(test)]
    _guard: Option<init_tracing_opentelemetry::Guard>,
}

/// Starts a JetStream-enabled NATS container, returning the container and a
/// connected client.
pub async fn start_nats() -> (ContainerAsync<Nats>, async_nats::Client) {
    start_nats_with_port(None).await
}

/// Starts a JetStream-enabled NATS container. If `static_port` is provided,
/// the container's 4222 port is mapped to it on the host (useful for tests
/// that stop/start the container and need a stable address).
pub async fn start_nats_with_port(
    static_port: Option<u16>,
) -> (ContainerAsync<Nats>, async_nats::Client) {
    let cmd = NatsServerCmd::default().with_jetstream();
    let image = Nats::default().with_cmd(&cmd);
    let container = if let Some(port) = static_port {
        image
            .with_mapped_port(port, 4222.tcp())
            .start()
            .await
            .unwrap()
    } else {
        image.start().await.unwrap()
    };

    let host = container.get_host().await.unwrap();
    let host_port = container.get_host_port_ipv4(4222).await.unwrap();
    let url = format!("{host}:{host_port}");
    let client = async_nats::connect(url).await.unwrap();
    (container, client)
}

impl AsyncTestContext for NatsTest {
    async fn setup() -> Self {
        // The global trace dispatcher can only be set once per process. With
        // multiple test contexts running, only the first succeeds; the rest
        // reuse the already-installed subscriber.
        #[cfg(test)]
        let _guard = init_tracing_opentelemetry::TracingConfig::production()
            .init_subscriber()
            .ok();

        let (container, client) = start_nats().await;
        let js = async_nats::jetstream::new(client.clone());
        let app = Streameroo::new(client.clone(), None);
        NatsTest {
            client,
            js,
            app,
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
