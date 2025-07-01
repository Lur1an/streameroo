#[cfg(test)]
mod test {
    use std::time::Duration;

    use rabbitmq_stream_client::{Environment, EnvironmentBuilder};
    use test_context::futures::FutureExt;
    use testcontainers_modules::rabbitmq::RabbitMq;
    use testcontainers_modules::testcontainers::core::{
        CmdWaitFor, ExecCommand, IntoContainerPort, WaitFor,
    };
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

    #[tokio::test]
    async fn test_deez() -> anyhow::Result<()> {
        // let container = GenericImage::new("rabbitmq", "4-management")
        //     .with_exposed_port(5552.tcp())
        //     .with_wait_for(WaitFor::message_on_stdout(
        //         "Server startup complete; 4 plugins started.",
        //     ))
        //     .with_env_var(
        //         "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS",
        //         "-rabbitmq_stream advertised_host localhost",
        //     )
        //     .start()
        //     .await?;
        // tokio::time::sleep(Duration::from_secs(5)).await;
        // let cmd = ExecCommand::new(["rabbitmq-plugins enable rabbitmq_stream"])
        //     .with_cmd_ready_condition(CmdWaitFor::message_on_stdout("Plugins changed;"));

        // let _ = container.exec(cmd).await;
        let environment = Environment::builder()
            // .host(&container.get_host().await?.to_string())
            // .port(container.get_host_port_ipv4(5552).await?)
            // .username("guest")
            // .password("guest")
            // .virtual_host("/")
            .build()
            .await
            .expect("Failed to build environment");
        Ok(())
    }
}
