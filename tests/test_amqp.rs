use lapin::ConnectionProperties;

#[tokio::test]
async fn test_amqp() -> anyhow::Result<()> {
    let connection = lapin::Connection::connect(
        "amqp://guest:guest@localhost:5432",
        ConnectionProperties::default(),
    )
    .await
    .unwrap();
    let channel = connection.create_channel().await.unwrap();
    Ok(())
}
