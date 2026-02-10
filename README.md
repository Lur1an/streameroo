# streameroo

A mini-framework for building resilient asynchronous AMQP/RabbitMQ consumer applications in Rust, inspired by [Axum](https://github.com/tokio-rs/axum)'s ergonomic handler pattern.

Built on top of [`amqprs`](https://github.com/gftea/amqprs) and [`tokio`](https://tokio.rs).

## Features

- **Axum-style handlers** -- plain async functions become message handlers via compile-time extraction
- **Automatic connection recovery** -- background IO loop with reconnection and a pre-allocated channel pool for publishing
- **Consumer resilience** -- consumers automatically recover from channel/connection failures
- **Flexible serialization** -- JSON, MessagePack, BSON, raw bytes, or runtime content-type dispatch via `Auto<T>`
- **Dependency injection** -- global state and per-delivery metadata extractors
- **Result-driven actions** -- handler return types control ack/nack, publish, or RPC reply behavior
- **Distributed tracing** -- optional OpenTelemetry trace propagation through AMQP headers
- **Graceful shutdown** -- signal-based shutdown that drains in-flight deliveries
- **RPC support** -- built-in [Direct Reply-To](https://www.rabbitmq.com/docs/direct-reply-to) pattern
- **Test utilities** -- testcontainers-based integration test harness

## Quickstart

Add `streameroo` to your `Cargo.toml`:

```toml
[dependencies]
streameroo = "0.4.2"
```

The default features enable `tokio` and `json` (serde_json). See [Feature Flags](#feature-flags) for additional options.

### Define an event

Any `DeserializeOwned` struct works with the built-in `Json<T>` wrapper:

```rust
#[derive(Debug, Deserialize)]
struct MyEvent {
    hello: String,
}
```

### Write a handler

A handler is any async function whose parameters are extractors followed by a final event parameter:

```rust
async fn my_handler(
    pool: State<PgPool>,
    redelivered: Redelivered,
    event: Json<MyEvent>,
) -> anyhow::Result<()> {
    tracing::info!(?event, "received");
    Ok(())
}
```

- `pool` -- shared state injected from the application `Context`
- `redelivered` -- extractor for the AMQP `redelivered` flag
- `event` -- the deserialized message body (last parameter, must implement `AMQPDecode`)
- Returning `Ok(())` acknowledges the delivery; returning `Err` nacks with requeue

### Run the application

```rust
use streameroo::amqp::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = amqprs::connection::OpenConnectionArguments::new(
        "localhost", 5672, "guest", "guest",
    );
    let connection = AMQPConnection::connect(args).await?;

    let mut context = Context::new();
    context.data(pgpool); // register any shared state

    let mut app = Streameroo::new(connection, context, "my-consumer");
    app.with_graceful_shutdown(tokio::signal::ctrl_c());
    app.consume(my_handler, "my-queue", 10).await?;
    app.join().await;

    Ok(())
}
```

The third argument to `consume` sets both the number of concurrent handler tasks and the prefetch count. For full control over QoS and consume options, use `consume_with_options`.

## Handlers

### `AMQPHandler`

The `AMQPHandler` trait is automatically implemented for async functions with up to 13 extractor parameters plus one event parameter. You never implement it manually.

```rust
async fn handler(
    // 0..13 extractors (impl FromDeliveryContext)
    state: StateOwned<Arc<AtomicU8>>,
    exchange: Exchange,
    redelivered: Redelivered,
    // last parameter: the event (impl AMQPDecode)
    event: Json<MyEvent>,
) -> anyhow::Result<()> {
    // ...
    Ok(())
}
```

**Constraints:**
- The **last** parameter must implement `AMQPDecode` (the message payload)
- All preceding parameters must implement `FromDeliveryContext`
- The error type must implement `Into<Box<dyn Error + Send + Sync>>` (e.g. `anyhow::Error`)
- The return type `T` must implement `AMQPResult`
- The function must be `Clone + Send + Sync + 'static`

### Extractors

Extractors pull data from the delivery context and are used as handler parameters (before the event).

| Extractor | Inner Type | Description |
|-----------|-----------|-------------|
| `State<T>` | `&'static T` | Static reference to shared state from `Context` |
| `StateOwned<T>` | `T` | Cloned copy of shared state (requires `T: Clone`) |
| `Exchange` | `String` | The exchange the message was published to |
| `RoutingKey` | `String` | The routing key of the delivery |
| `ReplyTo` | `Option<String>` | The `reply-to` property, if present |
| `DeliveryTag` | `u64` | The AMQP delivery tag |
| `Redelivered` | `bool` | Whether this is a redelivery |
| `BasicProperties` | `BasicProperties` | The full AMQP properties (cloned) |
| `Channel` | `Channel` | The AMQP channel that received the delivery |

All wrapper extractors implement `Deref` to their inner type and provide `into_inner()`.

### `AMQPResult`

The handler's return type controls what happens after successful execution. The trait provides a `manual()` flag: when `false` (the default), the framework auto-acks on success.

| Return Type | Behavior |
|-------------|----------|
| `()` | No-op. Framework auto-acks on `Ok`, nacks with requeue on `Err`. |
| `Publish<E>` | Publishes `E` to a specified exchange/routing_key, then auto-acks. |
| `PublishReply<E>` | Publishes `E` to the `reply-to` address (RPC pattern), then auto-acks. |
| `DeliveryAction` | Manual ack/nack control. Framework does **not** auto-ack. |

#### `Publish<E>`

Publishes a message to another queue after handling:

```rust
async fn forward_handler(
    event: Json<MyEvent>,
) -> anyhow::Result<Publish<Json<MyEvent>>> {
    Ok(Publish::new(event, "", "forwarded-queue"))
}
```

The `Publish` struct also exposes `options` and `properties` fields for full control over the publish arguments.

#### `PublishReply<E>`

Implements the RPC reply-to pattern:

```rust
async fn rpc_handler(
    event: Json<Request>,
) -> anyhow::Result<PublishReply<Json<Response>>> {
    let response = process(event.into_inner());
    Ok(PublishReply::new(Json(response)))
}
```

If no `reply-to` header is present on the incoming message, the reply is silently discarded.

#### `DeliveryAction`

For fine-grained ack/nack control:

```rust
async fn manual_handler(
    event: Json<MyEvent>,
) -> Result<DeliveryAction, Infallible> {
    if should_requeue(&event) {
        Ok(DeliveryAction::Nack { requeue: true, multiple: false })
    } else {
        Ok(DeliveryAction::Ack { multiple: false })
    }
}
```

### Error Handling

The framework distinguishes error types to decide requeue behavior:

| Error Source | Behavior |
|-------------|----------|
| Decode failure (`Error::Event`) | Nack **without** requeue (would fail again) |
| Handler error (`Error::Handler`) | Nack **with** requeue (assumed transient) |
| Result action failure | Nack **with** requeue |

## Serialization

### `Encode` / `Decode` Traits

The `event` module provides format-agnostic serialization traits:

```rust
pub trait Decode: Sized {
    type Error: std::error::Error + Send + Sync + 'static;
    fn decode(data: Vec<u8>) -> Result<Self, Self::Error>;
}

pub trait Encode {
    type Error: std::error::Error + Send + Sync + 'static;
    fn encode(&self) -> Result<Vec<u8>, Self::Error>;
    fn content_type() -> Option<&'static str> { None }
}
```

The `content_type()` associated function is used by the publishing system to automatically set the AMQP `content_type` property on outgoing messages.

### Wrapper Types

| Type | Feature | Content-Type | Decode | Encode |
|------|---------|-------------|--------|--------|
| `Json<T>` | `json` (default) | `application/json` | `T: DeserializeOwned` | `T: Serialize` |
| `MsgPack<T>` | `msgpack` | `application/msgpack` | `T: DeserializeOwned` | `T: Serialize` |
| `Bson<T>` | `bson` | `application/bson` | `T: DeserializeOwned` | `T: Serialize` |
| `serde_json::Value` | `json` | `application/json` | Yes | Yes |
| `Vec<u8>` | always | none | passthrough | passthrough |
| `bytes::Bytes` | `bytes` | none | decode only | -- |

All wrapper types implement `Deref`/`DerefMut` to the inner `T` and provide `into_inner()`.

### `Auto<T>` -- Runtime Content-Type Dispatch

`Auto<T>` selects the deserializer at runtime based on the message's `content_type` AMQP property:

```rust
async fn handler(event: Auto<MyEvent>) -> anyhow::Result<()> {
    // Works with JSON, MsgPack, or BSON depending on the content-type header
    Ok(())
}
```

Supported content-type values:

| Header Value | Decoder | Feature Required |
|-------------|---------|-----------------|
| `application/json` or `json` | serde_json | `json` (default) |
| `application/msgpack` or `msgpack` | rmp_serde | `msgpack` |
| `application/bson` or `bson` | bson | `bson` |

Returns an error if the content-type header is missing or unsupported. `Auto<T>` is decode-only; it does not implement `Encode`.

## Connection Management

### `AMQPConnection`

`AMQPConnection` is a `Clone`-able handle that wraps the raw connection behind an actor-like IO loop:

```rust
let args = OpenConnectionArguments::new("localhost", 5672, "guest", "guest");
let connection = AMQPConnection::connect(args).await?;
```

Key behaviors:
- **Channel pool** -- pre-allocates 10 channels for publishing, used in round-robin
- **Automatic reconnection** -- on connection failure, retries every 3 seconds indefinitely, re-opening all pool channels on success
- **RPC timeout** -- all operations (open channel, publish) have a 10-second timeout

Public API:
- `connect(args)` -- create a new connection with IO loop
- `open_channel()` -- open a new channel (for consuming or advanced operations)
- `basic_publish(properties, data, args)` -- publish via the channel pool

### `ChannelExt`

Extension trait implemented for both `amqprs::Channel` and `AMQPConnection`:

```rust
pub trait ChannelExt {
    fn publish<E: Encode>(&self, exchange: &str, routing_key: &str, event: E) -> ...;
    fn publish_with_options<E: Encode>(&self, args, properties, event: E) -> ...;
    fn direct_rpc<E: Encode, T: Decode>(&mut self, exchange: &str, routing_key: &str, timeout: Duration, event: E) -> ...;
}
```

`publish` and `publish_with_options` automatically set the `content_type` property from the `Encode` implementation when not already present. With the `telemetry` feature enabled, they also inject OpenTelemetry trace context into AMQP headers.

#### Direct RPC

Implements the [RabbitMQ Direct Reply-To](https://www.rabbitmq.com/docs/direct-reply-to) pattern:

```rust
let response: Json<MyResponse> = channel
    .direct_rpc(
        "",
        &queue,
        Duration::from_secs(5),
        Json(MyRequest { data: "hello".into() }),
    )
    .await?;
```

## Graceful Shutdown

Register a shutdown signal (any `Future`) to stop all consumers:

```rust
app.with_graceful_shutdown(tokio::signal::ctrl_c());
```

When the signal fires, all consumer loops are notified. In-flight handler tasks are awaited before the consumer channels are closed. Call `app.join().await` to block until all consumers have fully stopped.

You can also obtain a manual shutdown handle:

```rust
let handle = app.shutdown_handle();
// Later, from anywhere:
handle.notify_waiters();
```

## OpenTelemetry Integration

Enable distributed tracing with the `telemetry` feature:

```toml
streameroo = { version = "0.4", features = ["telemetry"] }
```

### What it does

- **Producer side**: when publishing via `ChannelExt`, a `SpanKind::Producer` span is created and trace context (W3C `traceparent`/`tracestate`) is injected into AMQP message headers
- **Consumer side**: when a delivery arrives, trace context is extracted from AMQP headers and set as the parent of a `SpanKind::Consumer` span that wraps the handler execution

This creates unbroken traces across `producer -> broker -> consumer` boundaries, visible in any OpenTelemetry-compatible backend (Jaeger, Grafana Tempo, Honeycomb, Datadog, etc.).

### Span attributes

Consumer and producer spans include:
- `otel.name` -- `{exchange}.{routing_key}`
- `otel.kind` -- `Consumer` or `Producer`
- `amqp.exchange`, `amqp.routing_key`
- `amqp.correlation_id`, `amqp.reply_to`, `amqp.content_type`
- `delivery_tag` (consumer only)

### Configuration

The library does **not** initialize a tracing subscriber or OpenTelemetry pipeline. Your application is responsible for setting up the OTel exporter. The library uses `opentelemetry::global::get_text_map_propagator` for context propagation, so standard `OTEL_*` environment variables apply:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
OTEL_EXPORTER_OTLP_PROTOCOL=grpc
OTEL_PROPAGATORS=tracecontext,baggage
```

A `compose.yaml` is included in the repository for running a local Jaeger instance:

```bash
docker compose up -d
# Jaeger UI at http://localhost:16686
```

### Public API

The `telemetry` module is public when the feature is enabled:

```rust
use streameroo::amqp::telemetry;

telemetry::inject_context(&otel_context, &mut headers);  // producer: inject into AMQP headers
let ctx = telemetry::extract_context(&headers);           // consumer: extract from AMQP headers
let span = telemetry::make_span_from_delivery_context(&delivery_ctx);
let span = telemetry::make_span_from_properties(&props, SpanKind::Producer, "exchange", "key");
```

## Utilities

### `field_table!` Macro

Convenience macro for building `amqprs::FieldTable` values (e.g. for queue arguments):

```rust
use streameroo::field_table;
use streameroo::amqp::XQueueType;

let args = field_table!(
    ("x-queue-type", XQueueType::Quorum),
    ("x-delivery-limit", amqprs::FieldValue::u(5)),
);
```

### `XQueueType`

Enum for RabbitMQ queue types, convertible to `FieldValue`:

```rust
pub enum XQueueType { Classic, Quorum, Stream }
```

### `table_from_map`

Converts a `HashMap<String, String>` into a `FieldTable`:

```rust
let map = HashMap::from([("key".into(), "value".into())]);
let table = table_from_map(&map);
```

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `tokio` | yes | Tokio async runtime |
| `json` | yes | `Json<T>` wrapper + `serde_json::Value` support |
| `msgpack` | no | `MsgPack<T>` wrapper via MessagePack |
| `bson` | no | `Bson<T>` wrapper via BSON |
| `bytes` | no | `Decode` impl for `bytes::Bytes` |
| `telemetry` | no | OpenTelemetry distributed tracing through AMQP headers |
| `amqp-test` | no | Test utilities: `AMQPTest` context, `start_rabbitmq()`, `consume_next()` |

## Testing

### Using the test utilities

Enable the `amqp-test` feature to access the test harness in your own integration tests:

```toml
[dev-dependencies]
streameroo = { version = "0.4", features = ["amqp-test"] }
test-context = "0.3"
```

```rust
use streameroo::amqp::amqp_test::AMQPTest;
use test_context::test_context;

#[test_context(AMQPTest)]
#[tokio::test]
async fn my_integration_test(ctx: &mut AMQPTest) {
    let channel = ctx.connection.open_channel().await.unwrap();
    // declare queues, publish messages, consume, assert...
}
```

`AMQPTest` automatically spins up a RabbitMQ container via testcontainers, creates a connection, and tears everything down after the test. The `consume_next` helper on `AMQPConnection` consumes and acks a single message from a queue:

```rust
let result: Json<MyEvent> = ctx.connection.consume_next("my-queue").await;
```
## License

Apache-2.0
