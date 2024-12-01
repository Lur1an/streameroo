# streameroo
A collection of mini-frameworks & tools for building asynchronous applications in rust.

## What is this crate for?
`streameroo` is a collection of mini-frameworks & tools for building asynchronous applications in rust.
I built a small version of my ideas for working with AMQP using `lapin` and `tokio`, I have a few plans for working with [fluvio](https://github.com/infinyon/fluvio).

## AMQP

Through traits & wrapper types you can instrument the framework to take care of a lot of boilerplate and verbose pain when working with `lapin`.
The inner workings of `streameroo` might change, however I am satisfied with the API, so that will very very likely NOT change in a breaking manner.

This mini-framework takes care of:
- `ack/nack` operations on various types of errors
- deserialization and serialization of your events
- Global state management for your handlers
- Post event-consumption actions:
  1. Publish an event
  2. Respond to a RPC request (`reply-to`, standard)
  3. Custom?
- Provides `ChannelExt` for slightly more convenient publishing of events

### Whats missing?
- Batch consuming/acknowledging to maximize throughput. `streameroo` currently runs on a single `Channel`, (You can initialize multiple instances of `Streameroo` with different channels) and also doesn't support message batching & batch acks


### Quickstart
Create a struct for the event you want to handle. If you are using common formats like JSON, MsgPack or other support formats by `streameroo` all you need is a `DeserializeOwned` implementing `struct`.
```rust
#[derive(Debug, Deserialize)]
struct MyEvent {
    hello: String
}
```
Now create a handler. A handler is anything that implements the `AMQPHandler` trait, you're not supposed to implement this trait yourself, its implemented automatically for matching async functions.
```rust
async fn test_event_handler(
    pgpool: State<PgPool>,
    redelivered: Redelivered,
    event: Json<MyEvent>,
)
```
Lets dissect this a bit:
- `pgpool`: This is a global state that you can access in your handler, if you don't provide it in the context it'll panic. You can extract any piece of global data, as long as you have initialized it in the context.
- `exchange`: This uses the `Redelivered` extractor to access the `redelivered` flag of the delivery.
- `event`: This is the event you want to handle. The `Json` extractor instructs `streameroo` to deserialize `MyEvent` using `serde_json`. For custom protocols that are not covered by the framework you can either just use `Vec<u8>` or implement `Decode` yourself.

Now you can create a `Streameroo` instance and consume events from a queue.
The instance of the application runs on top of a `Channel`, if you want to use multiple channels you need to re-initialize the context, plans to improve on this will come later. However most of the time only a single `Channel` is used for consumption.
```rust
// Connect to the broker and create a channel
// This could be shortened and implemented on top of `Streameroo` in the future if this feels clunky
let url = "amqp://user:password@localhost:5672";
let connection = lapin::Connection::connect(url, Default::default()).await?;
let channel = connection.create_channel().await?;
// Initialize your application's context (global state + channel)
let mut context = Context::new(ctx.channel.clone());
// Add any global state you want to access in your handler, for example a database connection
context.data(pgpool);
// Give your app a consumer tag and the context
let mut app = Streameroo::new(context, "test-consumer");
// Now you can spawn as many consumers as you want
app.consume(test_event_handler, "test").await?;
```

### `AMQPHandler`

The `AMQPHandler` trait is used to define a handler for a specific event. Its implemented for all async functions that return a `Result<T, E>` where `T` is `AMQPResult` and `E` can be turned into a boxed error type.
```rust
async fn test_event_handler(
    counter: StateOwned<Arc<AtomicU8>>,
    exchange: Exchange,
    redelivered: Redelivered,
    event: Json<TestEvent>,
) -> anyhow::Result<()> {
    let event = event.into_inner();

    assert_eq!(exchange.into_inner(), "");
    assert_eq!(event.0, "hello");
    let count = counter.load(Ordering::Relaxed);
    tracing::info!(?count);
    if count == 0 {
        assert!(!redelivered.into_inner());
    } else {
        assert!(redelivered.into_inner());
    }
    if count < 3 {
        counter.fetch_add(1, Ordering::Relaxed);
        anyhow::bail!("Go again");
    }
    Ok(())
}
```
This is a valid handler. All required values are extracted and injected into it at runtime.

### `AMQPResult`
The `AMQPResult` trait is used to control what happens to a delivery after it has been handled by your handler. The body of the `AMQPResult` implementation is executed before attempting to acknowledge the delivery. If the impl fails the delivery is `nack`'ed
Some common impls are:
- `()` Nothing happens, the delivery is acknowledged as is.
- `Publish<E>` Publishes the event to the exchange specified in the `Publish` struct. 
`E` implements `Encode`. `Encode` is implemented for most common formats like JSON, MsgPack and others through the wrapper types.
- `DeliveryAction` is for fine-grained control, the best way to use this is set your error type to `Infallible` and handle everything yourself by returning the correct `Deliveryaction` for your usecase. When returning this value the automatic `ack` of the framework is disabled as its taken care of by the `DeliveryAction` impl.
- `PublishReply<E>` follows the RPC pattern for rabbitmq. It publishes the event `E` t the queue specified in the `reply-to` header.

### `ChannelExt`
ChannelExt is an extension trait for lapin's `Channel` type that allows working with events with slightly less boilerplate and reuse the wrapper types and `Decode/Encode` traits.
The most useful one is the `direct_rpc` method which implements the direct reply to RPC pattern from the [RabbitMQ docs](https://www.rabbitmq.com/docs/direct-reply-to).
```rust
let result: Json<TestEvent> = channel
    .direct_rpc(
        "",
        &queue,
        Duration::from_secs(5),
        Json(TestEvent("hello".into())),
    )
    .await?;
```


