# streameroo
A collection of mini-frameworks & tools for building asynchronous applications in rust.

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

### Whats missing?
- Batch consuming/acknowledging to maximize throughput. `streameroo` currently runs on a single `Channel`. (You can initialize multiple instances of `Streameroo` with different channels)

### Quickstart
```rust
let url = "amqp://user:password@localhost:5672";
// Create a connection to the broker -> create a channel
let connection = lapin::Connection::connect(url, Default::default()).await?;
let channel = connection.create_channel().await?;
// Initialize your application's context (global state + channel)
```

### Handler


