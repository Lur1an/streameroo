#![cfg(all(feature = "nats", feature = "json"))]

#[path = "nats/consumer.rs"]
mod consumer;
#[path = "nats/publish.rs"]
mod publish;
#[path = "nats/support/mod.rs"]
mod support;
