//! JetStream primitives: durable consumers, the [`Handler`] contract,
//! dead-letter queues and the codec-aware [`publish`]/[`publish_with_headers`]
//! helpers.

mod consumer;
mod dlq;
mod handler;
mod producer;

pub use consumer::{Consumer, ConsumerConfig};
pub use dlq::{
    DLQ_DEAD_LETTERED_AT, DLQ_DELIVERED, DLQ_ERROR, DLQ_RETRIABLE, DLQ_SOURCE_SUBJECT,
    DLQ_STREAM_SEQUENCE, DlqConfig,
};
pub use handler::{BackoffPolicy, ErrorAction, Handler, HandlerError, MessageContext};
pub use producer::{publish, publish_with_headers};
