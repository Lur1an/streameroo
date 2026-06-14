//! The [`Handler`] trait and its supporting types.
//!
//! The trait is intentionally free of `Clone`/`Send`/`'static` bounds. Those are
//! requirements of *how* a handler is driven (sequentially or concurrently) and
//! are imposed by the processor structs rather than the contract itself.

use crate::event::Decode;
use async_nats::HeaderMap;
use std::fmt::Display;
use std::future::Future;

/// Errors returned by a [`Handler`] must declare whether they are retriable.
///
/// - Retriable errors cause the message to be NAK'd and redelivered by the
///   server (subject to the consumer's `max_deliver` and `backoff`).
/// - Non-retriable errors are dead-lettered (if a DLQ is configured) and then
///   terminated, so they are never redelivered.
pub trait HandlerError: Display + Send + 'static {
    /// Whether the message should be retried. Return `false` for permanent
    /// failures (validation errors, unknown message types, etc.).
    fn is_retriable(&self) -> bool;
}

/// Metadata about a JetStream message passed to a [`Handler`].
pub struct MessageContext {
    /// The subject the message was published to.
    pub subject: String,
    /// The message headers, if any.
    pub headers: Option<HeaderMap>,
    /// Number of times this message has been delivered (1 on first delivery).
    pub delivered: i64,
    /// The message's sequence number within the stream.
    pub stream_sequence: u64,
    /// The message's sequence number within the consumer.
    pub consumer_sequence: u64,
}

/// Processes messages from a JetStream consumer.
///
/// The trait declares only the message type, error type, and the handling
/// method. Lifecycle bounds are added by the processor:
/// - the sequential processor requires `Handler + Send`,
/// - the queue (concurrent) processor requires `Handler + Clone + Send + 'static`.
pub trait Handler {
    /// The decoded message type. Codec is chosen by the type (e.g. `Json<T>`).
    type Event: Decode + Send;
    /// The error type, which declares its own retriability.
    type Error: HandlerError;

    fn handle(
        &self,
        ctx: &MessageContext,
        event: Self::Event,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
