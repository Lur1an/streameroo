//! The [`Handler`] trait and its supporting types.
//!
//! The trait is intentionally free of `Clone`/`Send`/`'static` bounds. Those are
//! requirements of *how* a handler is driven (sequentially or concurrently) and
//! are imposed by [`Consumer`](crate::nats::Consumer) rather than the contract
//! itself.

use crate::event::Decode;
use async_nats::HeaderMap;
use std::fmt::Display;
use std::future::Future;
use std::time::Duration;

/// Errors returned by a [`Handler`] must declare whether they are retriable.
///
/// - Retriable errors cause the message to be NAK'd and redelivered by the
///   server (subject to the consumer's `max_deliver` and `backoff`).
/// - Non-retriable errors are dead-lettered (if a DLQ is configured) and then
///   terminated, so they are never redelivered.
pub trait HandlerError: Display + Send + 'static {
    /// Whether the message should be retried. Return `false` for permanent
    /// failures (validation errors, unknown message types, etc.).
    fn action(&self) -> ErrorAction;
}

#[derive(Debug)]
pub enum ErrorAction {
    /// Retry the message. Acks with `AckKind::Nak(duration)`, with `duration`
    /// determined from the backoff policy.
    Retry,
    /// Publish the message to the configured DLQ. Acks with `AckKind::Term`
    Dlq,
    /// Discard the message and don't requeue it. Acks with `AckKind::Term`.
    Term,
}

#[derive(Debug)]
pub enum BackoffPolicy {
    /// Exponential backoff with a maximum of `max_backoff` nanoseconds.
    Exponential { max_backoff: Duration },
    /// Linear backoff with a maximum of `max_backoff` nanoseconds.
    Linear { max_backoff: Duration },
    /// No backoff
    None,
}

/// Metadata about a JetStream message passed to a [`Handler`].
///
/// The context borrows the subject and headers from the underlying message,
/// avoiding a clone on every dispatch. It is only valid for the duration of the
/// [`Handler::handle`] call.
pub struct MessageContext<'a> {
    /// The subject the message was published to.
    pub subject: &'a str,
    /// The stream the message was consumed from.
    pub source_stream: &'a str,
    /// The message headers, if any.
    pub headers: Option<&'a HeaderMap>,
    /// Number of times this message has been delivered (1 on first delivery).
    pub delivered: i64,
    /// The message's sequence number within the stream.
    pub stream_sequence: u64,
    /// The message's sequence number within the consumer.
    pub consumer_sequence: u64,
}

/// Processes messages from a JetStream consumer.
pub trait Handler {
    /// The decoded message type.
    type Event: Decode + Send;
    /// Error type of the handler function. Specifies how to handle errors by implementing
    /// `HandlerError`
    type Error: HandlerError;

    fn handle(
        &mut self,
        ctx: &MessageContext<'_>,
        event: Self::Event,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
