//! The [`Handler`] trait and its supporting types.
//!
//! The trait is intentionally free of `Clone`/`Send`/`'static` bounds. Those are
//! requirements of *how* a handler is driven (sequentially or concurrently) and
//! are imposed by [`Consumer`](crate::nats::jetstream::Consumer) rather than the contract
//! itself.

use crate::event::Decode;
use async_nats::HeaderMap;
use std::fmt::Display;
use std::future::Future;
use std::time::Duration;

/// Errors returned by a [`Handler`] must declare how the message should be
/// acknowledged via [`HandlerError::action`].
pub trait HandlerError: Display + Send + 'static {
    /// How the failed message should be handled.
    ///
    /// - [`ErrorAction::Retry`] NAKs the message for redelivery, with the delay
    ///   computed from the consumer's [`BackoffPolicy`] and delivery count.
    /// - [`ErrorAction::Dlq`] dead-letters the message (if a DLQ is configured),
    ///   then terminates it.
    /// - [`ErrorAction::Term`] terminates the message without redelivery.
    fn action(&self) -> ErrorAction;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorAction {
    /// Retry the message. Acks with `AckKind::Nak(duration)`, with `duration`
    /// determined from the backoff policy.
    Retry,
    /// Publish the message to the configured DLQ. Acks with `AckKind::Term`
    Dlq,
    /// Discard the message and don't requeue it. Acks with `AckKind::Term`.
    Term,
}

/// Determines the delay applied to a NAK before the server redelivers a
/// retried message. The delay grows with the delivery count and is capped at
/// `max_backoff`.
#[derive(Debug, Clone, Copy, Default)]
pub enum BackoffPolicy {
    /// Exponential backoff: `base * 2^(delivered - 1)`, capped at `max_backoff`.
    Exponential {
        base: Duration,
        max_backoff: Duration,
    },
    /// Linear backoff: `base * delivered`, capped at `max_backoff`.
    Linear {
        base: Duration,
        max_backoff: Duration,
    },
    /// No backoff. The message is NAK'd without an explicit delay, letting the
    /// server apply its default redelivery timing.
    #[default]
    None,
}

impl BackoffPolicy {
    /// Computes the NAK delay for a message that has been delivered `delivered`
    /// times (1 on first delivery). Returns `None` for [`BackoffPolicy::None`],
    /// meaning the message should be NAK'd without an explicit delay.
    pub fn nak_delay(&self, delivered: i64) -> Option<Duration> {
        let attempt = delivered.max(1) as u32;
        match self {
            BackoffPolicy::None => None,
            BackoffPolicy::Linear { base, max_backoff } => {
                let scaled = base
                    .checked_mul(attempt)
                    .unwrap_or(*max_backoff)
                    .min(*max_backoff);
                Some(scaled)
            }
            BackoffPolicy::Exponential { base, max_backoff } => {
                // base * 2^(attempt - 1), saturating to max_backoff on overflow.
                let factor = 1u32.checked_shl(attempt - 1);
                let scaled = factor
                    .and_then(|f| base.checked_mul(f))
                    .unwrap_or(*max_backoff)
                    .min(*max_backoff);
                Some(scaled)
            }
        }
    }
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn none_policy_has_no_delay() {
        assert_eq!(BackoffPolicy::None.nak_delay(1), None);
        assert_eq!(BackoffPolicy::None.nak_delay(10), None);
    }

    #[test]
    fn linear_scales_with_delivery_count_and_caps() {
        let policy = BackoffPolicy::Linear {
            base: Duration::from_secs(2),
            max_backoff: Duration::from_secs(10),
        };
        assert_eq!(policy.nak_delay(1), Some(Duration::from_secs(2)));
        assert_eq!(policy.nak_delay(3), Some(Duration::from_secs(6)));
        // 2s * 6 = 12s, capped at 10s.
        assert_eq!(policy.nak_delay(6), Some(Duration::from_secs(10)));
    }

    #[test]
    fn exponential_doubles_each_attempt_and_caps() {
        let policy = BackoffPolicy::Exponential {
            base: Duration::from_secs(1),
            max_backoff: Duration::from_secs(20),
        };
        assert_eq!(policy.nak_delay(1), Some(Duration::from_secs(1)));
        assert_eq!(policy.nak_delay(2), Some(Duration::from_secs(2)));
        assert_eq!(policy.nak_delay(4), Some(Duration::from_secs(8)));
        // 1s * 2^5 = 32s, capped at 20s.
        assert_eq!(policy.nak_delay(6), Some(Duration::from_secs(20)));
    }

    #[test]
    fn delivery_count_below_one_is_treated_as_first_attempt() {
        let policy = BackoffPolicy::Exponential {
            base: Duration::from_secs(1),
            max_backoff: Duration::from_secs(20),
        };
        assert_eq!(policy.nak_delay(0), Some(Duration::from_secs(1)));
        assert_eq!(policy.nak_delay(-5), Some(Duration::from_secs(1)));
    }

    #[test]
    fn large_delivery_count_saturates_to_max() {
        let policy = BackoffPolicy::Exponential {
            base: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
        };
        // A huge attempt count must not panic on overflow; it saturates.
        assert_eq!(policy.nak_delay(1_000), Some(Duration::from_secs(30)));
    }
}
