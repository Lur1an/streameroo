use async_nats::jetstream::consumer::StreamError;
use async_nats::jetstream::consumer::pull::MessagesError;
use async_nats::jetstream::context::{CreateStreamError, PublishError};
use async_nats::jetstream::stream::ConsumerError;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Configuration error for a consumer
    #[error("Invalid consumer configuration: {0}")]
    Config(&'static str),

    /// Failure encoding or decoding an event payload via the `Encode`/`Decode` traits.
    #[error("Event encoding/decoding error: {0}")]
    Event(BoxError),

    /// Failure publishing on the core NATS client.
    #[error("NATS publish error: {0}")]
    Publish(#[from] async_nats::PublishError),

    /// Failure publishing to JetStream (includes DLQ publishes).
    #[error("JetStream publish error: {0}")]
    JetStreamPublish(#[from] PublishError),

    /// Failure creating or fetching a JetStream stream.
    #[error("JetStream stream error: {0}")]
    Stream(#[from] CreateStreamError),

    /// Failure creating or fetching a JetStream consumer.
    #[error("JetStream consumer error: {0}")]
    Consumer(#[from] ConsumerError),

    /// Failure opening the message stream for a consumer.
    #[error("JetStream message stream error: {0}")]
    Messages(#[from] StreamError),

    /// Non-recoverable failure while pulling messages from a consumer.
    #[error("JetStream consumer pull error: {0}")]
    Pull(#[from] MessagesError),
}

impl Error {
    pub(crate) fn event(e: impl Into<BoxError>) -> Self {
        Self::Event(e.into())
    }
}

pub type NatsResult<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod test {
    use super::*;
    use std::assert_matches;

    #[derive(Debug, thiserror::Error)]
    #[error("decode boom")]
    struct DecodeBoom;

    #[test]
    fn event_wraps_into_event_variant() {
        let err = Error::event(DecodeBoom);
        assert_matches!(err, Error::Event(_));
        assert_eq!(
            err.to_string(),
            "Event encoding/decoding error: decode boom"
        );
    }

    #[test]
    fn config_error_displays_message() {
        let err = Error::Config("max_ack_pending must be 1");
        assert_eq!(
            err.to_string(),
            "Invalid consumer configuration: max_ack_pending must be 1"
        );
    }
}
