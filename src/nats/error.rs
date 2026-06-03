type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Event encoding/decoding error: {0}")]
    Event(BoxError),
    #[error("NATS publish error: {0}")]
    Publish(#[from] async_nats::PublishError),
}

impl Error {
    pub(crate) fn event(e: impl Into<BoxError>) -> Self {
        Self::Event(e.into())
    }
}

pub type NatsResult<T> = std::result::Result<T, Error>;
