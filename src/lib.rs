#[cfg(feature = "amqp")]
pub mod amqp;
pub mod event;
#[cfg(feature = "nats")]
pub mod nats;

#[cfg(all(test, feature = "telemetry"))]
pub mod test_util;

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;
