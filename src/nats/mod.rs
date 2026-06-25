mod error;
mod extensions;
pub mod jetstream;
#[cfg(feature = "telemetry")]
pub(crate) mod telemetry;
#[cfg(test)]
mod test_util;

pub use error::{Error, NatsResult};
pub use extensions::ClientExt;
