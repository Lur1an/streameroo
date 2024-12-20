use std::ops::{Deref, DerefMut};

use serde::de::DeserializeOwned;

use super::{AMQPDecode, Error};

/// Type to instruct the framework to auto decode the event given its content-type and content-encoding headers
/// with a serde-compatible deserializer.
pub struct Auto<T>(pub T);

impl<T> Deref for Auto<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Auto<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> AMQPDecode for Auto<T>
where
    T: DeserializeOwned,
{
    fn decode(payload: Vec<u8>, context: &super::DeliveryContext) -> Result<Self, Error> {
        let Some(content_type) = context.properties.content_type() else {
            return Err(Error::Event(
                "Content-Type header missing for Auto decode event".into(),
            ));
        };
        match content_type.as_str() {
            "application/json" | "json" => {
                let event = serde_json::from_slice(&payload).map_err(Error::event)?;
                Ok(Self(event))
            }
            "application/msgpack" | "msgpack" => {
                #[cfg(feature = "msgpack")]
                {
                    let event = rmp_serde::from_slice(&payload).map_err(Error::event)?;
                    Ok(Self(event))
                }
                #[cfg(not(feature = "msgpack"))]
                {
                    Err(Error::Event(
                        "To Auto decode msgpack content, enable the 'msgpack' feature".into(),
                    ))
                }
            }
            "application/bson" | "bson" => {
                #[cfg(feature = "bson")]
                {
                    let event = bson::from_slice(&payload).map_err(Error::event)?;
                    Ok(Self(event))
                }
                #[cfg(not(feature = "bson"))]
                {
                    Err(Error::Event(
                        "To Auto decode bson content, enable the 'bson' feature".into(),
                    ))
                }
            }
            _ => Err(Error::Event(
                "Unsupported content-type for Auto decode event".into(),
            )),
        }
    }
}
