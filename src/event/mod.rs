use std::convert::Infallible;
use std::fmt::Debug;

pub trait Decode: Sized {
    type Error: std::error::Error + Send + Sync + 'static;

    fn decode(data: Vec<u8>) -> Result<Self, Self::Error>;
}

pub trait Encode {
    type Error: std::error::Error + Send + Sync + 'static;

    fn encode(&self) -> Result<Vec<u8>, Self::Error>;

    fn content_type() -> Option<&'static str> {
        None
    }
}

impl Decode for Vec<u8> {
    type Error = Infallible;

    fn decode(data: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(data)
    }
}

impl Encode for Vec<u8> {
    type Error = Infallible;

    fn encode(&self) -> Result<Vec<u8>, Self::Error> {
        Ok(self.clone())
    }
}

#[cfg(feature = "bytes")]
impl Decode for bytes::Bytes {
    type Error = Infallible;

    fn decode(data: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(bytes::Bytes::from(data))
    }
}

#[cfg(feature = "msgpack")]
mod msgpack {
    use std::ops::{Deref, DerefMut};

    use super::*;
    use serde::de::DeserializeOwned;
    use serde::Serialize;

    #[derive(Debug)]
    pub struct MsgPack<E>(pub E);

    impl<E> MsgPack<E> {
        /// Consumes the wrapper and returns the inner value
        pub fn into_inner(self) -> E {
            self.0
        }
    }

    impl<E> Deref for MsgPack<E> {
        type Target = E;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl<E> DerefMut for MsgPack<E> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }

    impl<E> Decode for MsgPack<E>
    where
        E: DeserializeOwned,
    {
        type Error = rmp_serde::decode::Error;

        fn decode(data: Vec<u8>) -> Result<Self, Self::Error> {
            Ok(MsgPack(rmp_serde::from_slice(&data)?))
        }
    }

    impl<E> Encode for MsgPack<E>
    where
        E: Serialize,
    {
        type Error = rmp_serde::encode::Error;

        fn encode(&self) -> Result<Vec<u8>, Self::Error> {
            rmp_serde::to_vec(&self.0)
        }

        fn content_type() -> Option<&'static str> {
            Some("application/msgpack")
        }
    }
}

#[cfg(feature = "msgpack")]
pub use msgpack::*;

#[cfg(feature = "json")]
mod json {
    use std::ops::{Deref, DerefMut};

    use super::*;
    use serde::de::DeserializeOwned;
    use serde::Serialize;

    #[derive(Debug)]
    pub struct Json<E>(pub E);

    impl<E> Json<E> {
        /// Consumes the wrapper and returns the inner value
        pub fn into_inner(self) -> E {
            self.0
        }
    }

    impl<E> Deref for Json<E> {
        type Target = E;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl<E> DerefMut for Json<E> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }

    impl<E> Encode for Json<E>
    where
        E: Serialize,
    {
        type Error = serde_json::Error;

        fn encode(&self) -> Result<Vec<u8>, Self::Error> {
            serde_json::to_vec(&self.0)
        }

        fn content_type() -> Option<&'static str> {
            Some("application/json")
        }
    }

    impl Encode for serde_json::Value {
        type Error = serde_json::Error;

        fn encode(&self) -> Result<Vec<u8>, Self::Error> {
            serde_json::to_vec(&self)
        }

        fn content_type() -> Option<&'static str> {
            Some("application/json")
        }
    }

    impl Decode for serde_json::Value {
        type Error = serde_json::Error;

        fn decode(data: Vec<u8>) -> Result<Self, Self::Error> {
            serde_json::from_slice(&data)
        }
    }

    impl<E> Decode for Json<E>
    where
        E: DeserializeOwned,
    {
        type Error = serde_json::Error;

        fn decode(data: Vec<u8>) -> Result<Self, Self::Error> {
            Ok(Json(serde_json::from_slice(&data)?))
        }
    }
}

#[cfg(feature = "json")]
pub use json::*;

#[cfg(feature = "bson")]
mod streameroo_bson {
    use std::ops::{Deref, DerefMut};

    use super::*;
    use serde::de::DeserializeOwned;
    use serde::Serialize;

    #[derive(Debug)]
    pub struct Bson<E>(pub E);

    impl<E> Bson<E> {
        /// Consumes the wrapper and returns the inner value
        pub fn into_inner(self) -> E {
            self.0
        }
    }

    impl<E> Deref for Bson<E> {
        type Target = E;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl<E> DerefMut for Bson<E> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }

    impl<E> Decode for Bson<E>
    where
        E: DeserializeOwned,
    {
        type Error = bson::de::Error;

        fn decode(data: Vec<u8>) -> Result<Self, Self::Error> {
            Ok(Bson(bson::from_slice(&data)?))
        }
    }

    impl<E> Encode for Bson<E>
    where
        E: Serialize,
    {
        type Error = bson::ser::Error;

        fn encode(&self) -> Result<Vec<u8>, Self::Error> {
            bson::to_vec(&self.0)
        }

        fn content_type() -> Option<&'static str> {
            Some("application/bson")
        }
    }
}

#[cfg(feature = "bson")]
pub use streameroo_bson::*;
