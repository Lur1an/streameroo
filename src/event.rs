use std::fmt::Debug;

pub trait Event: Sized + Debug + Decode {}

pub trait Decode: Sized {
    type Error: std::error::Error;

    fn decode(data: &[u8]) -> Result<Self, Self::Error>;
}

pub trait Encode {
    type Error: std::error::Error;

    fn encode(&self) -> Result<Vec<u8>, Self::Error>;
}
