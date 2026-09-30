use crate::amqp::{AMQPResult, DeliveryContext, Error};
use crate::event::Decode;
use std::fmt::Display;
use std::future::Future;

/// Handles a decoded event. A single type can implement this trait for multiple events.
///
/// ```no_run
/// # #[cfg(feature = "json")]
/// # {
/// use std::convert::Infallible;
/// use streameroo::amqp::{DeliveryContext, Handler, Streameroo, StreamerooResult};
/// use streameroo::event::Json;
///
/// #[derive(Clone)]
/// struct MyHandler;
///
/// impl Handler<Vec<u8>, (), Infallible> for MyHandler {
///     async fn handle(&self, _: &DeliveryContext, event: Vec<u8>) -> Result<(), Infallible> {
///         println!("Received {} bytes", event.len());
///         Ok(())
///     }
/// }
///
/// impl Handler<Json<String>, (), Infallible> for MyHandler {
///     async fn handle(&self, _: &DeliveryContext, event: Json<String>) -> Result<(), Infallible> {
///         println!("Received {}", event.into_inner());
///         Ok(())
///     }
/// }
///
/// async fn register(app: &mut Streameroo) -> StreamerooResult<()> {
///     let handler = MyHandler;
///     app.consume::<Vec<u8>, _, _, _>(handler.clone(), "bytes", 1).await?;
///     app.consume::<Json<String>, _, _, _>(handler, "strings", 1).await?;
///     Ok(())
/// }
/// # }
/// ```
pub trait Handler<E, R, Err>: Clone + Send + 'static
where
    E: AMQPDecode + Send,
    R: AMQPResult,
    Err: Display + Send,
{
    fn handle(
        &self,
        ctx: &DeliveryContext,
        event: E,
    ) -> impl Future<Output = Result<R, Err>> + Send;
}

pub trait AMQPDecode: Sized {
    fn decode(payload: Vec<u8>, context: &DeliveryContext) -> Result<Self, Error>;
}

impl<E> AMQPDecode for E
where
    E: Decode,
{
    fn decode(payload: Vec<u8>, _: &DeliveryContext) -> Result<Self, Error> {
        E::decode(payload).map_err(Error::event)
    }
}
