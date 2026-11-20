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
/// impl Handler<Vec<u8>> for MyHandler {
///     type Output = ();
///     type Error = Infallible;
///
///     async fn handle(&self, _: &DeliveryContext, event: Vec<u8>) -> Result<(), Infallible> {
///         println!("Received {} bytes", event.len());
///         Ok(())
///     }
/// }
///
/// impl Handler<Json<String>> for MyHandler {
///     type Output = ();
///     type Error = Infallible;
///
///     async fn handle(&self, _: &DeliveryContext, event: Json<String>) -> Result<(), Infallible> {
///         println!("Received {}", event.into_inner());
///         Ok(())
///     }
/// }
///
/// async fn register(app: &mut Streameroo) -> StreamerooResult<()> {
///     let handler = MyHandler;
///     app.consume::<Vec<u8>, _>(handler.clone(), "bytes", 1).await?;
///     app.consume::<Json<String>, _>(handler, "strings", 1).await?;
///     Ok(())
/// }
/// # }
/// ```
pub trait Handler<E>: Clone + Send + 'static
where
    E: AMQPDecode + Send,
{
    /// Result whose [`AMQPResult::handle_result`] runs after the handler succeeds.
    type Output: AMQPResult + 'static;
    /// Error whose [`HandlerError::requeue`] decides how the delivery is nacked.
    type Error: HandlerError + 'static;

    fn handle(
        &self,
        ctx: &DeliveryContext,
        event: E,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send;
}

/// Error returned by a [`Handler`]. Decides whether the failed delivery is nacked with or
/// without requeue.
pub trait HandlerError: Display + Send {
    fn requeue(&self) -> bool {
        true
    }
}

impl HandlerError for std::convert::Infallible {}

#[cfg(feature = "anyhow")]
impl HandlerError for anyhow::Error {}

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
