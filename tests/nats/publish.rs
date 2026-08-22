use crate::support::nats::{NatsTest, TestEvent};
use assert_matches::assert_matches;
use async_nats::HeaderMap;
use futures::StreamExt;
use std::time::Duration;
use streameroo::event::{Decode, Encode, Json};
use streameroo::nats::jetstream::Producer;
use streameroo::nats::{ClientExt, Error as NatsError};
use test_context::test_context;

struct FailEncode;

#[derive(Debug, thiserror::Error)]
#[error("intentional encode failure")]
struct EncodeFailure;

impl Encode for FailEncode {
    type Error = EncodeFailure;

    fn encode(&self) -> Result<Vec<u8>, Self::Error> {
        Err(EncodeFailure)
    }
}

#[test_context(NatsTest)]
#[tokio::test]
async fn client_xpublish_roundtrips(ctx: &mut NatsTest) {
    let names = ctx.names();
    let mut sub = ctx.client.subscribe(names.subject.clone()).await.unwrap();
    ctx.client.flush().await.unwrap();

    ctx.client
        .xpublish(&names.subject, Json(TestEvent::new("ping")))
        .await
        .expect("xpublish failed");

    let msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("timed out waiting for message")
        .expect("subscription closed");
    let decoded = Json::<TestEvent>::decode(msg.payload.to_vec()).unwrap();
    assert_eq!(decoded.into_inner(), TestEvent::new("ping"));
}

#[test_context(NatsTest)]
#[tokio::test]
async fn client_xpublish_with_headers_preserves_headers(ctx: &mut NatsTest) {
    let names = ctx.names();
    let mut sub = ctx.client.subscribe(names.subject.clone()).await.unwrap();
    ctx.client.flush().await.unwrap();

    let mut headers = HeaderMap::new();
    headers.insert("X-Test", "value-1");
    ctx.client
        .xpublish_with_headers(&names.subject, headers, Json(TestEvent::new("h")))
        .await
        .expect("xpublish_with_headers failed");

    let msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("timed out")
        .expect("subscription closed");
    let headers = msg.headers.expect("message must carry headers");
    assert_eq!(headers.get("X-Test").unwrap().as_str(), "value-1");
}

#[test_context(NatsTest)]
#[tokio::test]
async fn client_xpublish_surfaces_encode_errors(ctx: &mut NatsTest) {
    let names = ctx.names();
    let result = ctx.client.xpublish(&names.subject, FailEncode).await;
    assert_matches!(result, Err(NatsError::Event(_)));
}

#[test_context(NatsTest)]
#[tokio::test]
async fn jetstream_publish_persists_and_acks(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;

    let ack = ctx
        .js
        .produce(&names.subject, Json(TestEvent::new("stored")))
        .await
        .expect("jetstream publish failed");
    assert_eq!(ack.stream, names.stream);
    assert_eq!(ack.sequence, 1);

    let drained = ctx
        .drain_stream(&names.stream, 1, Duration::from_secs(5))
        .await;
    assert_eq!(drained.len(), 1);
    let decoded = Json::<TestEvent>::decode(drained[0].payload.to_vec()).unwrap();
    assert_eq!(decoded.into_inner(), TestEvent::new("stored"));
}

#[test_context(NatsTest)]
#[tokio::test]
async fn jetstream_publish_with_headers_preserves_headers(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;

    let mut headers = HeaderMap::new();
    headers.insert("X-Js", "js-value");
    ctx.js
        .produce_with_headers(&names.subject, headers, Json(TestEvent::new("hh")))
        .await
        .expect("jetstream produce_with_headers failed");

    let drained = ctx
        .drain_stream(&names.stream, 1, Duration::from_secs(5))
        .await;
    assert_eq!(drained.len(), 1);
    let headers = drained[0].headers.as_ref().expect("headers missing");
    assert_eq!(headers.get("X-Js").unwrap().as_str(), "js-value");
}

#[test_context(NatsTest)]
#[tokio::test]
async fn jetstream_publish_surfaces_encode_errors(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    let result = ctx.js.produce(&names.subject, FailEncode).await;
    assert_matches!(result, Err(NatsError::Event(_)));
}
