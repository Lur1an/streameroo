use crate::support::nats::{Mode, NatsTest, TestError, TestEvent, TestHandler, wait_for};
use assert_matches::assert_matches;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use streameroo::event::{Encode, Json};
use streameroo::nats::Error;
use streameroo::nats::jetstream::*;
use test_context::test_context;
use time::OffsetDateTime;
use tokio::sync::oneshot;

async fn publish(ctx: &NatsTest, subject: &str, message: &str) {
    ctx.js
        .produce(subject, Json(TestEvent::new(message)))
        .await
        .expect("publish failed");
}

async fn stop_sequential<H: Handler + Send + 'static>(
    consumer: Consumer<H>,
    done: impl Fn() -> bool,
) {
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(consumer.run_sequential(async move {
        let _ = rx.await;
    }));
    wait_for(Duration::from_secs(15), done).await;
    let _ = tx.send(());
    task.await.unwrap().expect("consumer returned an error");
}

type SeenContexts = Arc<Mutex<Vec<(Option<String>, OffsetDateTime)>>>;

#[derive(Clone, Default)]
struct CapturingHandler(SeenContexts);

impl Handler for CapturingHandler {
    type Event = Json<TestEvent>;
    type Error = TestError;

    async fn handle(
        &mut self,
        ctx: &MessageContext<'_>,
        _: Json<TestEvent>,
    ) -> Result<(), TestError> {
        self.0
            .lock()
            .unwrap()
            .push((ctx.message_id().map(String::from), ctx.published));
        Ok(())
    }
}

#[derive(Clone, Default)]
struct OrderingRetryHandler(Arc<Mutex<Vec<(String, i64, std::time::Instant)>>>);

impl Handler for OrderingRetryHandler {
    type Event = Json<TestEvent>;
    type Error = TestError;

    async fn handle(
        &mut self,
        ctx: &MessageContext<'_>,
        event: Json<TestEvent>,
    ) -> Result<(), TestError> {
        let value = event.into_inner().0;
        self.0
            .lock()
            .unwrap()
            .push((value.clone(), ctx.delivered, std::time::Instant::now()));
        if value == "a" && ctx.delivered == 1 {
            Err(TestError {
                message: "retry once".into(),
                action: ErrorAction::Retry,
            })
        } else {
            Ok(())
        }
    }
}

#[test_context(NatsTest)]
#[tokio::test]
async fn sequential_processes_in_order(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    for message in ["a", "b", "c"] {
        publish(ctx, &names.subject, message).await;
    }
    let handler = TestHandler::new(Mode::Succeed);
    stop_sequential(
        Consumer::new(
            ctx.js.clone(),
            ctx.consumer_config(&names, false),
            handler.clone(),
        ),
        || handler.handled().len() == 3,
    )
    .await;
    assert_eq!(handler.handled(), ["a", "b", "c"]);
}

#[test_context(NatsTest)]
#[tokio::test]
async fn sequential_rejects_bad_max_ack_pending(ctx: &mut NatsTest) {
    let names = ctx.names();
    let mut config = ctx.consumer_config(&names, false);
    config.config.max_ack_pending = 5;
    assert_matches!(
        Consumer::new(ctx.js.clone(), config, TestHandler::new(Mode::Succeed))
            .run_sequential(std::future::pending())
            .await,
        Err(Error::Config(_))
    );
}

#[test_context(NatsTest)]
#[tokio::test]
async fn concurrent_processes_all(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    let expected = ["m1", "m2", "m3", "m4", "m5"];
    for message in expected {
        publish(ctx, &names.subject, message).await;
    }
    let mut config = ctx.consumer_config(&names, false);
    config.config.max_ack_pending = 10;
    let handler = TestHandler::new(Mode::Succeed);
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(
        Consumer::new(ctx.js.clone(), config, handler.clone()).run_concurrent(async move {
            let _ = rx.await;
        }),
    );
    wait_for(Duration::from_secs(5), || handler.handled().len() == 5).await;
    let _ = tx.send(());
    task.await.unwrap().unwrap();
    let mut actual = handler.handled();
    actual.sort();
    assert_eq!(actual, expected.map(String::from));
}

#[test_context(NatsTest)]
#[tokio::test]
async fn retriable_error_redelivers(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    publish(ctx, &names.subject, "x").await;
    let handler = TestHandler::new(Mode::RetryUntil(2));
    stop_sequential(
        Consumer::new(
            ctx.js.clone(),
            ctx.consumer_config(&names, false),
            handler.clone(),
        ),
        || handler.handled() == ["x"],
    )
    .await;
    assert!(handler.call_count() >= 2);
}

#[test_context(NatsTest)]
#[tokio::test]
async fn backoff_delays_redelivery(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    publish(ctx, &names.subject, "x").await;
    let mut config = ctx.consumer_config(&names, false);
    config.backoff = BackoffPolicy::Linear {
        base: Duration::from_secs(2),
        max_backoff: Duration::from_secs(10),
    };
    config.config.ack_wait = Duration::from_secs(10);
    let handler = TestHandler::new(Mode::RetryUntil(2));
    let started = std::time::Instant::now();
    stop_sequential(
        Consumer::new(ctx.js.clone(), config, handler.clone()),
        || handler.handled() == ["x"],
    )
    .await;
    assert!(started.elapsed() >= Duration::from_millis(1800));
}

#[test_context(NatsTest)]
#[tokio::test]
async fn always_retriable_caps_at_max_deliver(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    publish(ctx, &names.subject, "loop").await;
    let handler = TestHandler::new(Mode::AlwaysRetriable);
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(
        Consumer::new(
            ctx.js.clone(),
            ctx.consumer_config(&names, false),
            handler.clone(),
        )
        .run_sequential(async move {
            let _ = rx.await;
        }),
    );
    wait_for(Duration::from_secs(5), || handler.call_count() >= 3).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let _ = tx.send(());
    task.await.unwrap().unwrap();
    assert_eq!(handler.call_count(), 3);
    assert!(handler.handled().is_empty());
}

#[test_context(NatsTest)]
#[tokio::test]
async fn non_retriable_error_dead_letters(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    publish(ctx, &names.subject, "bad").await;
    let handler = TestHandler::new(Mode::NonRetriable);
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(
        Consumer::new(
            ctx.js.clone(),
            ctx.consumer_config(&names, true),
            handler.clone(),
        )
        .run_sequential(async move {
            let _ = rx.await;
        }),
    );
    let dlq = ctx
        .drain_stream(&names.dlq_stream, 1, Duration::from_secs(5))
        .await;
    let _ = tx.send(());
    task.await.unwrap().unwrap();
    assert_eq!(dlq.len(), 1);
    let headers = dlq[0].headers.as_ref().unwrap();
    assert_eq!(headers.get(DLQ_RETRIABLE).unwrap().as_str(), "false");
    assert_eq!(
        headers.get(DLQ_SOURCE_SUBJECT).unwrap().as_str(),
        names.subject
    );
    assert_eq!(
        dlq[0].payload.to_vec(),
        Json(TestEvent::new("bad")).encode().unwrap()
    );
    assert_eq!(
        headers.get(DLQ_ERROR).unwrap().as_str(),
        "handler failed for \"bad\""
    );
    assert_eq!(headers.get(DLQ_DELIVERED).unwrap().as_str(), "1");
    assert_eq!(headers.get(DLQ_STREAM_SEQUENCE).unwrap().as_str(), "1");
    chrono::DateTime::parse_from_rfc3339(headers.get(DLQ_DEAD_LETTERED_AT).unwrap().as_str())
        .expect("DLQ timestamp must be RFC3339");
    assert_eq!(
        headers.get("Nats-Msg-Id").unwrap().as_str(),
        format!("{}-1", names.stream)
    );
    assert_eq!(handler.call_count(), 1);
}

#[test_context(NatsTest)]
#[tokio::test]
async fn decode_failure_dead_letters(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    let garbage = b"this is not json".to_vec();
    ctx.js
        .publish(names.subject.clone(), garbage.clone().into())
        .await
        .unwrap()
        .await
        .unwrap();
    let handler = TestHandler::new(Mode::Succeed);
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(
        Consumer::new(
            ctx.js.clone(),
            ctx.consumer_config(&names, true),
            handler.clone(),
        )
        .run_sequential(async move {
            let _ = rx.await;
        }),
    );
    let dlq = ctx
        .drain_stream(&names.dlq_stream, 1, Duration::from_secs(5))
        .await;
    let _ = tx.send(());
    task.await.unwrap().unwrap();
    assert_eq!(dlq[0].payload.to_vec(), garbage);
    assert_eq!(handler.call_count(), 0);
}

#[test_context(NatsTest)]
#[tokio::test]
async fn non_retriable_without_dlq_terminates(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    publish(ctx, &names.subject, "drop-me").await;
    let handler = TestHandler::new(Mode::NonRetriable);
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(
        Consumer::new(
            ctx.js.clone(),
            ctx.consumer_config(&names, false),
            handler.clone(),
        )
        .run_sequential(async move {
            let _ = rx.await;
        }),
    );
    wait_for(Duration::from_secs(5), || handler.call_count() == 1).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let _ = tx.send(());
    task.await.unwrap().unwrap();
    assert_eq!(handler.call_count(), 1);
}

#[test_context(NatsTest)]
#[tokio::test]
async fn shutdown_stops_consumer(ctx: &mut NatsTest) {
    let names = ctx.names();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        Consumer::new(
            ctx.js.clone(),
            ctx.consumer_config(&names, false),
            TestHandler::new(Mode::Succeed),
        )
        .run_concurrent(std::future::ready(())),
    )
    .await
    .expect("consumer did not honor shutdown");
    assert!(result.is_ok());
}

#[test_context(NatsTest)]
#[tokio::test]
async fn dlq_duplicate_window_is_reconciled(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    ctx.js
        .create_stream(async_nats::jetstream::stream::Config {
            name: names.dlq_stream.clone(),
            subjects: vec![names.dlq_subject.clone()],
            duplicate_window: Duration::from_secs(1),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut config = ctx.consumer_config(&names, true);
    config.dlq.as_mut().unwrap().duplicate_window = Some(Duration::from_secs(5));
    Consumer::new(ctx.js.clone(), config, TestHandler::new(Mode::Succeed))
        .run_concurrent(std::future::ready(()))
        .await
        .unwrap();
    assert_eq!(
        ctx.js
            .get_stream(&names.dlq_stream)
            .await
            .unwrap()
            .info()
            .await
            .unwrap()
            .config
            .duplicate_window,
        Duration::from_secs(5)
    );
}

#[test_context(NatsTest)]
#[tokio::test]
async fn consumer_deleted_returns_error(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    let mut config = ctx.consumer_config(&names, false);
    config.config.max_ack_pending = 10;
    let task = tokio::spawn(
        Consumer::new(ctx.js.clone(), config, TestHandler::new(Mode::Succeed))
            .run_concurrent(std::future::pending::<()>()),
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    ctx.js
        .get_stream(&names.stream)
        .await
        .unwrap()
        .delete_consumer(&names.durable)
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
}

#[test_context(NatsTest)]
#[tokio::test]
async fn sequential_preserves_order_with_delayed_nak(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    for message in ["a", "b"] {
        publish(ctx, &names.subject, message).await;
    }
    let mut config = ctx.consumer_config(&names, false);
    config.backoff = BackoffPolicy::Linear {
        base: Duration::from_secs(1),
        max_backoff: Duration::from_secs(1),
    };
    let handler = OrderingRetryHandler::default();
    stop_sequential(
        Consumer::new(ctx.js.clone(), config, handler.clone()),
        || handler.0.lock().unwrap().len() == 3,
    )
    .await;
    let attempts = handler.0.lock().unwrap();
    assert_eq!((&attempts[0].0, attempts[0].1), (&"a".to_string(), 1));
    assert_eq!((&attempts[1].0, attempts[1].1), (&"a".to_string(), 2));
    assert_eq!((&attempts[2].0, attempts[2].1), (&"b".to_string(), 1));
    assert!(attempts[1].2.duration_since(attempts[0].2) >= Duration::from_millis(900));
}

#[test_context(NatsTest)]
#[tokio::test]
async fn two_sequential_consumers_preserve_global_order(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    let expected: Vec<String> = (0..12).map(|i| i.to_string()).collect();
    for message in &expected {
        publish(ctx, &names.subject, message).await;
    }
    let handler = TestHandler::new(Mode::Succeed);
    let (tx_a, rx_a) = oneshot::channel();
    let (tx_b, rx_b) = oneshot::channel();
    let a = tokio::spawn(
        Consumer::new(
            ctx.js.clone(),
            ctx.consumer_config(&names, false),
            handler.clone(),
        )
        .run_sequential(async move {
            let _ = rx_a.await;
        }),
    );
    let b = tokio::spawn(
        Consumer::new(
            ctx.js.clone(),
            ctx.consumer_config(&names, false),
            handler.clone(),
        )
        .run_sequential(async move {
            let _ = rx_b.await;
        }),
    );
    wait_for(Duration::from_secs(15), || handler.handled().len() == 12).await;
    let _ = tx_a.send(());
    let _ = tx_b.send(());
    a.await.unwrap().unwrap();
    b.await.unwrap().unwrap();
    assert_eq!(handler.handled(), expected);
}

async fn assert_heartbeat(ctx: &mut NatsTest, concurrent: bool) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    publish(ctx, &names.subject, "slow").await;
    let mut config = ctx.consumer_config(&names, false);
    config.config.ack_wait = Duration::from_secs(4);
    if concurrent {
        config.config.max_ack_pending = 10;
    }
    let handler = TestHandler::new(Mode::SlowSucceed(Duration::from_secs(10)));
    let (tx, rx) = oneshot::channel();
    let task = if concurrent {
        tokio::spawn(
            Consumer::new(ctx.js.clone(), config, handler.clone()).run_concurrent(async move {
                let _ = rx.await;
            }),
        )
    } else {
        tokio::spawn(
            Consumer::new(ctx.js.clone(), config, handler.clone()).run_sequential(async move {
                let _ = rx.await;
            }),
        )
    };
    wait_for(Duration::from_secs(25), || handler.handled().len() == 1).await;
    let _ = tx.send(());
    task.await.unwrap().unwrap();
    assert_eq!(handler.call_count(), 1);
}

#[test_context(NatsTest)]
#[tokio::test]
async fn slow_handler_is_not_redelivered_concurrent(ctx: &mut NatsTest) {
    assert_heartbeat(ctx, true).await;
}
#[test_context(NatsTest)]
#[tokio::test]
async fn slow_handler_is_not_redelivered_sequential(ctx: &mut NatsTest) {
    assert_heartbeat(ctx, false).await;
}

#[test_context(NatsTest)]
#[tokio::test]
async fn heartbeat_uses_server_side_ack_wait(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    publish(ctx, &names.subject, "slow").await;
    ctx.js
        .get_stream(&names.stream)
        .await
        .unwrap()
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            durable_name: Some(names.durable.clone()),
            filter_subject: names.subject.clone(),
            ack_wait: Duration::from_secs(4),
            max_deliver: 3,
            max_ack_pending: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut config = ctx.consumer_config(&names, false);
    config.config.ack_wait = Duration::ZERO;
    config.config.max_ack_pending = 10;
    let handler = TestHandler::new(Mode::SlowSucceed(Duration::from_secs(10)));
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(
        Consumer::new(ctx.js.clone(), config, handler.clone()).run_concurrent(async move {
            let _ = rx.await;
        }),
    );
    wait_for(Duration::from_secs(25), || handler.handled().len() == 1).await;
    let _ = tx.send(());
    task.await.unwrap().unwrap();
    assert_eq!(handler.call_count(), 1);
}

#[test_context(NatsTest)]
#[tokio::test]
async fn sequential_rejects_server_side_bad_max_ack_pending(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    ctx.js
        .get_stream(&names.stream)
        .await
        .unwrap()
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            durable_name: Some(names.durable.clone()),
            filter_subject: names.subject.clone(),
            max_ack_pending: 5,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut config = ctx.consumer_config(&names, false);
    config.config.max_ack_pending = 1;
    assert_matches!(
        Consumer::new(ctx.js.clone(), config, TestHandler::new(Mode::Succeed))
            .run_sequential(std::future::pending())
            .await,
        Err(Error::Config(_))
    );
}

#[test_context(NatsTest)]
#[tokio::test]
async fn sequential_accepts_server_side_max_ack_pending(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    publish(ctx, &names.subject, "x").await;
    ctx.js
        .get_stream(&names.stream)
        .await
        .unwrap()
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            durable_name: Some(names.durable.clone()),
            filter_subject: names.subject.clone(),
            max_ack_pending: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut config = ctx.consumer_config(&names, false);
    config.config.max_ack_pending = 5;
    let handler = TestHandler::new(Mode::Succeed);
    stop_sequential(
        Consumer::new(ctx.js.clone(), config, handler.clone()),
        || handler.handled() == ["x"],
    )
    .await;
}

#[test_context(NatsTest)]
#[tokio::test]
async fn dlq_publish_failure_leaves_message_unacked(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    publish(ctx, &names.subject, "bad").await;
    let mut config = ctx.consumer_config(&names, true);
    config.dlq.as_mut().unwrap().subject = format!("unrouted.{}", names.dlq_stream);
    let handler = TestHandler::new(Mode::NonRetriable);
    stop_sequential(
        Consumer::new(ctx.js.clone(), config, handler.clone()),
        || handler.call_count() >= 2,
    )
    .await;
    assert!(handler.handled().is_empty());
    assert!(
        ctx.drain_stream(&names.dlq_stream, 1, Duration::from_secs(2))
            .await
            .is_empty()
    );
}

#[test_context(NatsTest)]
#[tokio::test]
async fn concurrent_dead_letters_on_handler_error(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    publish(ctx, &names.subject, "bad").await;
    let mut config = ctx.consumer_config(&names, true);
    config.config.max_ack_pending = 10;
    let handler = TestHandler::new(Mode::NonRetriable);
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(
        Consumer::new(ctx.js.clone(), config, handler.clone()).run_concurrent(async move {
            let _ = rx.await;
        }),
    );
    let dlq = ctx
        .drain_stream(&names.dlq_stream, 1, Duration::from_secs(5))
        .await;
    let _ = tx.send(());
    task.await.unwrap().unwrap();
    assert_eq!(dlq.len(), 1);
    assert_eq!(handler.call_count(), 1);
}

#[test_context(NatsTest)]
#[tokio::test]
async fn term_action_terminates_without_dlq(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    publish(ctx, &names.subject, "term-me").await;
    let handler = TestHandler::new(Mode::Terminate);
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(
        Consumer::new(
            ctx.js.clone(),
            ctx.consumer_config(&names, true),
            handler.clone(),
        )
        .run_sequential(async move {
            let _ = rx.await;
        }),
    );
    wait_for(Duration::from_secs(5), || handler.call_count() == 1).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let _ = tx.send(());
    task.await.unwrap().unwrap();
    assert_eq!(handler.call_count(), 1);
    assert!(handler.handled().is_empty());
    assert!(
        ctx.drain_stream(&names.dlq_stream, 1, Duration::from_secs(2))
            .await
            .is_empty()
    );
}

#[test_context(NatsTest)]
#[tokio::test]
async fn decode_failure_without_dlq_drops_silently(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    ctx.js
        .publish(names.subject.clone(), b"not json".to_vec().into())
        .await
        .unwrap()
        .await
        .unwrap();
    publish(ctx, &names.subject, "good").await;
    let handler = TestHandler::new(Mode::Succeed);
    stop_sequential(
        Consumer::new(
            ctx.js.clone(),
            ctx.consumer_config(&names, false),
            handler.clone(),
        ),
        || handler.handled() == ["good"],
    )
    .await;
    assert_eq!(handler.call_count(), 1);
}

#[test_context(NatsTest)]
#[tokio::test]
async fn context_exposes_message_id_and_published(ctx: &mut NatsTest) {
    let names = ctx.names();
    ctx.ensure_stream(&names).await;
    let before = OffsetDateTime::now_utc();
    let mut headers = async_nats::HeaderMap::new();
    headers.insert(async_nats::header::NATS_MESSAGE_ID, "idem-123");
    ctx.js
        .publish_with_headers(
            names.subject.clone(),
            headers,
            Json(TestEvent::new("hi")).encode().unwrap().into(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    let handler = CapturingHandler::default();
    stop_sequential(
        Consumer::new(
            ctx.js.clone(),
            ctx.consumer_config(&names, false),
            handler.clone(),
        ),
        || handler.0.lock().unwrap().len() == 1,
    )
    .await;
    let seen = handler.0.lock().unwrap();
    assert_eq!(seen[0].0.as_deref(), Some("idem-123"));
    assert!(seen[0].1 >= before - time::Duration::seconds(5));
    assert!(seen[0].1 <= OffsetDateTime::now_utc() + time::Duration::seconds(5));
}
