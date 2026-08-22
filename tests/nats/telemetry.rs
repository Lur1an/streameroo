use crate::support::nats::{TestError, TestEvent, connect, wait_for};
use crate::support::telemetry::FakeTelemetry;
use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::stream::Config as StreamConfig;
use fake_opentelemetry_collector::ExportedSpan;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use streameroo::event::Json;
use streameroo::nats::jetstream::{Consumer, ConsumerConfig, Handler, MessageContext, Producer};

// Fixed identifiers so the span snapshot (which captures the subject as a
// span name / attribute) is deterministic.
const SUBJECT: &str = "telemetry.test";
const STREAM: &str = "telemetry-test-stream";
const DURABLE: &str = "telemetry-test-consumer";

#[derive(Clone)]
struct SignalHandler {
    done: Arc<AtomicBool>,
}

impl Handler for SignalHandler {
    type Event = Json<TestEvent>;
    type Error = TestError;

    async fn handle(
        &mut self,
        _ctx: &MessageContext<'_>,
        event: Json<TestEvent>,
    ) -> Result<(), TestError> {
        // Child span so we can assert the trace continues from the propagated
        // consumer span.
        let _span = tracing::info_span!("handle_event").entered();
        assert_eq!(event.into_inner().0, "hello");
        self.done.store(true, Ordering::Relaxed);
        Ok(())
    }
}

fn is_ours(span: &ExportedSpan) -> bool {
    span.name == "root" || span.name == "handle_event" || span.name.ends_with(SUBJECT)
}

fn find_span<'a>(spans: &'a [ExportedSpan], kind: &str) -> &'a ExportedSpan {
    spans
        .iter()
        .find(|span| span.kind == kind)
        .unwrap_or_else(|| panic!("no span with kind {kind} found in {spans:#?}"))
}

/// Full publish -> consume flow over a real JetStream broker, asserting the W3C
/// trace context propagates from the producer span to the consumer span via
/// message headers.
#[tokio::test(flavor = "multi_thread")]
async fn publish_consume_trace_propagation() -> anyhow::Result<()> {
    // This standalone test target has its own process, allowing this test to
    // install the global subscriber required by spans created in Tokio tasks.
    let mut telemetry = FakeTelemetry::install().await;

    let client = connect().await;
    let js = async_nats::jetstream::new(client);

    let _ = js.delete_stream(STREAM).await;
    js.get_or_create_stream(StreamConfig {
        name: STREAM.to_string(),
        subjects: vec![SUBJECT.to_string()],
        ..Default::default()
    })
    .await?;

    let done = Arc::new(AtomicBool::new(false));
    let handler = SignalHandler { done: done.clone() };
    let config = ConsumerConfig {
        dlq: None,
        backoff: Default::default(),
        config: PullConfig {
            durable_name: Some(DURABLE.to_string()),
            filter_subject: SUBJECT.to_string(),
            max_ack_pending: 1,
            ack_wait: Duration::from_secs(2),
            max_deliver: 3,
            ..Default::default()
        },
        stream: StreamConfig {
            name: STREAM.to_string(),
            subjects: vec![SUBJECT.to_string()],
            ..Default::default()
        },
    };
    let consumer = Consumer::new(js.clone(), config, handler);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(consumer.run_sequential(async move {
        let _ = rx.await;
    }));

    {
        let root = tracing::info_span!("root").entered();
        js.produce(SUBJECT, Json(TestEvent::new("hello"))).await?;
        drop(root);
    }

    wait_for(Duration::from_secs(10), || done.load(Ordering::Relaxed)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = tx.send(());
    task.await?.expect("consumer returned an error");

    let mut mine = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        let spans = telemetry.collect_spans(4, Duration::from_secs(2)).await;
        mine = spans.into_iter().filter(is_ours).collect::<Vec<_>>();
        if mine.len() >= 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let _ = js.delete_stream(STREAM).await;

    assert_eq!(
        mine.len(),
        4,
        "expected 4 spans (root, send, process, handle_event), got {mine:#?}"
    );

    let producer = find_span(&mine, "SPAN_KIND_PRODUCER");
    let consumer = find_span(&mine, "SPAN_KIND_CONSUMER");
    assert_eq!(
        producer.trace_id, consumer.trace_id,
        "producer and consumer must share a trace id"
    );
    assert_eq!(
        consumer.parent_span_id, producer.span_id,
        "consumer span must be a child of the producer span"
    );

    mine.sort_by(|a, b| a.kind.cmp(&b.kind).then(a.name.cmp(&b.name)));
    insta::assert_yaml_snapshot!(mine, {
        "[].trace_id" => "[trace_id]",
        "[].span_id" => "[span_id]",
        "[].parent_span_id" => "[parent_span_id]",
        "[].start_time_unix_nano" => "[ts]",
        "[].end_time_unix_nano" => "[ts]",
        "[].attributes.busy_ns" => "[ns]",
        "[].attributes.idle_ns" => "[ns]",
        "[].attributes[\"thread.id\"]" => "[thread.id]",
        "[].attributes[\"thread.name\"]" => "[thread.name]",
        "[].attributes[\"code.line.number\"]" => "[code.line.number]",
    });

    Ok(())
}
