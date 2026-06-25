use async_nats::HeaderMap;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::SpanKind;
use tracing_opentelemetry_instrumentation_sdk::otel_trace_span;

pub struct HeaderInjector<'a>(pub &'a mut HeaderMap);

impl<'a> Injector for HeaderInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key, value);
    }
}

pub struct HeaderExtractor<'a>(pub &'a HeaderMap);

impl<'a> Extractor for HeaderExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|v| v.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|(k, _)| k.as_ref()).collect()
    }
}

pub fn inject_context(context: &opentelemetry::Context, headers: &mut HeaderMap) {
    let mut injector = HeaderInjector(headers);
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(context, &mut injector);
    });
}

pub fn extract_context(headers: &HeaderMap) -> opentelemetry::Context {
    let extractor = HeaderExtractor(headers);
    opentelemetry::global::get_text_map_propagator(|propagator| propagator.extract(&extractor))
}

/// The OTel messaging operation name/type for a given [`SpanKind`].
/// Producer -> `send`, Consumer -> `process`.
pub fn operation_for_kind(kind: &SpanKind) -> &'static str {
    match kind {
        SpanKind::Consumer => "process",
        _ => "send",
    }
}

/// Builds a span for a NATS subject following the OpenTelemetry messaging
/// semantic conventions: the span name is `{operation} {destination}` and the
/// standard `messaging.*` attributes are attached.
pub fn make_span_for_subject(subject: &str, kind: SpanKind) -> tracing::Span {
    let operation = operation_for_kind(&kind);
    let name = format!("{operation} {subject}");
    otel_trace_span!(
        "NATS Event",
        otel.name = name,
        otel.kind = ?kind,
        messaging.system = "nats",
        messaging.operation.name = operation,
        messaging.operation.type = operation,
        messaging.destination.name = subject,
    )
}

#[cfg(test)]
mod test {
    use super::*;
    use opentelemetry::Context;
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
    };
    use opentelemetry_sdk::propagation::TraceContextPropagator;

    #[test]
    fn inject_then_extract_roundtrips_trace_context() {
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

        let span_context = SpanContext::new(
            TraceId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
            SpanId::from_bytes([0, 0, 0, 0, 0, 0, 0, 9]),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );
        let cx = Context::new().with_remote_span_context(span_context.clone());

        let mut headers = HeaderMap::new();
        inject_context(&cx, &mut headers);
        assert_eq!(
            headers.get("traceparent").map(|v| v.as_str()),
            Some("00-0102030405060708090a0b0c0d0e0f10-0000000000000009-01")
        );
        assert_eq!(
            headers.get("tracestate").map(|v| v.as_str()),
            Some("")
        );

        let extracted = extract_context(&headers);
        let extracted_sc = extracted.span().span_context().clone();
        insta::assert_debug_snapshot!(extracted_sc);

        assert_eq!(extracted_sc.trace_id(), span_context.trace_id());
        assert_eq!(extracted_sc.span_id(), span_context.span_id());
    }

    #[test]
    fn span_name_follows_messaging_conventions() {
        // A subscriber must be active for spans to carry metadata.
        let subscriber = tracing_subscriber::registry();
        tracing::subscriber::with_default(subscriber, || {
            let producer = make_span_for_subject("orders.created", SpanKind::Producer);
            let consumer = make_span_for_subject("orders.created", SpanKind::Consumer);
            // Both use the static OTel-friendly metadata name.
            assert_eq!(producer.metadata().unwrap().name(), "NATS Event");
            assert_eq!(consumer.metadata().unwrap().name(), "NATS Event");
        });
    }

    #[test]
    fn operation_name_depends_on_kind() {
        assert_eq!(operation_for_kind(&SpanKind::Producer), "send");
        assert_eq!(operation_for_kind(&SpanKind::Consumer), "process");
        assert_eq!(operation_for_kind(&SpanKind::Client), "send");
    }

    mod integration {
        use crate::event::Json;
        use crate::nats::test_util::{TestError, TestEvent, connect, wait_for};
        use crate::nats::jetstream::{self, Consumer, ConsumerConfig, Handler, MessageContext};
        use async_nats::jetstream::consumer::pull::Config as PullConfig;
        use async_nats::jetstream::stream::Config as StreamConfig;
        use fake_opentelemetry_collector::ExportedSpan;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::Duration;

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
                // Child span so we can assert the trace continues from the
                // (propagated) consumer span.
                let _span = tracing::info_span!("handle_event").entered();
                assert_eq!(event.into_inner().0, "hello");
                self.done.store(true, Ordering::Relaxed);
                Ok(())
            }
        }

        /// Keeps only the spans belonging to this test's flow, so the snapshot is
        /// not polluted by other tests sharing the global subscriber.
        fn is_ours(span: &ExportedSpan) -> bool {
            span.name == "root" || span.name == "handle_event" || span.name.ends_with(SUBJECT)
        }

        fn find_span<'a>(spans: &'a [ExportedSpan], kind: &str) -> &'a ExportedSpan {
            spans
                .iter()
                .find(|s| s.kind == kind)
                .unwrap_or_else(|| panic!("no span with kind {kind} found in {spans:#?}"))
        }

        /// Full publish -> consume flow over a real JetStream broker, asserting
        /// the W3C trace context propagates from the producer span (in
        /// `jetstream::publish`) to the consumer span (in `process`) via the
        /// message headers.
        #[tokio::test(flavor = "multi_thread")]
        async fn publish_consume_trace_propagation() -> anyhow::Result<()> {
            use crate::test_util::FakeTelemetry;

            let mut telemetry = FakeTelemetry::install().await;

            let client = connect().await;
            let js = async_nats::jetstream::new(client.clone());

            // Clean slate, then create the stream.
            let _ = js.delete_stream(STREAM).await;
            js.get_or_create_stream(StreamConfig {
                name: STREAM.to_string(),
                subjects: vec![SUBJECT.to_string()],
                ..Default::default()
            })
            .await?;

            // Run a consumer.
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

            // Publish inside a root span so the producer span has a stable parent.
            {
                let root = tracing::info_span!("root").entered();
                jetstream::publish(&js, SUBJECT, Json(TestEvent::new("hello"))).await?;
                drop(root);
            }

            wait_for(Duration::from_secs(10), || done.load(Ordering::Relaxed)).await;
            // Let the consumer span close after the handler returns.
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = tx.send(());
            task.await?.expect("consumer returned an error");

            // Collect spans, tolerating pollution from concurrent tests by
            // filtering to our own flow (root -> send -> process -> handle_event).
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

            // Propagation invariants.
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

            // Snapshot the structure. IDs/timestamps are redacted because span-id
            // ordering is not stable when other tests share the subscriber.
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
    }
}
