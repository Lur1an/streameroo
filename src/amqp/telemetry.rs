use std::collections::HashMap;
use std::ops::Deref;

use super::DeliveryContext;
use amqprs::{BasicProperties, FieldTable, FieldValue, ShortStr};
use opentelemetry::Context;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::SpanKind;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_opentelemetry_instrumentation_sdk::otel_trace_span;

pub struct ValueStore<'a> {
    table: &'a FieldTable,
    extra_values: HashMap<&'a str, String>,
}

impl<'a> ValueStore<'a> {
    pub fn new(table: &'a FieldTable) -> Self {
        let mut extra_values = HashMap::new();
        for (key, value) in table.as_ref().iter() {
            match value {
                FieldValue::S(_) => {}
                value => {
                    extra_values.insert(key.as_ref().as_str(), value.to_string());
                }
            }
        }
        Self {
            table,
            extra_values,
        }
    }
}

impl Extractor for ValueStore<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        let short_key = ShortStr::try_from(key).ok()?;
        if let Some(value) = self.table.get(&short_key) {
            match value {
                FieldValue::S(long_str) => Some(long_str.as_ref().as_str()),
                _ => None,
            }
        } else if let Some(value) = self.extra_values.get(key) {
            Some(value.as_str())
        } else {
            None
        }
    }

    fn keys(&self) -> Vec<&str> {
        let map = self.table.as_ref();
        map.keys()
            .map(|k| k.as_ref().as_str())
            .chain(self.extra_values.keys().map(Deref::deref))
            .collect()
    }
}

pub struct HeaderInjector<'a>(&'a mut FieldTable);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(key) = ShortStr::try_from(key) {
            self.0.insert(key, value.into());
        }
    }
}

pub fn inject_context(context: &Context, headers: &mut FieldTable) {
    let mut injector = HeaderInjector(headers);
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(context, &mut injector);
    });
}

pub fn extract_context(headers: &FieldTable) -> Context {
    let extractor = ValueStore::new(headers);
    opentelemetry::global::get_text_map_propagator(|propagator| propagator.extract(&extractor))
}

pub fn make_span_from_delivery_context(delivery_context: &DeliveryContext) -> tracing::Span {
    let span = make_span_from_properties(
        &delivery_context.properties,
        SpanKind::Consumer,
        &delivery_context.exchange,
        &delivery_context.routing_key,
    );
    span.set_attribute(
        "messaging.rabbitmq.message.delivery_tag",
        i64::try_from(delivery_context.delivery_tag).unwrap_or(-1),
    );
    if let Some(field_table) = delivery_context.properties.headers() {
        let context = extract_context(field_table);
        if let Err(e) = span.set_parent(context) {
            tracing::warn!("Failed to set parent context for span: {e}");
        }
    }
    span
}

/// The OTel messaging operation name/type for a given [`SpanKind`].
/// Producer -> `send`, Consumer -> `process`.
fn operation_for_kind(kind: &SpanKind) -> &'static str {
    match kind {
        SpanKind::Consumer => "process",
        _ => "send",
    }
}

/// Builds a span for an AMQP event following the OpenTelemetry messaging
/// semantic conventions: the span name is `{operation} {destination}` and the
/// standard `messaging.*` attributes are attached. RabbitMQ-specific values use
/// the `messaging.rabbitmq.*` namespace; values without a standard attribute are
/// kept under descriptive custom keys.
pub fn make_span_from_properties(
    properties: &BasicProperties,
    kind: SpanKind,
    exchange: &str,
    routing_key: &str,
) -> tracing::Span {
    let operation = operation_for_kind(&kind);
    // The destination is the exchange; fall back to the routing key for the
    // default (direct-to-queue) exchange where the exchange name is empty.
    let destination = if exchange.is_empty() {
        routing_key
    } else {
        exchange
    };
    let name = format!("{operation} {destination}");
    otel_trace_span!(
        "AMQP Event",
        otel.name = name,
        otel.kind = ?kind,
        messaging.system = "rabbitmq",
        messaging.operation.name = operation,
        messaging.operation.type = operation,
        messaging.destination.name = exchange,
        messaging.rabbitmq.destination.routing_key = routing_key,
        messaging.message.conversation_id = properties.correlation_id(),
        messaging.rabbitmq.reply_to = properties.reply_to(),
        messaging.rabbitmq.content_type = properties.content_type(),
    )
}

#[cfg(test)]
mod test {
    use crate::amqp::connection::AMQPConnection;
    use crate::amqp::connection::amqp_test::start_rabbitmq;
    use crate::amqp::{ChannelExt, DeliveryContext, Handler, Streameroo};
    use crate::event::Json;
    use crate::test_util::FakeTelemetry;
    use amqprs::channel::QueueDeclareArguments;
    use fake_opentelemetry_collector::ExportedSpan;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    #[derive(Debug, Serialize, Deserialize)]
    struct TestEvent(String);

    #[derive(Clone)]
    struct SignalHandler {
        done: Arc<AtomicBool>,
    }

    impl Handler for SignalHandler {
        type Event = Json<TestEvent>;
        type Result = ();
        type Error = anyhow::Error;

        async fn handle(
            &self,
            _ctx: &DeliveryContext,
            event: Json<TestEvent>,
        ) -> anyhow::Result<()> {
            // Emit a child span inside the handler so we can assert the trace
            // continues from the consumer span (which is the propagated parent).
            let _span = tracing::info_span!("handle_event").entered();
            assert_eq!(event.into_inner().0, "hello");
            self.done.store(true, Ordering::Relaxed);
            Ok(())
        }
    }

    /// Full publish -> consume flow over a real RabbitMQ broker, asserting that
    /// the W3C trace context is propagated from the producer span (created in
    /// `publish_with_options`) to the consumer span (created in
    /// `make_span_from_delivery_context`) via the message headers.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_publish_consume_trace_propagation() -> anyhow::Result<()> {
        // --- Set up the fake OTLP collector + deterministic-id tracer provider +
        // global tracing subscriber. A *global* subscriber is required because the
        // consumer runs the handler (and creates the consumer span) inside spawned
        // tokio tasks, which do not inherit a thread-local subscriber.
        let mut telemetry = FakeTelemetry::install().await;

        // --- Start RabbitMQ and connect.
        let (container, args) = start_rabbitmq().await;
        let connection = AMQPConnection::connect(args).await?;
        // Fixed queue name so the snapshot (which captures the routing key as a
        // span name / attribute) is deterministic.
        let queue = "telemetry-test-queue".to_string();
        let channel = connection.open_channel().await?;
        channel
            .queue_declare(QueueDeclareArguments::new(&queue))
            .await?;

        // --- Run a consumer.
        let done = Arc::new(AtomicBool::new(false));
        let handler = SignalHandler { done: done.clone() };
        let mut app = Streameroo::new(connection.clone(), "test-consumer");
        app.consume(handler, &queue, 1).await?;

        // --- Publish inside a root span so the producer span has a parent and a
        // stable trace id we can follow through the whole flow.
        {
            let root = tracing::info_span!("root").entered();
            connection
                .publish("", &queue, Json(TestEvent("hello".into())))
                .await?;
            drop(root);
        }

        // --- Wait for the handler to run.
        let t = Instant::now();
        while !done.load(Ordering::Relaxed) {
            if t.elapsed().as_secs() > 10 {
                panic!("Test timed out waiting for handler");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // Give the consumer span time to close after the handler returns.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // --- Flush and collect the exported spans. We expect 4 spans:
        // root -> send (producer) -> process (consumer) -> handle_event.
        let mut spans = telemetry.collect_spans(4, Duration::from_secs(2)).await;

        container.rm().await?;

        // --- Sanity: assert the propagation invariants directly before snapshotting.
        let producer = find_span(&spans, "SPAN_KIND_PRODUCER");
        let consumer = find_span(&spans, "SPAN_KIND_CONSUMER");
        assert_eq!(
            producer.trace_id, consumer.trace_id,
            "producer and consumer must share the same trace id"
        );
        assert_eq!(
            consumer.parent_span_id, producer.span_id,
            "consumer span must be a child of the producer span"
        );

        // --- Snapshot the spans. Sort deterministically and redact volatile
        // fields (ids, timestamps, instance-specific attributes).
        spans.sort_by(|a, b| a.kind.cmp(&b.kind).then(a.name.cmp(&b.name)));
        // Trace/span ids are deterministic (see DeterministicIdGenerator), so they
        // are asserted literally in the snapshot rather than redacted.
        insta::assert_yaml_snapshot!(spans, {
            "[].start_time_unix_nano" => "[ts]",
            "[].end_time_unix_nano" => "[ts]",
            "[].attributes.busy_ns" => "[ns]",
            "[].attributes.idle_ns" => "[ns]",
            "[].attributes[\"thread.id\"]" => "[thread.id]",
            "[].attributes[\"thread.name\"]" => "[thread.name]",
            // Redact only the line number; it shifts whenever this file is edited.
            // file.path and module.name are stable and kept in the snapshot.
            "[].attributes[\"code.line.number\"]" => "[code.line.number]",
        });

        Ok(())
    }

    fn find_span<'a>(spans: &'a [ExportedSpan], kind: &str) -> &'a ExportedSpan {
        spans
            .iter()
            .find(|s| s.kind == kind)
            .unwrap_or_else(|| panic!("no span with kind {kind} found in {spans:#?}"))
    }
}
