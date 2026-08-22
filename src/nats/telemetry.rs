use async_nats::HeaderMap;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::SpanKind;
use tracing_opentelemetry_instrumentation_sdk::otel_trace_span;

/// Creates a producer span for `subject`, parents it to the current
/// OpenTelemetry context, and injects the resulting trace context into
/// `headers` so it propagates to consumers.
pub fn inject_producer_context(subject: &str, headers: &mut HeaderMap) {
    use opentelemetry::Context as OtelContext;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    use tracing_opentelemetry_instrumentation_sdk::find_context_from_tracing;

    let span = make_span_for_subject(subject, SpanKind::Producer);
    if let Err(e) = span.set_parent(OtelContext::current()) {
        tracing::warn!("Failed to set parent context for span: {e}");
    }
    inject_context(&find_context_from_tracing(&span), headers);
}

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
            TraceState::from_key_value([("foo", "bar")]).unwrap(),
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
            Some("foo=bar")
        );

        let extracted = extract_context(&headers);
        let extracted_sc = extracted.span().span_context().clone();
        insta::assert_debug_snapshot!(extracted_sc, @r"");

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
}
