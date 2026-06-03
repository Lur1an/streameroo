use async_nats::HeaderMap;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::SpanKind;
use tracing_opentelemetry_instrumentation_sdk::otel_trace_span;

pub(crate) struct HeaderInjector<'a>(pub(crate) &'a mut HeaderMap);

impl<'a> Injector for HeaderInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key, value);
    }
}

pub(super) struct HeaderExtractor<'a>(pub(crate) &'a HeaderMap);

impl<'a> Extractor for HeaderExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|v| v.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|(k, _)| k.as_ref()).collect()
    }
}

pub(super) fn inject_context(context: &opentelemetry::Context, headers: &mut HeaderMap) {
    let mut injector = HeaderInjector(headers);
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(context, &mut injector);
    });
}

pub(super) fn extract_context(headers: &HeaderMap) -> opentelemetry::Context {
    let extractor = HeaderExtractor(headers);
    opentelemetry::global::get_text_map_propagator(|propagator| propagator.extract(&extractor))
}

/// The OTel messaging operation name/type for a given [`SpanKind`].
/// Producer -> `send`, Consumer -> `process`.
fn operation_for_kind(kind: &SpanKind) -> &'static str {
    match kind {
        SpanKind::Consumer => "process",
        _ => "send",
    }
}

/// Builds a span for a NATS subject following the OpenTelemetry messaging
/// semantic conventions: the span name is `{operation} {destination}` and the
/// standard `messaging.*` attributes are attached.
pub(crate) fn make_span_for_subject(subject: &str, kind: SpanKind) -> tracing::Span {
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
