use std::collections::HashMap;
use std::future::Future;
use std::ops::Deref;

use super::DeliveryContext;
use amqprs::{BasicProperties, FieldTable, FieldValue, ShortStr};
use opentelemetry::Context;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::{SpanKind, Status};
use tracing::Instrument;
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
    make_delivery_span(delivery_context, None)
}

pub(crate) fn make_delivery_span(
    delivery_context: &DeliveryContext,
    queue: Option<&str>,
) -> tracing::Span {
    let span = messaging_span(
        &delivery_context.properties,
        SpanKind::Consumer,
        &delivery_context.exchange,
        &delivery_context.routing_key,
        queue,
    );
    span.set_attribute(
        "messaging.rabbitmq.message.delivery_tag",
        delivery_context.delivery_tag as i64,
    );
    if let Some(field_table) = delivery_context.properties.headers() {
        let context = extract_context(field_table);
        if let Err(e) = span.set_parent(context) {
            tracing::warn!("Failed to set parent context for span: {e}");
        }
    }
    span
}

pub fn make_span_from_properties(
    properties: &BasicProperties,
    kind: SpanKind,
    exchange: &str,
    routing_key: &str,
) -> tracing::Span {
    messaging_span(properties, kind, exchange, routing_key, None)
}

fn destination_name(exchange: &str, routing_key: &str, queue: Option<&str>) -> String {
    let mut parts = Vec::new();
    if !exchange.is_empty() {
        parts.push(exchange);
    }
    if !routing_key.is_empty() {
        parts.push(routing_key);
    }
    if let Some(queue) = queue.filter(|queue| !queue.is_empty() && *queue != routing_key) {
        parts.push(queue);
    }
    if parts.is_empty() {
        "amq.default".to_owned()
    } else {
        parts.join(":")
    }
}

fn messaging_span(
    properties: &BasicProperties,
    kind: SpanKind,
    exchange: &str,
    routing_key: &str,
    queue: Option<&str>,
) -> tracing::Span {
    let (operation_name, operation_type) = match kind {
        SpanKind::Consumer => ("process", "process"),
        _ => ("publish", "send"),
    };
    let destination = destination_name(exchange, routing_key, queue);
    let name = format!("{operation_name} {destination}");
    let span = otel_trace_span!(
        "AMQP Event",
        otel.name = name,
        otel.kind = ?kind,
        messaging.system = "rabbitmq",
        messaging.destination.name = destination,
        messaging.operation.name = operation_name,
        messaging.operation.type = operation_type,
    );
    if !routing_key.is_empty() {
        span.set_attribute(
            "messaging.rabbitmq.destination.routing_key",
            routing_key.to_owned(),
        );
    }
    if let Some(id) = properties.correlation_id() {
        span.set_attribute("messaging.message.conversation_id", id.to_string());
    }
    if let Some(id) = properties.message_id() {
        span.set_attribute("messaging.message.id", id.to_string());
    }
    span
}

pub(crate) fn publish_span(
    properties: &mut BasicProperties,
    args: &amqprs::channel::BasicPublishArguments,
) -> tracing::Span {
    use tracing_opentelemetry_instrumentation_sdk::find_context_from_tracing;

    let span = make_span_from_properties(
        properties,
        SpanKind::Producer,
        &args.exchange,
        &args.routing_key,
    );
    let mut headers = properties.headers().cloned().unwrap_or_default();
    inject_context(&find_context_from_tracing(&span), &mut headers);
    properties.with_headers(headers);
    span
}

pub(crate) fn record_error(span: &tracing::Span, error_type: &'static str) {
    span.set_attribute("error.type", error_type);
    span.set_status(Status::error(error_type));
}

pub(crate) async fn trace_publish<F, E>(publish: F, span: tracing::Span) -> Result<(), E>
where
    F: Future<Output = Result<(), E>>,
{
    async move {
        let result = publish.await;
        if result.is_err() {
            record_error(&tracing::Span::current(), std::any::type_name::<E>());
        }
        result
    }
    .instrument(span)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tracing_subscriber::prelude::*;

    #[test]
    fn messaging_spans_use_rabbitmq_names_and_attributes() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));
        let _subscriber = tracing::subscriber::set_default(subscriber);

        for (exchange, routing_key, queue, destination) in [
            ("orders", "created", None, "orders:created"),
            ("orders", "", None, "orders"),
            ("", "created", None, "created"),
            ("", "", None, "amq.default"),
            ("orders", "created", Some("worker"), "orders:created:worker"),
            ("orders", "created", Some("created"), "orders:created"),
            ("", "created", Some("created"), "created"),
            ("", "", Some("worker"), "worker"),
        ] {
            exporter.reset();
            let mut properties = BasicProperties::default();
            properties
                .with_correlation_id("conversation")
                .with_message_id("message");
            let kind = if queue.is_some() {
                SpanKind::Consumer
            } else {
                SpanKind::Producer
            };
            let (operation, operation_type) = if queue.is_some() {
                ("process", "process")
            } else {
                ("publish", "send")
            };
            drop(messaging_span(
                &properties,
                kind.clone(),
                exchange,
                routing_key,
                queue,
            ));
            let spans = exporter.get_finished_spans().unwrap();
            let span = &spans[0];
            assert_eq!(span.name, format!("{operation} {destination}"));
            assert_eq!(span.span_kind, kind);
            for (key, value) in [
                ("messaging.system", "rabbitmq"),
                ("messaging.destination.name", destination),
                ("messaging.operation.name", operation),
                ("messaging.operation.type", operation_type),
                ("messaging.message.conversation_id", "conversation"),
                ("messaging.message.id", "message"),
            ] {
                let attribute = span
                    .attributes
                    .iter()
                    .find(|attr| attr.key.as_str() == key)
                    .unwrap();
                assert_eq!(attribute.value.as_str(), value, "{key}");
            }
            let routing_attribute = span
                .attributes
                .iter()
                .find(|attr| attr.key.as_str() == "messaging.rabbitmq.destination.routing_key");
            if routing_key.is_empty() {
                assert!(routing_attribute.is_none());
            } else {
                assert_eq!(routing_attribute.unwrap().value.as_str(), routing_key);
            }
            assert!(
                !span
                    .attributes
                    .iter()
                    .any(|attr| attr.key.as_str().starts_with("amqp."))
            );
        }
    }
}
