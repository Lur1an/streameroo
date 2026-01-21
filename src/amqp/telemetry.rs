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
    span.set_attribute("delivery_tag", delivery_context.delivery_tag.to_string());
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
    let name = if exchange.is_empty() {
        routing_key.to_string()
    } else {
        format!("{exchange}.{routing_key}")
    };
    otel_trace_span!(
        "AMQP Event",
        otel.name = name,
        otel.kind = ?kind,
        amqp.exchange = exchange,
        amqp.routing_key = routing_key,
        amqp.correlation_id = properties.correlation_id(),
        amqp.reply_to = properties.reply_to(),
        amqp.content_type = properties.content_type(),
    )
}
