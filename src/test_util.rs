//! Shared test utilities for telemetry / trace-propagation tests.
//!
//! This module is only compiled for tests when the `telemetry` feature is
//! enabled. It is `pub` so transport-specific test modules (AMQP, NATS, ...)
//! can reuse the same fake OpenTelemetry collector setup.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fake_opentelemetry_collector::{ExportedSpan, FakeCollectorServer};
use opentelemetry::trace::{SpanId, TraceId, TracerProvider};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{IdGenerator, SdkTracerProvider};
use tracing_subscriber::layer::SubscriberExt;

/// An [`IdGenerator`] that produces deterministic, reproducible ids so exported
/// spans can be asserted literally in snapshots (no id redaction needed).
///
/// - All spans share a single fixed [`TraceId`] (`00..01`).
/// - [`SpanId`]s are handed out from a monotonic counter in creation order
///   (`00..01`, `00..02`, ...), which makes parent/child links stable across
///   runs as long as span creation order is stable.
#[derive(Debug)]
pub struct DeterministicIdGenerator {
    next_span_id: AtomicU64,
}

impl Default for DeterministicIdGenerator {
    fn default() -> Self {
        Self {
            next_span_id: AtomicU64::new(1),
        }
    }
}

impl IdGenerator for DeterministicIdGenerator {
    fn new_trace_id(&self) -> TraceId {
        TraceId::from_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
    }

    fn new_span_id(&self) -> SpanId {
        let id = self.next_span_id.fetch_add(1, Ordering::SeqCst);
        SpanId::from_bytes(id.to_be_bytes())
    }
}

/// A fully wired fake telemetry environment for tests:
/// an in-memory OTLP collector, a deterministic-id tracer provider exporting to
/// it, the W3C trace-context propagator, and a global tracing subscriber.
///
/// Note: this installs a **global** tracing subscriber (required so spans
/// created inside spawned tokio tasks are captured). It can only be installed
/// once per process, so a test using it must be the only one setting a global
/// subscriber in that test binary run.
pub struct FakeTelemetry {
    collector: FakeCollectorServer,
    provider: SdkTracerProvider,
}

impl FakeTelemetry {
    /// Starts the fake collector, builds a deterministic tracer provider, sets
    /// the propagator and the global tracing subscriber.
    pub async fn install() -> Self {
        let collector = FakeCollectorServer::start()
            .await
            .expect("failed to start fake collector");

        // A real endpoint in the environment would otherwise hijack the exporter.
        unsafe {
            std::env::remove_var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
        }

        let provider = SdkTracerProvider::builder()
            .with_id_generator(DeterministicIdGenerator::default())
            .with_batch_exporter(
                SpanExporter::builder()
                    .with_tonic()
                    .with_endpoint(collector.endpoint())
                    .build()
                    .expect("failed to build span exporter"),
            )
            .build();

        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));
        tracing::subscriber::set_global_default(subscriber)
            .expect("failed to set global subscriber");

        Self {
            collector,
            provider,
        }
    }

    /// Flushes the tracer provider and collects at least `at_least` exported
    /// spans, waiting up to `timeout`.
    pub async fn collect_spans(&mut self, at_least: usize, timeout: Duration) -> Vec<ExportedSpan> {
        let _ = self.provider.force_flush();
        self.collector.exported_spans(at_least, timeout).await
    }
}
