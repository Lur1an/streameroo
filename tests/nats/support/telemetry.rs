use fake_opentelemetry_collector::{ExportedSpan, FakeCollectorServer};
use opentelemetry::trace::{SpanId, TraceId, TracerProvider};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{IdGenerator, SdkTracerProvider};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

#[derive(Debug)]
struct DeterministicIdGenerator {
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
        SpanId::from_bytes(
            self.next_span_id
                .fetch_add(1, Ordering::SeqCst)
                .to_be_bytes(),
        )
    }
}

pub struct FakeTelemetry {
    collector: FakeCollectorServer,
    provider: SdkTracerProvider,
}

impl FakeTelemetry {
    pub async fn install() -> Self {
        let collector = FakeCollectorServer::start()
            .await
            .expect("failed to start fake collector");
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

    pub async fn collect_spans(&mut self, at_least: usize, timeout: Duration) -> Vec<ExportedSpan> {
        let _ = self.provider.force_flush();
        self.collector.exported_spans(at_least, timeout).await
    }
}
