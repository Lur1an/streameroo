//! Shared test scaffolding for the NATS module.
//!
//! Tests run against a single long-lived JetStream broker (see `compose.yaml`)
//! rather than a container-per-test. Isolation comes from UUID-suffixed stream,
//! subject and durable names; every stream a test creates is tracked and deleted
//! on teardown so the shared broker stays clean.

use crate::event::Json;
use crate::nats::jetstream::{
    BackoffPolicy, ConsumerConfig, DlqConfig, ErrorAction, Handler, HandlerError, MessageContext,
};
use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::stream::Config as StreamConfig;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use test_context::AsyncTestContext;
use uuid::Uuid;

/// The NATS URL tests connect to, overridable via the `NATS_URL` env var so the
/// same suite runs locally (`docker compose up -d nats`) and in CI.
pub fn nats_url() -> String {
    std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string())
}

/// Connects to the broker, panicking with an actionable message if it is
/// unreachable (we intentionally do not auto-spawn a container).
pub async fn connect() -> async_nats::Client {
    let url = nats_url();
    async_nats::connect(&url).await.unwrap_or_else(|e| {
        panic!(
            "failed to connect to NATS at {url}: {e}. \
             Start the broker with `docker compose up -d nats` \
             (or set NATS_URL).",
        )
    })
}

/// A connected client + JetStream context with automatic stream cleanup.
///
/// Use [`NatsTest::names`] to mint unique, collision-free identifiers and the
/// `*_config` builders to assemble ready-to-run consumer configurations whose
/// streams are cleaned up automatically.
pub struct NatsTest {
    pub client: async_nats::Client,
    pub js: async_nats::jetstream::Context,
    tracked_streams: Arc<Mutex<Vec<String>>>,
}

impl NatsTest {
    /// Mints a fresh, unique set of names for one consumer scenario and tracks
    /// the stream (and DLQ stream) for teardown.
    pub fn names(&self) -> TestNames {
        let id = Uuid::new_v4().simple().to_string();
        let names = TestNames {
            subject: format!("test.{id}"),
            stream: format!("stream-{id}"),
            durable: format!("durable-{id}"),
            dlq_subject: format!("dlq.{id}"),
            dlq_stream: format!("dlq-stream-{id}"),
        };
        self.track(&names.stream);
        self.track(&names.dlq_stream);
        names
    }

    /// Registers a stream name to be deleted on teardown. Safe to call for
    /// streams that may never be created.
    pub fn track(&self, stream: &str) {
        self.tracked_streams
            .lock()
            .unwrap()
            .push(stream.to_string());
    }

    /// A minimal stream capturing the scenario's subject.
    pub fn stream_config(&self, names: &TestNames) -> StreamConfig {
        StreamConfig {
            name: names.stream.clone(),
            subjects: vec![names.subject.clone()],
            ..Default::default()
        }
    }

    /// A pull consumer config with sensible, fast test defaults. `max_ack_pending`
    /// defaults to 1 so the result is valid for sequential consumers; override it
    /// for concurrent scenarios.
    pub fn pull_config(&self, names: &TestNames) -> PullConfig {
        PullConfig {
            durable_name: Some(names.durable.clone()),
            filter_subject: names.subject.clone(),
            ack_wait: Duration::from_secs(2),
            max_deliver: 3,
            max_ack_pending: 1,
            ..Default::default()
        }
    }

    /// A DLQ config whose stream captures the DLQ subject.
    pub fn dlq_config(&self, names: &TestNames) -> DlqConfig {
        DlqConfig {
            subject: names.dlq_subject.clone(),
            stream: StreamConfig {
                name: names.dlq_stream.clone(),
                subjects: vec![names.dlq_subject.clone()],
                ..Default::default()
            },
            duplicate_window: None,
        }
    }

    /// Assembles a full [`ConsumerConfig`] from the scenario names, optionally
    /// wiring in a DLQ.
    pub fn consumer_config(&self, names: &TestNames, with_dlq: bool) -> ConsumerConfig {
        ConsumerConfig {
            dlq: with_dlq.then(|| self.dlq_config(names)),
            backoff: BackoffPolicy::None,
            config: self.pull_config(names),
            stream: self.stream_config(names),
        }
    }

    /// Creates the scenario's main stream up-front so messages can be published
    /// before a consumer runs.
    pub async fn ensure_stream(&self, names: &TestNames) {
        self.js
            .get_or_create_stream(self.stream_config(names))
            .await
            .expect("failed to create stream");
    }

    /// Reads every message currently stored on a stream by spinning up a
    /// throwaway ephemeral consumer. Useful for asserting DLQ contents.
    pub async fn drain_stream(
        &self,
        stream: &str,
        expected: usize,
        timeout: Duration,
    ) -> Vec<async_nats::jetstream::Message> {
        use futures::StreamExt;

        // The stream may be created asynchronously by a consumer task, so wait
        // for it to appear before draining.
        let deadline = tokio::time::Instant::now() + timeout;
        let stream = loop {
            match self.js.get_stream(stream).await {
                Ok(stream) => break stream,
                Err(e) if tokio::time::Instant::now() < deadline => {
                    tracing::debug!(%e, "waiting for stream to be created");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => panic!("stream never appeared: {e}"),
            }
        };
        let consumer = stream
            .create_consumer(PullConfig {
                ack_policy: async_nats::jetstream::consumer::AckPolicy::None,
                ..Default::default()
            })
            .await
            .expect("failed to create drain consumer");
        let mut messages = consumer
            .messages()
            .await
            .expect("failed to open drain message stream");

        let mut out = Vec::new();
        let deadline = tokio::time::Instant::now() + timeout;
        while out.len() < expected {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, messages.next()).await {
                Ok(Some(Ok(msg))) => out.push(msg),
                Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
            }
        }
        out
    }
}

/// A unique bundle of identifiers for a single test scenario.
#[derive(Debug, Clone)]
pub struct TestNames {
    pub subject: String,
    pub stream: String,
    pub durable: String,
    pub dlq_subject: String,
    pub dlq_stream: String,
}

/// Polls `predicate` until it returns `true`, panicking after `timeout`.
pub async fn wait_for(timeout: Duration, predicate: impl Fn() -> bool) {
    let start = Instant::now();
    while !predicate() {
        if start.elapsed() > timeout {
            panic!("timed out after {timeout:?} waiting for condition");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// The event type used across the NATS test suite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestEvent(pub String);

impl TestEvent {
    pub fn new(s: impl Into<String>) -> Self {
        TestEvent(s.into())
    }
}

/// Error returned by [`TestHandler`], carrying the action it maps to.
#[derive(Debug)]
pub struct TestError {
    pub message: String,
    pub action: ErrorAction,
}

impl fmt::Display for TestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl HandlerError for TestError {
    fn action(&self) -> ErrorAction {
        self.action
    }
}

/// How a [`TestHandler`] should respond to each delivered message.
#[derive(Debug, Clone)]
pub enum Mode {
    /// Always succeed.
    Succeed,
    /// Sleep for the given duration, then succeed. Exercises the working-ack
    /// heartbeat (`AckKind::Progress`) for handlers that run longer than
    /// `ack_wait`, which must keep the message in-flight rather than letting it
    /// be redelivered mid-handle.
    SlowSucceed(Duration),
    /// Fail retriably until `delivered >= n`, then succeed.
    RetryUntil(i64),
    /// Always fail with a retriable error (drives NAK / redelivery).
    AlwaysRetriable,
    /// Always fail with a non-retriable error (drives dead-lettering).
    NonRetriable,
}

/// A configurable handler that records what it processes and how often it ran.
#[derive(Clone)]
pub struct TestHandler {
    pub mode: Mode,
    /// Payloads of messages that were handled successfully, in handling order.
    pub handled: Arc<Mutex<Vec<String>>>,
    /// Total number of `handle` invocations (including failed ones).
    pub calls: Arc<AtomicUsize>,
}

impl TestHandler {
    pub fn new(mode: Mode) -> Self {
        TestHandler {
            mode,
            handled: Arc::new(Mutex::new(Vec::new())),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Number of times `handle` has been invoked so far.
    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// Snapshot of successfully handled payloads, in order.
    pub fn handled(&self) -> Vec<String> {
        self.handled.lock().unwrap().clone()
    }
}

impl Handler for TestHandler {
    type Event = Json<TestEvent>;
    type Error = TestError;

    async fn handle(
        &mut self,
        ctx: &MessageContext<'_>,
        event: Json<TestEvent>,
    ) -> Result<(), TestError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let value = event.into_inner().0;

        if let Mode::SlowSucceed(d) = &self.mode {
            tokio::time::sleep(*d).await;
        }

        let succeed = match self.mode {
            Mode::Succeed | Mode::SlowSucceed(_) => true,
            Mode::RetryUntil(n) => ctx.delivered >= n,
            Mode::AlwaysRetriable | Mode::NonRetriable => false,
        };

        if succeed {
            self.handled.lock().unwrap().push(value);
            return Ok(());
        }

        let action = match self.mode {
            Mode::NonRetriable => ErrorAction::Dlq,
            _ => ErrorAction::Retry,
        };

        Err(TestError {
            message: format!("handler failed for {value:?}"),
            action,
        })
    }
}

impl AsyncTestContext for NatsTest {
    async fn setup() -> Self {
        let client = connect().await;
        let js = async_nats::jetstream::new(client.clone());
        NatsTest {
            client,
            js,
            tracked_streams: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn teardown(self) {
        let streams = std::mem::take(&mut *self.tracked_streams.lock().unwrap());
        for stream in streams {
            // Best-effort: a stream may never have been created by the test.
            let _ = self.js.delete_stream(&stream).await;
        }
    }
}
