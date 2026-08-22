use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::stream::Config as StreamConfig;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use streameroo::event::Json;
use streameroo::nats::jetstream::{
    BackoffPolicy, ConsumerConfig, DlqConfig, ErrorAction, Handler, HandlerError, MessageContext,
};
use test_context::AsyncTestContext;
use uuid::Uuid;

pub fn nats_url() -> String {
    std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string())
}

pub async fn connect() -> async_nats::Client {
    let url = nats_url();
    async_nats::connect(&url).await.unwrap_or_else(|error| {
        panic!(
            "failed to connect to NATS at {url}: {error}. Start the broker with `docker compose up -d nats` (or set NATS_URL)."
        )
    })
}

pub struct NatsTest {
    pub client: async_nats::Client,
    pub js: async_nats::jetstream::Context,
    tracked_streams: Arc<Mutex<Vec<String>>>,
}

impl NatsTest {
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

    pub fn track(&self, stream: &str) {
        self.tracked_streams
            .lock()
            .unwrap()
            .push(stream.to_string());
    }

    pub fn stream_config(&self, names: &TestNames) -> StreamConfig {
        StreamConfig {
            name: names.stream.clone(),
            subjects: vec![names.subject.clone()],
            ..Default::default()
        }
    }

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

    pub fn consumer_config(&self, names: &TestNames, with_dlq: bool) -> ConsumerConfig {
        ConsumerConfig {
            dlq: with_dlq.then(|| self.dlq_config(names)),
            backoff: BackoffPolicy::None,
            config: self.pull_config(names),
            stream: self.stream_config(names),
        }
    }

    pub async fn ensure_stream(&self, names: &TestNames) {
        self.js
            .get_or_create_stream(self.stream_config(names))
            .await
            .expect("failed to create stream");
    }

    pub async fn drain_stream(
        &self,
        stream: &str,
        expected: usize,
        timeout: Duration,
    ) -> Vec<async_nats::jetstream::Message> {
        use futures::StreamExt;

        let deadline = tokio::time::Instant::now() + timeout;
        let stream = loop {
            match self.js.get_stream(stream).await {
                Ok(stream) => break stream,
                Err(error) if tokio::time::Instant::now() < deadline => {
                    tracing::debug!(%error, "waiting for stream to be created");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => panic!("stream never appeared: {error}"),
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

        let mut messages_out = Vec::new();
        let deadline = tokio::time::Instant::now() + timeout;
        while messages_out.len() < expected {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, messages.next()).await {
                Ok(Some(Ok(message))) => messages_out.push(message),
                Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
            }
        }
        messages_out
    }
}

#[derive(Debug, Clone)]
pub struct TestNames {
    pub subject: String,
    pub stream: String,
    pub durable: String,
    pub dlq_subject: String,
    pub dlq_stream: String,
}

pub async fn wait_for(timeout: Duration, predicate: impl Fn() -> bool) {
    let start = Instant::now();
    while !predicate() {
        if start.elapsed() > timeout {
            panic!("timed out after {timeout:?} waiting for condition");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestEvent(pub String);

impl TestEvent {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug)]
pub struct TestError {
    pub message: String,
    pub action: ErrorAction,
}

impl fmt::Display for TestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl HandlerError for TestError {
    fn action(&self) -> ErrorAction {
        self.action
    }
}

#[derive(Debug, Clone)]
pub enum Mode {
    Succeed,
    SlowSucceed(Duration),
    RetryUntil(i64),
    AlwaysRetriable,
    NonRetriable,
    Terminate,
}

#[derive(Clone)]
pub struct TestHandler {
    pub mode: Mode,
    handled: Arc<Mutex<Vec<String>>>,
    calls: Arc<AtomicUsize>,
}

impl TestHandler {
    pub fn new(mode: Mode) -> Self {
        Self {
            mode,
            handled: Arc::new(Mutex::new(Vec::new())),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

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
        if let Mode::SlowSucceed(duration) = &self.mode {
            tokio::time::sleep(*duration).await;
        }
        let succeeds = match self.mode {
            Mode::Succeed | Mode::SlowSucceed(_) => true,
            Mode::RetryUntil(delivered) => ctx.delivered >= delivered,
            Mode::AlwaysRetriable | Mode::NonRetriable | Mode::Terminate => false,
        };
        if succeeds {
            self.handled.lock().unwrap().push(value);
            return Ok(());
        }
        let action = match self.mode {
            Mode::NonRetriable => ErrorAction::Dlq,
            Mode::Terminate => ErrorAction::Term,
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
        Self {
            client,
            js,
            tracked_streams: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn teardown(self) {
        let streams = std::mem::take(&mut *self.tracked_streams.lock().unwrap());
        for stream in streams {
            let _ = self.js.delete_stream(&stream).await;
        }
    }
}
