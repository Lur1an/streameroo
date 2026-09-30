# Changelog

Notable changes to `streameroo`, newest first, reconstructed from Git history
and the [crates.io release history](https://crates.io/crates/streameroo/versions).
Published dates are UTC dates from crates.io. **Yanked** records remain here for
completeness. **Git-only** versions appeared in `Cargo.toml` but were not published
to crates.io; their dates identify the version-bump commits, not releases.
Entries follow development/release order, including the historical `0.1.77`,
`0.1.771`, and `0.2.11` version numbers.

## [0.7.0] — Unreleased

### Breaking changes

- Replace `Handler`'s associated `Event`, `Result`, and `Error` types with
  `Handler<E, R, Err>`, allowing one handler struct to implement multiple event types.
- Change `consume` and `consume_with_options` generic parameters to `<E, R, Err, H>`.
  Select an event explicitly with `app.consume::<MyEvent, _, _, _>(...)` when a
  handler has multiple implementations. Consumer registration requires `E`, `R`,
  and `Err` to be `'static`.
- Includes the unpublished `0.6.0` handler and feature changes below. Applications
  upgrading from `0.5.0` must also migrate state and metadata access.

### Documentation

- Rewrite the README for struct-based handlers, explicit delivery metadata,
  multiple event types, and the `amqp` feature.
- Clarify that the `consume` count controls AMQP prefetch, and document tracing
  initialization and test helper requirements.

## [0.6.0] — Git-only

The version-bump commit has an author date of 2026-05-27, but was rebased after
`0.5.0`. This intermediate version was superseded by `0.7.0` before publication.

### Breaking changes

- Replace extractor-based async-function handlers (`AMQPHandler`) with explicitly
  implemented, cloneable `Handler` structs. This intermediate API uses associated
  `Event`, `Result`, and `Error` types and `handle(&self, &DeliveryContext, event)`.
- Remove `Context`, `Store`, `FromDeliveryContext`, `State`, `StateOwned`, and
  metadata wrapper extractors. Store dependencies on the handler and access
  public fields on `DeliveryContext` instead.
- Change construction to `Streameroo::new(connection, consumer_tag)`.
- Rename the AMQP `Result<T>` alias to `StreamerooResult<T>`.
- Remove `Error::Handler`; handler errors require `Display + Send` and are logged
  directly rather than converted into boxed errors.
- Replace the `tokio` feature with `amqp`, gating AMQP code and its dependencies.
  Default features become `amqp` and `json`; `telemetry` and `amqp-test` enable `amqp`.

### Changed

- Separate decoding, handler execution, and result actions in delivery processing.
  All decode errors nack without requeue; handler and result-action errors nack
  with requeue.
- Remove the `fnv` dependency and add a poison-message integration test.

## [0.5.0] — 2026-09-30

### Breaking changes

- Update the telemetry dependency stack to `opentelemetry` 0.33,
  `tracing-opentelemetry` 0.34, and
  `tracing-opentelemetry-instrumentation-sdk` 0.42.1. Applications sharing
  OpenTelemetry types with the crate must use compatible versions.
- Update the test tracing setup dependency, `init-tracing-opentelemetry`, to 0.43.0.

## [0.4.4] — 2026-03-01

### Fixed

- Enable automatic acknowledgement in `drain_queue` with `manual_ack(false)`,
  also fixing acknowledgement for `consume_next`, which delegates to it.

## [0.4.3] — 2026-03-01 — Yanked

### Added

- Add `AMQPConnection::drain_queue` under `amqp-test`, returning a stream of
  decoded messages that ends after a per-message timeout.

### Changed

- Implement `consume_next` using `drain_queue` with a two-second timeout.
- Expand the README with API, serialization, connection, tracing, and testing documentation.

## [0.4.2] — 2026-01-21

### Fixed

- Restore a dedicated producer span for publishing through an `amqprs` channel,
  set its parent to the current OpenTelemetry context, and inject its context
  into message headers. The `0.4.1` archive injected the current tracing span instead.
- Remove unused telemetry imports from the AMQP tests.

The Git commit for this version also contains the `0.4.1` changes below; the
published crate archives distinguish the two releases.

## [0.4.1] — 2026-01-21 — Yanked

### Added

- Make `amqp::telemetry` public.
- Add a `BasicProperties` handler extractor.
- Add `table_from_map` to convert string key/value maps into AMQP field tables.

### Changed

- Update to `opentelemetry` 0.31, `tracing-opentelemetry` 0.32.1,
  `tracing-opentelemetry-instrumentation-sdk` 0.32.3, and
  `init-tracing-opentelemetry` 0.32.0.
- Log failures when setting a consumer span's parent context.
- Update the test tracing initialization and teardown.

This release is absent from the current Git ancestry. Its changes were verified
against the published `0.4.0`, `0.4.1`, and `0.4.2` source archives.

## [0.4.0] — 2025-07-31

### Added

- Add the optional `telemetry` feature with OpenTelemetry context propagation
  through RabbitMQ message headers and producer/consumer spans.
- Add tracing setup for integration tests and a local Jaeger Compose configuration.

### Breaking changes

- Move the crate from Rust edition 2021 to edition 2024.

## [0.3.9] — 2025-07-18

### Breaking changes

- Replace `handle_ctrl_c` and `with_shutdown` with
  `with_graceful_shutdown`, accepting a caller-provided `Send + 'static` future.

### Fixed

- Wait for in-flight delivery tasks before closing consumer channels during shutdown.
- Wait five seconds before retrying a failed `basic_consume` registration.

## [0.3.8] — 2025-07-14

### Fixed

- Acknowledge the message consumed by the `consume_next` test helper.

## [0.3.7] — 2025-07-14

### Added

- Add `AMQPConnection::consume_next` to consume and decode a single message in tests.

## [0.3.6] — 2025-07-14

### Added

- Add `Streameroo::with_shutdown` to use an external shutdown notifier before
  starting consumers.

## [0.3.5] — 2025-07-14

### Added

- Add the `amqp-test` feature, exposing RabbitMQ testcontainers setup and the
  `AMQPTest` test context to downstream integration tests.

## [0.3.4] — 2025-07-13

### Added

- Enable `amqprs`'s `urispec` feature for AMQP URI connection configuration.

## [0.3.3] — 2025-07-12

### Changed

- Implement `ChannelExt` on `AMQPConnection`, sharing publishing and Direct
  Reply-To RPC helpers with channels. Replace the connection's standalone
  publishing helpers with this trait implementation.

## [0.3.2] — 2025-06-09

### Added

- Add a round-robin publishing channel pool to `AMQPConnection`.
- Allow publishing directly through the connection and rebuild pooled channels
  after reconnection.

## [0.3.1] — 2025-06-09

### Fixed

- Re-export connection module types, including `AMQPConnection`, from `amqp`.

## [0.3.0] — 2025-06-09 — Yanked

### Breaking changes

- Replace `lapin` with `amqprs`, including public AMQP types, channel operations,
  delivery metadata, field tables, and errors.
- Change `Streameroo::new` to accept a connection, shared context, and consumer tag.
- Add a prefetch-count argument to `consume`; change `consume_with_options` to
  take `BasicConsumeArguments` and `BasicQosArguments`.
- Replace `join(graceful)` with `join()`, separate Ctrl-C registration through
  `handle_ctrl_c`, and expose `shutdown_handle` for manual notification.

### Added

- Introduce an IO-loop-backed connection with automatic reconnection and
  channel-open request timeouts.
- Recreate consumers after channel/connection failure.
- Move integration tests to RabbitMQ testcontainers and test reconnection.

## [0.2.11] — 2025-03-31

### Changed

- Shorten the delivery-processing tracing span name from `streameroo::amqp` to
  `streameroo`.

## [0.2.1] — 2025-03-28

### Breaking changes

- Change `join()` to `join(graceful: bool)`. Register the Ctrl-C listener only
  when graceful shutdown is requested, rather than in `Streameroo::new`.
- This is the first published release containing the Git-only `0.1.8` and `0.2.0`
  changes below.

## [0.2.0] — Git-only, 2025-02-18

### Added

- Add Ctrl-C-driven graceful shutdown.
- Track delivery tasks in a `JoinSet`, drain completed tasks, and await pending
  handlers when the consumer exits normally.

## [0.1.8] — Git-only, 2025-01-06

### Added

- Add `Encode::content_type()` and content types for JSON, MessagePack, and BSON.
- Automatically fill missing AMQP content-type properties when publishing
  through `ChannelExt`.
- Add acknowledgement and negative-acknowledgement tracing.

### Breaking changes

- Terminate consumer tasks on AMQP delivery errors and propagate those errors
  through `join`, whose error type changes from `JoinError` to `lapin::Error`.

## [0.1.771] — 2024-12-22

### Fixed

- Log the actual resolved consumer tag, including queue suffixes and overrides.

## [0.1.77] — 2024-12-22 — Yanked

### Breaking changes

- Add a consumer-tag override argument to `consume_with_options` and default
  consumer tags to `{consumer_tag}-{queue}`.

### Added

- Wrap delivery processing in tracing spans containing the delivery tag.

## [0.1.7] — 2024-12-20

### Added

- Implement `Deref` and `DerefMut` for `Auto<T>`.

## [0.1.6] — 2024-12-18

### Fixed

- Encode `XQueueType` values as AMQP long strings rather than short strings so
  RabbitMQ accepts queue-type declarations.

## [0.1.5] — 2024-12-17

### Added

- Add `Auto<T>` for runtime deserialization based on the message content type.
- Add `AMQPDecode` for decoding with delivery metadata, with a blanket
  implementation for existing `Decode` types.
- Add the `bson` feature and `Bson<T>` codec wrapper.

### Fixed

- Nack decode failures without requeue to avoid repeatedly processing invalid payloads.

## [0.1.4] — 2024-12-15

### Added

- Add the `field_table!` macro and `XQueueType` helpers for queue arguments.

## [0.1.3] — 2024-12-15

### Added

- Implement `Encode` and `Decode` for `serde_json::Value`.

## [0.1.2] — 2024-12-05

### Added

- Make `Context::channel` public for direct broker interaction.

### Changed

- Clarify the error log when calling an AMQP handler fails.

## [0.1.1] — 2024-12-05 — Yanked

### Changed

- Accept independent `Into<String>` arguments for the exchange and routing key
  in `Publish::new`, allowing string slices as well as owned strings.

## [0.1.0] — 2024-12-01

### Added

- Initial published AMQP consumer framework built on `lapin` and Tokio.
- Async-function handlers with shared-state and delivery-metadata extractors.
- `Encode`/`Decode` traits, JSON and MessagePack wrappers, raw-byte support, and
  optional `bytes::Bytes` decoding.
- Automatic acknowledgement/error handling and result-driven actions through
  `AMQPResult`, `Publish`, `PublishReply`, and `DeliveryAction`.
- `ChannelExt` publishing helpers and RabbitMQ Direct Reply-To RPC support.

[0.7.0]: https://github.com/lur1an/streameroo/compare/99ba165...2881437
[0.6.0]: https://github.com/lur1an/streameroo/compare/5db2665...99ba165
[0.5.0]: https://github.com/lur1an/streameroo/compare/38e12e8...v0.5.0
[0.4.4]: https://github.com/lur1an/streameroo/compare/e2dd059...38e12e8
[0.4.3]: https://github.com/lur1an/streameroo/compare/653a44e...e2dd059
[0.4.2]: https://crates.io/crates/streameroo/0.4.2
[0.4.1]: https://crates.io/crates/streameroo/0.4.1
[0.4.0]: https://github.com/lur1an/streameroo/compare/bfbeba1...cc26da7
[0.3.9]: https://github.com/lur1an/streameroo/compare/580cfd2...bfbeba1
[0.3.8]: https://github.com/lur1an/streameroo/compare/278740f...580cfd2
[0.3.7]: https://github.com/lur1an/streameroo/compare/a47321a...278740f
[0.3.6]: https://github.com/lur1an/streameroo/compare/da756db...a47321a
[0.3.5]: https://github.com/lur1an/streameroo/compare/3c54ede...da756db
[0.3.4]: https://github.com/lur1an/streameroo/compare/db99379...3c54ede
[0.3.3]: https://github.com/lur1an/streameroo/compare/449580d...db99379
[0.3.2]: https://github.com/lur1an/streameroo/compare/69239fb...449580d
[0.3.1]: https://github.com/lur1an/streameroo/compare/4b88d87...69239fb
[0.3.0]: https://github.com/lur1an/streameroo/compare/b03a4ac...4b88d87
[0.2.11]: https://github.com/lur1an/streameroo/compare/b25b267...b03a4ac
[0.2.1]: https://github.com/lur1an/streameroo/compare/c9db21c...b25b267
[0.2.0]: https://github.com/lur1an/streameroo/compare/37855a2...c9db21c
[0.1.8]: https://github.com/lur1an/streameroo/compare/d7b1d0c...37855a2
[0.1.771]: https://github.com/lur1an/streameroo/compare/ba6f783...d7b1d0c
[0.1.77]: https://github.com/lur1an/streameroo/compare/7cd4763...ba6f783
[0.1.7]: https://github.com/lur1an/streameroo/compare/3f61d87...7cd4763
[0.1.6]: https://github.com/lur1an/streameroo/compare/a53e511...3f61d87
[0.1.5]: https://github.com/lur1an/streameroo/compare/d760766...a53e511
[0.1.4]: https://github.com/lur1an/streameroo/compare/7eaf2f6...d760766
[0.1.3]: https://github.com/lur1an/streameroo/compare/532b855...7eaf2f6
[0.1.2]: https://github.com/lur1an/streameroo/compare/0707aa7...532b855
[0.1.1]: https://github.com/lur1an/streameroo/compare/f505598...0707aa7
[0.1.0]: https://crates.io/crates/streameroo/0.1.0
