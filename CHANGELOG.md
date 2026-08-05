# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-08-05

### Added

- **`Config.MaxReconnectAttempts`** bounds how many attempts to re-establish a lost
  connection are made before the client gives up. Zero or negative reconnects forever,
  which is both the default and the behaviour before this field existed, so upgrading
  changes nothing until you set it. One attempt is one full walk of the `Brokers` list —
  the same unit `ReconnectBackoff` already counts in.

  Zero meaning *unlimited* rather than *never reconnect* is deliberate: it makes the new
  field's zero value the pre-existing behaviour, and it is what `vinculum-bus`'s
  `AutoReconnector` already means by the same number. "Do not reconnect at all" is
  therefore not expressible here, as it is not there. `vinculum-mqtt` v0.11.0 adds the same
  field with the same semantics.

  The check sits at the top of the loop, before the attempt, so the limit counts attempts
  made rather than waits endured: a limit of 3 dials the broker list three times and exits
  before a fourth.

  It governs **reconnection only** — the initial connection made by `Start` is unaffected,
  since that path never enters the reconnect loop.

  Giving up is terminal and quiet, mirroring `AutoReconnector`: an error is logged, the
  supervision goroutine returns, and the client stays down without the process exiting.
  Senders and receivers stay stopped, so publishes fail as they do for any disconnected
  client.

## [0.3.0] - 2026-08-03

### Changed

- Requires `github.com/tsarna/vinculum-wire` v0.5.0, for `wire.IsReservedAttr`. The
  receiver's decode-error test now checks every `Attrs` key against it. A key that
  collides with one of `DecodeError`'s own fields is dropped by a consumer rather than
  allowed to shadow the fixed field, so its value is silently lost between the receiver
  that set it and whatever reads it — which is what happened to `vinculum-mqtt`'s
  `Attrs["topic"]`, a duplicate of `Topic` that never reached a config. This module's
  keys (`routing_key`, `exchange`, `queue`) are and always were clean; the check is what
  keeps a future rename from quietly breaking one.

## [0.2.0] - 2026-07-19

### Changed

- **BREAKING: deserialize failures are no longer swallowed.** `RMQReceiver.handleDelivery`
  used to log a warning and pass the **raw bytes** through as the message payload when the
  configured wire format failed to decode. That happened even when the caller explicitly
  configured `wire.JSON`, so there was no way to say "messages on this queue must be JSON".
  A decode failure is now fatal to the message: it is nacked without requeue and never
  reaches `subscriber.OnEvent`.

  For rabbitmq this is safe — the message is dropped, or routed to a dead-letter exchange
  if one is bound to the queue.

  Callers wanting best-effort decoding should use `wire.Auto`, which never fails (it yields
  a `string` for anything it can't parse as JSON). Note that is not an exact replacement:
  the old fallback produced `[]byte`, so a subscriber that type-switches on `[]byte` must
  be adjusted.

- Requires `github.com/tsarna/vinculum-wire` v0.3.0 for the `DecodeError` /
  `DecodeErrorHook` types.

### Added

- `WithDecodeErrorHook(wire.DecodeErrorHook)` on the receiver builder. The hook observes a
  decode failure — it receives the raw body, the error, the format name, the fields
  extracted so far, and the routing key, exchange, and queue — but cannot suppress it: the
  message is nacked either way. nil (the default) means no observer.

- Deserialize failures are recorded on the existing received counter with
  `error.type = "deserialize"`, alongside the existing `routing`, `no_subscription`,
  `vinculum_topic`, and `subscriber` classifications.

## [0.1.0] - 2026-05-27

### Added

- Initial release. AMQP 0-9-1 sender and receiver for vinculum, sharing one connection
  with a channel per sender and per receiver: queue consumption with routing-key pattern
  subscriptions and field extraction, publisher-confirm and mandatory delivery modes,
  topology declaration (queues and bindings) on connect and channel recovery, multi-broker
  failover and reconnect with configurable exponential backoff, wire-format-driven
  serialization, W3C trace-context and baggage propagation over AMQP headers, and OTel
  metrics and consumer spans.
