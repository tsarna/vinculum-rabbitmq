# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.10.0] - 2026-09-20

### Fixed

- **A forced connection close goes straight to the reconnect loop.** A channel's
  close was treated as connection-level only for codes of 500 and up, but AMQP
  0-9-1's hard exceptions include some below that, among them
  `320 CONNECTION_FORCED` — what a broker sends when an operator closes the
  connection or the broker shuts down. A forced close sent every channel watcher
  into a recovery on a connection that was already gone, and a warning for each
  when it failed — a warning that could land after `Client.Stop` had returned,
  since the watchers are not joined. A watcher that lost the race with the
  reconnect loop could instead open a second channel on the new connection and
  replace the one the reconnect had just installed.

  The classification now uses the library's own `Error.Recover`, which is set
  only for the soft, channel-level codes. A channel closed with any other code,
  `200` included, is no longer reopened by its watcher. Brokers report hard
  exceptions by closing the connection, which the reconnect loop repairs.

## [0.9.0] - 2026-09-06

### Added

- **`Client.Drain` and `RMQReceiver.Drain`, so consuming can stop without the
  connection closing.** Stopping used to be one action: cancel everything and
  close. That is the wrong shape for a graceful shutdown, where a process wants
  to stop *accepting* deliveries long before it stops being able to
  acknowledge them — because the acknowledgement travels over the channel the
  stop closes.

  The receiver drains with `basic.cancel` rather than by cancelling a context,
  which is the distinction that matters on this transport: the broker stops
  sending, and the deliveries already prefetched into the local buffer are
  *handled* rather than abandoned. Cancelling the context would have thrown
  away work the process had already been given, and RabbitMQ would have
  redelivered every message of it. `Client.Drain` fans out to every receiver
  and leaves senders, channels and the connection untouched.

  Draining is terminal. The reconnect and channel-recovery paths outlive it and
  both end in `receiver.Start`, which would otherwise register the consumer
  again — so a drained client refuses to start one, and a shutdown does not
  find itself consuming during the phase that waits for consuming to be over.

- **`RMQReceiver.Unsettled`, the count of deliveries nothing has settled yet.**
  Not the broker's unacked count: a delivery left unacked by a nack is
  RabbitMQ's business. This is the narrower number a shutdown can usefully wait
  for — acknowledgements that are still coming.

- **A named consumer tag.** Each receiver now registers as
  `vinculum-<client>-<queue>` instead of taking a server-generated tag, so
  `rabbitmqctl list_consumers` names the config that owns each one. Long tags
  are capped to stay within the protocol's limit.

### Changed

- **`RMQReceiver.Stop` returns an `error`** (it was `func()`). It reports a
  delivery a previous drain gave up on, which is also the case where it now
  declines to wait: the drain has already given that delivery a bounded chance
  to finish, and waiting again with no bound would let one stuck action stop
  the process from exiting. `Client.Stop` collects those alongside whatever
  closing the connection reports.

  A delivery's *acknowledgement* is unaffected. `Stop` retires the channel's
  delivery tags and `Drain` deliberately does not, so a settle that arrives
  during a drain still refers to something the broker recognises.

## [0.8.0] - 2026-09-01

### Changed

- **`AckAfterHandling` acknowledges when the work finishes, not when delivery
  returns.** The two are the same thing only while delivery is synchronous. Put
  an async queue, a bus hop, or a state machine downstream and delivery returns
  the moment the delivery is *enqueued* — so it was acknowledged before
  anything had handled it, and a handler that then failed had nothing left to
  redeliver or dead-letter.

  The receiver now marks its settler as framework-settled and lets whatever
  finishes the work settle it, however many hops away that happens. This makes
  `queue_size` alongside automatic acknowledgement correct, where before it was
  a way to lose messages. It matters most here: an unsettled delivery holds a
  prefetch slot and nothing on this transport self-heals.

  The mode keeps its name. What changed is when it acts, not what it means.

- **A handler failure nacks in every mode, which is now the general rule rather
  than this receiver's local one.** The reasoning is unchanged and was written
  here first: an unsettled delivery under manual is bounded by a settle
  deadline whose expiry nacks anyway, so the choice is between dead-lettering a
  known failure now and holding a prefetch slot until the deadline says the
  same thing later. `vinculum-bus` applies it at every settle point.

## [0.7.0] - 2026-08-31

### Added

- **Manual acknowledgement.** Each delivery now carries a `bus.Settler` on its
  context, so work that finishes several hops from the receiver — behind an
  async queue, on another goroutine, in a subscription on a bus downstream —
  settles the delivery it actually handled:

  ```go
  if s := bus.SettlerFromContext(ctx); s != nil {
      settled, err := s.Ack(ctx)
  }
  ```

  Acknowledgement is a property of the inbound delivery, and there was no way to
  express that: an `amqp.Delivery` never left the receiver, and the vinculum
  `fields` map cannot carry one, being rewritten per subscription with that
  subscription's own topic captures. The context is the per-message channel that
  survives every hop, and `context.WithoutCancel` means an async queue preserves
  it across the goroutine boundary.

  Settle-once, staleness, and the bool return come from `bus.NewSettler`, so
  they are not written once per protocol, subtly differently. Two subscribers
  both acknowledging produce one broker acknowledgement: the first call reports
  `true`, the second `false`.

- **A delivery tag is refused once its channel is gone.** A tag only means
  something on the channel that issued it, and AMQP re-points tags from 1 on
  each new channel. Each settler stamps the channel generation it was built
  under and reports a `bus.StaleError` — "channel reconnected" — rather than
  issuing a call the broker would reject with an error saying nothing about what
  became of the message. The generation changes in `Stop`, after the delivery
  loop has drained, so a message being handled when shutdown began still
  acknowledges normally.

### Changed

- **`WithAutoAck(bool)` is replaced by `WithAckMode(AckMode)`**, with three
  values where there were two: `AckAfterHandling` (the default, and exactly what
  `WithAutoAck(false)` did — acknowledge once `subscriber.OnEvent` returns
  without error), `AckManual` (settle nothing; the context's settler decides),
  and `AckNone` (AMQP's own no-ack consumer flag, what `WithAutoAck(true)` did).

  A boolean could not name the third state, and the two it did name were not
  opposites: one is a vinculum policy about when to acknowledge, the other is a
  broker mode in which nothing is ever acknowledged at all.

  **Breaking for callers of `WithAutoAck`**: `WithAutoAck(true)` becomes
  `WithAckMode(receiver.AckNone)`, and `WithAutoAck(false)` is the default and
  can be dropped.

- Every acknowledgement now goes through the settler, automatic ones included,
  so "the receiver acknowledges for you" is one policy over one mechanism rather
  than a second route to the broker — and a handler that settled the message
  itself is not settled over the top of. A delivery that never reaches the
  subscriber (one that fails to decode, one no subscription matched under the
  `error` default transform) is still nacked by the receiver in every mode,
  because the consumer of a delivery cannot answer for one it never saw.

- Requires `github.com/tsarna/vinculum-bus` v0.18.0, for `Settler`, `SettleOps`,
  `NewSettler`, and `StaleError`.

## [0.6.0] - 2026-08-28

### Changed

- **`Start` no longer dials. It launches the connection machinery and returns.**
  A background goroutine walks the brokers list with backoff until one accepts,
  sets up channels, fires `OnConnect`, and then watches for the connection to
  drop — at which point the same loop repairs it.

  This fixes a client that was permanently dead after a broker outage at startup.
  `Start` dialed synchronously and returned the failure *before*
  `go watchConnAndReconnect` was reached, so the reconnect watcher only ever
  existed after a first success. A broker that was not listening yet left the
  client dead for the life of the process, with the schedule `ReconnectBackoff`
  and `MaxReconnectAttempts` describe never running at all. Measured with a
  listener that accepts and closes: one dial before, unbounded retries after.

  The initial connection and a repair are now literally the same function, which
  is the point — a broker that is not up yet and one that went away are the same
  situation, and were only ever different because of where the code sat.

  **This is a breaking behavioural change for callers that relied on `Start`
  reporting connectivity.** It now returns only for a configuration it cannot
  use — no brokers, or a second `Start` — and never for a broker it cannot
  reach. Use `IsConnected` to observe the connection and `OnConnect` /
  `OnDisconnect` to react to it. A host that reports readiness needs no change:
  `IsConnected` already said what it says now, for longer.

- **`MaxReconnectAttempts` bounds a repair, never the first connection.** It is
  documented as governing reconnection, and giving up on a broker that has not
  finished starting is the wrong default — the host reports itself not-ready
  throughout, which is the honest thing to do and costs nothing to keep doing.
  A limit that also ended the initial connect would turn a slow broker into a
  client that never connects and never says why.

- The reconnection counter is no longer incremented for the first connection,
  which was never lost. Every process start used to record one.

## [0.5.0] - 2026-08-25

### Added

- **`Client.IsConnected()`** reports whether the client currently holds a live
  connection to a broker — `OnConnect` has fired without a subsequent
  `OnDisconnect`. The state was already tracked internally to guarantee
  `OnDisconnect` fires exactly once per cycle; this exposes it.

  It exists for health reporting. A host that answers a readiness probe needs to
  say "this process cannot do its job right now" while the broker is away, and
  recover when the reconnect loop succeeds — which is precisely the window this
  reports. It is a snapshot, not a guarantee: the connection may drop between
  the call and the next publish, so it is useful for a probe and useless as a
  precondition. Code that wants to publish should publish and handle the error.

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
