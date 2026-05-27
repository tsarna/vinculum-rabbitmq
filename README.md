# vinculum-rabbitmq

RabbitMQ / AMQP 0-9-1 client integration for [Vinculum](https://github.com/tsarna/vinculum), built on [amqp091-go](https://github.com/rabbitmq/amqp091-go).

Provides an `RMQClient` that owns a single AMQP connection shared by any number of `RMQSender`s (vinculum → exchange) and `RMQReceiver`s (queue → vinculum), each on its own AMQP channel. The VCL configuration wiring (the `client "rabbitmq"` block) lives in the main vinculum repo (`clients/rabbitmq/`) to avoid circular imports — this module only depends on `vinculum-bus` and `vinculum-wire`.

**Scope**: AMQP 0-9-1 only. AMQP 1.0 (Azure Service Bus, ActiveMQ Artemis) uses a different protocol model and warrants a separate module.

---

## Packages

### `client` — RMQClient

Owns a single AMQP 0-9-1 connection shared by all senders and receivers within a vinculum `client "rabbitmq"` block.

**Responsibilities:**
- Initial connect with broker failover (walks the `Brokers` list in order on `Start` and on every reconnect).
- Connection-level reconnect loop with configurable exponential backoff. `OnDisconnect` and `OnConnect` hooks fire around each cycle; topology is re-declared and consumers re-registered after every successful reconnect.
- Channel-level recovery: a per-channel watcher reopens an individual channel (and re-declares its topology) when a channel-level AMQP error closes it without dropping the connection.
- One AMQP channel per sender and per receiver — as recommended by RabbitMQ for isolating publish confirms, consumer prefetch, and channel-level failures.

```go
c := client.NewClient(client.Config{
    Brokers:   []string{"amqp://localhost:5672/"},
    Username:  "vinculum",
    Password:  os.Getenv("AMQP_PASSWORD"),
    Heartbeat: 10 * time.Second,
})
c.AddSender(s)
c.AddReceiver(r)
if err := c.Start(ctx); err != nil { ... }
defer c.Stop()
```

`Start` is synchronous: it blocks until the first connection is established, every channel is open, every receiver's topology is declared, and every consumer is registered (so the caller can publish immediately on return).

### `sender` — RMQSender

Implements `bus.Subscriber`. `OnEvent` resolves a vinculum topic to an AMQP exchange + routing key, serializes the payload via the configured `wire.WireFormat`, encodes vinculum fields as AMQP basic-properties headers, and publishes.

**Features:**
- MQTT-style topic mapping with `+` and `#` wildcards (first match wins); per-mapping `RoutingKeyFunc` resolves the routing key from topic + payload + fields.
- Fallback transforms for unmatched topics: `slash_to_dot` (default), `verbatim`, `error`, `ignore`.
- Publisher confirms (`WithConfirmMode(true)`) wait for the broker `Basic.Ack` before `OnEvent` returns.
- **Mandatory-return error surfacing**: with `mandatory=true` and `confirm_mode=true`, the sender sets a unique `MessageId` on each publish and drains the broker's `Basic.Return` channel after each ack. If *this* publish was returned (no binding matched on the exchange), `OnEvent` returns an error carrying the broker's reply code + text. AMQP guarantees `Return` precedes `Ack` on the wire, so no grace window or extra latency is needed.
- Per-mapping overrides for the destination exchange and the persistent flag.
- W3C trace context (and OTel baggage) injected into AMQP headers; a `SpanKindProducer` span covers the publish (and the confirm wait, in confirm mode).

```go
s, err := sender.NewSender().
    WithClientName("events").
    WithExchange("alerts").
    WithConfirmMode(true).
    WithMandatory(true).
    WithPersistent(true).
    WithTopicMapping(sender.TopicMapping{
        Pattern: "alerts/+severity/#",
        RoutingKeyFunc: func(topic string, msg any, fields map[string]string) (string, error) {
            return "alerts." + fields["severity"], nil
        },
    }).
    Build()
```

### `receiver` — RMQReceiver

Consumes messages from a single AMQP queue and dispatches them as vinculum events via a `bus.Subscriber`.

**Features:**
- Optional active queue declare (`durable`, `auto_delete`); without it the receiver passive-declares (fails fast if the queue is missing).
- Bindings declared in-process (the AMQP binding always uses the bare wildcard); idempotent on every reconnect.
- Routing-key → vinculum-topic mapping via per-subscription patterns with named captures:
  - `"sensor.*.data"` — standard AMQP one-word wildcard
  - `"sensor.*deviceId.data"` — extracts the matched word into `fields["deviceId"]`
  - `"alerts.#"` — multi-word wildcard
  - `"alerts.#suffix"` — extracts the matched (dot-joined) tail into `fields["suffix"]`
- Default transforms when no subscription pattern matches: `dot_to_slash` (default), `verbatim`, `error`, `ignore`.
- Payload deserialization via `wire.WireFormat` (default: `wire.Auto`). On deserialize error the receiver currently falls back to raw bytes (see [the JSON-FAIL-SPEC](https://github.com/tsarna/vinculum/blob/main/specs/JSON-FAIL-SPEC.md) in the main repo for proposed handling).
- AMQP headers merged into the vinculum fields map (with W3C trace headers stripped after the propagator extracts them).
- At-least-once delivery: with `AutoAck=false` (default) each message is acked after `subscriber.OnEvent` returns without error; on error it is nacked **without** requeue (forwarded to a DLX if the queue has one configured).
- Configurable `Prefetch` (default 10) caps in-flight unacked messages.
- W3C trace context extracted from headers; a new-root `SpanKindConsumer` span linked to the producer span covers `subscriber.OnEvent` (OTel async-messaging convention).

```go
r, err := receiver.NewReceiver().
    WithClientName("events").
    WithQueue("vinculum.sensors").
    WithSubscriber(myBus).
    WithDeclare(receiver.Declare{Durable: true}).
    WithBinding(receiver.Binding{
        Exchange: "sensor-events", RoutingKey: "sensor.#",
    }).
    WithSubscription(receiver.Subscription{
        RoutingKeyPattern: "sensor.*deviceId.reading",
        VinculumTopicFunc: func(_, _ string, fields map[string]string, _ any) (string, error) {
            return "sensor/" + fields["deviceId"] + "/reading", nil
        },
    }).
    WithPrefetch(20).
    Build()
```

### `carrier` — AMQP-headers TextMapCarrier

Implements `propagation.TextMapCarrier` over the AMQP 0-9-1 basic-properties headers table. Used by the sender and receiver to inject and extract W3C trace context without dragging in any other dependency. Not generally interesting to direct users.

---

## Topology

`vinculum-rabbitmq` deliberately does **not** declare AMQP exchanges — they are operational topology, expected to pre-exist (provisioned via Terraform, the management UI, `rabbitmqadmin`, or a separate provisioning step).

Queues and queue→exchange bindings *are* declared by the receiver (actively if `Declare` is set, passively otherwise). All declares and binds are idempotent and replayed on every reconnect, so topology converges even after broker restarts.

---

## Metrics

All packages expose OpenTelemetry instrumentation via a `metric.MeterProvider`. Pass `nil` (or simply omit `WithMeterProvider`) to disable; all instruments are nil-safe.

Every instrument carries `messaging.system="rabbitmq"` and `vinculum.client.name=<client>`. Failures are tagged via the OTel `error.type` attribute on the existing counters/histograms rather than separate error counters.

### Client

| Metric | Type | Description |
|---|---|---|
| `rabbitmq.client.connected` | Gauge (float64) | 1 = connected, 0 = disconnected |
| `rabbitmq.client.reconnections` | Counter | Connection-level reconnection events |
| `rabbitmq.client.channel_reopens` | Counter | Channel-level recovery events |

### Sender

| Metric | Type | Attributes |
|---|---|---|
| `messaging.client.sent.messages` | Counter | `messaging.destination.name=<exchange>`, `error.type` on failure |
| `messaging.client.operation.duration` | Histogram (seconds) | Includes confirm round-trip in confirm mode |
| `rabbitmq.publisher.returned` | Counter | Mandatory-returned messages |

### Receiver

| Metric | Type | Attributes |
|---|---|---|
| `messaging.client.consumed.messages` | Counter | `messaging.destination.name=<queue>`, `messaging.operation.name="receive"` |
| `messaging.process.duration` | Histogram (seconds) | Duration of `subscriber.OnEvent`, `messaging.operation.name="process"` |
| `rabbitmq.consumer.nacks` | Counter | Messages nacked without requeue |

Attribute names follow OpenTelemetry [messaging semconv v1.26.0](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/).

---

## Tracing

The sender starts a `SpanKindProducer` span around the publish (including the confirm wait), injects W3C trace context + baggage into the outgoing AMQP headers, and records `error.type` on failure.

The receiver extracts the producer's trace context from the headers, then starts a **new-root** `SpanKindConsumer` span linked to the producer span (the OTel convention for async messaging — consumer is its own trace, not a child of the producer). The link survives `AsyncQueueingSubscriber` queues.

---

## VCL configuration

Used via vinculum, the full `client "rabbitmq"` block is available — for example:

```hcl
client "rabbitmq" "events" {
  brokers = ["amqp://broker:5672/production"]

  auth {
    username = "vinculum"
    password = env.AMQP_PASSWORD
  }

  reconnect {
    initial_delay  = "1s"
    max_delay      = "60s"
    backoff_factor = 2.0
  }

  sender "out" {
    exchange     = "alerts"
    mandatory    = true
    persistent   = true

    topic "alerts/#" {
      routing_key = "alerts"
    }
  }

  receiver "in" {
    queue      = "vinculum-sensors"
    subscriber = bus.main
    prefetch   = 20

    declare { durable = true }
    binding "sensor.#" { exchange = "sensor-events" }

    subscription "sensor.*deviceId.reading" {
      vinculum_topic = "sensor/${ctx.fields.deviceId}/reading"
    }
  }
}
```

See the [RabbitMQ client documentation](https://github.com/tsarna/vinculum/blob/main/doc/client-rabbitmq.md) in the main vinculum repo for the full VCL reference.

---

## Dependencies

- [`github.com/rabbitmq/amqp091-go`](https://github.com/rabbitmq/amqp091-go) — AMQP 0-9-1 client (no CGo)
- [`github.com/tsarna/vinculum-bus`](https://github.com/tsarna/vinculum-bus) — `Subscriber`, `EventBus` interfaces
- [`github.com/tsarna/vinculum-wire`](https://github.com/tsarna/vinculum-wire) — pluggable wire formats
- [`go.opentelemetry.io/otel`](https://pkg.go.dev/go.opentelemetry.io/otel) — tracing + metrics
- [`go.uber.org/zap`](https://pkg.go.dev/go.uber.org/zap) — structured logging

## License

MIT — see [LICENSE](LICENSE).
