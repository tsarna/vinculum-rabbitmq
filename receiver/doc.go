// Package receiver provides RMQReceiver, which consumes messages from a
// RabbitMQ queue and dispatches them as vinculum events via a bus.Subscriber.
//
// # Usage
//
// Build a receiver using the fluent builder:
//
//	r, err := receiver.NewReceiver().
//	    WithQueue("vinculum-events").
//	    WithSubscriber(myBus).
//	    WithSubscription(receiver.Subscription{
//	        RoutingKeyPattern: "sensor.*deviceId.reading",
//	        VinculumTopic:     vinculumTopicFunc,
//	    }).
//	    WithPrefetch(10).
//	    Build()
//
// Register the receiver with client.Client before calling Start. The client
// injects a live AMQP channel via SetChannel on each connect and reconnect.
//
// # Routing key patterns
//
// RoutingKeyPattern uses AMQP topic-exchange wildcards with optional field
// names:
//
//	"sensor.*.data"           — standard AMQP wildcard (one word)
//	"sensor.*deviceId.data"   — extracts "deviceId" from the matched word
//	"alerts.#"                — multi-word wildcard (zero or more words)
//
// The actual AMQP binding (when declared via the binding block) uses the plain
// wildcard; field names are stripped. Field extraction populates the vinculum
// fields map.
//
// # Message deserialization
//
// Bodies are deserialized via wire.WireFormat (default: wire.Auto). AMQP
// headers-table entries become vinculum fields; non-string values are
// converted via fmt.Sprintf("%v", v). W3C trace headers (traceparent,
// tracestate, baggage) are stripped from the visible fields map after the
// propagator extracts them.
//
// # Acknowledgement
//
// WithAckMode says who settles a delivery, and when. Under AckAfterHandling
// (the default) each message is acked once subscriber.OnEvent returns without
// error; on error it is nacked without requeue, so it is forwarded to a
// dead-letter exchange if the queue has one configured and dropped if it does
// not.
//
// Under AckManual nothing is settled when handling returns. Each delivery
// carries a bus.Settler on its context — see bus.SettlerFromContext — so work
// that finishes several hops later, behind an async queue or on another
// goroutine, can settle the delivery it actually handled. A message that never
// reaches the subscriber (one that fails to decode, or that no subscription
// matched under an "error" default transform) is still nacked here, because the
// consumer of the delivery cannot answer for a delivery it never saw.
//
// AckNone is AMQP's own no-ack consumer mode: the broker treats a message as
// delivered the moment it sends it, this receiver acknowledges nothing, and
// deliveries carry no settler.
//
// A delivery tag means nothing once its channel is gone, so a settler stamps
// the channel generation it was built under and refuses afterwards, reporting a
// bus.StaleError rather than issuing a call the broker would reject.
package receiver
