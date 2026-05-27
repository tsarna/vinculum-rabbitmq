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
// With AutoAck=false (default), each message is acked after subscriber.OnEvent
// returns without error. On error the message is nacked without requeue
// (forwarded to a dead-letter exchange if the queue has one configured).
package receiver
