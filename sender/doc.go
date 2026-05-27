// Package sender provides RMQSender, a bus.Subscriber that forwards vinculum
// events to a RabbitMQ exchange over AMQP 0-9-1.
//
// # Usage
//
// Build a sender using the fluent builder:
//
//	s, err := sender.NewSender().
//	    WithExchange("events").
//	    WithTopicMapping(sender.TopicMapping{
//	        Pattern:    "sensor/+deviceId/reading",
//	        RoutingKey: routingKeyFunc,
//	    }).
//	    WithConfirmMode(true).
//	    WithPersistent(true).
//	    Build()
//
// The sender requires an AMQP channel injected by client.Client after the
// connection is established:
//
//	s.SetChannel(ch)
//
// # Topic mapping
//
// Mappings are matched in declaration order against the vinculum topic; the
// first match wins. When no mapping matches, DefaultTopicTransform applies:
//   - DefaultTopicSlashToDot (default): replace "/" with "." in the vinculum topic
//   - DefaultTopicVerbatim: use the vinculum topic as the routing key unchanged
//   - DefaultTopicError: return an error from OnEvent
//   - DefaultTopicIgnore: silently drop the message
//
// # Payload serialization
//
// Payloads are serialized via wire.WireFormat (default: wire.Auto). The AMQP
// basic-properties content-type is set based on the wire format used.
// vinculum fields are encoded as entries in the AMQP basic-properties headers
// table (one entry per field).
package sender
