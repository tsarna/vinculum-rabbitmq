package sender

import "strings"

// DefaultTopicTransform controls fallback behaviour when no TopicMapping
// matches the incoming vinculum topic.
type DefaultTopicTransform int

const (
	// DefaultTopicSlashToDot replaces "/" with "." in the vinculum topic to
	// form the AMQP routing key. Natural for AMQP topic exchanges. This is
	// the default.
	DefaultTopicSlashToDot DefaultTopicTransform = iota

	// DefaultTopicVerbatim uses the vinculum topic as the routing key
	// unchanged. Slashes in routing keys may behave unexpectedly with topic
	// exchanges.
	DefaultTopicVerbatim

	// DefaultTopicError returns an error from OnEvent when no TopicMapping
	// matches.
	DefaultTopicError

	// DefaultTopicIgnore silently drops messages with no matching TopicMapping.
	DefaultTopicIgnore
)

// RoutingKeyFunc resolves the AMQP routing key for an outbound message matched
// by a TopicMapping. The topic and fields arguments reflect the vinculum
// event; extracted pattern fields are merged into fields before this call.
type RoutingKeyFunc func(topic string, msg any, fields map[string]string) (string, error)

// TopicMapping maps a vinculum topic pattern to AMQP delivery settings. The
// first mapping whose Pattern matches the inbound vinculum topic is used.
type TopicMapping struct {
	// Pattern is a vinculum topic pattern (supports + and # MQTT-style
	// wildcards, with optional field names e.g. "+deviceId").
	Pattern string

	// RoutingKeyFunc resolves the AMQP routing key per message. If nil,
	// DefaultTopicSlashToDot is applied to the vinculum topic.
	RoutingKeyFunc RoutingKeyFunc

	// Exchange overrides the sender-level exchange for messages matching
	// this pattern. An empty string means use the sender-level exchange.
	Exchange string

	// Persistent overrides the sender-level Persistent setting for messages
	// matching this pattern. nil means inherit from the sender.
	Persistent *bool
}

// slashToDot replaces "/" with "." in the given topic.
func slashToDot(topic string) string {
	return strings.ReplaceAll(topic, "/", ".")
}
