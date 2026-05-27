package receiver

import (
	"errors"
	"fmt"

	bus "github.com/tsarna/vinculum-bus"
	wire "github.com/tsarna/vinculum-wire"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// ReceiverBuilder constructs an RMQReceiver with a fluent API.
type ReceiverBuilder struct {
	clientName     string
	queue          string
	subscriber     bus.Subscriber
	subscriptions  []Subscription
	defaultXform   DefaultRoutingKeyTransform
	prefetch       int
	exclusive      bool
	autoAck        bool
	wireFormat     wire.WireFormat
	consumerTag    string
	declare        *Declare
	bindings       []Binding
	logger         *zap.Logger
	meterProvider  metric.MeterProvider
	tracerProvider trace.TracerProvider
}

// NewReceiver returns a ReceiverBuilder with default settings:
// default_routing_key_transform=dot_to_slash, prefetch=10, auto_ack=false.
func NewReceiver() *ReceiverBuilder {
	return &ReceiverBuilder{
		defaultXform: DefaultRKDotToSlash,
		prefetch:     10,
		logger:       zap.NewNop(),
	}
}

// WithClientName sets the vinculum client name used in metric attributes.
func (b *ReceiverBuilder) WithClientName(name string) *ReceiverBuilder {
	b.clientName = name
	return b
}

// WithQueue sets the AMQP queue to consume from (required).
func (b *ReceiverBuilder) WithQueue(queue string) *ReceiverBuilder {
	b.queue = queue
	return b
}

// WithSubscriber sets the vinculum bus.Subscriber that receives dispatched
// events (required).
func (b *ReceiverBuilder) WithSubscriber(s bus.Subscriber) *ReceiverBuilder {
	b.subscriber = s
	return b
}

// WithSubscription appends a routing-key subscription. Subscriptions are
// evaluated against each delivery's routing key in declaration order; first
// match wins.
func (b *ReceiverBuilder) WithSubscription(sub Subscription) *ReceiverBuilder {
	b.subscriptions = append(b.subscriptions, sub)
	return b
}

// WithDefaultTransform sets the fallback behaviour when no Subscription
// matches the delivery's routing key.
func (b *ReceiverBuilder) WithDefaultTransform(t DefaultRoutingKeyTransform) *ReceiverBuilder {
	b.defaultXform = t
	return b
}

// WithPrefetch sets the AMQP basic.qos prefetch count (default 10). 0 means
// unlimited: the broker will push the entire queue contents to the consumer
// in one burst, which can exhaust memory — avoid it outside small or
// short-lived queues.
func (b *ReceiverBuilder) WithPrefetch(n int) *ReceiverBuilder {
	b.prefetch = n
	return b
}

// WithExclusive sets the AMQP exclusive consumer flag. When true, only one
// consumer may be active on the queue at a time.
func (b *ReceiverBuilder) WithExclusive(v bool) *ReceiverBuilder {
	b.exclusive = v
	return b
}

// WithAutoAck sets the AMQP auto_ack consumer flag. When true, the broker
// considers messages delivered as soon as they are sent over TCP (lossy on
// crash). Default: false (manual ack after subscriber.OnEvent returns).
func (b *ReceiverBuilder) WithAutoAck(v bool) *ReceiverBuilder {
	b.autoAck = v
	return b
}

// WithWireFormat sets the wire format used to deserialize inbound payloads.
func (b *ReceiverBuilder) WithWireFormat(f wire.WireFormat) *ReceiverBuilder {
	b.wireFormat = f
	return b
}

// WithWireFormatName sets the wire format by name (e.g. "json", "auto").
func (b *ReceiverBuilder) WithWireFormatName(name string) *ReceiverBuilder {
	b.wireFormat = wire.ByName(name)
	return b
}

// WithConsumerTag sets the AMQP consumer tag. Empty (default) means the
// broker assigns one.
func (b *ReceiverBuilder) WithConsumerTag(tag string) *ReceiverBuilder {
	b.consumerTag = tag
	return b
}

// WithDeclare configures an active queue declaration. When set, DeclareTopology
// will create the queue (if it does not exist) with the given parameters on
// every connect / channel recovery. When unset (the default), DeclareTopology
// performs a passive declare that fails fast if the queue is missing.
func (b *ReceiverBuilder) WithDeclare(d Declare) *ReceiverBuilder {
	b.declare = &d
	return b
}

// WithBinding appends a queue→exchange binding to declare. DeclareTopology
// runs each binding on every connect / channel recovery. Bindings are
// idempotent at the broker.
func (b *ReceiverBuilder) WithBinding(binding Binding) *ReceiverBuilder {
	b.bindings = append(b.bindings, binding)
	return b
}

// WithLogger sets the logger.
func (b *ReceiverBuilder) WithLogger(l *zap.Logger) *ReceiverBuilder {
	if l != nil {
		b.logger = l
	}
	return b
}

// WithMeterProvider sets the OTel MeterProvider. nil disables metrics.
func (b *ReceiverBuilder) WithMeterProvider(p metric.MeterProvider) *ReceiverBuilder {
	b.meterProvider = p
	return b
}

// WithTracerProvider sets the OTel TracerProvider used to create process
// spans. nil means the global TracerProvider is used.
func (b *ReceiverBuilder) WithTracerProvider(tp trace.TracerProvider) *ReceiverBuilder {
	b.tracerProvider = tp
	return b
}

// Build validates configuration and returns an RMQReceiver.
func (b *ReceiverBuilder) Build() (*RMQReceiver, error) {
	if b.queue == "" {
		return nil, errors.New("rabbitmq receiver: queue is required")
	}
	if b.subscriber == nil {
		return nil, errors.New("rabbitmq receiver: subscriber is required")
	}
	for i, s := range b.subscriptions {
		if s.RoutingKeyPattern == "" {
			return nil, fmt.Errorf("rabbitmq receiver: subscription %d has empty RoutingKeyPattern", i)
		}
	}
	if b.wireFormat == nil {
		b.wireFormat = wire.Auto
	}
	if b.logger == nil {
		b.logger = zap.NewNop()
	}
	var meter metric.Meter
	if b.meterProvider != nil {
		meter = b.meterProvider.Meter("github.com/tsarna/vinculum-rabbitmq/receiver")
	}
	return &RMQReceiver{
		clientName:     b.clientName,
		queue:          b.queue,
		subscriber:     b.subscriber,
		subscriptions:  b.subscriptions,
		defaultXform:   b.defaultXform,
		prefetch:       b.prefetch,
		exclusive:      b.exclusive,
		autoAck:        b.autoAck,
		wireFormat:     b.wireFormat,
		consumerTag:    b.consumerTag,
		declare:        b.declare,
		bindings:       b.bindings,
		logger:         b.logger,
		meterProvider:  b.meterProvider,
		tracerProvider: b.tracerProvider,
		metrics:        NewReceiverMetrics(b.clientName, meter),
	}, nil
}
