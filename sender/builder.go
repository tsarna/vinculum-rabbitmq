package sender

import (
	"fmt"

	wire "github.com/tsarna/vinculum-wire"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// SenderBuilder constructs an RMQSender with a fluent API.
type SenderBuilder struct {
	clientName     string
	exchange       string
	topicMappings  []TopicMapping
	defaultXform   DefaultTopicTransform
	mandatory      bool
	persistent     bool
	confirmMode    bool
	wireFormat     wire.WireFormat
	logger         *zap.Logger
	meterProvider  metric.MeterProvider
	tracerProvider trace.TracerProvider
}

// NewSender returns a SenderBuilder with default settings:
// default_topic_transform=slash_to_dot, persistent=true, mandatory=false,
// confirm_mode=false. (The VCL layer defaults confirm_mode to true; direct
// Go-API callers must opt in.)
func NewSender() *SenderBuilder {
	return &SenderBuilder{
		defaultXform: DefaultTopicSlashToDot,
		persistent:   true,
		mandatory:    false,
		confirmMode:  false,
		logger:       zap.NewNop(),
	}
}

// WithClientName sets the vinculum client name used in metric attributes.
func (b *SenderBuilder) WithClientName(name string) *SenderBuilder {
	b.clientName = name
	return b
}

// WithExchange sets the sender-level AMQP exchange to publish to. An empty
// string is valid and routes to the RabbitMQ default exchange (which delivers
// to the queue named by the routing key).
func (b *SenderBuilder) WithExchange(exchange string) *SenderBuilder {
	b.exchange = exchange
	return b
}

// WithTopicMapping appends a topic mapping. Mappings are evaluated in
// declaration order against the inbound vinculum topic; first match wins.
func (b *SenderBuilder) WithTopicMapping(tm TopicMapping) *SenderBuilder {
	b.topicMappings = append(b.topicMappings, tm)
	return b
}

// WithDefaultTransform sets the fallback behaviour when no topic mapping
// matches.
func (b *SenderBuilder) WithDefaultTransform(t DefaultTopicTransform) *SenderBuilder {
	b.defaultXform = t
	return b
}

// WithMandatory sets the AMQP mandatory flag for publishes. When true,
// unroutable messages are returned by the broker (and surfaced as publish
// errors).
func (b *SenderBuilder) WithMandatory(mandatory bool) *SenderBuilder {
	b.mandatory = mandatory
	return b
}

// WithPersistent sets the AMQP delivery mode. true = persistent (delivery
// mode 2), false = transient (delivery mode 1). Default: true.
func (b *SenderBuilder) WithPersistent(persistent bool) *SenderBuilder {
	b.persistent = persistent
	return b
}

// WithConfirmMode enables RabbitMQ publisher confirms. When true, the channel
// is put into confirm mode on SetChannel, and OnEvent blocks until the broker
// acks (or nacks) each publish. A Nack surfaces as an OnEvent error.
//
// Default: false. The VCL layer enables this by default.
func (b *SenderBuilder) WithConfirmMode(v bool) *SenderBuilder {
	b.confirmMode = v
	return b
}

// WithWireFormat sets the wire format used to serialize outbound payloads.
func (b *SenderBuilder) WithWireFormat(f wire.WireFormat) *SenderBuilder {
	b.wireFormat = f
	return b
}

// WithWireFormatName sets the wire format by name (e.g. "json", "auto").
func (b *SenderBuilder) WithWireFormatName(name string) *SenderBuilder {
	b.wireFormat = wire.ByName(name)
	return b
}

// WithLogger sets the logger.
func (b *SenderBuilder) WithLogger(l *zap.Logger) *SenderBuilder {
	if l != nil {
		b.logger = l
	}
	return b
}

// WithMeterProvider sets the OTel MeterProvider. nil disables metrics.
func (b *SenderBuilder) WithMeterProvider(p metric.MeterProvider) *SenderBuilder {
	b.meterProvider = p
	return b
}

// WithTracerProvider sets the OTel TracerProvider used to create send spans.
// When nil, the global TracerProvider is used.
func (b *SenderBuilder) WithTracerProvider(tp trace.TracerProvider) *SenderBuilder {
	b.tracerProvider = tp
	return b
}

// Build returns an RMQSender. The sender starts disconnected; call SetChannel
// before publishing events (client.Client does this on connect).
func (b *SenderBuilder) Build() (*RMQSender, error) {
	if b.wireFormat == nil {
		b.wireFormat = wire.Auto
	}
	if b.logger == nil {
		b.logger = zap.NewNop()
	}
	for i, m := range b.topicMappings {
		if m.Pattern == "" {
			return nil, fmt.Errorf("rabbitmq sender: topic mapping %d has empty Pattern", i)
		}
	}
	var meter metric.Meter
	if b.meterProvider != nil {
		meter = b.meterProvider.Meter("github.com/tsarna/vinculum-rabbitmq/sender")
	}
	return &RMQSender{
		clientName:     b.clientName,
		exchange:       b.exchange,
		topicMappings:  b.topicMappings,
		defaultXform:   b.defaultXform,
		mandatory:      b.mandatory,
		persistent:     b.persistent,
		confirmMode:    b.confirmMode,
		wireFormat:     b.wireFormat,
		logger:         b.logger,
		meterProvider:  b.meterProvider,
		tracerProvider: b.tracerProvider,
		metrics:        NewSenderMetrics(b.clientName, meter),
	}, nil
}
