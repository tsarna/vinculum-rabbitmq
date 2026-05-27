package sender

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// SenderMetrics holds the OTel instruments for an RMQSender.
// A nil *SenderMetrics is valid and results in no-op recording.
//
// Instruments follow the OTel messaging semantic conventions (semconv v1.26+):
//
//   - messaging.client.sent.messages      Int64Counter     (one per publish attempt)
//   - messaging.client.operation.duration Float64Histogram (publish duration in seconds,
//                                                            including confirm RTT in confirm mode)
//
// Failures are recorded on these same instruments with the error.type
// attribute set (the OTel convention) rather than via a separate error
// counter. The duration histogram and the message counter are both recorded
// on success and on failure.
//
// One RabbitMQ-specific instrument supplements the standard set:
//
//   - rabbitmq.publisher.returned         Int64Counter     (mandatory-returned messages)
//
// All instruments carry messaging.system="rabbitmq",
// messaging.destination.name=<exchange>, messaging.operation.name="send",
// and vinculum.client.name=<clientName>. operation.duration additionally
// carries messaging.operation.type="publish".
type SenderMetrics struct {
	sent      metric.Int64Counter
	duration  metric.Float64Histogram
	returned  metric.Int64Counter
	clientTag attribute.KeyValue
}

// NewSenderMetrics creates a SenderMetrics using the given Meter. Returns nil
// if meter is nil, which is safe to call all methods on.
func NewSenderMetrics(clientName string, meter metric.Meter) *SenderMetrics {
	if meter == nil {
		return nil
	}
	sent, _ := meter.Int64Counter("messaging.client.sent.messages",
		metric.WithUnit("{message}"),
		metric.WithDescription("Number of messages the producer attempted to send to the broker"),
	)
	dur, _ := meter.Float64Histogram("messaging.client.operation.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of RabbitMQ publish operations (includes confirm RTT in confirm mode)"),
	)
	ret, _ := meter.Int64Counter("rabbitmq.publisher.returned",
		metric.WithUnit("{message}"),
		metric.WithDescription("Mandatory messages returned by the broker (no binding matched)"),
	)
	return &SenderMetrics{
		sent:      sent,
		duration:  dur,
		returned:  ret,
		clientTag: attribute.String("vinculum.client.name", clientName),
	}
}

// sendAttrs builds the common attribute set for sender records. errType is ""
// for a successful publish; on failure it is a short, low-cardinality category
// recorded as the OTel error.type attribute. extra holds instrument-specific
// attributes (e.g. messaging.operation.type on the duration histogram).
func (m *SenderMetrics) sendAttrs(exchange, errType string, extra ...attribute.KeyValue) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 6)
	attrs = append(attrs,
		attribute.String("messaging.system", "rabbitmq"),
		attribute.String("messaging.destination.name", exchange),
		attribute.String("messaging.operation.name", "send"),
		m.clientTag,
	)
	if errType != "" {
		attrs = append(attrs, attribute.String("error.type", errType))
	}
	attrs = append(attrs, extra...)
	return attrs
}

// RecordPublish records one publish attempt: it increments
// messaging.client.sent.messages and records messaging.client.operation.duration.
// errType is "" for a successful publish; on failure it is the OTel error.type
// category (e.g. "nack", "publish", "serialize").
func (m *SenderMetrics) RecordPublish(ctx context.Context, exchange string, d time.Duration, errType string) {
	if m == nil {
		return
	}
	m.sent.Add(ctx, 1, metric.WithAttributes(m.sendAttrs(exchange, errType)...))
	m.duration.Record(ctx, d.Seconds(), metric.WithAttributes(
		m.sendAttrs(exchange, errType, attribute.String("messaging.operation.type", "publish"))...,
	))
}

// RecordReturned increments the rabbitmq.publisher.returned counter. Called by
// the returns watcher goroutine when the broker returns a mandatory-flagged
// unroutable message.
func (m *SenderMetrics) RecordReturned(ctx context.Context, exchange string) {
	if m == nil {
		return
	}
	m.returned.Add(ctx, 1, metric.WithAttributes(
		attribute.String("messaging.system", "rabbitmq"),
		attribute.String("messaging.destination.name", exchange),
		m.clientTag,
	))
}
