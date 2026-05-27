package receiver

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ReceiverMetrics holds the OTel instruments for an RMQReceiver.
// A nil *ReceiverMetrics is valid and results in no-op recording.
//
// Instruments follow the OTel messaging semantic conventions (semconv v1.26+):
//
//   - messaging.client.consumed.messages Int64Counter     (one per delivery pulled from the broker)
//   - messaging.process.duration         Float64Histogram (subscriber.OnEvent duration in seconds)
//
// Failures are recorded on these same instruments with the error.type
// attribute set (the OTel convention) rather than via a separate error
// counter. consumed.messages is recorded for every delivery (with error.type
// on failure); process.duration is recorded whenever the message reaches
// subscriber.OnEvent (success or failure).
//
// One RabbitMQ-specific instrument supplements the standard set:
//
//   - rabbitmq.consumer.nacks            Int64Counter     (Nack-without-requeue actions)
//
// All instruments carry messaging.system="rabbitmq",
// messaging.destination.name=<queue>, and vinculum.client.name=<clientName>.
// consumed.messages carries messaging.operation.name="receive";
// process.duration carries messaging.operation.name="process".
type ReceiverMetrics struct {
	consumed  metric.Int64Counter
	duration  metric.Float64Histogram
	nacks     metric.Int64Counter
	clientTag attribute.KeyValue
}

// NewReceiverMetrics creates a ReceiverMetrics using the given Meter.
// Returns nil if meter is nil, which is safe to call all methods on.
func NewReceiverMetrics(clientName string, meter metric.Meter) *ReceiverMetrics {
	if meter == nil {
		return nil
	}
	consumed, _ := meter.Int64Counter("messaging.client.consumed.messages",
		metric.WithUnit("{message}"),
		metric.WithDescription("Number of messages pulled from the broker and delivered to the application"),
	)
	dur, _ := meter.Float64Histogram("messaging.process.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of subscriber.OnEvent for RabbitMQ deliveries"),
	)
	nacks, _ := meter.Int64Counter("rabbitmq.consumer.nacks",
		metric.WithUnit("{message}"),
		metric.WithDescription("Messages nacked (without requeue) due to processing errors"),
	)
	return &ReceiverMetrics{
		consumed:  consumed,
		duration:  dur,
		nacks:     nacks,
		clientTag: attribute.String("vinculum.client.name", clientName),
	}
}

// queueAttrs builds the common attribute set for receiver records. errType is
// "" on success; on failure it is a short, low-cardinality category recorded
// as the OTel error.type attribute. opName is the messaging.operation.name
// value ("receive" for consumed.messages, "process" for process.duration).
func (m *ReceiverMetrics) queueAttrs(queue, opName, errType string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 5)
	attrs = append(attrs,
		attribute.String("messaging.system", "rabbitmq"),
		attribute.String("messaging.destination.name", queue),
		attribute.String("messaging.operation.name", opName),
		m.clientTag,
	)
	if errType != "" {
		attrs = append(attrs, attribute.String("error.type", errType))
	}
	return attrs
}

// RecordReceived increments messaging.client.consumed.messages for one
// delivery pulled from the broker. errType is "" on success; on failure it is
// the OTel error.type category (e.g. "subscriber", "no_subscription").
func (m *ReceiverMetrics) RecordReceived(ctx context.Context, queue, errType string) {
	if m == nil {
		return
	}
	m.consumed.Add(ctx, 1, metric.WithAttributes(m.queueAttrs(queue, "receive", errType)...))
}

// RecordProcessDuration records messaging.process.duration for one
// subscriber.OnEvent call. errType is "" on success; on failure it is the
// OTel error.type category.
func (m *ReceiverMetrics) RecordProcessDuration(ctx context.Context, queue string, d time.Duration, errType string) {
	if m == nil {
		return
	}
	m.duration.Record(ctx, d.Seconds(), metric.WithAttributes(m.queueAttrs(queue, "process", errType)...))
}

// RecordNack increments rabbitmq.consumer.nacks. Called whenever a delivery
// is Nack'd (without requeue).
func (m *ReceiverMetrics) RecordNack(ctx context.Context, queue string) {
	if m == nil {
		return
	}
	m.nacks.Add(ctx, 1, metric.WithAttributes(
		attribute.String("messaging.system", "rabbitmq"),
		attribute.String("messaging.destination.name", queue),
		m.clientTag,
	))
}
