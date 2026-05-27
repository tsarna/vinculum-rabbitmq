package client

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ClientMetrics holds the OTel instruments for an rmqclient.Client.
// A nil *ClientMetrics is valid and results in no-op recording.
//
// Instruments:
//
//   - rabbitmq.client.connected       Float64Gauge   (1 = connected, 0 = disconnected)
//   - rabbitmq.client.reconnections   Int64Counter   (connection-level reconnection events)
//   - rabbitmq.client.channel_reopens Int64Counter   (channel-level recovery events)
//
// All instruments carry messaging.system="rabbitmq" and
// vinculum.client.name=<clientName>.
type ClientMetrics struct {
	connected      metric.Float64Gauge
	reconnects     metric.Int64Counter
	channelReopens metric.Int64Counter
	baseAttrs      metric.MeasurementOption
}

// NewClientMetrics creates a ClientMetrics using the given Meter. Returns nil
// if meter is nil, which is safe to call all methods on.
func NewClientMetrics(clientName string, meter metric.Meter) *ClientMetrics {
	if meter == nil {
		return nil
	}
	connected, _ := meter.Float64Gauge("rabbitmq.client.connected",
		metric.WithUnit("1"),
		metric.WithDescription("RabbitMQ client connection status (1=connected, 0=disconnected)"),
	)
	reconnects, _ := meter.Int64Counter("rabbitmq.client.reconnections",
		metric.WithUnit("{reconnection}"),
		metric.WithDescription("Connection-level reconnection events"),
	)
	channelReopens, _ := meter.Int64Counter("rabbitmq.client.channel_reopens",
		metric.WithUnit("{reopen}"),
		metric.WithDescription("Channel-level recovery events (a channel was re-opened without a full reconnect)"),
	)
	return &ClientMetrics{
		connected:      connected,
		reconnects:     reconnects,
		channelReopens: channelReopens,
		baseAttrs: metric.WithAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("vinculum.client.name", clientName),
		),
	}
}

// SetConnected sets the connected gauge to 1 (up) or 0 (down).
func (m *ClientMetrics) SetConnected(ctx context.Context, up bool) {
	if m == nil {
		return
	}
	v := 0.0
	if up {
		v = 1.0
	}
	m.connected.Record(ctx, v, m.baseAttrs)
}

// IncrReconnections increments the connection-level reconnection counter.
// Called after a successful reconnect.
func (m *ClientMetrics) IncrReconnections(ctx context.Context) {
	if m == nil {
		return
	}
	m.reconnects.Add(ctx, 1, m.baseAttrs)
}

// IncrChannelReopens increments the channel-level recovery counter. Called
// after a sender or receiver channel is re-opened on the same connection
// (without a full reconnect).
func (m *ClientMetrics) IncrChannelReopens(ctx context.Context) {
	if m == nil {
		return
	}
	m.channelReopens.Add(ctx, 1, m.baseAttrs)
}
