package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestNewClientMetrics_NilMeterReturnsNil(t *testing.T) {
	assert.Nil(t, NewClientMetrics("client-x", nil))
}

func TestClientMetrics_NilReceiverIsSafe(t *testing.T) {
	var m *ClientMetrics
	ctx := context.Background()
	require.NotPanics(t, func() {
		m.SetConnected(ctx, true)
		m.SetConnected(ctx, false)
		m.IncrReconnections(ctx)
		m.IncrChannelReopens(ctx)
	})
}

func TestClientMetrics_Instruments_Fire(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	meter := mp.Meter("test")

	m := NewClientMetrics("events", meter)
	require.NotNil(t, m)

	ctx := context.Background()
	m.SetConnected(ctx, true)
	m.IncrReconnections(ctx)
	m.IncrReconnections(ctx)
	m.IncrChannelReopens(ctx)

	rm := metricdata.ResourceMetrics{}
	require.NoError(t, reader.Collect(ctx, &rm))

	// Connected should be the last-set value: 1.0.
	connected := findGaugeFloat64(rm, "rabbitmq.client.connected")
	require.NotNil(t, connected)
	assert.Equal(t, 1.0, connected.Value)
	assertGaugeHasAttr(t, *connected, "messaging.system", "rabbitmq")
	assertGaugeHasAttr(t, *connected, "vinculum.client.name", "events")

	reconnects := findClientSumInt64(rm, "rabbitmq.client.reconnections")
	require.NotNil(t, reconnects)
	assert.Equal(t, int64(2), reconnects.Value)

	reopens := findClientSumInt64(rm, "rabbitmq.client.channel_reopens")
	require.NotNil(t, reopens)
	assert.Equal(t, int64(1), reopens.Value)
}

func TestClientMetrics_SetConnected_TogglesValue(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	meter := mp.Meter("test")
	m := NewClientMetrics("events", meter)
	require.NotNil(t, m)

	ctx := context.Background()
	m.SetConnected(ctx, true)
	m.SetConnected(ctx, false)

	rm := metricdata.ResourceMetrics{}
	require.NoError(t, reader.Collect(ctx, &rm))

	g := findGaugeFloat64(rm, "rabbitmq.client.connected")
	require.NotNil(t, g)
	assert.Equal(t, 0.0, g.Value)
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func findGaugeFloat64(rm metricdata.ResourceMetrics, name string) *metricdata.DataPoint[float64] {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if g, ok := m.Data.(metricdata.Gauge[float64]); ok && len(g.DataPoints) > 0 {
				p := g.DataPoints[0]
				return &p
			}
		}
	}
	return nil
}

func findClientSumInt64(rm metricdata.ResourceMetrics, name string) *metricdata.DataPoint[int64] {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok && len(sum.DataPoints) > 0 {
				p := sum.DataPoints[0]
				return &p
			}
		}
	}
	return nil
}

func assertGaugeHasAttr(t *testing.T, p metricdata.DataPoint[float64], key, value string) {
	t.Helper()
	for _, a := range p.Attributes.ToSlice() {
		if string(a.Key) == key && a.Value.AsString() == value {
			return
		}
	}
	t.Errorf("attribute %s=%q not found in %v", key, value, p.Attributes.ToSlice())
}
