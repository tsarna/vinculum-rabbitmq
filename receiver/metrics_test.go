package receiver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestNewReceiverMetrics_NilMeterReturnsNil(t *testing.T) {
	assert.Nil(t, NewReceiverMetrics("client-x", nil))
}

func TestReceiverMetrics_NilReceiverIsSafe(t *testing.T) {
	var m *ReceiverMetrics
	ctx := context.Background()
	require.NotPanics(t, func() {
		m.RecordReceived(ctx, "q", "")
		m.RecordReceived(ctx, "q", "subscriber")
		m.RecordProcessDuration(ctx, "q", 5*time.Millisecond, "")
		m.RecordNack(ctx, "q")
	})
}

func TestReceiverMetrics_SuccessfulConsume(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	m := NewReceiverMetrics("events", mp.Meter("test"))
	require.NotNil(t, m)

	ctx := context.Background()
	m.RecordReceived(ctx, "q1", "")
	m.RecordReceived(ctx, "q1", "")
	m.RecordReceived(ctx, "q1", "")
	m.RecordProcessDuration(ctx, "q1", 7*time.Millisecond, "")

	rm := metricdata.ResourceMetrics{}
	require.NoError(t, reader.Collect(ctx, &rm))

	consumed := findReceiverSum(rm, "messaging.client.consumed.messages")
	require.NotNil(t, consumed)
	assert.Equal(t, int64(3), consumed.Value)
	cAttrs := consumed.Attributes.ToSlice()
	assertRAttr(t, cAttrs, "messaging.system", "rabbitmq")
	assertRAttr(t, cAttrs, "messaging.destination.name", "q1")
	assertRAttr(t, cAttrs, "messaging.operation.name", "receive")
	assertRAttr(t, cAttrs, "vinculum.client.name", "events")
	assertRAttrAbsent(t, cAttrs, "error.type")

	hist := findReceiverHist(rm, "messaging.process.duration")
	require.NotNil(t, hist)
	assert.Equal(t, uint64(1), hist.Count)
	assertRAttr(t, hist.Attributes.ToSlice(), "messaging.operation.name", "process")
}

func TestReceiverMetrics_FailedConsume_SetsErrorType(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	m := NewReceiverMetrics("events", mp.Meter("test"))
	require.NotNil(t, m)

	ctx := context.Background()
	m.RecordReceived(ctx, "q1", "subscriber")
	m.RecordProcessDuration(ctx, "q1", 2*time.Millisecond, "subscriber")

	rm := metricdata.ResourceMetrics{}
	require.NoError(t, reader.Collect(ctx, &rm))

	consumed := findReceiverSum(rm, "messaging.client.consumed.messages")
	require.NotNil(t, consumed)
	assertRAttr(t, consumed.Attributes.ToSlice(), "error.type", "subscriber")

	hist := findReceiverHist(rm, "messaging.process.duration")
	require.NotNil(t, hist)
	assertRAttr(t, hist.Attributes.ToSlice(), "error.type", "subscriber")
}

func TestReceiverMetrics_NoSeparateErrorsCounter(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	m := NewReceiverMetrics("events", mp.Meter("test"))
	require.NotNil(t, m)

	ctx := context.Background()
	m.RecordReceived(ctx, "q1", "subscriber")

	rm := metricdata.ResourceMetrics{}
	require.NoError(t, reader.Collect(ctx, &rm))

	var names []string
	for _, sm := range rm.ScopeMetrics {
		for _, mm := range sm.Metrics {
			names = append(names, mm.Name)
		}
	}
	assert.NotContains(t, names, "rabbitmq.consumer.errors",
		"the separate errors counter must be gone; failures live on error.type")
}

func TestReceiverMetrics_Nacks(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	m := NewReceiverMetrics("events", mp.Meter("test"))
	require.NotNil(t, m)

	ctx := context.Background()
	m.RecordNack(ctx, "q1")
	m.RecordNack(ctx, "q1")

	rm := metricdata.ResourceMetrics{}
	require.NoError(t, reader.Collect(ctx, &rm))

	nacks := findReceiverSum(rm, "rabbitmq.consumer.nacks")
	require.NotNil(t, nacks)
	assert.Equal(t, int64(2), nacks.Value)
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func findReceiverSum(rm metricdata.ResourceMetrics, name string) *metricdata.DataPoint[int64] {
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

func findReceiverHist(rm metricdata.ResourceMetrics, name string) *metricdata.HistogramDataPoint[float64] {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if h, ok := m.Data.(metricdata.Histogram[float64]); ok && len(h.DataPoints) > 0 {
				p := h.DataPoints[0]
				return &p
			}
		}
	}
	return nil
}

func assertRAttr(t *testing.T, attrs []attribute.KeyValue, key, value string) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key && a.Value.AsString() == value {
			return
		}
	}
	t.Errorf("attribute %s=%q not found in %v", key, value, attrs)
}

func assertRAttrAbsent(t *testing.T, attrs []attribute.KeyValue, key string) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key {
			t.Errorf("attribute %s should be absent but was present as %v", key, a.Value.Emit())
		}
	}
}
