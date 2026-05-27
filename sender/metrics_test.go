package sender

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

func TestNewSenderMetrics_NilMeterReturnsNil(t *testing.T) {
	assert.Nil(t, NewSenderMetrics("client-x", nil))
}

func TestSenderMetrics_NilReceiverIsSafe(t *testing.T) {
	var m *SenderMetrics
	ctx := context.Background()
	require.NotPanics(t, func() {
		m.RecordPublish(ctx, "ex", 5*time.Millisecond, "")
		m.RecordPublish(ctx, "ex", 5*time.Millisecond, "nack")
		m.RecordReturned(ctx, "ex")
	})
}

func TestSenderMetrics_SuccessfulPublish(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	m := NewSenderMetrics("events", mp.Meter("test"))
	require.NotNil(t, m)

	ctx := context.Background()
	m.RecordPublish(ctx, "alerts", 10*time.Millisecond, "")
	m.RecordPublish(ctx, "alerts", 12*time.Millisecond, "")

	rm := metricdata.ResourceMetrics{}
	require.NoError(t, reader.Collect(ctx, &rm))

	sent := findSumPoint[int64](rm, "messaging.client.sent.messages")
	require.NotNil(t, sent)
	assert.Equal(t, int64(2), sent.Value)
	sentAttrs := sent.Attributes.ToSlice()
	assertAttrPresent(t, sentAttrs, "messaging.system", "rabbitmq")
	assertAttrPresent(t, sentAttrs, "messaging.destination.name", "alerts")
	assertAttrPresent(t, sentAttrs, "messaging.operation.name", "send")
	assertAttrPresent(t, sentAttrs, "vinculum.client.name", "events")
	assertAttrAbsent(t, sentAttrs, "error.type")
	// operation.type belongs only on the duration histogram, not the counter.
	assertAttrAbsent(t, sentAttrs, "messaging.operation.type")

	hist := findHistogramPoint[float64](rm, "messaging.client.operation.duration")
	require.NotNil(t, hist)
	assert.Equal(t, uint64(2), hist.Count)
	histAttrs := hist.Attributes.ToSlice()
	assertAttrPresent(t, histAttrs, "messaging.operation.name", "send")
	assertAttrPresent(t, histAttrs, "messaging.operation.type", "publish")
	assertAttrAbsent(t, histAttrs, "error.type")
}

func TestSenderMetrics_FailedPublish_SetsErrorType(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	m := NewSenderMetrics("events", mp.Meter("test"))
	require.NotNil(t, m)

	ctx := context.Background()
	m.RecordPublish(ctx, "alerts", 3*time.Millisecond, "nack")

	rm := metricdata.ResourceMetrics{}
	require.NoError(t, reader.Collect(ctx, &rm))

	// Both the counter and the histogram are recorded on failure, with
	// error.type set — the OTel way to track failures (no separate counter).
	sent := findSumPoint[int64](rm, "messaging.client.sent.messages")
	require.NotNil(t, sent)
	assert.Equal(t, int64(1), sent.Value)
	assertAttrPresent(t, sent.Attributes.ToSlice(), "error.type", "nack")

	hist := findHistogramPoint[float64](rm, "messaging.client.operation.duration")
	require.NotNil(t, hist)
	assert.Equal(t, uint64(1), hist.Count)
	assertAttrPresent(t, hist.Attributes.ToSlice(), "error.type", "nack")
}

func TestSenderMetrics_NoSeparateErrorsCounter(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	m := NewSenderMetrics("events", mp.Meter("test"))
	require.NotNil(t, m)

	ctx := context.Background()
	m.RecordPublish(ctx, "alerts", time.Millisecond, "publish")

	rm := metricdata.ResourceMetrics{}
	require.NoError(t, reader.Collect(ctx, &rm))

	assert.NotContains(t, collectNames(rm), "rabbitmq.publisher.errors",
		"the separate errors counter must be gone; failures live on error.type")
}

func TestSenderMetrics_Returned(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	m := NewSenderMetrics("events", mp.Meter("test"))
	require.NotNil(t, m)

	ctx := context.Background()
	m.RecordReturned(ctx, "alerts")

	rm := metricdata.ResourceMetrics{}
	require.NoError(t, reader.Collect(ctx, &rm))

	ret := findSumPoint[int64](rm, "rabbitmq.publisher.returned")
	require.NotNil(t, ret)
	assert.Equal(t, int64(1), ret.Value)
	assertAttrPresent(t, ret.Attributes.ToSlice(), "messaging.destination.name", "alerts")
}

// ─── shared test helpers ─────────────────────────────────────────────────────

func collectNames(rm metricdata.ResourceMetrics) []string {
	var out []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out = append(out, m.Name)
		}
	}
	return out
}

func findSumPoint[N int64 | float64](rm metricdata.ResourceMetrics, name string) *metricdata.DataPoint[N] {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if sum, ok := m.Data.(metricdata.Sum[N]); ok {
				if len(sum.DataPoints) > 0 {
					p := sum.DataPoints[0]
					return &p
				}
			}
		}
	}
	return nil
}

func findHistogramPoint[N int64 | float64](rm metricdata.ResourceMetrics, name string) *metricdata.HistogramDataPoint[N] {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if h, ok := m.Data.(metricdata.Histogram[N]); ok {
				if len(h.DataPoints) > 0 {
					p := h.DataPoints[0]
					return &p
				}
			}
		}
	}
	return nil
}

func assertAttrPresent(t *testing.T, attrs []attribute.KeyValue, key, value string) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key && a.Value.AsString() == value {
			return
		}
	}
	t.Errorf("attribute %s=%q not found in %v", key, value, attrs)
}

func assertAttrAbsent(t *testing.T, attrs []attribute.KeyValue, key string) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key {
			t.Errorf("attribute %s should be absent but was present as %v", key, a.Value.Emit())
		}
	}
}
