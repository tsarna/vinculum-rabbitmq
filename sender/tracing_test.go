package sender

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// withGlobalPropagator temporarily installs prop as the global text-map
// propagator for the duration of fn, restoring the previous one afterward.
func withGlobalPropagator(t *testing.T, prop propagation.TextMapPropagator, fn func()) {
	t.Helper()
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(prop)
	defer otel.SetTextMapPropagator(prev)
	fn()
}

// attrMap flattens a recorded span's attributes into a map for assertions.
func attrMap(span sdktrace.ReadOnlySpan) map[string]string {
	out := make(map[string]string)
	for _, a := range span.Attributes() {
		out[string(a.Key)] = a.Value.Emit()
	}
	return out
}

func TestOnEvent_InjectsTraceContextIntoHeaders(t *testing.T) {
	// Use a real tracer provider + W3C propagator so Inject writes a
	// traceparent into the AMQP headers.
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prop := propagation.TraceContext{}

	s, err := NewSender().
		WithExchange("events").
		WithClientName("c1").
		WithTracerProvider(tp).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	s.setChannel(fc)

	// Run inside a parent span so there is a trace context to propagate.
	tracer := tp.Tracer("test")
	ctx, parent := tracer.Start(context.Background(), "parent")
	defer parent.End()

	// Temporarily install the propagator as global so the sender's
	// otel.GetTextMapPropagator() picks it up.
	withGlobalPropagator(t, prop, func() {
		require.NoError(t, s.OnEvent(ctx, "sensor/abc/reading", "payload", nil))
	})

	require.NotNil(t, fc.lastPub.Headers)
	assert.Contains(t, fc.lastPub.Headers, "traceparent",
		"sender must inject the W3C trace context into the AMQP headers")
}

func TestOnEvent_CreatesProducerSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))

	s, err := NewSender().
		WithExchange("events").
		WithClientName("c1").
		WithTracerProvider(tp).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	s.setChannel(fc)

	require.NoError(t, s.OnEvent(context.Background(), "sensor/abc/reading", "payload", nil))

	spans := sr.Ended()
	require.Len(t, spans, 1)
	span := spans[0]
	assert.Equal(t, "send events", span.Name())
	assert.Equal(t, trace.SpanKindProducer, span.SpanKind())

	attrs := attrMap(span)
	assert.Equal(t, "rabbitmq", attrs["messaging.system"])
	assert.Equal(t, "events", attrs["messaging.destination.name"])
	assert.Equal(t, "sensor.abc.reading", attrs["messaging.rabbitmq.destination.routing_key"])
	assert.Equal(t, "publish", attrs["messaging.operation.type"])
	assert.Equal(t, "send", attrs["messaging.operation.name"])
	assert.Equal(t, "c1", attrs["vinculum.client.name"])
}

func TestOnEvent_SpanRecordsErrorOnNack(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))

	s, err := NewSender().
		WithExchange("e").
		WithConfirmMode(true).
		WithTracerProvider(tp).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{nextAck: false} // broker nacks
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "t", "m", nil)
	require.Error(t, err)

	spans := sr.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "Error", spans[0].Status().Code.String())
}
