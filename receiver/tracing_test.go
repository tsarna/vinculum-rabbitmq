package receiver

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bus "github.com/tsarna/vinculum-bus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

var assertErr = errors.New("subscriber failed")

func attrMap(span sdktrace.ReadOnlySpan) map[string]string {
	out := make(map[string]string)
	for _, a := range span.Attributes() {
		out[string(a.Key)] = a.Value.Emit()
	}
	return out
}

func TestHandleDelivery_CreatesConsumerSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))

	sub := &fakeSubscriber{}
	r, err := NewReceiver().
		WithQueue("vinculum-events").
		WithSubscriber(sub).
		WithClientName("c1").
		WithTracerProvider(tp).
		Build()
	require.NoError(t, err)

	d, _ := newDelivery("ex", "sensor.abc.reading", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	spans := sr.Ended()
	require.Len(t, spans, 1)
	span := spans[0]
	assert.Equal(t, "process sensor/abc/reading", span.Name())
	assert.Equal(t, trace.SpanKindConsumer, span.SpanKind())

	attrs := attrMap(span)
	assert.Equal(t, "rabbitmq", attrs["messaging.system"])
	assert.Equal(t, "vinculum-events", attrs["messaging.destination.name"])
	assert.Equal(t, "sensor.abc.reading", attrs["messaging.rabbitmq.destination.routing_key"])
	assert.Equal(t, "process", attrs["messaging.operation.type"])
	assert.Equal(t, "process", attrs["messaging.operation.name"])
	assert.Equal(t, "c1", attrs["vinculum.client.name"])
}

func TestHandleDelivery_ConsumerSpanIsNewRootLinkedToProducer(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prop := propagation.TraceContext{}

	// Build a producer trace context and inject it into AMQP headers.
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(prop)
	defer otel.SetTextMapPropagator(prev)

	producerTracer := tp.Tracer("producer")
	producerCtx, producerSpan := producerTracer.Start(context.Background(), "producer")
	producerSC := producerSpan.SpanContext()
	producerSpan.End()

	headers := amqp.Table{}
	prop.Inject(producerCtx, newHeaderCarrier(headers))

	sub := &fakeSubscriber{}
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(sub).
		WithTracerProvider(tp).
		Build()
	require.NoError(t, err)

	d, _ := newDelivery("ex", "a.b", []byte("x"), headers)
	r.handleDelivery(context.Background(), d)

	// Find the consumer span (process …), ignoring the producer span.
	var consumer sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.SpanKind() == trace.SpanKindConsumer {
			consumer = s
			break
		}
	}
	require.NotNil(t, consumer, "consumer span should be recorded")

	// New root: the consumer's trace ID differs from the producer's.
	assert.NotEqual(t, producerSC.TraceID(), consumer.SpanContext().TraceID(),
		"consumer span must be a new trace root, not a child of the producer")

	// Linked: the consumer span carries a link to the producer span context.
	links := consumer.Links()
	require.Len(t, links, 1, "consumer span must link to the producer span")
	assert.Equal(t, producerSC.TraceID(), links[0].SpanContext.TraceID())
	assert.Equal(t, producerSC.SpanID(), links[0].SpanContext.SpanID())
}

func TestHandleDelivery_SubscriberErrorRecordedOnSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))

	sub := &fakeSubscriber{returnErr: assertErr}
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(sub).
		WithTracerProvider(tp).
		Build()
	require.NoError(t, err)

	d, _ := newDelivery("ex", "a.b", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	spans := sr.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "Error", spans[0].Status().Code.String())
}

// ctxCapturingSubscriber records the context passed to OnEvent so tests can
// inspect what propagated across the delivery boundary (baggage, span, etc.).
type ctxCapturingSubscriber struct {
	bus.BaseSubscriber
	lastCtx context.Context
}

func (c *ctxCapturingSubscriber) OnEvent(ctx context.Context, _ string, _ any, _ map[string]string) error {
	c.lastCtx = ctx
	return nil
}

func TestHandleDelivery_BaggagePropagatesToSubscriber(t *testing.T) {
	// Producer-side baggage written into the AMQP headers must be visible in
	// the context handed to subscriber.OnEvent, even though the consumer span
	// is a new trace root.
	prop := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(prop)
	defer otel.SetTextMapPropagator(prev)

	member, err := baggage.NewMember("tenant", "acme")
	require.NoError(t, err)
	bag, err := baggage.New(member)
	require.NoError(t, err)
	producerCtx := baggage.ContextWithBaggage(context.Background(), bag)

	headers := amqp.Table{}
	prop.Inject(producerCtx, newHeaderCarrier(headers))
	require.Contains(t, headers, "baggage")

	sub := &ctxCapturingSubscriber{}
	r, err := NewReceiver().WithQueue("q").WithSubscriber(sub).Build()
	require.NoError(t, err)

	d, _ := newDelivery("ex", "a.b", []byte("x"), headers)
	r.handleDelivery(context.Background(), d)

	require.NotNil(t, sub.lastCtx, "subscriber.OnEvent should have been called")
	got := baggage.FromContext(sub.lastCtx)
	assert.Equal(t, "acme", got.Member("tenant").Value(),
		"producer baggage must survive into the subscriber context")
}

// newHeaderCarrier wraps an amqp.Table as a TextMapCarrier for the test's
// producer-side inject. It mirrors the real carrier.Carrier without importing
// it (keeps this test focused on receiver behavior).
func newHeaderCarrier(t amqp.Table) propagation.TextMapCarrier {
	return headerCarrier(t)
}

type headerCarrier amqp.Table

func (h headerCarrier) Get(key string) string {
	if v, ok := h[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
func (h headerCarrier) Set(key, value string) { h[key] = value }
func (h headerCarrier) Keys() []string {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	return keys
}
