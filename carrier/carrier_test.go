package carrier

import (
	"context"
	"sort"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestCarrier_GetSet_RoundTrip(t *testing.T) {
	table := amqp.Table{"existing": "value-1"}
	c := New(table)

	assert.Equal(t, "value-1", c.Get("existing"))
	assert.Equal(t, "", c.Get("missing"))

	c.Set("new", "value-2")
	assert.Equal(t, "value-2", c.Get("new"))
	assert.Equal(t, "value-2", table["new"], "Set mutates the wrapped table")
}

func TestCarrier_New_Nil_AllocatesEmptyTable(t *testing.T) {
	c := New(nil)
	require.NotNil(t, c.Table())
	c.Set("k", "v")
	assert.Equal(t, "v", c.Get("k"))
}

func TestCarrier_Keys(t *testing.T) {
	c := New(amqp.Table{"a": "1", "b": "2", "c": "3"})
	got := c.Keys()
	sort.Strings(got)
	assert.Equal(t, []string{"a", "b", "c"}, got)
}

func TestCarrier_Get_NonStringTypes(t *testing.T) {
	c := New(amqp.Table{
		"str":    "hello",
		"bytes":  []byte("world"),
		"int32":  int32(42),
		"int64":  int64(100),
		"bool":   true,
		"float":  1.5,
	})
	assert.Equal(t, "hello", c.Get("str"))
	assert.Equal(t, "world", c.Get("bytes"))
	assert.Equal(t, "42", c.Get("int32"))
	assert.Equal(t, "100", c.Get("int64"))
	assert.Equal(t, "true", c.Get("bool"))
	assert.Equal(t, "1.5", c.Get("float"))
}

func TestCarrier_TraceContext_Inject_Extract_RoundTrip(t *testing.T) {
	// Exercises the full propagator round-trip — the actual use case
	// for this carrier in the sender and receiver.
	propagator := propagation.TraceContext{}

	tp := sdktrace.NewTracerProvider()
	tracer := tp.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "producer")
	defer span.End()
	origSC := span.SpanContext()

	headers := amqp.Table{}
	propagator.Inject(ctx, New(headers))

	// The propagator should have written a traceparent into the table.
	require.Contains(t, headers, "traceparent",
		"propagator should inject traceparent into the amqp.Table")

	// Now extract on the receiver side.
	extractedCtx := propagator.Extract(context.Background(), New(headers))
	extractedSC := trace.SpanContextFromContext(extractedCtx)

	require.True(t, extractedSC.IsValid())
	assert.Equal(t, origSC.TraceID(), extractedSC.TraceID(),
		"trace ID survives round-trip through amqp.Table")
	assert.Equal(t, origSC.SpanID(), extractedSC.SpanID())
}

func TestCarrier_Baggage_Propagates(t *testing.T) {
	// Composite propagator (TraceContext + Baggage) — same shape vinculum
	// uses globally.
	prop := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)

	member, err := baggage.NewMember("tenant", "acme")
	require.NoError(t, err)
	bag, err := baggage.New(member)
	require.NoError(t, err)
	ctx := baggage.ContextWithBaggage(context.Background(), bag)

	headers := amqp.Table{}
	prop.Inject(ctx, New(headers))
	assert.Contains(t, headers, "baggage", "Baggage propagator should write a baggage header")

	extracted := prop.Extract(context.Background(), New(headers))
	got := baggage.FromContext(extracted)
	assert.Equal(t, "acme", got.Member("tenant").Value())
}
