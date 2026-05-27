package receiver

import (
	"context"
	"errors"
	"sync"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bus "github.com/tsarna/vinculum-bus"
	wire "github.com/tsarna/vinculum-wire"
)

// fakeSubscriber captures the most recent OnEvent call and returns the
// configured error.
type fakeSubscriber struct {
	bus.BaseSubscriber
	mu        sync.Mutex
	calls     int
	lastTopic string
	lastMsg   any
	lastFlds  map[string]string
	returnErr error
}

func (f *fakeSubscriber) OnEvent(_ context.Context, topic string, msg any, fields map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastTopic = topic
	f.lastMsg = msg
	f.lastFlds = fields
	return f.returnErr
}

// fakeAcknowledger satisfies amqp.Acknowledger and records calls.
type fakeAcknowledger struct {
	mu       sync.Mutex
	acks     int
	nacks    int
	rejects  int
	requeue  bool
	multiple bool
}

func (a *fakeAcknowledger) Ack(_ uint64, multiple bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.acks++
	a.multiple = multiple
	return nil
}

func (a *fakeAcknowledger) Nack(_ uint64, multiple, requeue bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nacks++
	a.multiple = multiple
	a.requeue = requeue
	return nil
}

func (a *fakeAcknowledger) Reject(_ uint64, requeue bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.rejects++
	a.requeue = requeue
	return nil
}

func newDelivery(exchange, routingKey string, body []byte, headers amqp.Table) (amqp.Delivery, *fakeAcknowledger) {
	a := &fakeAcknowledger{}
	return amqp.Delivery{
		Acknowledger: a,
		Exchange:     exchange,
		RoutingKey:   routingKey,
		Body:         body,
		Headers:      headers,
		DeliveryTag:  1,
	}, a
}

func TestBuilder_RequiresQueue(t *testing.T) {
	_, err := NewReceiver().WithSubscriber(&fakeSubscriber{}).Build()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "queue is required")
}

func TestBuilder_RequiresSubscriber(t *testing.T) {
	_, err := NewReceiver().WithQueue("q").Build()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "subscriber is required")
}

func TestBuilder_RejectsEmptyPattern(t *testing.T) {
	_, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(&fakeSubscriber{}).
		WithSubscription(Subscription{RoutingKeyPattern: ""}).
		Build()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty RoutingKeyPattern")
}

func TestHandleDelivery_DotToSlashByDefault(t *testing.T) {
	sub := &fakeSubscriber{}
	r, err := NewReceiver().WithQueue("q").WithSubscriber(sub).Build()
	require.NoError(t, err)

	d, ack := newDelivery("ex", "sensor.abc.reading", []byte(`{"v":1}`), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, 1, sub.calls)
	assert.Equal(t, "sensor/abc/reading", sub.lastTopic)
	assert.Equal(t, map[string]any{"v": float64(1)}, sub.lastMsg) // auto deserializes JSON
	assert.Equal(t, 1, ack.acks)
	assert.Equal(t, 0, ack.nacks)
}

func TestHandleDelivery_DefaultTransform_Verbatim(t *testing.T) {
	sub := &fakeSubscriber{}
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(sub).
		WithDefaultTransform(DefaultRKVerbatim).
		Build()
	require.NoError(t, err)

	d, ack := newDelivery("ex", "sensor.abc.reading", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, "sensor.abc.reading", sub.lastTopic)
	assert.Equal(t, 1, ack.acks)
}

func TestHandleDelivery_DefaultTransform_Ignore(t *testing.T) {
	sub := &fakeSubscriber{}
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(sub).
		WithDefaultTransform(DefaultRKIgnore).
		Build()
	require.NoError(t, err)

	d, ack := newDelivery("ex", "junk", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, 0, sub.calls)
	assert.Equal(t, 1, ack.acks)
	assert.Equal(t, 0, ack.nacks)
}

func TestHandleDelivery_DefaultTransform_Error(t *testing.T) {
	sub := &fakeSubscriber{}
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(sub).
		WithDefaultTransform(DefaultRKError).
		Build()
	require.NoError(t, err)

	d, ack := newDelivery("ex", "junk", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, 0, sub.calls)
	assert.Equal(t, 0, ack.acks)
	assert.Equal(t, 1, ack.nacks)
	assert.False(t, ack.requeue)
}

func TestHandleDelivery_Subscription_FirstMatchWins(t *testing.T) {
	sub := &fakeSubscriber{}
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(sub).
		WithSubscription(Subscription{
			RoutingKeyPattern: "sensor.*deviceId.reading",
			VinculumTopicFunc: func(rk, ex string, fields map[string]string, msg any) (string, error) {
				return "sensor/" + fields["deviceId"] + "/data", nil
			},
		}).
		WithSubscription(Subscription{
			RoutingKeyPattern: "#",
			VinculumTopicFunc: func(string, string, map[string]string, any) (string, error) {
				return "catchall", nil
			},
		}).
		Build()
	require.NoError(t, err)

	d, ack := newDelivery("ex", "sensor.abc.reading", []byte("payload"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, "sensor/abc/data", sub.lastTopic)
	assert.Equal(t, "abc", sub.lastFlds["deviceId"])
	assert.Equal(t, 1, ack.acks)
}

func TestHandleDelivery_VinculumTopicFunc_EmptyFallsBackToDotToSlash(t *testing.T) {
	sub := &fakeSubscriber{}
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(sub).
		WithSubscription(Subscription{
			RoutingKeyPattern: "sensor.*deviceId.reading",
			VinculumTopicFunc: func(rk, ex string, fields map[string]string, msg any) (string, error) {
				return "", nil
			},
		}).
		Build()
	require.NoError(t, err)

	d, _ := newDelivery("ex", "sensor.abc.reading", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, "sensor/abc/reading", sub.lastTopic)
}

func TestHandleDelivery_VinculumTopicFunc_NilUsesDotToSlash(t *testing.T) {
	sub := &fakeSubscriber{}
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(sub).
		WithSubscription(Subscription{
			RoutingKeyPattern: "alerts.#",
			// VinculumTopicFunc nil
		}).
		Build()
	require.NoError(t, err)

	d, _ := newDelivery("ex", "alerts.cpu.high", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, "alerts/cpu/high", sub.lastTopic)
}

func TestHandleDelivery_VinculumTopicFunc_ErrorNacks(t *testing.T) {
	sub := &fakeSubscriber{}
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(sub).
		WithSubscription(Subscription{
			RoutingKeyPattern: "#",
			VinculumTopicFunc: func(string, string, map[string]string, any) (string, error) {
				return "", errors.New("boom")
			},
		}).
		Build()
	require.NoError(t, err)

	d, ack := newDelivery("ex", "anything", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, 0, sub.calls)
	assert.Equal(t, 1, ack.nacks)
	assert.False(t, ack.requeue)
}

func TestHandleDelivery_SubscriberError_NacksWithoutRequeue(t *testing.T) {
	sub := &fakeSubscriber{returnErr: errors.New("downstream broke")}
	r, err := NewReceiver().WithQueue("q").WithSubscriber(sub).Build()
	require.NoError(t, err)

	d, ack := newDelivery("ex", "foo.bar", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, 1, sub.calls)
	assert.Equal(t, 0, ack.acks)
	assert.Equal(t, 1, ack.nacks)
	assert.False(t, ack.requeue, "must never requeue on processing error (avoids poison-message redelivery loops)")
}

func TestHandleDelivery_HeadersBecomeFields_TraceStripped(t *testing.T) {
	sub := &fakeSubscriber{}
	r, err := NewReceiver().WithQueue("q").WithSubscriber(sub).Build()
	require.NoError(t, err)

	headers := amqp.Table{
		"source":      "test",
		"count":       int32(42),
		"raw":         []byte("bytes"),
		"traceparent": "00-aaa-bbb-01", // should be filtered
		"tracestate":  "k=v",
		"baggage":     "x=y",
	}
	d, _ := newDelivery("ex", "foo", []byte("x"), headers)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, "test", sub.lastFlds["source"])
	assert.Equal(t, "42", sub.lastFlds["count"])
	assert.Equal(t, "bytes", sub.lastFlds["raw"])
	_, ok := sub.lastFlds["traceparent"]
	assert.False(t, ok, "traceparent must be stripped")
	_, ok = sub.lastFlds["tracestate"]
	assert.False(t, ok)
	_, ok = sub.lastFlds["baggage"]
	assert.False(t, ok)
}

func TestHandleDelivery_ExtractedFieldsMergedAndPrecede(t *testing.T) {
	sub := &fakeSubscriber{}
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(sub).
		WithSubscription(Subscription{
			RoutingKeyPattern: "sensor.*deviceId.reading",
		}).
		Build()
	require.NoError(t, err)

	headers := amqp.Table{
		"source":   "test",
		"deviceId": "should-be-overridden",
	}
	d, _ := newDelivery("ex", "sensor.abc.reading", []byte("x"), headers)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, "test", sub.lastFlds["source"])
	assert.Equal(t, "abc", sub.lastFlds["deviceId"], "pattern-extracted value takes precedence")
}

func TestHandleDelivery_DeserializeFallback(t *testing.T) {
	sub := &fakeSubscriber{}
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(sub).
		WithWireFormat(wire.JSON).
		Build()
	require.NoError(t, err)

	// Invalid JSON triggers the wire format error; receiver should pass raw bytes.
	body := []byte("not json {{")
	d, ack := newDelivery("ex", "foo", body, nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, body, sub.lastMsg)
	assert.Equal(t, 1, ack.acks)
}

func TestHandleDelivery_AutoAckSkipsAckCall(t *testing.T) {
	sub := &fakeSubscriber{}
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(sub).
		WithAutoAck(true).
		Build()
	require.NoError(t, err)

	d, ack := newDelivery("ex", "foo", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, 1, sub.calls)
	assert.Equal(t, 0, ack.acks)
	assert.Equal(t, 0, ack.nacks)
}

func TestHandleDelivery_AutoAckSkipsNackOnError(t *testing.T) {
	sub := &fakeSubscriber{returnErr: errors.New("boom")}
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(sub).
		WithAutoAck(true).
		Build()
	require.NoError(t, err)

	d, ack := newDelivery("ex", "foo", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, 1, sub.calls)
	assert.Equal(t, 0, ack.acks)
	assert.Equal(t, 0, ack.nacks)
}

func TestRunLoop_ExitsOnContextCancel(t *testing.T) {
	sub := &fakeSubscriber{}
	r, err := NewReceiver().WithQueue("q").WithSubscriber(sub).Build()
	require.NoError(t, err)

	deliveries := make(chan amqp.Delivery)
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	go r.runLoop(ctx, deliveries, done)
	cancel()
	<-done // must return promptly
}

func TestRunLoop_ProcessesUntilChannelClosed(t *testing.T) {
	sub := &fakeSubscriber{}
	r, err := NewReceiver().WithQueue("q").WithSubscriber(sub).Build()
	require.NoError(t, err)

	deliveries := make(chan amqp.Delivery, 3)
	done := make(chan struct{})

	a := &fakeAcknowledger{}
	deliveries <- amqp.Delivery{Acknowledger: a, RoutingKey: "a.b", Body: []byte("1"), DeliveryTag: 1}
	deliveries <- amqp.Delivery{Acknowledger: a, RoutingKey: "c.d", Body: []byte("2"), DeliveryTag: 2}
	close(deliveries)

	go r.runLoop(context.Background(), deliveries, done)
	<-done

	assert.Equal(t, 2, sub.calls)
	assert.Equal(t, 2, a.acks)
}
