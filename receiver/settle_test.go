package receiver

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bus "github.com/tsarna/vinculum-bus"
)

// settlingSubscriber settles the delivery it is handed, the way a vinculum
// action calling inbound::ack() does — from the context, with no handle passed
// and nothing naming RabbitMQ.
type settlingSubscriber struct {
	bus.BaseSubscriber

	// settle runs against the settler found on the delivery's context. A nil
	// settle only records what arrived.
	settle func(ctx context.Context, s bus.Settler)

	mu         sync.Mutex
	calls      int
	sawSettler bool
	returnErr  error
}

func (s *settlingSubscriber) OnEvent(ctx context.Context, _ string, _ any, _ map[string]string) error {
	s.mu.Lock()
	s.calls++
	settler := bus.SettlerFromContext(ctx)
	s.sawSettler = settler != nil
	s.mu.Unlock()

	if s.settle != nil && settler != nil {
		s.settle(ctx, settler)
	}
	return s.returnErr
}

func manualReceiver(t *testing.T, sub bus.Subscriber) *RMQReceiver {
	t.Helper()
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(sub).
		WithAckMode(AckManual).
		Build()
	require.NoError(t, err)
	return r
}

// The whole of manual settle from the receiver's side: hand the delivery over
// and settle nothing.
func TestManual_HandlingReturnsWithoutSettling(t *testing.T) {
	sub := &settlingSubscriber{}
	r := manualReceiver(t, sub)

	d, ack := newDelivery("ex", "foo", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, 1, sub.calls)
	assert.True(t, sub.sawSettler, "the delivery should carry a settler")
	assert.Equal(t, 0, ack.acks)
	assert.Equal(t, 0, ack.nacks)
}

func TestManual_SubscriberAcksThroughTheContext(t *testing.T) {
	var settled bool
	sub := &settlingSubscriber{settle: func(ctx context.Context, s bus.Settler) {
		var err error
		settled, err = s.Ack(ctx)
		require.NoError(t, err)
	}}
	r := manualReceiver(t, sub)

	d, ack := newDelivery("ex", "foo", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.True(t, settled, "the first Ack should report that it settled the delivery")
	assert.Equal(t, 1, ack.acks)
	assert.Equal(t, 0, ack.nacks)
}

func TestManual_SubscriberNacksThroughTheContext(t *testing.T) {
	sub := &settlingSubscriber{settle: func(ctx context.Context, s bus.Settler) {
		settled, err := s.Nack(ctx, "could not be handled")
		require.NoError(t, err)
		assert.True(t, settled)
	}}
	r := manualReceiver(t, sub)

	d, ack := newDelivery("ex", "foo", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, 1, ack.nacks)
	assert.False(t, ack.requeue, "a nack must not requeue: that is a redelivery loop, not a dead letter")
	assert.Equal(t, 0, ack.acks)
}

// A delivery settles once however many subscribers see it. An acknowledgement
// means someone took responsibility, not that everyone finished.
func TestManual_SettlesOnce(t *testing.T) {
	var results []bool
	sub := &settlingSubscriber{settle: func(ctx context.Context, s bus.Settler) {
		for range 3 {
			settled, err := s.Ack(ctx)
			require.NoError(t, err)
			results = append(results, settled)
		}
	}}
	r := manualReceiver(t, sub)

	d, ack := newDelivery("ex", "foo", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, []bool{true, false, false}, results)
	assert.Equal(t, 1, ack.acks, "one broker acknowledgement, whatever the subscribers did")
}

// Ack after nack is a no-op rather than an error, which is what makes these
// safe to call from shared code that does not know what else ran.
func TestManual_AckAfterNackIsANoOp(t *testing.T) {
	sub := &settlingSubscriber{settle: func(ctx context.Context, s bus.Settler) {
		nacked, err := s.Nack(ctx, "no")
		require.NoError(t, err)
		assert.True(t, nacked)

		acked, err := s.Ack(ctx)
		require.NoError(t, err)
		assert.False(t, acked)
	}}
	r := manualReceiver(t, sub)

	d, ack := newDelivery("ex", "foo", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, 1, ack.nacks)
	assert.Equal(t, 0, ack.acks)
}

// AMQP has no per-delivery lease, so there is nothing to extend and the
// receiver says so rather than reporting a renewal it did not make.
func TestManual_KeepaliveExtendsNothing(t *testing.T) {
	sub := &settlingSubscriber{settle: func(ctx context.Context, s bus.Settler) {
		extended, err := s.Keepalive(ctx)
		require.NoError(t, err)
		assert.False(t, extended)
	}}
	r := manualReceiver(t, sub)

	d, ack := newDelivery("ex", "foo", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, 0, ack.acks)
	assert.Equal(t, 0, ack.nacks)
}

// A delivery tag belongs to the channel that issued it. Once the receiver has
// been stopped — a reconnect, or a channel recovered on its own — settling says
// so instead of reaching a broker that would reject it.
func TestManual_SettleAfterChannelReplacedIsStale(t *testing.T) {
	var captured bus.Settler
	sub := &settlingSubscriber{settle: func(_ context.Context, s bus.Settler) {
		captured = s
	}}
	r := manualReceiver(t, sub)

	fc := &fakeChannel{}
	require.NoError(t, r.Start(context.Background(), fc))

	d, ack := newDelivery("ex", "foo", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)
	require.NotNil(t, captured)

	r.Stop() // as the client does before re-Starting on a fresh channel

	settled, err := captured.Ack(context.Background())
	assert.False(t, settled)
	assert.True(t, bus.IsStale(err), "expected a StaleError, got %v", err)
	assert.Equal(t, 0, ack.acks, "a stale settle must not reach the broker")

	extended, err := captured.Keepalive(context.Background())
	assert.False(t, extended)
	assert.True(t, bus.IsStale(err), "expected a StaleError, got %v", err)
}

// A message that never reaches the subscriber cannot be settled by whatever
// would have handled it, so the receiver still answers for it — in manual mode
// exactly as in auto.
func TestManual_UndeliverableMessageIsStillNacked(t *testing.T) {
	sub := &settlingSubscriber{}
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(sub).
		WithAckMode(AckManual).
		WithDefaultTransform(DefaultRKError).
		Build()
	require.NoError(t, err)

	d, ack := newDelivery("ex", "foo", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, 0, sub.calls)
	assert.Equal(t, 1, ack.nacks)
}

// Handling failed and nothing settled the delivery: dead-letter it now rather
// than hold a prefetch slot until settle_timeout says the same thing later.
func TestManual_HandlerErrorNacks(t *testing.T) {
	sub := &settlingSubscriber{returnErr: errors.New("boom")}
	r := manualReceiver(t, sub)

	d, ack := newDelivery("ex", "foo", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, 1, ack.nacks)
	assert.Equal(t, 0, ack.acks)
}

// ...but a configuration that acknowledged the message and then failed at
// something else is not overruled. The settle already happened; the error is
// about what came after it.
func TestManual_HandlerErrorAfterAckDoesNotNack(t *testing.T) {
	sub := &settlingSubscriber{
		returnErr: errors.New("boom"),
		settle: func(ctx context.Context, s bus.Settler) {
			_, err := s.Ack(ctx)
			require.NoError(t, err)
		},
	}
	r := manualReceiver(t, sub)

	d, ack := newDelivery("ex", "foo", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, 1, ack.acks)
	assert.Equal(t, 0, ack.nacks)
}

// Under AMQP's own no-ack mode the broker settled the message when it sent it.
// There is nothing to answer for, so inbound::ack() finds nothing rather than
// succeeding at nothing.
func TestAckNone_CarriesNoSettler(t *testing.T) {
	sub := &settlingSubscriber{}
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(sub).
		WithAckMode(AckNone).
		Build()
	require.NoError(t, err)

	d, _ := newDelivery("ex", "foo", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, 1, sub.calls)
	assert.False(t, sub.sawSettler)
}

// The default mode acknowledges through the settler too, so a configuration
// that settled the message itself is not settled over the top of.
func TestAckAfterHandling_GoesThroughTheSettler(t *testing.T) {
	sub := &settlingSubscriber{settle: func(ctx context.Context, s bus.Settler) {
		nacked, err := s.Nack(ctx, "not for me")
		require.NoError(t, err)
		assert.True(t, nacked)
	}}
	r, err := NewReceiver().WithQueue("q").WithSubscriber(sub).Build()
	require.NoError(t, err)

	d, ack := newDelivery("ex", "foo", []byte("x"), nil)
	r.handleDelivery(context.Background(), d)

	assert.Equal(t, 1, ack.nacks)
	assert.Equal(t, 0, ack.acks, "automatic acknowledgement must not overrule a settle that already happened")
}
