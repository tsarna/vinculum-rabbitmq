package receiver

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bus "github.com/tsarna/vinculum-bus"
	"github.com/tsarna/vinculum-bus/subutils"
)

// gatedSubscriber holds the goroutine delivering to it until released, so a
// test can look at the acknowledger while the work is provably still going.
type gatedSubscriber struct {
	bus.BaseSubscriber
	entered     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
	err         error
}

func newGatedSubscriber() *gatedSubscriber {
	return &gatedSubscriber{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (g *gatedSubscriber) Release() { g.releaseOnce.Do(func() { close(g.release) }) }

func (g *gatedSubscriber) OnEvent(context.Context, string, any, map[string]string) error {
	select {
	case g.entered <- struct{}{}:
	default:
	}
	<-g.release
	return g.err
}

func (a *fakeAcknowledger) counts() (acks, nacks int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.acks, a.nacks
}

// queuedReceiver puts an async queue between the receiver and the subscriber,
// which is what `queue_size` on the client block builds. That queue is the
// reason this change exists: its OnEvent returns the moment the delivery is
// enqueued.
func queuedReceiver(t *testing.T, target bus.Subscriber) *RMQReceiver {
	t.Helper()

	queue := subutils.NewAsyncQueueingSubscriber(target, 10).Start()
	t.Cleanup(func() { queue.Close() })

	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(queue).
		WithAckMode(AckAfterHandling).
		Build()
	require.NoError(t, err)
	return r
}

// The defect, and the reason `queue_size` alongside `ack = "auto"` used to be
// refused outright. Delivery into the queue returns as soon as the message is
// enqueued, so acknowledging on that return told the broker the message was
// handled before anything had handled it — and a handler failure then had
// nothing left to redeliver.
func TestAckAfterHandlingWaitsForTheWorkBehindAQueue(t *testing.T) {
	target := newGatedSubscriber()
	defer target.Release()

	r := queuedReceiver(t, target)
	d, ack := newDelivery("ex", "foo", []byte("x"), nil)

	go r.handleDelivery(context.Background(), d)

	<-target.entered

	// Never, not once. Acknowledging on the enqueue's return happens within
	// microseconds of it, so a single check just after the subscriber is
	// entered is a race the wrong behaviour can win.
	assert.Never(t, func() bool { acks, _ := ack.counts(); return acks > 0 },
		250*time.Millisecond, 25*time.Millisecond,
		"the subscriber is still working; the delivery must not be acknowledged yet")

	target.Release()

	assert.Eventually(t, func() bool { acks, _ := ack.counts(); return acks == 1 },
		3*time.Second, 20*time.Millisecond,
		"the acknowledgement should follow the work out of the queue")

	_, nacks := ack.counts()
	assert.Equal(t, 0, nacks)
}

// The half that matters most on RabbitMQ, where an unsettled delivery holds a
// prefetch slot and nothing self-heals. A handler that fails behind a queue
// nacks, so the broker can redeliver or dead-letter it — where before it was
// acknowledged at the enqueue and the failure lost the message outright.
func TestAFailureBehindAQueueNacks(t *testing.T) {
	target := newGatedSubscriber()
	target.err = errors.New("the action threw")
	defer target.Release()

	r := queuedReceiver(t, target)
	d, ack := newDelivery("ex", "foo", []byte("x"), nil)

	go r.handleDelivery(context.Background(), d)

	<-target.entered
	target.Release()

	assert.Eventually(t, func() bool { _, nacks := ack.counts(); return nacks == 1 },
		3*time.Second, 20*time.Millisecond,
		"a failed handler must return the delivery to the broker")

	acks, _ := ack.counts()
	assert.Equal(t, 0, acks, "and must never have acknowledged it")
}
