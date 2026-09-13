package receiver

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Finding 3. Recovery Stop S1 waits on stuck loop L1; the connection drops and
// the reconnect's Stop S2 runs. On v0.9.0 and the first WIP commit S2 returns at
// once, the reconnect Starts L2 over the top of L1, and a shutdown Drain drains
// the idle L2 cleanly while S1 waits forever. With the candidate, S2 waits on
// L1 too, so no second loop starts; the Drain must time out on L1, and both
// Stops must return once it has.
func TestReview_ConcurrentStopThenRestartThenDrain(t *testing.T) {
	b := newBlocker()
	r, fc, a := drainFixture(t, AckAfterHandling, b)
	defer close(b.release)

	require.True(t, deliver(fc, a, "stuck", 1))
	<-b.entered
	s1 := stopInBackground(t, r) // recovery goroutine

	s2 := make(chan error, 1) // reconnect goroutine's handleDisconnect
	go func() { s2 <- r.Stop() }()
	select {
	case err := <-s2:
		// Returned without waiting: the reconnect would go on to start L2.
		t.Logf("second concurrent Stop returned at once: %v", err)
		fc2 := &fakeChannel{deliveriesChan: make(chan amqp.Delivery, 16)}
		assert.NoError(t, r.Start(context.Background(), fc2), "reconnect restarts the receiver")
		s2 <- err
	case <-time.After(100 * time.Millisecond):
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	assert.Error(t, r.Drain(ctx), "drain reported success with L1's delivery still running")

	for name, ch := range map[string]<-chan error{"S1 (recovery)": s1, "S2 (reconnect)": s2} {
		select {
		case err := <-ch:
			t.Logf("%s returned %v", name, err)
		case <-time.After(time.Second):
			t.Errorf("%s is still waiting after the drain gave up: Client.Stop would hang", name)
		}
	}
}

// consumeBlockingChannel holds Consume until release is closed.
type consumeBlockingChannel struct {
	*fakeChannel
	entered chan struct{}
	release chan struct{}
}

func (c *consumeBlockingChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	close(c.entered)
	<-c.release
	return c.fakeChannel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

// A Drain landing while Start is inside Consume.
func TestReview_DrainDuringStartConsume(t *testing.T) {
	r, err := NewReceiver().WithClientName("c").WithQueue("q").WithSubscriber(newBlocker()).Build()
	require.NoError(t, err)
	ch := &consumeBlockingChannel{
		fakeChannel: &fakeChannel{deliveriesChan: make(chan amqp.Delivery, 16)},
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	started := make(chan error, 1)
	go func() { started <- r.Start(context.Background(), ch) }()
	<-ch.entered
	require.NoError(t, drained(t, r), "drain of a not-yet-started receiver")
	close(ch.release)
	serr := <-started
	t.Logf("Start after drain returned: %v", serr)
	r.mu.Lock()
	running := r.stopWork != nil
	r.mu.Unlock()
	assert.False(t, running, "a consumer was registered and a loop started after Drain reported the receiver drained")
	_ = r.Stop()
}
