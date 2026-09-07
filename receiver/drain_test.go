package receiver

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bus "github.com/tsarna/vinculum-bus"
)

// drainFixture starts a receiver against a fake channel whose delivery channel
// is buffered, so a test can put messages on it before or after a drain and see
// which of them are handled.
func drainFixture(t *testing.T, mode AckMode, sub bus.Subscriber) (*RMQReceiver, *fakeChannel, *fakeAcknowledger) {
	t.Helper()

	r, err := NewReceiver().
		WithClientName("c").
		WithQueue("q").
		WithSubscriber(sub).
		WithAckMode(mode).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{deliveriesChan: make(chan amqp.Delivery, 16)}
	require.NoError(t, r.Start(context.Background(), fc))
	t.Cleanup(func() { _ = r.Stop() })

	return r, fc, &fakeAcknowledger{}
}

// drained drains with a bound, so a receiver that will not stop consuming
// fails the line that knows what went wrong instead of blocking until the
// package times out somewhere else.
func drained(t *testing.T, r *RMQReceiver) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return r.Drain(ctx)
}

// deliver puts one message on the fake channel, if it is still open.
func deliver(fc *fakeChannel, a amqp.Acknowledger, body string, tag uint64) bool {
	fc.mu.Lock()
	ch := fc.deliveriesChan
	fc.mu.Unlock()
	if ch == nil {
		return false
	}
	ch <- amqp.Delivery{Acknowledger: a, RoutingKey: "a.b", Body: []byte(body), DeliveryTag: tag}
	return true
}

func waitCalls(t *testing.T, sub *fakeSubscriber, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		sub.mu.Lock()
		defer sub.mu.Unlock()
		return sub.calls >= n
	}, 3*time.Second, 5*time.Millisecond, "the receiver never delivered %d messages", n)
}

func calls(sub *fakeSubscriber) int {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	return sub.calls
}

// A receiver names its own consumer so that draining has something to withdraw.
// A broker-assigned tag is never handed back, so without this there would be no
// way to stop consuming short of giving up the channel — and giving up the
// channel is exactly what invalidates every outstanding delivery tag.
func TestStartRegistersANamedConsumer(t *testing.T) {
	_, fc, _ := drainFixture(t, AckAfterHandling, &fakeSubscriber{})

	fc.mu.Lock()
	defer fc.mu.Unlock()
	assert.Equal(t, "vinculum-c-q", fc.lastConsumeArgs.consumer,
		"a receiver with no configured tag should name itself")
}

func TestAConfiguredConsumerTagIsUsedAsGiven(t *testing.T) {
	r, err := NewReceiver().
		WithClientName("c").
		WithQueue("q").
		WithSubscriber(&fakeSubscriber{}).
		WithConsumerTag("chosen").
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{deliveriesChan: make(chan amqp.Delivery, 4)}
	require.NoError(t, r.Start(context.Background(), fc))
	t.Cleanup(func() { _ = r.Stop() })

	fc.mu.Lock()
	defer fc.mu.Unlock()
	assert.Equal(t, "chosen", fc.lastConsumeArgs.consumer)
}

// The point of a drain: deliveries already sent are finished, and no more are
// taken on. Stopping does both at once, which is why a shutdown that only has
// Stop cannot stop consuming before it disconnects.
func TestDrainWithdrawsTheConsumer(t *testing.T) {
	sub := &fakeSubscriber{}
	r, fc, a := drainFixture(t, AckAfterHandling, sub)

	require.True(t, deliver(fc, a, "before", 1))
	waitCalls(t, sub, 1)

	require.NoError(t, drained(t, r))

	fc.mu.Lock()
	cancels, tag := fc.cancelCalls, fc.lastCancelled
	fc.mu.Unlock()
	assert.Equal(t, 1, cancels, "the drain did not withdraw the consumer")
	assert.Equal(t, "vinculum-c-q", tag)

	// The broker sends nothing after a cancel, which the fake models by closing
	// the delivery channel — so there is no way left to offer one.
	assert.False(t, deliver(fc, a, "after", 2), "the consumer was not withdrawn")
	assert.Equal(t, 1, calls(sub))
}

// Withdrawing the consumer rather than cancelling a context is what makes the
// prefetched backlog survive. The broker closes the delivery channel *behind*
// what it has already sent, so those messages are handled and acknowledged
// instead of being redelivered on the next boot — up to `prefetch` of them,
// ten by default.
func TestDrainHandlesWhatTheBrokerHadAlreadySent(t *testing.T) {
	sub := &fakeSubscriber{}
	r, fc, a := drainFixture(t, AckAfterHandling, sub)

	// Queued but not yet read: the loop is blocked on the first one only after
	// it takes it, so filling the buffer models a prefetched backlog.
	for i := 1; i <= 5; i++ {
		require.True(t, deliver(fc, a, "m", uint64(i)))
	}

	require.NoError(t, drained(t, r))

	assert.Equal(t, 5, calls(sub), "the drain abandoned the prefetched backlog")
	a.mu.Lock()
	defer a.mu.Unlock()
	assert.Equal(t, 5, a.acks, "the backlog was handled but not acknowledged")
}

// A drained receiver is still connected on the same channel, and the delivery
// tag it handed out before the drain still acknowledges. On AMQP this is the
// whole reason draining and stopping are separate: a tag means nothing except
// on the channel that issued it, so a receiver that closed its channel to stop
// consuming would invalidate every outstanding acknowledgement in the act of
// stopping.
func TestDrainLeavesAnOutstandingTagAbleToAcknowledge(t *testing.T) {
	sub := &capturingSubscriber{}
	r, fc, a := drainFixture(t, AckManual, sub)

	require.True(t, deliver(fc, a, "hi", 7))
	require.Eventually(t, func() bool { return sub.ctx.Load() != nil },
		3*time.Second, 5*time.Millisecond)

	require.NoError(t, drained(t, r))
	require.Equal(t, 1, r.Unsettled(), "nothing settled it yet")

	settler := bus.SettlerFromContext(*sub.ctx.Load())
	require.NotNil(t, settler)
	settled, err := settler.Ack(context.Background())
	require.NoError(t, err, "the tag went stale during the drain")
	assert.True(t, settled)

	a.mu.Lock()
	defer a.mu.Unlock()
	assert.Equal(t, 1, a.acks)
	assert.Equal(t, 0, r.Unsettled())
}

// And the same acknowledgement after Stop must fail, because there the channel
// really is going away. The epoch is what separates the two, and moving it in
// Drain would make draining indistinguishable from stopping.
func TestStopRetiresTheTagsThatDrainKept(t *testing.T) {
	sub := &capturingSubscriber{}
	r, fc, a := drainFixture(t, AckManual, sub)

	require.True(t, deliver(fc, a, "hi", 7))
	require.Eventually(t, func() bool { return sub.ctx.Load() != nil },
		3*time.Second, 5*time.Millisecond)

	require.NoError(t, drained(t, r))
	require.NoError(t, r.Stop())

	settler := bus.SettlerFromContext(*sub.ctx.Load())
	settled, err := settler.Ack(context.Background())
	assert.False(t, settled)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "channel reconnected")

	a.mu.Lock()
	defer a.mu.Unlock()
	assert.Zero(t, a.acks, "an acknowledgement went out on a channel that had gone")
}

// A channel too far gone to carry a basic.cancel must not leave the drain
// waiting on a loop nothing will end. Those deliveries are unacknowledged, so
// the broker redelivers them — the same outcome as the channel dying on its
// own, which is what has happened.
func TestDrainFallsBackWhenTheConsumerCannotBeWithdrawn(t *testing.T) {
	sub := &fakeSubscriber{}
	r, fc, _ := drainFixture(t, AckAfterHandling, sub)

	fc.mu.Lock()
	fc.cancelErr = errors.New("channel/connection is not open")
	fc.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, r.Drain(ctx), "the drain did not fall back to ending the loop")
}

// What the shutdown phase reads. Under manual settle nothing acknowledges the
// delivery until the configuration does, so the count is what says the process
// still owes the broker an answer.
func TestUnsettledCountsWhatIsStillOwed(t *testing.T) {
	sub := &capturingSubscriber{}
	r, fc, a := drainFixture(t, AckManual, sub)

	assert.Equal(t, 0, r.Unsettled())

	require.True(t, deliver(fc, a, "one", 1))
	// Waiting on the captured context rather than on the count: the count is
	// incremented where the settler is built, which is *before* the subscriber
	// runs, so a wait on it can win the race to read a context that is not
	// there yet.
	require.Eventually(t, func() bool { return sub.ctx.Load() != nil },
		3*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, r.Unsettled())

	settler := bus.SettlerFromContext(*sub.ctx.Load())
	_, err := settler.Nack(context.Background(), "no")
	require.NoError(t, err)
	assert.Equal(t, 0, r.Unsettled(), "a nacked delivery is still on the books")

	require.NoError(t, drained(t, r))
}

// Under AckAfterHandling the framework settles when the work finishes, so the
// count comes back to zero on its own and a shutdown waits for nothing.
func TestUnsettledReturnsToZeroWhenTheFrameworkSettles(t *testing.T) {
	sub := &fakeSubscriber{}
	r, fc, a := drainFixture(t, AckAfterHandling, sub)

	require.True(t, deliver(fc, a, "hi", 1))
	waitCalls(t, sub, 1)

	assert.Eventually(t, func() bool { return r.Unsettled() == 0 },
		2*time.Second, 10*time.Millisecond,
		"an automatically settled delivery stayed on the books")
}

// Under AckNone the broker considered the message delivered when it sent it.
// There is no settler and nothing to answer for, so nothing should be counted —
// a count that never comes down makes every later shutdown wait out its budget.
func TestAckNoneCountsNothing(t *testing.T) {
	sub := &fakeSubscriber{}
	r, fc, a := drainFixture(t, AckNone, sub)

	require.True(t, deliver(fc, a, "hi", 1))
	waitCalls(t, sub, 1)

	assert.Equal(t, 0, r.Unsettled())
	require.NoError(t, drained(t, r))
	assert.Equal(t, 0, r.Unsettled())
}

// A delivery whose channel has gone can never be acknowledged: the tag it holds
// means nothing on any other channel. The settler asks Valid() *before* it runs
// Ack or Nack and abandons the delivery when the answer is no, so a stale
// delivery never reaches the release those two carry — and a count that only
// released there would keep it forever, with every later shutdown spending its
// whole budget on it.
func TestUnsettledLetsGoOfADeliveryWhoseChannelHasGone(t *testing.T) {
	sub := &capturingSubscriber{}
	r, fc, a := drainFixture(t, AckManual, sub)

	require.True(t, deliver(fc, a, "hi", 1))
	// Waiting on the captured context rather than on the count: the count is
	// incremented where the settler is built, which is *before* the subscriber
	// runs, so a wait on it can win the race to read a context that is not
	// there yet.
	require.Eventually(t, func() bool { return sub.ctx.Load() != nil },
		3*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, r.Unsettled())

	// What a reconnect does: the loop stops and every tag it issued retires.
	require.NoError(t, r.Stop())

	settler := bus.SettlerFromContext(*sub.ctx.Load())
	settled, err := settler.Ack(context.Background())
	require.Error(t, err)
	assert.False(t, settled)

	assert.Equal(t, 0, r.Unsettled(),
		"a delivery this receiver can no longer settle stayed on the books")
}

// capturingSubscriber keeps the delivery context so a test can settle through
// the settler riding on it, as a configuration does.
type capturingSubscriber struct {
	bus.BaseSubscriber
	ctx atomic.Pointer[context.Context]
}

func (s *capturingSubscriber) OnEvent(ctx context.Context, _ string, _ any, _ map[string]string) error {
	s.ctx.Store(&ctx)
	return nil
}

// watcher sees deliveries go past and settles none of them, which is a
// disposition of its own rather than a subscriber that forgot.
type watcher struct {
	bus.BaseSubscriber
	seen atomic.Int64
}

func (w *watcher) OnEvent(context.Context, string, any, map[string]string) error {
	w.seen.Add(1)
	return nil
}

func (w *watcher) DeliveryDisposition() bus.Disposition { return bus.Observed }

// The only path through a delivery that reaches no settler at all: every
// failure here nacks, and a nack releases. An observing subscriber makes the
// framework settle point return without acting, so nothing is ever going to
// settle the delivery.
func TestUnsettledDoesNotLeakOnAnObservingSubscriber(t *testing.T) {
	w := &watcher{}
	r, fc, a := drainFixture(t, AckAfterHandling, w)

	require.True(t, deliver(fc, a, "hi", 1))
	require.Eventually(t, func() bool { return w.seen.Load() == 1 },
		3*time.Second, 5*time.Millisecond)

	assert.Eventually(t, func() bool { return r.Unsettled() == 0 },
		2*time.Second, 10*time.Millisecond,
		"a delivery nothing will ever settle stayed on the books")
}

// blocker holds a delivery until it is released, so a test can be sure the
// receiver is mid-delivery when it drains, and records what the delivery's own
// context looked like on the far side of the wait.
type blocker struct {
	bus.BaseSubscriber
	entered chan struct{}
	release chan struct{}

	// ctx is the delivery's own context, captured on entry so a test can reach
	// the settler riding on it while the delivery is still held.
	ctx             atomic.Pointer[context.Context]
	errAfterRelease atomic.Pointer[error]
}

func newBlocker() *blocker {
	return &blocker{entered: make(chan struct{}, 1), release: make(chan struct{})}
}

// entered is signalled rather than closed, so a test that restarts a receiver
// past this subscriber does not panic on a second delivery.
func (b *blocker) OnEvent(ctx context.Context, _ string, _ any, _ map[string]string) error {
	b.ctx.Store(&ctx)
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.release
	err := ctx.Err()
	b.errAfterRelease.Store(&err)
	return nil
}

// Draining must not cancel the work it is waiting for. Reading and delivering
// share one context until this splits them, and the fallback path cancels the
// read one — so a delivery running when the fallback fires would be aborted,
// and with it the acknowledgement it was about to produce.
func TestTheFallbackDoesNotCancelTheDeliveryInFlight(t *testing.T) {
	b := newBlocker()
	r, fc, a := drainFixture(t, AckAfterHandling, b)

	require.True(t, deliver(fc, a, "hi", 1))
	<-b.entered

	// Force the fallback: the cancel cannot be sent, so the drain ends the loop
	// through the read context instead.
	fc.mu.Lock()
	fc.cancelErr = errors.New("channel/connection is not open")
	fc.mu.Unlock()

	result := make(chan error, 1)
	go func() { result <- drained(t, r) }()

	time.Sleep(100 * time.Millisecond)
	close(b.release)

	require.NoError(t, <-result)
	got := b.errAfterRelease.Load()
	require.NotNil(t, got)
	assert.NoError(t, *got, "the drain cancelled the context the delivery was running on")
}

// Delivery runs user-supplied work, so a drain that waited for it
// unconditionally would hand one stuck expression the power to stop a process
// from exiting. The caller's context is the bound.
func TestDrainIsBoundedByItsContext(t *testing.T) {
	b := newBlocker()
	r, fc, a := drainFixture(t, AckAfterHandling, b)
	defer close(b.release)

	require.True(t, deliver(fc, a, "hi", 1))
	<-b.entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := r.Drain(ctx)
	require.Error(t, err, "drain waited for an action that never returned")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), time.Second)

	// And it stays reported.
	assert.Error(t, r.Drain(context.Background()))
}

// The bound above is worth nothing if the same stuck action then meets an
// unbounded wait one phase later. Stop cancels and reports instead.
func TestStopDoesNotWaitAgainForADeliveryTheDrainGaveUpOn(t *testing.T) {
	b := newBlocker()
	r, fc, a := drainFixture(t, AckAfterHandling, b)
	defer close(b.release)

	require.True(t, deliver(fc, a, "hi", 1))
	<-b.entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.Error(t, r.Drain(ctx))

	stopped := make(chan error, 1)
	go func() { stopped <- r.Stop() }()

	select {
	case err := <-stopped:
		assert.Error(t, err, "stopping past a running delivery should say so")
		assert.Error(t, r.Stop(), "the second call should repeat the answer")
	case <-time.After(2 * time.Second):
		t.Fatal("Stop blocked on the delivery the drain had already given up on")
	}
}

// A whole phase runs between Drain and Stop, so a delivery that overran the
// drain's deadline by a moment has very likely finished by the time Stop asks.
// Remembering the drain's verdict instead of re-checking puts an error in the
// log of a shutdown where nothing went wrong.
func TestStopReportsCleanlyWhenTheDeliveryFinishedAfterTheDrainGaveUp(t *testing.T) {
	b := newBlocker()
	r, fc, a := drainFixture(t, AckAfterHandling, b)

	require.True(t, deliver(fc, a, "hi", 1))
	<-b.entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.Error(t, r.Drain(ctx), "the drain should have given up")

	close(b.release)
	assert.Eventually(t, func() bool { return r.Drain(context.Background()) == nil },
		2*time.Second, 10*time.Millisecond,
		"Drain kept reporting a timeout for a delivery that had finished")

	assert.NoError(t, r.Stop(),
		"Stop reported a delivery still running that had already finished")
}

// A Stop that gives up on a delivery still retires every tag it handed out.
// That branch returns before the ordinary epoch bump, so it needs one of its
// own — and without it the abandoned delivery's settler reads valid past the
// channel's close, which on a channel that renumbers from 1 after a reconnect
// means acknowledging some other message.
func TestStopRetiresTagsEvenWhenItGivesUpOnADelivery(t *testing.T) {
	b := newBlocker()
	r, fc, a := drainFixture(t, AckManual, b)
	defer close(b.release)

	require.True(t, deliver(fc, a, "stuck", 1))
	<-b.entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.Error(t, r.Drain(ctx))
	require.Error(t, r.Stop(), "the stop should report the delivery it gave up on")

	// The stuck delivery's own tag, taken from the context it is still running
	// on. The channel is going with the connection, so this must be refused —
	// the give-up branch returns before the ordinary epoch bump and needs one
	// of its own.
	held := b.ctx.Load()
	require.NotNil(t, held)
	settled, err := bus.SettlerFromContext(*held).Ack(context.Background())
	assert.False(t, settled)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "channel reconnected")

	a.mu.Lock()
	defer a.mu.Unlock()
	assert.Zero(t, a.acks, "an acknowledgement went out on a channel that had gone")
}

// The count is decremented once per delivery however many times an op runs.
// The settler releases its claim when an op returns an error — so a failed
// acknowledgement can be retried and reach Ack a second time — and a count that
// went negative would *subtract* from the shutdown phase's total, cancelling a
// real backlog somewhere else and skipping the wait entirely.
func TestAFailedAckDoesNotReleaseTheCountTwice(t *testing.T) {
	sub := &capturingSubscriber{}
	r, fc, a := drainFixture(t, AckManual, sub)

	a.ackErr = errors.New("channel/connection is not open")

	require.True(t, deliver(fc, a, "hi", 1))
	// Waiting on the captured context rather than on the count: the count is
	// incremented where the settler is built, which is *before* the subscriber
	// runs, so a wait on it can win the race to read a context that is not
	// there yet.
	require.Eventually(t, func() bool { return sub.ctx.Load() != nil },
		3*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, r.Unsettled())

	settler := bus.SettlerFromContext(*sub.ctx.Load())
	_, err := settler.Ack(context.Background())
	require.Error(t, err, "the acknowledgement should have failed")
	assert.Equal(t, 0, r.Unsettled())

	// The settler let go of its claim, so this reaches the op a second time.
	a.mu.Lock()
	a.ackErr = nil
	a.mu.Unlock()
	settled, err := settler.Ack(context.Background())
	require.NoError(t, err)
	assert.True(t, settled)

	assert.Equal(t, 0, r.Unsettled(), "the count went negative on a retried settle")
	require.NoError(t, drained(t, r))
}

// A consumer tag is a shortstr, and the wire encoder writes its length in one
// byte — so an over-length tag is silently truncated modulo 256 rather than
// rejected, and a tag that arrives shortened is one basic.cancel cannot name.
func TestTheDefaultConsumerTagStaysWithinAShortstr(t *testing.T) {
	long := capConsumerTag("vinculum-client-" + strings.Repeat("q", 500))
	assert.LessOrEqual(t, len(long), 255)
	assert.True(t, strings.HasPrefix(long, "vinculum-client-"),
		"the recognisable part is what should survive truncation")

	// Cut on a rune boundary: a shortstr is nominally UTF-8, and half a rune is
	// not a name.
	multibyte := capConsumerTag("vinculum-client-" + strings.Repeat("é", 300))
	assert.LessOrEqual(t, len(multibyte), 255)
	assert.True(t, utf8.ValidString(multibyte), "the tag was cut mid-rune")

	assert.Equal(t, "vinculum-c-q", capConsumerTag("vinculum-c-q"),
		"a short name should pass through untouched")

	assert.Equal(t, strings.Repeat("z", 255), capConsumerTag(strings.Repeat("z", 255)),
		"a tag that already fits must not be rewritten")
}

// A configured tag is capped on the way to the broker, not just by the helper.
// It can overrun a shortstr as easily as a generated one, and the failure is
// just as silent: the length goes out in one byte, so what arrives is a
// different tag from the one basic.cancel will name.
func TestAConfiguredTagIsCappedWhereItIsRegistered(t *testing.T) {
	r, err := NewReceiver().
		WithClientName("c").
		WithQueue("q").
		WithSubscriber(&fakeSubscriber{}).
		WithConsumerTag(strings.Repeat("z", 400)).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{deliveriesChan: make(chan amqp.Delivery, 4)}
	require.NoError(t, r.Start(context.Background(), fc))
	t.Cleanup(func() { _ = r.Stop() })

	fc.mu.Lock()
	defer fc.mu.Unlock()
	assert.LessOrEqual(t, len(fc.lastConsumeArgs.consumer), 255,
		"an over-length tag reached Consume and would arrive truncated")
}

// Draining is terminal, and it has to be. The client's reconnect and
// channel-recovery paths both end in Start and both stay live until the
// connection closes, so a refusal that lapsed would leave a window in which one
// of them puts the receiver back to consuming — during the phase that waits for
// it to have finished.
//
// It covers the abandoned loop too: a Stop that gave up on a delivery leaves
// that delivery's loop still holding loopDone, and a second loop over the top
// would leave the next Stop waiting forever on it. There is no way to reach
// that state except through a Drain, so the one refusal answers for both.
func TestStartRefusesAfterADrain(t *testing.T) {
	b := newBlocker()
	r, fc, a := drainFixture(t, AckAfterHandling, b)
	defer close(b.release)

	require.True(t, deliver(fc, a, "hi", 1))
	<-b.entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.Error(t, r.Drain(ctx))

	// Deliberately no Stop before this. With one, `stopWork` is nil and the
	// refusal comes from the "already started" branch instead — which is how
	// this test used to hide the guards being in the wrong order.
	fresh := &fakeChannel{deliveriesChan: make(chan amqp.Delivery, 4)}
	err := r.Start(context.Background(), fresh)
	require.Error(t, err, "starting again would put the receiver back to consuming")
	assert.Contains(t, err.Error(), "drained",
		"a drained receiver reports the drain, not a double-Start that did not happen")

	require.Error(t, r.Stop())

	fc.mu.Lock()
	defer fc.mu.Unlock()
	assert.Equal(t, 1, fc.consumeCalls, "a second consumer was registered")
}

// The same refusal on a receiver the drain reached *before* it started, which
// is the interleaving a client-level flag alone cannot close: the connect path
// decides whether to start a receiver and then starts it, and a drain landing
// between those two steps would otherwise slip through.
func TestStartRefusesAfterADrainThatFoundItUnstarted(t *testing.T) {
	r, err := NewReceiver().
		WithClientName("c").
		WithQueue("q").
		WithSubscriber(&fakeSubscriber{}).
		Build()
	require.NoError(t, err)

	require.NoError(t, r.Drain(context.Background()),
		"draining a receiver that never started is a clean no-op")

	fc := &fakeChannel{deliveriesChan: make(chan amqp.Delivery, 4)}
	startErr := r.Start(context.Background(), fc)
	require.Error(t, startErr, "a drain must be remembered by a receiver that had not started")
	assert.Contains(t, startErr.Error(), "drained")

	fc.mu.Lock()
	defer fc.mu.Unlock()
	assert.Zero(t, fc.consumeCalls, "the consumer was registered after the drain")
}

// Withdrawing the consumer is a synchronous round trip with no deadline of its
// own: it unblocks when the heartbeat reader gives up — three missed intervals
// — and never at all when heartbeats are disabled. Running it inline would put
// the unbounded part *before* the bound, handing a broker that has stopped
// answering the power to keep the process from exiting.
func TestDrainIsBoundedEvenWhileWithdrawingTheConsumer(t *testing.T) {
	sub := &fakeSubscriber{}
	r, fc, _ := drainFixture(t, AckAfterHandling, sub)

	block := make(chan struct{})
	defer close(block)
	fc.mu.Lock()
	fc.cancelBlock = block
	fc.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	// Off the test's own goroutine, so a drain that blocks on the cancel fails
	// this line rather than hanging the package until some later timeout.
	result := make(chan error, 1)
	go func() { result <- r.Drain(ctx) }()

	select {
	case err := <-result:
		require.Error(t, err, "the drain waited for a cancel that never came back")
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(2 * time.Second):
		t.Fatal("the drain blocked on withdrawing the consumer, past its own deadline")
	}
}

// A cancel that outlives the budget must still stop the loop. Withdrawing the
// consumer is the graceful way out; when it does not come back in time the
// blunt one has to happen anyway, or the receiver goes on consuming straight
// through the phase that is waiting for it to have finished — which is the
// whole hazard the drain exists to remove, reached by way of its own timeout.
func TestATimedOutDrainStopsTheLoopAnyway(t *testing.T) {
	sub := &fakeSubscriber{}
	r, fc, a := drainFixture(t, AckAfterHandling, sub)

	block := make(chan struct{})
	defer close(block)
	fc.mu.Lock()
	fc.cancelBlock = block
	fc.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	result := make(chan error, 1)
	go func() { result <- r.Drain(ctx) }()
	select {
	case err := <-result:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("the drain never returned")
	}

	// The consumer was never withdrawn — the cancel is still blocked — so the
	// delivery channel is still open and a message can still be offered. It
	// must not be consumed.
	require.True(t, deliver(fc, a, "after", 1),
		"the fake's delivery channel should still be open")
	time.Sleep(200 * time.Millisecond)

	sub.mu.Lock()
	defer sub.mu.Unlock()
	assert.Zero(t, sub.calls, "the receiver kept consuming after a drain that timed out")
}

// And it reports the timeout rather than a clean drain. Once the deadline has
// passed the loop ends almost at once, so a wait that raced the two would pick
// between them at random — and half the time would report success for a
// receiver whose consumer may still be registered at the broker.
//
// Repeated because the failure it guards against is a random choice between two
// ready channels, so one run proves nothing. The count is calibrated for
// `-race`, which is how this project runs its tests (and which schedules the
// two arms far enough apart to make the bug reliable): under `-race` a
// regression fails ~18 runs in 20, without it roughly one in fifteen.
func TestATimedOutDrainNeverReportsSuccess(t *testing.T) {
	for i := 0; i < 50; i++ {
		sub := &fakeSubscriber{}
		r, fc, _ := drainFixture(t, AckAfterHandling, sub)

		block := make(chan struct{})
		fc.mu.Lock()
		fc.cancelBlock = block
		fc.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)

		// Off the test goroutine: a regression that blocked here rather than
		// returning early would otherwise hang the package instead of failing
		// the line that knows what went wrong — fifty chances to be the hang.
		result := make(chan error, 1)
		go func() { result <- r.Drain(ctx) }()

		var err error
		select {
		case err = <-result:
		case <-time.After(2 * time.Second):
			close(block)
			cancel()
			t.Fatalf("the drain blocked on run %d instead of returning at its deadline", i)
		}
		cancel()
		close(block)

		require.Error(t, err, "a drain that blew its deadline reported success on run %d", i)
	}
}

// Teardown calls both, in that order, and a receiver that was never started is
// torn down along with everything else. None of that may panic or block.
func TestDrainAndStopComposeInAnyOrder(t *testing.T) {
	r, _, _ := drainFixture(t, AckAfterHandling, &fakeSubscriber{})

	require.NoError(t, drained(t, r))
	require.NoError(t, drained(t, r))
	require.NoError(t, r.Stop())
	require.NoError(t, r.Stop())
	require.NoError(t, drained(t, r))

	fresh, err := NewReceiver().WithQueue("never-started").WithSubscriber(&fakeSubscriber{}).Build()
	require.NoError(t, err)
	assert.NoError(t, fresh.Drain(context.Background()))
	assert.NoError(t, fresh.Stop())
}
