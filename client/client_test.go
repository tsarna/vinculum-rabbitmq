package client

import (
	"context"
	"net"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bus "github.com/tsarna/vinculum-bus"
	"github.com/tsarna/vinculum-rabbitmq/receiver"
)

func TestDefaultReconnectBackoff_ExponentialWithCap(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 32 * time.Second},
		{6, 60 * time.Second}, // 64s capped at 60s
		{7, 60 * time.Second},
		{20, 60 * time.Second},
	}
	for _, tt := range tests {
		got := DefaultReconnectBackoff(tt.attempt)
		assert.Equal(t, tt.want, got, "attempt %d", tt.attempt)
	}
}

func TestNewClient_FillsDefaultBackoffWhenNil(t *testing.T) {
	c := NewClient(Config{ClientName: "x"})
	require.NotNil(t, c.cfg.ReconnectBackoff)
	// Should match the default.
	assert.Equal(t, DefaultReconnectBackoff(0), c.cfg.ReconnectBackoff(0))
	assert.Equal(t, DefaultReconnectBackoff(5), c.cfg.ReconnectBackoff(5))
}

func TestNewClient_KeepsCustomBackoff(t *testing.T) {
	custom := func(attempt int) time.Duration { return 7 * time.Millisecond }
	c := NewClient(Config{ClientName: "x", ReconnectBackoff: custom})
	assert.Equal(t, 7*time.Millisecond, c.cfg.ReconnectBackoff(0))
	assert.Equal(t, 7*time.Millisecond, c.cfg.ReconnectBackoff(99))
}

func TestStop_BeforeStartIsNoop(t *testing.T) {
	c := NewClient(Config{ClientName: "x", Brokers: []string{"amqp://localhost/"}})
	require.NoError(t, c.Stop())
	// Calling Stop a second time is still a no-op.
	require.NoError(t, c.Stop())
}

func TestStart_RejectsEmptyBrokers(t *testing.T) {
	c := NewClient(Config{ClientName: "x"})
	err := c.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no brokers")
}

func TestStart_DoubleStartIsAnError(t *testing.T) {
	c := NewClient(Config{
		ClientName:        "x",
		Brokers:           []string{"amqp://127.0.0.1:1/"},
		ConnectionTimeout: 50 * time.Millisecond,
	})

	t.Cleanup(func() { _ = c.Stop() })

	// The first Start returns without waiting for a broker, and sets started.
	require.NoError(t, c.Start(context.Background()))

	// Second Start should report already-started, not launch a second loop.
	err := c.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already started")
}

func TestReconnectBackoff_Custom_CanReturnZero(t *testing.T) {
	// A zero backoff is allowed (rapid retry).
	zeroBackoff := func(int) time.Duration { return 0 }
	c := NewClient(Config{ClientName: "x", ReconnectBackoff: zeroBackoff})
	assert.Equal(t, time.Duration(0), c.cfg.ReconnectBackoff(0))
	assert.Equal(t, time.Duration(0), c.cfg.ReconnectBackoff(5))
}

// errBackoff records every backoff invocation so tests can introspect.
type backoffSpy struct {
	calls []int
}

func (b *backoffSpy) call(attempt int) time.Duration {
	b.calls = append(b.calls, attempt)
	return time.Millisecond
}

func TestReconnect_ContextCancelExitsBackoff(t *testing.T) {
	// We can't easily trigger a full reconnect cycle without a real broker,
	// but we can verify the inner reconnect loop respects ctx cancellation
	// during backoff sleep by driving it directly.
	spy := &backoffSpy{}
	c := NewClient(Config{
		ClientName:       "x",
		Brokers:          []string{"amqp://127.0.0.1:1/"},
		ReconnectBackoff: spy.call,
	})

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after a brief delay so the loop has a chance to make at least
	// one dial attempt and call into the backoff function.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	conn, ok := c.connect(ctx, false)
	assert.Nil(t, conn)
	assert.False(t, ok)
	assert.NotEmpty(t, spy.calls, "backoff should have been consulted at least once")
}

// countingBackoff returns a near-zero delay and runs a hook after each call, so
// a test can end an otherwise unbounded loop deterministically rather than on a
// timer.
type countingBackoff struct {
	calls []int
	after func(call int)
}

func (b *countingBackoff) call(attempt int) time.Duration {
	b.calls = append(b.calls, attempt)
	if b.after != nil {
		b.after(len(b.calls))
	}
	return time.Millisecond
}

func TestReconnect_GivesUpAtMaxAttempts(t *testing.T) {
	spy := &countingBackoff{}
	c := NewClient(Config{
		ClientName:           "x",
		Brokers:              []string{"amqp://127.0.0.1:1/"},
		ReconnectBackoff:     spy.call,
		MaxReconnectAttempts: 3,
	})

	// No cancellation anywhere: the limit alone has to end the loop, which is
	// the whole point of the field.
	conn, ok := c.connect(context.Background(), false)
	assert.Nil(t, conn)
	assert.False(t, ok)

	// A limit of 3 dials the broker list three times, so the backoff is
	// consulted for attempts 0, 1 and 2 and the loop exits before a fourth dial.
	assert.Equal(t, []int{0, 1, 2}, spy.calls)
}

// Zero is the Go zero value of the field, so it has to keep meaning "forever" —
// otherwise adding the field would silently stop every existing client from
// reconnecting.
func TestReconnect_ZeroMaxAttemptsIsUnlimited(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	spy := &countingBackoff{after: func(call int) {
		if call == 6 {
			cancel() // well past any plausible small limit
		}
	}}
	c := NewClient(Config{
		ClientName:           "x",
		Brokers:              []string{"amqp://127.0.0.1:1/"},
		ReconnectBackoff:     spy.call,
		MaxReconnectAttempts: 0,
	})

	conn, ok := c.connect(ctx, false)
	assert.Nil(t, conn)
	assert.False(t, ok)
	assert.Equal(t, []int{0, 1, 2, 3, 4, 5}, spy.calls,
		"kept retrying until the context was cancelled")
}

// MaxReconnectAttempts bounds a repair, never the first connection. Config
// documents it as governing reconnection, and a broker that is not listening
// yet at process start is an ordinary situation rather than one to give up on —
// the host reports itself not-ready throughout, which is the honest thing to do
// and costs nothing to keep doing.
func TestConnect_InitialIgnoresMaxAttempts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	spy := &countingBackoff{after: func(call int) {
		if call == 6 {
			cancel() // well past the limit of 3 below
		}
	}}
	c := NewClient(Config{
		ClientName:           "x",
		Brokers:              []string{"amqp://127.0.0.1:1/"},
		ReconnectBackoff:     spy.call,
		MaxReconnectAttempts: 3,
	})

	conn, ok := c.connect(ctx, true)
	assert.Nil(t, conn)
	assert.False(t, ok)
	assert.Equal(t, []int{0, 1, 2, 3, 4, 5}, spy.calls,
		"the initial connection retries past the limit that bounds a reconnect")
}

func TestStart_MarksStartedWithoutConnecting(t *testing.T) {
	// A port nothing is listening on: the client is started and trying, but
	// has not connected and will not while this test runs.
	c := NewClient(Config{
		ClientName:        "x",
		Brokers:           []string{"amqp://127.0.0.1:1/"},
		ConnectionTimeout: 50 * time.Millisecond,
	})
	t.Cleanup(func() { _ = c.Stop() })

	require.False(t, c.started)
	require.NoError(t, c.Start(context.Background()),
		"an unreachable broker is not a Start failure; it is what the retry loop is for")
	assert.True(t, c.started)
	assert.False(t, c.IsConnected(), "connected must be false until a broker answers")
}

// The behaviour this restructure exists for. Start used to dial synchronously,
// so a caller could only learn the broker was down by blocking on it — and the
// reconnect watcher was spawned only after a first success, leaving the client
// dead for the life of the process.
func TestStart_DoesNotWaitForABroker(t *testing.T) {
	c := NewClient(Config{
		ClientName: "x",
		// Several unreachable brokers, so a synchronous implementation would
		// walk the whole list before returning.
		Brokers: []string{
			"amqp://127.0.0.1:1/",
			"amqp://127.0.0.1:2/",
			"amqp://127.0.0.1:3/",
		},
		ReconnectBackoff: func(int) time.Duration { return 10 * time.Millisecond },
	})
	t.Cleanup(func() { _ = c.Stop() })

	done := make(chan error, 1)
	go func() { done <- c.Start(context.Background()) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Start blocked on brokers that are not listening")
	}
}

// Start launching a *loop* is the whole fix, so this counts real TCP dials
// rather than trusting that a goroutine exists. The listener accepts and
// immediately closes, so every attempt gets a connection and then fails the
// AMQP handshake — which is what a broker that is up but not ready looks like,
// and exactly the case the old code turned into a permanent outage.
//
// Before the restructure this could not reach two: a failed first dial returned
// from Start, and the watcher that would have retried was spawned only after a
// success that never came.
func TestStart_KeepsDialingAfterTheFirstFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	dials := make(chan struct{}, 16)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.Close() // fail the handshake
			select {
			case dials <- struct{}{}:
			default:
			}
		}
	}()

	c := NewClient(Config{
		ClientName:       "x",
		Brokers:          []string{"amqp://" + ln.Addr().String() + "/"},
		ReconnectBackoff: func(int) time.Duration { return 5 * time.Millisecond },
	})
	t.Cleanup(func() { _ = c.Stop() })

	require.NoError(t, c.Start(context.Background()))

	for i := 1; i <= 3; i++ {
		select {
		case <-dials:
		case <-time.After(2 * time.Second):
			t.Fatalf("only saw %d dial(s); the connect loop is not retrying", i-1)
		}
	}
	assert.False(t, c.IsConnected(), "a closed connection is not a usable one")
}

// Stop has to end the connect loop as well as tear down. The loop is unbounded
// for an initial connection, so without this a client that never reached a
// broker would keep dialing for the life of the process.
func TestStop_EndsAConnectThatNeverSucceeded(t *testing.T) {
	c := NewClient(Config{
		ClientName:       "x",
		Brokers:          []string{"amqp://127.0.0.1:1/"},
		ReconnectBackoff: func(int) time.Duration { return 10 * time.Millisecond },
	})
	require.NoError(t, c.Start(context.Background()))

	// Let it get into the retry loop rather than catching it before it starts.
	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		_ = c.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return promptly while the connect loop was running")
	}
}

// Configuration, not connectivity: the two things Start can still refuse.
func TestStart_RejectsAConfigurationItCannotUse(t *testing.T) {
	c := NewClient(Config{ClientName: "x"})
	assert.ErrorContains(t, c.Start(context.Background()), "no brokers configured")

	// And rejecting it must not leave state behind that makes Stop wait for a
	// goroutine no one launched.
	done := make(chan struct{})
	go func() {
		_ = c.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop hung after a Start that never launched anything")
	}

	c2 := NewClient(Config{ClientName: "x", Brokers: []string{"amqp://127.0.0.1:1/"}})
	t.Cleanup(func() { _ = c2.Stop() })
	require.NoError(t, c2.Start(context.Background()))
	assert.ErrorContains(t, c2.Start(context.Background()), "already started")
}

func TestRedactURL(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"amqp://user:pass@host:5672/vhost", "amqp://host:5672/vhost"},
		{"amqps://broker.example.com:5671/", "amqps://broker.example.com:5671/"},
		{"amqp://broker:5672/", "amqp://broker:5672/"},
		// Invalid input falls back to raw.
		{"http://x", "http://x"},
	}
	for _, tt := range tests {
		got := redactURL(tt.in)
		assert.Equal(t, tt.want, got, "input %q", tt.in)
	}
}

// TestStart_ErrorIsWrappedNotShadowed is gone with the code path it checked.
// It asserted that Start's returned dial error wrapped the underlying network
// error; Start no longer dials, and the two errors it can still return are
// configuration failures with nothing beneath them to wrap.

// ─── channel-level recovery ──────────────────────────────────────────────────

func TestIsConnectionLevel(t *testing.T) {
	tests := []struct {
		name string
		err  *amqp.Error
		want bool
	}{
		{"nil error means conn-level (library quirk)", nil, true},
		{"404 NOT_FOUND is channel-level", &amqp.Error{Code: 404}, false},
		{"405 RESOURCE_LOCKED is channel-level", &amqp.Error{Code: 405}, false},
		{"406 PRECONDITION_FAILED is channel-level", &amqp.Error{Code: 406}, false},
		{"403 ACCESS_REFUSED is channel-level", &amqp.Error{Code: 403}, false},
		{"500 is connection-level", &amqp.Error{Code: 500}, true},
		{"501 FRAME_ERROR is connection-level", &amqp.Error{Code: 501}, true},
		{"504 CHANNEL_ERROR is connection-level (per AMQP)", &amqp.Error{Code: 504}, true},
		{"540 NOT_IMPLEMENTED is connection-level", &amqp.Error{Code: 540}, true},
		{"541 INTERNAL_ERROR is connection-level", &amqp.Error{Code: 541}, true},
		{"200 (success / normal close) is not conn-level", &amqp.Error{Code: 200}, false},
		// 320 (CONNECTION_FORCED) is semantically connection-level but lives
		// in the 3xx soft-error range. The classifier currently treats it
		// as channel-level; recovery will fail fast (conn is dead) and the
		// connection-level reconnect loop will take over. Documenting via
		// test so a future tightening of the classifier is intentional.
		{"320 CONNECTION_FORCED currently routes to channel recovery", &amqp.Error{Code: 320}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isConnectionLevel(tt.err))
		})
	}
}

func TestRecoverSenderChannel_FailsWhenNotConnected(t *testing.T) {
	c := NewClient(Config{ClientName: "x"})
	// senders nil + not connected → should fail fast without panicking on
	// the senders slice index because we bail out before touching it.
	_, err := c.recoverSenderChannel(0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection is not currently established")
}

func TestRecoverReceiverChannel_FailsWhenNotConnected(t *testing.T) {
	c := NewClient(Config{ClientName: "x"})
	_, err := c.recoverReceiverChannel(context.Background(), 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection is not currently established")
}

// A channel recovery that lands after the drain must not put the receiver back
// to consuming. The receiver refuses on its own account, under its own lock,
// which is what makes that a guarantee; this is the client declining before it
// spends two round trips on a channel nothing will use — and reporting it as a
// decision rather than as a failure, since there is no reconnect coming.
func TestRecoverReceiverChannel_DeclinesAfterADrain(t *testing.T) {
	c := NewClient(Config{ClientName: "x"})
	c.receivers = []*receiver.RMQReceiver{mustReceiver(t)}

	// Connected, so the check under test is the one that answers.
	c.mu.Lock()
	c.connected = true
	c.conn = &amqp.Connection{}
	c.mu.Unlock()

	require.NoError(t, c.Drain(context.Background()),
		"draining a client whose receivers never started is a clean no-op")

	_, err := c.recoverReceiverChannel(context.Background(), 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, errDrained,
		"a decline must be distinguishable from a recovery that failed")
}

// The other direction — that a client which was never drained still recovers —
// is not tested here and cannot cheaply be: everything past the check calls
// conn.Channel(), and the package has no connection fake. What stands in for it
// is that a client is only drained by Drain or Stop, asserted below, and that
// nothing else in this file drains.

// Stop implies the drain, so a recovery racing a plain Stop is declined too
// rather than reopening a channel the teardown is about to close.
func TestStopMarksTheClientDrained(t *testing.T) {
	c := NewClient(Config{ClientName: "x"})
	require.NoError(t, c.Stop())
	assert.True(t, c.isDrained())
}

func mustReceiver(t *testing.T) *receiver.RMQReceiver {
	t.Helper()
	r, err := receiver.NewReceiver().
		WithQueue("q").
		WithSubscriber(&noopSubscriber{}).
		Build()
	require.NoError(t, err)
	return r
}

type noopSubscriber struct{ bus.BaseSubscriber }

func (noopSubscriber) OnEvent(context.Context, string, any, map[string]string) error { return nil }

// ─── on_connect / on_disconnect lifecycle ───────────────────────────────────

// hookCounter records hook invocations for assertions.
type hookCounter struct {
	connects    int
	disconnects int
}

func newHookCounter() (*hookCounter, func(context.Context), func(context.Context)) {
	hc := &hookCounter{}
	return hc,
		func(context.Context) { hc.connects++ },
		func(context.Context) { hc.disconnects++ }
}

func TestLifecycle_HooksNotFiredBeforeStart(t *testing.T) {
	hc, onC, onD := newHookCounter()
	_ = NewClient(Config{
		ClientName:   "x",
		Brokers:      []string{"amqp://127.0.0.1:1/"},
		OnConnect:    onC,
		OnDisconnect: onD,
	})
	assert.Equal(t, 0, hc.connects)
	assert.Equal(t, 0, hc.disconnects)
}

// The hooks describe transitions of a connection, so a client that never had
// one must fire neither — however long it spends trying. The contract survives
// the restructure unchanged; only the way the failure is reached has moved,
// from a returned error to a retry loop that has not succeeded yet.
func TestLifecycle_HooksNotFiredWithoutAConnection(t *testing.T) {
	hc, onC, onD := newHookCounter()
	c := NewClient(Config{
		ClientName:        "x",
		Brokers:           []string{"amqp://127.0.0.1:1/"},
		ConnectionTimeout: 50 * time.Millisecond,
		ReconnectBackoff:  func(int) time.Duration { return 10 * time.Millisecond },
		OnConnect:         onC,
		OnDisconnect:      onD,
	})
	require.NoError(t, c.Start(context.Background()))

	// Long enough for several failed attempts, so this is "never fired", not
	// "not fired yet".
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 0, hc.connects, "OnConnect must not fire without a connection")
	assert.Equal(t, 0, hc.disconnects, "OnDisconnect must not fire without a connection")

	// And a follow-up Stop must not fire it either.
	require.NoError(t, c.Stop())
	assert.Equal(t, 0, hc.disconnects)
}

func TestLifecycle_HandleDisconnectFiresOnceWhenConnected(t *testing.T) {
	hc, _, onD := newHookCounter()
	c := NewClient(Config{ClientName: "x", OnDisconnect: onD})

	// Simulate a successful Start by flipping connected=true.
	c.mu.Lock()
	c.connected = true
	c.mu.Unlock()

	c.handleDisconnect(context.Background())
	assert.Equal(t, 1, hc.disconnects)

	c.mu.Lock()
	assert.False(t, c.connected, "handleDisconnect must clear the connected flag")
	c.mu.Unlock()
}

func TestLifecycle_HandleDisconnect_DoesNotFireWhenNotConnected(t *testing.T) {
	hc, _, onD := newHookCounter()
	c := NewClient(Config{ClientName: "x", OnDisconnect: onD})

	// Not flipped to connected → handleDisconnect is a no-op for the hook.
	c.handleDisconnect(context.Background())
	assert.Equal(t, 0, hc.disconnects)
}

func TestLifecycle_StopAfterHandleDisconnect_DoesNotDoubleFire(t *testing.T) {
	hc, _, onD := newHookCounter()
	c := NewClient(Config{
		ClientName:   "x",
		Brokers:      []string{"amqp://127.0.0.1:1/"},
		OnDisconnect: onD,
	})

	// Simulate the connection-dropped path: client was connected, then the
	// watch goroutine ran handleDisconnect.
	c.mu.Lock()
	c.connected = true
	c.started = true
	c.mu.Unlock()
	c.handleDisconnect(context.Background())
	require.Equal(t, 1, hc.disconnects)

	// A subsequent graceful Stop must not fire OnDisconnect a second time.
	require.NoError(t, c.Stop())
	assert.Equal(t, 1, hc.disconnects, "OnDisconnect must fire exactly once across the disconnect-then-Stop sequence")
}

func TestLifecycle_StopWhileConnectedFiresOnDisconnect(t *testing.T) {
	hc, _, onD := newHookCounter()
	c := NewClient(Config{
		ClientName:   "x",
		Brokers:      []string{"amqp://127.0.0.1:1/"},
		OnDisconnect: onD,
	})

	// Simulate a healthy connected state: started=true, connected=true.
	// No real conn or reconnect goroutine, so Stop just runs the
	// teardown path.
	c.mu.Lock()
	c.started = true
	c.connected = true
	c.mu.Unlock()

	require.NoError(t, c.Stop())
	assert.Equal(t, 1, hc.disconnects, "Stop on a connected client must fire OnDisconnect once")
}

func TestLifecycle_NilHooksAreSafe(t *testing.T) {
	// Build a client with no hooks; the same paths must not panic.
	c := NewClient(Config{ClientName: "x"})
	c.mu.Lock()
	c.connected = true
	c.started = true
	c.mu.Unlock()

	require.NotPanics(t, func() { c.handleDisconnect(context.Background()) })
	require.NotPanics(t, func() { _ = c.Stop() })
}
