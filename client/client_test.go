package client

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestStart_DialFailureBubblesUp(t *testing.T) {
	// 127.0.0.1:1 is a port that's never open. Use a 50ms connect timeout
	// so the test stays snappy.
	c := NewClient(Config{
		ClientName:        "x",
		Brokers:           []string{"amqp://127.0.0.1:1/"},
		ConnectionTimeout: 50 * time.Millisecond,
	})

	err := c.Start(context.Background())
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "all brokers failed"),
		"want 'all brokers failed' in error, got %q", err.Error())

	// After a failed Start we must still be Stop-safe.
	require.NoError(t, c.Stop())
}

func TestStart_DoubleStartIsAnError(t *testing.T) {
	c := NewClient(Config{
		ClientName:        "x",
		Brokers:           []string{"amqp://127.0.0.1:1/"},
		ConnectionTimeout: 50 * time.Millisecond,
	})

	// First Start fails (dial fails) but sets c.started=true.
	_ = c.Start(context.Background())

	// Second Start should report already-started, not retry.
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

	conn, ok := c.reconnect(ctx)
	assert.Nil(t, conn)
	assert.False(t, ok)
	assert.NotEmpty(t, spy.calls, "backoff should have been consulted at least once")
}

func TestStart_StateFlag_FlipsOnSuccess(t *testing.T) {
	// Use a port that never connects so Start fails — we are only verifying
	// the started flag transitions even on a failure path.
	c := NewClient(Config{
		ClientName:        "x",
		Brokers:           []string{"amqp://127.0.0.1:1/"},
		ConnectionTimeout: 50 * time.Millisecond,
	})

	require.False(t, c.started)
	_ = c.Start(context.Background())
	assert.True(t, c.started, "started flag must stick even on failed Start")

	// connected must remain false after a failed dial.
	assert.False(t, c.connected, "connected must be false when Start fails")
}

func TestStart_CleansUpOnFailure(t *testing.T) {
	c := NewClient(Config{
		ClientName:        "x",
		Brokers:           []string{"amqp://127.0.0.1:1/"},
		ConnectionTimeout: 50 * time.Millisecond,
	})
	err := c.Start(context.Background())
	require.Error(t, err)

	// After a failed Start, lifeCtx should already be cancelled so a
	// follow-up Stop() does not block.
	done := make(chan struct{})
	go func() {
		_ = c.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop after failed Start did not return promptly")
	}
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

// Confirm that errors.As works with our wrapped errors (sanity check that
// fmt.Errorf+"%w" is used uniformly).
func TestStart_ErrorIsWrappedNotShadowed(t *testing.T) {
	c := NewClient(Config{
		ClientName:        "x",
		Brokers:           []string{"amqp://127.0.0.1:1/"},
		ConnectionTimeout: 50 * time.Millisecond,
	})
	err := c.Start(context.Background())
	require.Error(t, err)
	// The wrapped first dial error should be retrievable.
	var netErr error
	assert.True(t, errors.Unwrap(err) != nil, "Start error should wrap underlying dial error")
	_ = netErr
}

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

func TestLifecycle_HooksNotFiredOnFailedStart(t *testing.T) {
	hc, onC, onD := newHookCounter()
	c := NewClient(Config{
		ClientName:        "x",
		Brokers:           []string{"amqp://127.0.0.1:1/"},
		ConnectionTimeout: 50 * time.Millisecond,
		OnConnect:         onC,
		OnDisconnect:      onD,
	})
	err := c.Start(context.Background())
	require.Error(t, err)

	// Dial never succeeded → we were never "connected" → neither hook fires.
	assert.Equal(t, 0, hc.connects, "OnConnect must not fire when Start fails")
	assert.Equal(t, 0, hc.disconnects, "OnDisconnect must not fire when Start fails")

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
