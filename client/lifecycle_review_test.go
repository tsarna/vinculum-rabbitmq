package client

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"github.com/tsarna/vinculum-rabbitmq/receiver"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// 406 starts a recovery whose receiver Stop waits on the stuck delivery; then
// the connection drops, the reconnect's Stop returns at once and the reconnect
// Starts a fresh loop on a new connection. Shutdown: Drain (bounded) then Stop.
func TestReview_RecoveryStuckThenReconnectThenShutdown(t *testing.T) {
	broker := newFakeBroker(t)
	core, logs := observer.New(zap.InfoLevel)

	stuck := &stuckSubscriber{entered: make(chan struct{}, 1), release: make(chan struct{})}
	t.Cleanup(func() { close(stuck.release) })
	r, err := receiver.NewReceiver().WithClientName("x").WithQueue("q").WithSubscriber(stuck).Build()
	require.NoError(t, err)
	c := NewClient(Config{
		ClientName:        "x",
		Brokers:           []string{broker.url()},
		Logger:            zap.New(core),
		ConnectionTimeout: 500 * time.Millisecond,
		ReconnectBackoff:  func(int) time.Duration { return 10 * time.Millisecond },
	})
	c.AddReceiver(r)
	require.NoError(t, c.Start(context.Background()))
	require.Eventually(t, c.IsConnected, 2*time.Second, 5*time.Millisecond)

	broker.deliver(1, "a.b", "stuck")
	<-stuck.entered

	broker.closeChannel(1, amqp.PreconditionFailed)
	require.Eventually(t, func() bool {
		return logs.FilterMessage("rabbitmq client: receiver channel closed; attempting recovery").Len() == 1
	}, 2*time.Second, 5*time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	broker.closeConnection(amqp.ConnectionForced)
	require.Eventually(t, func() bool {
		return logs.FilterMessage("rabbitmq client: connection closed; reconnecting").Len() == 1
	}, 3*time.Second, 5*time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	derr := c.Drain(ctx)
	t.Logf("Client.Drain returned %v", derr)

	stopped := make(chan struct{})
	go func() { _ = c.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		for _, e := range logs.All() {
			t.Log(e.Message)
		}
		t.Fatal("Client.Stop hung")
	}
}
