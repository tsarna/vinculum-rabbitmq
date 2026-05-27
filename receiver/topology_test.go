package receiver

import (
	"context"
	"errors"
	"sync"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeChannel records topology calls. It satisfies the package's channel
// interface so tests can drive DeclareTopology without a real broker.
type fakeChannel struct {
	mu sync.Mutex

	qosCalls       int
	lastPrefetch   int
	lastGlobal     bool
	qosErr         error

	consumeCalls   int
	deliveriesChan chan amqp.Delivery
	consumeErr     error
	lastConsumeArgs consumeArgs

	declareCalls   []declareArgs
	declareErr     error
	declareReturn  amqp.Queue

	passiveCalls   []declareArgs
	passiveErr     error
	passiveReturn  amqp.Queue

	bindCalls []bindArgs
	bindErr   error
}

type consumeArgs struct {
	queue, consumer string
	autoAck, exclusive, noLocal, noWait bool
}

type declareArgs struct {
	name                                    string
	durable, autoDelete, exclusive, noWait  bool
}

type bindArgs struct {
	name, key, exchange string
	noWait              bool
}

func (f *fakeChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.qosCalls++
	f.lastPrefetch = prefetchCount
	f.lastGlobal = global
	return f.qosErr
}

func (f *fakeChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.consumeCalls++
	f.lastConsumeArgs = consumeArgs{queue, consumer, autoAck, exclusive, noLocal, noWait}
	if f.consumeErr != nil {
		return nil, f.consumeErr
	}
	if f.deliveriesChan == nil {
		f.deliveriesChan = make(chan amqp.Delivery)
	}
	return f.deliveriesChan, nil
}

func (f *fakeChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.declareCalls = append(f.declareCalls, declareArgs{name, durable, autoDelete, exclusive, noWait})
	return f.declareReturn, f.declareErr
}

func (f *fakeChannel) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.passiveCalls = append(f.passiveCalls, declareArgs{name, durable, autoDelete, exclusive, noWait})
	return f.passiveReturn, f.passiveErr
}

func (f *fakeChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bindCalls = append(f.bindCalls, bindArgs{name, key, exchange, noWait})
	return f.bindErr
}

// ─── DeclareTopology: passive ────────────────────────────────────────────────

func TestDeclareTopology_PassiveByDefault(t *testing.T) {
	r, err := NewReceiver().WithQueue("q1").WithSubscriber(&fakeSubscriber{}).Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	require.NoError(t, r.DeclareTopology(fc))

	require.Len(t, fc.passiveCalls, 1)
	require.Len(t, fc.declareCalls, 0)
	assert.Equal(t, "q1", fc.passiveCalls[0].name)
	assert.True(t, fc.passiveCalls[0].durable, "passive declare uses durable=true so it agrees with the broker's default")
	assert.False(t, fc.passiveCalls[0].autoDelete)
}

func TestDeclareTopology_PassiveFailIsWrapped(t *testing.T) {
	r, err := NewReceiver().WithQueue("absent").WithSubscriber(&fakeSubscriber{}).Build()
	require.NoError(t, err)

	fc := &fakeChannel{passiveErr: errors.New("NOT_FOUND - no queue 'absent'")}
	err = r.DeclareTopology(fc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "passive declare queue")
	assert.Contains(t, err.Error(), "absent")
}

// ─── DeclareTopology: active ─────────────────────────────────────────────────

func TestDeclareTopology_ActiveDeclare(t *testing.T) {
	r, err := NewReceiver().
		WithQueue("q2").
		WithSubscriber(&fakeSubscriber{}).
		WithDeclare(Declare{Durable: true, AutoDelete: false}).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	require.NoError(t, r.DeclareTopology(fc))

	require.Len(t, fc.declareCalls, 1)
	require.Len(t, fc.passiveCalls, 0)
	d := fc.declareCalls[0]
	assert.Equal(t, "q2", d.name)
	assert.True(t, d.durable)
	assert.False(t, d.autoDelete)
	assert.False(t, d.exclusive)
	assert.False(t, d.noWait)
}

func TestDeclareTopology_ActiveDeclare_AutoDelete(t *testing.T) {
	r, err := NewReceiver().
		WithQueue("ephemeral").
		WithSubscriber(&fakeSubscriber{}).
		WithDeclare(Declare{Durable: false, AutoDelete: true}).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	require.NoError(t, r.DeclareTopology(fc))

	require.Len(t, fc.declareCalls, 1)
	assert.False(t, fc.declareCalls[0].durable)
	assert.True(t, fc.declareCalls[0].autoDelete)
}

func TestDeclareTopology_ActiveDeclareFailIsWrapped(t *testing.T) {
	r, err := NewReceiver().
		WithQueue("conflict").
		WithSubscriber(&fakeSubscriber{}).
		WithDeclare(Declare{Durable: false}).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{declareErr: errors.New("PRECONDITION_FAILED - inequivalent arg 'durable'")}
	err = r.DeclareTopology(fc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "declare queue")
	assert.Contains(t, err.Error(), "conflict")
}

// ─── DeclareTopology: bindings ───────────────────────────────────────────────

func TestDeclareTopology_BindingsRunAfterDeclare(t *testing.T) {
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(&fakeSubscriber{}).
		WithBinding(Binding{Exchange: "sensor-events", RoutingKey: "sensor.#"}).
		WithBinding(Binding{Exchange: "alerts", RoutingKey: "alerts"}).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	require.NoError(t, r.DeclareTopology(fc))

	require.Len(t, fc.bindCalls, 2)
	assert.Equal(t, "q", fc.bindCalls[0].name)
	assert.Equal(t, "sensor.#", fc.bindCalls[0].key)
	assert.Equal(t, "sensor-events", fc.bindCalls[0].exchange)
	assert.Equal(t, "q", fc.bindCalls[1].name)
	assert.Equal(t, "alerts", fc.bindCalls[1].key)
	assert.Equal(t, "alerts", fc.bindCalls[1].exchange)
}

func TestDeclareTopology_BindingFailureIsWrapped(t *testing.T) {
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(&fakeSubscriber{}).
		WithBinding(Binding{Exchange: "missing-exchange", RoutingKey: "k"}).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{bindErr: errors.New("NOT_FOUND - no exchange 'missing-exchange'")}
	err = r.DeclareTopology(fc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bind queue")
	assert.Contains(t, err.Error(), "missing-exchange")
}

func TestDeclareTopology_PassiveDeclarePlusBindings(t *testing.T) {
	r, err := NewReceiver().
		WithQueue("preexisting").
		WithSubscriber(&fakeSubscriber{}).
		WithBinding(Binding{Exchange: "ex", RoutingKey: "k"}).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	require.NoError(t, r.DeclareTopology(fc))

	require.Len(t, fc.passiveCalls, 1)
	require.Len(t, fc.declareCalls, 0)
	require.Len(t, fc.bindCalls, 1)
}

func TestDeclareTopology_Idempotent_AcrossMultipleInvocations(t *testing.T) {
	// DeclareTopology may be called repeatedly (e.g. on reconnect or channel
	// recovery) without side effects beyond the broker-side declares.
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(&fakeSubscriber{}).
		WithDeclare(Declare{Durable: true}).
		WithBinding(Binding{Exchange: "ex", RoutingKey: "k"}).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	require.NoError(t, r.DeclareTopology(fc))
	require.NoError(t, r.DeclareTopology(fc))
	require.NoError(t, r.DeclareTopology(fc))

	assert.Len(t, fc.declareCalls, 3)
	assert.Len(t, fc.bindCalls, 3)
}

// ─── Start integration with the channel interface ───────────────────────────

func TestStart_QoSAndConsume(t *testing.T) {
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(&fakeSubscriber{}).
		WithPrefetch(50).
		WithConsumerTag("my-tag").
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	require.NoError(t, r.Start(context.Background(), fc))
	defer r.Stop()

	assert.Equal(t, 1, fc.qosCalls)
	assert.Equal(t, 50, fc.lastPrefetch)
	assert.False(t, fc.lastGlobal)

	assert.Equal(t, 1, fc.consumeCalls)
	assert.Equal(t, "q", fc.lastConsumeArgs.queue)
	assert.Equal(t, "my-tag", fc.lastConsumeArgs.consumer)
	assert.False(t, fc.lastConsumeArgs.autoAck)
	assert.False(t, fc.lastConsumeArgs.exclusive)
}

func TestStart_ExclusiveAndAutoAck(t *testing.T) {
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(&fakeSubscriber{}).
		WithExclusive(true).
		WithAutoAck(true).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	require.NoError(t, r.Start(context.Background(), fc))
	defer r.Stop()

	assert.True(t, fc.lastConsumeArgs.exclusive)
	assert.True(t, fc.lastConsumeArgs.autoAck)
}

func TestStart_RejectsDoubleStart(t *testing.T) {
	r, err := NewReceiver().WithQueue("q").WithSubscriber(&fakeSubscriber{}).Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	require.NoError(t, r.Start(context.Background(), fc))
	defer r.Stop()

	err = r.Start(context.Background(), fc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already started")
}

func TestStart_PrefetchZeroSkipsQos(t *testing.T) {
	// prefetch=0 means "unlimited" — receiver should not call Qos at all.
	r, err := NewReceiver().
		WithQueue("q").
		WithSubscriber(&fakeSubscriber{}).
		WithPrefetch(0).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	require.NoError(t, r.Start(context.Background(), fc))
	defer r.Stop()

	assert.Equal(t, 0, fc.qosCalls, "Qos must not be called when prefetch=0")
}

func TestStart_ConsumeErrorPropagates(t *testing.T) {
	r, err := NewReceiver().WithQueue("q").WithSubscriber(&fakeSubscriber{}).Build()
	require.NoError(t, err)

	fc := &fakeChannel{consumeErr: errors.New("ACCESS_REFUSED")}
	err = r.Start(context.Background(), fc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "consume")
}
