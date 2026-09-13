package receiver

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	amqp "github.com/rabbitmq/amqp091-go"
	bus "github.com/tsarna/vinculum-bus"
	"github.com/tsarna/vinculum-rabbitmq/carrier"
	wire "github.com/tsarna/vinculum-wire"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// channel is the subset of *amqp.Channel that RMQReceiver depends on. It
// exists so tests can substitute a fake channel without standing up a real
// broker. *amqp.Channel satisfies this interface implicitly — every method
// signature matches.
type channel interface {
	Qos(prefetchCount, prefetchSize int, global bool) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	// Cancel withdraws the consumer, which is how a receiver stops being sent
	// deliveries without giving up the channel it settles over. See Drain.
	Cancel(consumer string, noWait bool) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
}

// Declare describes optional active queue declaration. nil means passive
// (verify-only) declare.
type Declare struct {
	Durable    bool
	AutoDelete bool
}

// Binding describes a queue→exchange binding to declare on each connect or
// channel recovery. RoutingKey is the AMQP routing-key pattern.
type Binding struct {
	RoutingKey string
	Exchange   string
}

// RMQReceiver consumes AMQP deliveries from a single queue and dispatches
// them as vinculum events via a configured bus.Subscriber. Create via
// NewReceiver().Build().
//
// The receiver is a source (not a sink) — it does NOT implement bus.Subscriber.
// client.Client is responsible for: calling DeclareTopology on the channel,
// invoking Start with the live channel, and invoking Stop on shutdown or
// channel-level recovery.
type RMQReceiver struct {
	clientName    string
	queue         string
	subscriber    bus.Subscriber
	subscriptions []Subscription
	defaultXform  DefaultRoutingKeyTransform
	prefetch      int
	exclusive     bool
	ackMode       AckMode
	wireFormat    wire.WireFormat
	onDecodeError wire.DecodeErrorHook
	consumerTag   string
	declare       *Declare
	bindings      []Binding

	logger         *zap.Logger
	meterProvider  metric.MeterProvider
	tracerProvider trace.TracerProvider
	metrics        *ReceiverMetrics

	// Two cancels, because stopping is two things and a graceful shutdown wants
	// them apart. stopRead is the fallback way to end the delivery loop;
	// stopWork cancels the context every delivery and every settle rides on,
	// and so is the one that ends the receiver. stopRead's context is derived
	// from stopWork's, so cancelling work ends reading too.
	//
	// ch is kept because draining needs it: withdrawing the consumer is an
	// operation on the channel, and it is the one way to stop being sent
	// deliveries while keeping the channel that their acknowledgements travel
	// over. Nothing else here touches it — the client owns opening and closing.
	mu       sync.Mutex
	stopRead context.CancelFunc
	stopWork context.CancelFunc
	loopDone chan struct{}
	ch       channel
	// activeTag is the consumer tag this cycle registered under, which is what
	// Drain withdraws. It differs from consumerTag when nothing configured one.
	activeTag string

	// epoch identifies the channel deliveries are currently arriving on. A
	// delivery tag only means anything on the channel that issued it, so a
	// settler stamps the epoch it was built under and refuses once it has
	// moved. Stop is the only place it changes, because a receiver's channel is
	// only ever replaced across one — and Drain is deliberately not such a
	// place, which is the whole of what draining adds here.
	epoch atomic.Uint64

	// unsettled counts deliveries handed out and not yet acknowledged, nacked,
	// or abandoned. See Unsettled.
	unsettled atomic.Int64

	// drained says this receiver has been told to stop consuming, and is the
	// reason Start refuses afterwards. It is set under mu, which is what makes
	// it a guarantee rather than a hint: the client's own connect and recovery
	// paths decide whether to start a receiver and then start it, and a drain
	// landing between those two steps would otherwise slip through and register
	// the consumer again — during the phase that waits for it to have finished.
	// Taking the decision and the registration under one lock is what closes
	// that window.
	drained bool

	// stillDelivering is the loop-done channel of the last loop a Drain or a
	// Stop took charge of, closed when that loop finally finishes. Nil until
	// the first of them, and set by every one rather than only by one that
	// gives up — what makes it answer "no" is the channel being closed, not the
	// field being absent.
	//
	// Stop sets it as well as Drain because the channel-recovery and reconnect
	// paths stop a receiver on their own goroutines, and a shutdown's Drain can
	// arrive while one of those Stops is still waiting for a delivery. The
	// Drain finds the loop already taken, and this is how it finds the loop to
	// wait for instead of reporting a clean drain that has not happened.
	//
	// A channel rather than a flag because the question is asked a phase later
	// and the answer moves in between: teardown runs a whole quiesce between
	// Drain and Stop, so a delivery that overran the drain's deadline by a
	// moment has very likely finished by the time Stop looks. A flag would say
	// otherwise and put an error in the log of a shutdown where nothing went
	// wrong. See stillRunning.
	stillDelivering atomic.Pointer[chan struct{}]

	// gaveUp is closed the first time a Drain's deadline passes with a delivery
	// still running. A Stop waiting for the loop gives up with it, because the
	// drain has already given that delivery its bounded chance, and a Stop
	// that went on waiting would hand the same stuck action the unbounded wait
	// the drain's deadline exists to prevent. Terminal, like drained: a drain
	// always precedes it, so no later cycle can start.
	gaveUp     chan struct{}
	gaveUpOnce sync.Once
}

// giveUp records that a drain's deadline passed with a delivery still running.
func (r *RMQReceiver) giveUp() {
	r.gaveUpOnce.Do(func() { close(r.gaveUp) })
}

func (r *RMQReceiver) hasGivenUp() bool {
	select {
	case <-r.gaveUp:
		return true
	default:
		return false
	}
}

// Unsettled reports how many deliveries this receiver has handed out that
// nothing has settled yet.
//
// It is not the number of unacknowledged deliveries the broker is holding. A
// delivery this receiver has given up on — one whose channel went away, taking
// the only tag that could have acknowledged it — is the broker's business, and
// nothing in this process is going to settle it. What this counts is the
// narrower thing a shutdown can usefully wait for: settles that are still
// coming.
func (r *RMQReceiver) Unsettled() int { return int(r.unsettled.Load()) }

// stillRunning reports whether a delivery a drain gave up on is running *now*,
// rather than whether one ever was.
func (r *RMQReceiver) stillRunning() bool {
	ch := r.stillDelivering.Load()
	if ch == nil {
		return false
	}
	select {
	case <-*ch:
		return false
	default:
		return true
	}
}

// Queue returns the AMQP queue this receiver consumes from.
func (r *RMQReceiver) Queue() string { return r.queue }

// Subscriptions returns the configured routing-key subscriptions.
// client.Client uses this when declaring bindings on connect/reconnect.
func (r *RMQReceiver) Subscriptions() []Subscription { return r.subscriptions }

// DeclareTopology runs the optional active queue declare (or a passive-only
// verify, when WithDeclare was not called) and all WithBinding declarations
// on ch. client.Client calls this on every connect and on every channel
// recovery before invoking Start.
func (r *RMQReceiver) DeclareTopology(ch channel) error {
	if r.declare != nil {
		_, err := ch.QueueDeclare(r.queue, r.declare.Durable, r.declare.AutoDelete, false, false, nil)
		if err != nil {
			return fmt.Errorf("rabbitmq receiver: declare queue %q: %w", r.queue, err)
		}
	} else {
		_, err := ch.QueueDeclarePassive(r.queue, true, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("rabbitmq receiver: passive declare queue %q: %w", r.queue, err)
		}
	}
	for _, b := range r.bindings {
		if err := ch.QueueBind(r.queue, b.RoutingKey, b.Exchange, false, nil); err != nil {
			return fmt.Errorf("rabbitmq receiver: bind queue %q to exchange %q key %q: %w", r.queue, b.Exchange, b.RoutingKey, err)
		}
	}
	return nil
}

// Start applies QoS, registers the consumer on ch, and spawns the delivery
// loop goroutine. It is called by client.Client after the connection is
// established and topology has been declared on ch.
//
// A second call to Start without an intervening Stop is an error.
func (r *RMQReceiver) Start(ctx context.Context, ch channel) error {
	r.mu.Lock()
	// Refused after a drain, whether or not this receiver had started when the
	// drain reached it. A reconnect or a channel recovery ends here, and both
	// stay live until the connection is closed — so without this a shutdown
	// could go back to consuming after it had reported that it had stopped.
	//
	// It also covers the loop a timed-out Stop abandoned, which is the other
	// reason not to start again: that loop still owns loopDone, and a second
	// one over the top would leave two consumers on one channel and this
	// cycle's Stop waiting forever on the abandoned one. There is no way to
	// reach that state except through a Drain — a Stop with no drain before it
	// always waits — so this one flag answers for both, and a separate check on
	// the abandoned loop would be unreachable.
	// Checked before "already started", because after a drain it is the more
	// specific and more useful answer: a drained receiver is still started
	// until Stop runs, and reporting that would send a caller looking for a
	// double-Start that did not happen.
	if r.drained {
		r.mu.Unlock()
		return fmt.Errorf("rabbitmq receiver: drained, not restarting for queue %q", r.queue)
	}
	if r.stopWork != nil {
		r.mu.Unlock()
		return fmt.Errorf("rabbitmq receiver: already started for queue %q", r.queue)
	}
	r.mu.Unlock()

	if r.prefetch > 0 {
		if err := ch.Qos(r.prefetch, 0, false); err != nil {
			return fmt.Errorf("rabbitmq receiver: qos prefetch=%d: %w", r.prefetch, err)
		}
	}

	// A consumer tag this receiver chose. Passing an empty one does not leave
	// the consumer nameless — amqp091-go generates `ctag-<program>-<n>` — but it
	// does leave *us* without the name: Consume does not return it, and the
	// only other place it appears is on a delivery, which is no use before the
	// first one arrives. Draining withdraws the consumer by name, so without
	// this there is no way to stop consuming short of giving up the channel,
	// and giving up the channel is what invalidates every outstanding
	// acknowledgement. It doubles as the identity `rabbitmqctl list_consumers`
	// shows, which is an improvement on the generated string.
	//
	// Unique by construction: the client opens one channel per receiver, and a
	// tag only has to be unique on its own channel.
	tag := r.consumerTag
	if tag == "" {
		tag = "vinculum-" + r.clientName + "-" + r.queue
	}
	// Capped whichever it came from. A configured tag is no less able to
	// overrun a shortstr than a generated one, and the failure is silent.
	tag = capConsumerTag(tag)

	deliveries, err := ch.Consume(r.queue, tag, r.ackMode == AckNone, r.exclusive, false, false, nil)
	if err != nil {
		return fmt.Errorf("rabbitmq receiver: consume %q: %w", r.queue, err)
	}

	workCtx, stopWork := context.WithCancel(ctx)
	readCtx, stopRead := context.WithCancel(workCtx)
	done := make(chan struct{})

	r.mu.Lock()
	r.stopWork = stopWork
	r.stopRead = stopRead
	r.loopDone = done
	r.ch = ch
	r.activeTag = tag
	// stillDelivering is deliberately left as it is. The guard above has
	// already established that it is nil or closed, and a closed channel
	// answers stillRunning the same way nil does — until this cycle's own Drain
	// replaces it.
	r.mu.Unlock()

	go r.runLoop(readCtx, workCtx, deliveries, done)
	return nil
}

// capConsumerTag truncates a consumer tag to fit an AMQP shortstr.
//
// The cap is not advisory: the wire encoder writes the length as a single byte,
// so an over-length tag goes out silently truncated modulo 256 — and a tag that
// arrives shortened is one basic.cancel cannot name, which would leave a drain
// unable to withdraw the consumer it registered. The prefix is what makes a tag
// recognisable, so the tail is what gives way, on a rune boundary rather than
// mid-sequence.
func capConsumerTag(tag string) string {
	// The library's own limit, and the only one there is: a shortstr's length
	// is one byte. Capping shorter would silently rewrite a configured tag that
	// would have worked.
	const maxTag = 255
	if len(tag) <= maxTag {
		return tag
	}
	cut := maxTag
	for cut > 0 && !utf8.RuneStart(tag[cut]) {
		cut--
	}
	return tag[:cut]
}

// Drain withdraws the consumer and waits for the loop to finish what the
// broker had already sent. It leaves everything else alone: the channel stays
// open, the epoch does not move, and every delivery tag handed out stays good —
// so a message still travelling through a queue downstream acknowledges
// normally when the work lands.
//
// That last part is what separates this from Stop, and on AMQP it is the whole
// point: a delivery tag means nothing except on the channel that issued it, so
// a receiver that closed its channel to stop consuming would invalidate every
// outstanding acknowledgement in the act of stopping.
//
// basic.cancel rather than a cancelled context, because the broker then stops
// sending and closes the delivery channel *after* what it has already sent, so
// the prefetched backlog is handled rather than abandoned. With the default
// prefetch of ten that is up to ten messages that would otherwise be redelivered
// on the next boot. When the cancel cannot be sent — a channel already dying —
// the loop is ended the other way rather than waited on forever.
//
// Bounded by ctx, which the caller sizes: delivery runs user-supplied work.
//
// Safe to call before Start, after Stop, or twice — though a second call after
// one that timed out reports the timeout again rather than a clean drain, since
// the delivery it gave up on is still running.
//
// Terminal for the receiver: Start refuses afterwards, including on a receiver
// that had not started when the drain reached it. Draining is a shutdown, not a
// pause, and the client's reconnect and channel-recovery paths both end in
// Start — so anything short of a permanent refusal leaves a window in which one
// of them puts the receiver back to consuming.
func (r *RMQReceiver) Drain(ctx context.Context) error {
	r.mu.Lock()
	// Recorded first and under the same lock Start takes, so a receiver that
	// has not started yet is still refused afterwards. Draining one of those is
	// otherwise a no-op that reports success, and the reconnect that was
	// half-way through starting it would then carry on.
	r.drained = true

	stopRead := r.stopRead
	if stopRead == nil {
		r.mu.Unlock()
		return r.awaitLoopTakenElsewhere(ctx)
	}
	ch, tag, done := r.ch, r.activeTag, r.loopDone
	r.stopRead = nil

	// Published under the same lock that cleared stopRead, because the two
	// together are what a concurrent second Drain reads. Between them it would
	// see the field already taken and no waiter yet, and report a clean drain
	// that has not happened.
	if done != nil {
		r.stillDelivering.Store(&done)
	}
	r.mu.Unlock()

	// Withdrawing the consumer is a synchronous AMQP round trip, and it is off
	// the lock and behind ctx for the same reason: basic.cancel waits for
	// basic.cancel-ok with no deadline of its own, so against a connection that
	// has stopped answering it unblocks only when the heartbeat reader gives up
	// — three missed intervals, past a ten-second budget at the ten-second
	// default, and never at all when heartbeats are disabled. Waiting for it
	// inline would hand a dead broker the power to stop the process exiting,
	// which is the failure the bound exists to prevent, and holding the mutex
	// across it would block Stop behind the same wait.
	var cancelErr error
	var timedOut bool
	if ch != nil {
		sent := make(chan error, 1) // buffered: the goroutine outlives a timeout
		go func() { sent <- ch.Cancel(tag, false) }()
		select {
		case cancelErr = <-sent:
		case <-ctx.Done():
			timedOut = true
		}
	}
	if ch == nil || cancelErr != nil || timedOut {
		// No consumer to withdraw, or the broker could not be told, or it was
		// not told in time. Ending the loop directly abandons whatever the
		// broker already sent — those deliveries are unacknowledged, so they
		// are redelivered rather than lost, which is the same outcome as the
		// channel dying underneath us.
		stopRead()
	}

	// Reported only when the broker actually refused. A cancel that merely
	// outlived the budget may yet land, and may yet close the delivery stream
	// behind the backlog exactly as intended, so claiming a redelivery here
	// would be asserting an outcome that has not happened. The caller is told
	// the drain timed out, which is what did happen.
	if cancelErr != nil {
		r.logger.Warn("rabbitmq receiver: could not withdraw the consumer; "+
			"the deliveries the broker has already sent will be redelivered",
			zap.String("queue", r.queue),
			zap.String("consumer_tag", tag),
			zap.Error(cancelErr))
	}

	// Checked before the wait below rather than raced against it. Once the
	// deadline has passed, `done` closes almost at once — stopRead just ended
	// the loop — so both arms of that select are ready and the choice between
	// them is random. Half the time it would report a clean drain of a receiver
	// whose consumer may still be registered.
	if ctx.Err() != nil {
		r.giveUp()
		return fmt.Errorf("rabbitmq receiver: drain queue %q: %w", r.queue, ctx.Err())
	}

	if done == nil {
		return nil
	}
	select {
	case <-done:
		// The loop has gone, so cancelling the read context releases its node
		// rather than abandoning anything. Not done before the wait: it would
		// race the loop's own exit and cut the backlog short.
		stopRead()
		return nil
	case <-ctx.Done():
		r.giveUp()
		return fmt.Errorf("rabbitmq receiver: drain queue %q: %w", r.queue, ctx.Err())
	}
}

// awaitLoopTakenElsewhere is Drain for a receiver whose loop an earlier Drain or
// a Stop has already taken charge of. There is no consumer left to withdraw,
// but the loop may still be finishing a delivery, and a drain is a promise that
// the deliveries already sent have been handled.
//
// It waits only when nothing has given up on that delivery yet — which is the
// case of a Stop from the channel-recovery or reconnect path, still waiting
// when the shutdown's Drain arrives. After a drain has already timed out on it,
// waiting again would hand the stuck action a second chance to hold up the
// process, so the timeout is reported straight away instead.
func (r *RMQReceiver) awaitLoopTakenElsewhere(ctx context.Context) error {
	if !r.stillRunning() {
		return nil
	}
	if r.hasGivenUp() {
		return fmt.Errorf("rabbitmq receiver: still delivering for queue %q", r.queue)
	}

	// Not reloaded after stillRunning: this Drain has set drained, so no Start
	// can begin a new loop, and whoever else stores the field stores this one.
	done := *r.stillDelivering.Load()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		// Both may be ready at once, and a delivery that finished at the
		// deadline was handled, so it is not a timeout.
		select {
		case <-done:
			return nil
		default:
		}
		r.giveUp()
		return fmt.Errorf("rabbitmq receiver: drain queue %q: %w", r.queue, ctx.Err())
	}
}

// Stop signals the delivery loop to exit and waits for it, then retires every
// delivery tag it handed out. It does not close the channel — that is the
// client's responsibility, so that channel-level recovery (Stop + Start with a
// fresh channel) is cheap.
//
// It waits for the loop, so a delivery still running finishes and settles
// normally — with one exception. A Drain that timed out has already given that
// delivery a bounded chance to finish, and it did not take it; waiting here
// would hand the same expression a second wait with no bound at all. So Stop
// cancels and reports rather than blocking, because the one thing a stuck
// action must never be able to do is stop the process from exiting. Whether it
// is *still* running is checked here rather than remembered from the drain, a
// whole phase earlier.
//
// That holds whichever comes first. The client's channel-recovery and
// reconnect paths call Stop on their own goroutines, and one of those can
// already be waiting when a shutdown's Drain arrives. The Drain then waits for
// the same loop under its own deadline, and if the deadline passes this Stop
// stops waiting too — otherwise the goroutine it runs on, and the client's Stop
// that joins that goroutine, would wait on the stuck action forever.
//
// Safe to call before Start (no-op) or repeatedly; repeated calls repeat the
// answer, as Drain's do.
func (r *RMQReceiver) Stop() error {
	r.mu.Lock()
	stopWork := r.stopWork
	done := r.loopDone
	r.stopRead, r.stopWork, r.loopDone, r.ch = nil, nil, nil, nil
	// Published under the lock that took the loop, for the same reason Drain
	// publishes it: a concurrent Drain reads the two together, and between them
	// would find the loop gone and nothing to wait for.
	if done != nil {
		r.stillDelivering.Store(&done)
	}
	r.mu.Unlock()

	if stopWork == nil {
		if p := r.stillDelivering.Load(); p != nil {
			select {
			case <-*p:
				return nil
			case <-r.gaveUp:
			}
		}
		return r.stoppedWithDeliveryRunning()
	}
	// Cancelling work cancels reading with it: the read context is derived from
	// this one, so a Stop that was not preceded by a Drain still ends the loop.
	stopWork()

	if done != nil {
		select {
		case <-done:
		case <-r.gaveUp:
			select {
			case <-done:
			default:
				// The epoch still moves. The channel is about to go, and a tag
				// that outlives it acknowledges nothing — or worse, on a
				// reconnected channel, acknowledges something else.
				r.epoch.Add(1)
				return r.stoppedWithDeliveryRunning()
			}
		}
	}

	// After the loop has drained, not before. Waiting for it means a delivery
	// still being handled synchronously when Stop was called acknowledges
	// normally, which is what makes an ordinary shutdown not redeliver its last
	// message. Only a settle arriving after handling has finished — one held by
	// an async queue, or by a configuration settling on its own schedule — sees
	// the new epoch, and for that one the tag really is gone.
	//
	// Draining first is what makes that set small: by the time a graceful
	// shutdown reaches here, the pipeline has emptied and those settles have
	// already been made.
	r.epoch.Add(1)
	return nil
}

func (r *RMQReceiver) stoppedWithDeliveryRunning() error {
	if !r.stillRunning() {
		return nil
	}
	return fmt.Errorf("rabbitmq receiver: stopped with a delivery still running for queue %q", r.queue)
}

// runLoop reads deliveries until the read context is cancelled or the
// deliveries channel is closed (the broker cancelled us, we withdrew the
// consumer, the channel closed, or the connection dropped). Each delivery is
// dispatched via handleDelivery.
//
// The two contexts are the same lifetime until a drain separates them. readCtx
// is the fallback way out of this loop; workCtx is what every delivery and
// every settle runs on, and outlives readCtx by the length of the shutdown.
// Passing readCtx to a delivery would mean the fallback cancelled the work it
// was ending, and cancelled the acknowledgement that work was about to produce.
//
// A drain does not use readCtx at all in the ordinary case: withdrawing the
// consumer closes the deliveries channel behind whatever the broker had already
// sent, so the loop finishes that backlog and leaves through the `!ok` branch.
func (r *RMQReceiver) runLoop(readCtx, workCtx context.Context, deliveries <-chan amqp.Delivery, done chan struct{}) {
	defer close(done)
	for {
		select {
		case <-readCtx.Done():
			return
		case d, ok := <-deliveries:
			if !ok {
				// Broker stopped sending us deliveries (consumer cancelled,
				// channel closed, or connection dropped). The client owns
				// recovery; we just exit.
				return
			}
			r.handleDelivery(workCtx, d)
		}
	}
}

// handleDelivery dispatches a single AMQP delivery. It is exposed for unit
// tests; production callers should use the goroutine started by Start.
func (r *RMQReceiver) handleDelivery(ctx context.Context, d amqp.Delivery) {
	// Extract inbound trace context + baggage from the AMQP headers onto the
	// live context. Done before headersToFields, which strips the W3C trace
	// keys from the business fields map. Extracting onto ctx (rather than a
	// fresh Background) keeps the producer's baggage available to
	// subscriber.OnEvent and the action expressions, while WithNewRoot below
	// still makes the consumer span an independent trace root.
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier.New(d.Headers))
	remoteSpanCtx := trace.SpanContextFromContext(ctx)

	// One settler per delivery, built before anything can fail, so that every
	// path out of here settles through the same object. Automatic
	// acknowledgement is then one policy over one mechanism rather than a
	// second route to the broker, and a configuration that settled the message
	// itself is not settled over the top of.
	settler, ops := r.newSettler(d)

	vinculumTopic, fields, sub, fallbackAction, err := r.resolveTopicPart1(d.RoutingKey)
	if err != nil {
		r.logger.Error("rabbitmq receiver: routing", zap.String("routing_key", d.RoutingKey), zap.Error(err))
		r.metrics.RecordReceived(ctx, r.queue, "routing")
		r.nack(ctx, settler)
		return
	}
	switch fallbackAction {
	case fallbackIgnore:
		r.metrics.RecordReceived(ctx, r.queue, "") // pulled, intentionally dropped
		r.ack(ctx, settler)
		return
	case fallbackError:
		r.logger.Error("rabbitmq receiver: no subscription matched and default_routing_key_transform is error",
			zap.String("routing_key", d.RoutingKey))
		r.metrics.RecordReceived(ctx, r.queue, "no_subscription")
		r.nack(ctx, settler)
		return
	}

	// Headers → fields (filtering W3C trace context) merged with extracted captures.
	mergedFields := headersToFields(d.Headers)
	for k, v := range fields {
		if mergedFields == nil {
			mergedFields = make(map[string]string)
		}
		mergedFields[k] = v
	}

	// Deserialize body. A decode failure is fatal to the message: the
	// configured wire format is a contract, so a body that doesn't satisfy
	// it is nacked rather than delivered as raw bytes. Use wire format
	// "auto" for best-effort decoding.
	var msg any
	if d.Body != nil {
		var deserErr error
		msg, deserErr = r.wireFormat.Deserialize(d.Body)
		if deserErr != nil {
			r.logger.Error("rabbitmq receiver: deserialize failed",
				zap.String("routing_key", d.RoutingKey),
				zap.String("wire_format", r.wireFormat.Name()),
				zap.Error(deserErr))
			r.metrics.RecordReceived(ctx, r.queue, "deserialize")
			if r.onDecodeError != nil {
				r.onDecodeError(ctx, wire.DecodeError{
					Raw:    d.Body,
					Err:    deserErr,
					Format: r.wireFormat.Name(),
					// vinculumTopic here is the pre-VinculumTopicFunc
					// value; the func needs msg, which we don't have.
					Topic:  vinculumTopic,
					Fields: mergedFields,
					Attrs: map[string]string{
						"routing_key": d.RoutingKey,
						"exchange":    d.Exchange,
						"queue":       r.queue,
					},
				})
			}
			r.nack(ctx, settler)
			return
		}
	}

	// If a subscription matched, run its VinculumTopicFunc (now that msg is
	// known). nil func → dot_to_slash on routing key.
	if sub != nil {
		if sub.VinculumTopicFunc != nil {
			t, terr := sub.VinculumTopicFunc(d.RoutingKey, d.Exchange, mergedFields, msg)
			if terr != nil {
				r.logger.Error("rabbitmq receiver: vinculum_topic eval",
					zap.String("routing_key", d.RoutingKey),
					zap.Error(terr))
				r.metrics.RecordReceived(ctx, r.queue, "vinculum_topic")
				r.nack(ctx, settler)
				return
			}
			if t != "" {
				vinculumTopic = t
			} else {
				vinculumTopic = dotToSlash(d.RoutingKey)
			}
		} else {
			vinculumTopic = dotToSlash(d.RoutingKey)
		}
	}

	// Start a consumer span as a new trace root linked to the producer span
	// (the OTel async-messaging convention: the consumer trace is independent
	// but linked, not a child). The span covers subscriber.OnEvent.
	tp := r.tracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	spanOpts := []trace.SpanStartOption{
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("rabbitmq"),
			semconv.MessagingDestinationNameKey.String(r.queue),
			semconv.MessagingRabbitmqDestinationRoutingKey(d.RoutingKey),
			semconv.MessagingOperationTypeDeliver, // value is "process" in semconv v1.26
			semconv.MessagingOperationNameKey.String("process"),
			attribute.String("vinculum.client.name", r.clientName),
		),
	}
	if remoteSpanCtx.IsValid() {
		spanOpts = append(spanOpts, trace.WithLinks(trace.Link{SpanContext: remoteSpanCtx}))
	}
	ctx, span := tp.Tracer("vinculum-rabbitmq/receiver").Start(ctx, "process "+vinculumTopic, spanOpts...)
	defer span.End()

	// Acknowledgement is a property of this delivery, and `fields` cannot carry
	// it — the bus rewrites those per subscription with that subscription's own
	// topic captures. The context can, and its values survive the async queue's
	// goroutine hop, so putting the settler here is what lets a subscription
	// several bus hops downstream acknowledge the message it handled.
	if settler != nil {
		ctx = bus.WithSettler(ctx, settler)
	}

	start := time.Now()
	err = r.subscriber.OnEvent(ctx, vinculumTopic, msg, mergedFields)
	elapsed := time.Since(start)

	// The settle point. Under AckAfterHandling this acknowledges a subscriber
	// that handled the delivery and leaves one that only queued it to settle at
	// its own completion; a failure nacks in every mode, because an unsettled
	// delivery is bounded by a settle deadline whose expiry nacks anyway — so
	// the choice is between dead-lettering a known failure now and holding a
	// prefetch slot until the deadline says the same thing later.
	//
	// It runs through the same settler a subscriber would have used, so a
	// configuration that settled the message itself does not have it settled
	// twice.
	bus.SettleOnReturn(ctx, r.subscriber, err)

	// An observing subscriber settles nothing and defers to nobody — it saw the
	// delivery go past. SettleOnReturn returns without acting, so no settle is
	// coming from anywhere and this delivery has to be released by hand or the
	// count never comes back down. It is the only path through here that
	// reaches no settler at all: every failure above nacks, and a nack releases.
	if ops != nil && bus.DispositionOf(r.subscriber) == bus.Observed {
		ops.release()
	}

	if err != nil {
		r.logger.Error("rabbitmq receiver: subscriber.OnEvent",
			zap.String("routing_key", d.RoutingKey),
			zap.String("vinculum_topic", vinculumTopic),
			zap.Error(err))
		span.SetAttributes(attribute.String("error.type", "subscriber"))
		span.RecordError(err)
		span.SetStatus(codes.Error, "subscriber")
		r.metrics.RecordProcessDuration(ctx, r.queue, elapsed, "subscriber")
		r.metrics.RecordReceived(ctx, r.queue, "subscriber")
		return
	}
	r.metrics.RecordProcessDuration(ctx, r.queue, elapsed, "")
	r.metrics.RecordReceived(ctx, r.queue, "")
}

// fallbackAction encodes what to do when no Subscription matched. It is set
// alongside vinculumTopic so the caller knows whether to deserialize/dispatch
// or take a shortcut.
type fallbackAction int

const (
	fallbackNone   fallbackAction = iota // a subscription matched OR a non-error/ignore default applies
	fallbackIgnore                       // ack and drop
	fallbackError                        // log + nack
)

// resolveTopicPart1 picks the matching Subscription (if any) and computes the
// fallback vinculum topic from the routing key + default transform. The
// VinculumTopicFunc (which needs the deserialized msg) is called by the
// caller after deserialization.
func (r *RMQReceiver) resolveTopicPart1(routingKey string) (vinculumTopic string, extracted map[string]string, sub *Subscription, fa fallbackAction, err error) {
	for i := range r.subscriptions {
		if f, ok := match(r.subscriptions[i].RoutingKeyPattern, routingKey); ok {
			return "", f, &r.subscriptions[i], fallbackNone, nil
		}
	}
	switch r.defaultXform {
	case DefaultRKDotToSlash:
		return dotToSlash(routingKey), nil, nil, fallbackNone, nil
	case DefaultRKVerbatim:
		return routingKey, nil, nil, fallbackNone, nil
	case DefaultRKError:
		return "", nil, nil, fallbackError, nil
	case DefaultRKIgnore:
		return "", nil, nil, fallbackIgnore, nil
	}
	return dotToSlash(routingKey), nil, nil, fallbackNone, nil
}

func dotToSlash(routingKey string) string {
	return strings.ReplaceAll(routingKey, ".", "/")
}

// traceHeaders is the set of W3C trace context keys injected by OTel
// propagators. These are filtered from the fields map so business metadata
// stays clean — the propagator has already extracted them into the context.
var traceHeaders = map[string]struct{}{
	"traceparent": {},
	"tracestate":  {},
	"baggage":     {},
}

// headersToFields converts an AMQP headers table to a string-keyed string
// map, filtering W3C trace context keys. Non-string values are formatted via
// fmt.Sprintf("%v", v). Returns nil for an empty input or when all entries
// are trace headers.
func headersToFields(h amqp.Table) map[string]string {
	if len(h) == 0 {
		return nil
	}
	m := make(map[string]string, len(h))
	for k, v := range h {
		if _, isTrace := traceHeaders[k]; isTrace {
			continue
		}
		switch val := v.(type) {
		case string:
			m[k] = val
		case []byte:
			m[k] = string(val)
		default:
			m[k] = fmt.Sprintf("%v", v)
		}
	}
	if len(m) == 0 {
		return nil
	}
	return m
}
