package receiver

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

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
	autoAck       bool
	wireFormat    wire.WireFormat
	consumerTag   string
	declare       *Declare
	bindings      []Binding

	logger         *zap.Logger
	meterProvider  metric.MeterProvider
	tracerProvider trace.TracerProvider
	metrics        *ReceiverMetrics

	mu       sync.Mutex
	cancel   context.CancelFunc
	loopDone chan struct{}
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
	if r.cancel != nil {
		r.mu.Unlock()
		return fmt.Errorf("rabbitmq receiver: already started for queue %q", r.queue)
	}
	r.mu.Unlock()

	if r.prefetch > 0 {
		if err := ch.Qos(r.prefetch, 0, false); err != nil {
			return fmt.Errorf("rabbitmq receiver: qos prefetch=%d: %w", r.prefetch, err)
		}
	}

	deliveries, err := ch.Consume(r.queue, r.consumerTag, r.autoAck, r.exclusive, false, false, nil)
	if err != nil {
		return fmt.Errorf("rabbitmq receiver: consume %q: %w", r.queue, err)
	}

	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	r.mu.Lock()
	r.cancel = cancel
	r.loopDone = done
	r.mu.Unlock()

	go r.runLoop(loopCtx, deliveries, done)
	return nil
}

// Stop signals the delivery loop to exit and waits for it. It does not call
// basic.cancel or close the channel — that is the client's responsibility, so
// that channel-level recovery (Stop + Start with a fresh channel) is cheap.
//
// Safe to call before Start (no-op) or repeatedly.
func (r *RMQReceiver) Stop() {
	r.mu.Lock()
	cancel := r.cancel
	done := r.loopDone
	r.cancel = nil
	r.loopDone = nil
	r.mu.Unlock()

	if cancel == nil {
		return
	}
	cancel()
	if done != nil {
		<-done
	}
}

// runLoop reads deliveries until ctx is cancelled or the deliveries channel
// is closed (the broker cancelled us, the channel closed, or the connection
// dropped). Each delivery is dispatched via handleDelivery.
func (r *RMQReceiver) runLoop(ctx context.Context, deliveries <-chan amqp.Delivery, done chan struct{}) {
	defer close(done)
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-deliveries:
			if !ok {
				// Broker stopped sending us deliveries (consumer cancelled,
				// channel closed, or connection dropped). The client owns
				// recovery; we just exit.
				return
			}
			r.handleDelivery(ctx, d)
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

	vinculumTopic, fields, sub, fallbackAction, err := r.resolveTopicPart1(d.RoutingKey)
	if err != nil {
		r.logger.Error("rabbitmq receiver: routing", zap.String("routing_key", d.RoutingKey), zap.Error(err))
		r.metrics.RecordReceived(ctx, r.queue, "routing")
		r.nack(d)
		return
	}
	switch fallbackAction {
	case fallbackIgnore:
		r.metrics.RecordReceived(ctx, r.queue, "") // pulled, intentionally dropped
		r.ack(d)
		return
	case fallbackError:
		r.logger.Error("rabbitmq receiver: no subscription matched and default_routing_key_transform is error",
			zap.String("routing_key", d.RoutingKey))
		r.metrics.RecordReceived(ctx, r.queue, "no_subscription")
		r.nack(d)
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

	// Deserialize body. Fall back to raw bytes on error (same as MQTT subscriber).
	var msg any
	if d.Body != nil {
		var deserErr error
		msg, deserErr = r.wireFormat.Deserialize(d.Body)
		if deserErr != nil {
			r.logger.Warn("rabbitmq receiver: deserialize failed, passing raw bytes",
				zap.String("routing_key", d.RoutingKey),
				zap.Error(deserErr))
			msg = d.Body
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
				r.nack(d)
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

	start := time.Now()
	err = r.subscriber.OnEvent(ctx, vinculumTopic, msg, mergedFields)
	elapsed := time.Since(start)
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
		r.nack(d)
		return
	}
	r.metrics.RecordProcessDuration(ctx, r.queue, elapsed, "")
	r.metrics.RecordReceived(ctx, r.queue, "")
	r.ack(d)
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

func (r *RMQReceiver) ack(d amqp.Delivery) {
	if r.autoAck {
		return
	}
	if err := d.Ack(false); err != nil {
		r.logger.Warn("rabbitmq receiver: ack failed", zap.Error(err))
	}
}

func (r *RMQReceiver) nack(d amqp.Delivery) {
	if r.autoAck {
		return
	}
	// requeue=false: never re-deliver a poison message in a tight loop. A
	// consistently-failing message would otherwise be redelivered immediately
	// and burn CPU; with requeue=false it is dropped (or dead-lettered if the
	// queue has a DLX configured).
	if err := d.Nack(false, false); err != nil {
		r.logger.Warn("rabbitmq receiver: nack failed", zap.Error(err))
	}
	r.metrics.RecordNack(context.Background(), r.queue)
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
