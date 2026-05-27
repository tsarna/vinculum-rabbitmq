package sender

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	bus "github.com/tsarna/vinculum-bus"
	"github.com/tsarna/vinculum-bus/topicmatch"
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

// pendingEntry holds the result slot for an in-flight mandatory publish so
// the post-ack drain can deliver a matching Return into it; the publish
// goroutine then reads it after the drain.
type pendingEntry struct {
	ret atomic.Pointer[amqp.Return]
}

// channel is the subset of channel operations RMQSender depends on. It exists
// so tests can substitute a fake channel without standing up a real broker.
// *amqp.Channel does not satisfy this interface directly because the
// confirm-mode publish method needs to return our own publisherConfirmation
// interface; SetChannel wraps *amqp.Channel in amqpChannel for that reason.
type channel interface {
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	publishConfirmed(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (publisherConfirmation, error)
	enableConfirms() error
	subscribeReturns(c chan amqp.Return) chan amqp.Return
}

// publisherConfirmation is the subset of *amqp.DeferredConfirmation that
// RMQSender depends on. *amqp.DeferredConfirmation satisfies it directly.
type publisherConfirmation interface {
	WaitContext(ctx context.Context) (bool, error)
}

// amqpChannel adapts a real *amqp.Channel to the channel interface — needed
// because the confirm-mode publish method has to return our interface type,
// not *amqp.DeferredConfirmation, and Go does not do covariant returns.
type amqpChannel struct {
	ch *amqp.Channel
}

func (a *amqpChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return a.ch.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func (a *amqpChannel) publishConfirmed(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (publisherConfirmation, error) {
	return a.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func (a *amqpChannel) enableConfirms() error {
	return a.ch.Confirm(false)
}

func (a *amqpChannel) subscribeReturns(c chan amqp.Return) chan amqp.Return {
	return a.ch.NotifyReturn(c)
}

// RMQSender implements bus.Subscriber and publishes received vinculum events
// to a RabbitMQ exchange over AMQP 0-9-1. Create via NewSender().Build().
//
// The sender starts in a "disconnected" state. The owning client.Client
// injects a live AMQP channel via SetChannel after the connection is
// established (and on each reconnect or channel-level recovery). OnEvent
// returns an error if called before SetChannel.
type RMQSender struct {
	bus.BaseSubscriber

	mu         sync.RWMutex
	ch         channel
	returnsCh  chan amqp.Return // non-nil when confirm_mode=true and connected
	returnStop chan struct{}    // non-nil when watchReturns goroutine is running

	drainMu sync.Mutex // serializes drainReturns across concurrent publishes
	pending sync.Map   // MessageId -> *pendingEntry (mandatory-return correlation)

	clientName    string
	exchange      string
	topicMappings []TopicMapping
	defaultXform  DefaultTopicTransform
	mandatory     bool
	persistent    bool
	confirmMode   bool
	wireFormat    wire.WireFormat

	logger         *zap.Logger
	meterProvider  metric.MeterProvider
	tracerProvider trace.TracerProvider
	metrics        *SenderMetrics
}

// SetChannel injects the AMQP channel. Thread-safe. Called by client.Client
// after the connection is established and on every channel-level recovery.
// Passing nil marks the sender as disconnected (OnEvent will error).
//
// If confirm mode is enabled, the channel is put into confirm mode here. If
// that fails, the sender remains in the disconnected state.
func (s *RMQSender) SetChannel(ch *amqp.Channel) {
	if ch == nil {
		s.setChannel(nil)
		return
	}
	s.setChannel(&amqpChannel{ch: ch})
}

func (s *RMQSender) setChannel(ch channel) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Tear down the previous returns state. The underlying chan from amqp091
	// is closed by the library when the channel closes; we also explicitly
	// signal any watcher goroutine to exit.
	if s.returnStop != nil {
		close(s.returnStop)
		s.returnStop = nil
	}
	s.returnsCh = nil

	if ch == nil {
		s.ch = nil
		return
	}

	if s.confirmMode {
		if err := ch.enableConfirms(); err != nil {
			s.logger.Error("rabbitmq sender: enable confirms failed; sender remains disconnected",
				zap.String("client", s.clientName),
				zap.Error(err))
			s.ch = nil
			return
		}
	}

	// Subscribe to mandatory-returned messages. The library guarantees this
	// chan is closed when the underlying channel closes.
	returns := ch.subscribeReturns(make(chan amqp.Return, 32))

	if s.confirmMode {
		// In confirm mode the publish path drains this channel after each
		// ack. AMQP dispatches the Return frame before the Ack frame on the
		// wire, so by the time WaitContext returns any return for this
		// publish is already buffered here — that lets us correlate returns
		// with the publish that triggered them and surface them as an error.
		s.returnsCh = returns
	} else {
		// Without an ack to synchronize on, returns can't be tied back to a
		// specific OnEvent call. A background goroutine handles log + metric
		// (best-effort observability) instead.
		stop := make(chan struct{})
		s.returnStop = stop
		go s.watchReturns(returns, stop)
	}

	s.ch = ch
}

// watchReturns runs only in confirm_mode=false: in that mode there is no
// per-publish synchronization point, so returns are handled out-of-band as
// log + metric only. Exits when either the stop channel is closed
// (SetChannel was called) or the deliveries channel is closed by the library.
func (s *RMQSender) watchReturns(returns <-chan amqp.Return, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case r, ok := <-returns:
			if !ok {
				return
			}
			s.handleReturn(r)
		}
	}
}

// handleReturn logs a returned message, increments the
// rabbitmq.publisher.returned counter, and — if a publish is waiting on this
// MessageId — stores the return into its pending entry so the publish path
// can surface it as an error.
func (s *RMQSender) handleReturn(ret amqp.Return) {
	s.logger.Warn("rabbitmq sender: mandatory message returned by broker",
		zap.String("client", s.clientName),
		zap.String("exchange", ret.Exchange),
		zap.String("routing_key", ret.RoutingKey),
		zap.Uint16("reply_code", ret.ReplyCode),
		zap.String("reply_text", ret.ReplyText))
	s.metrics.RecordReturned(context.Background(), ret.Exchange)
	if v, ok := s.pending.Load(ret.MessageId); ok {
		// Copy so the pointer outlives the channel buffer slot.
		r := ret
		v.(*pendingEntry).ret.Store(&r)
	}
}

// drainReturns processes every return currently buffered in the returns
// channel and is called by the publish path in confirm mode immediately
// after WaitContext returns. AMQP guarantees the Return frame is dispatched
// before the Ack frame, so any return for this publish is already in the
// channel by the time we get here. Concurrent publishes serialize on
// drainMu, but the drain itself is non-blocking and so the critical section
// is short.
func (s *RMQSender) drainReturns() {
	s.mu.RLock()
	ch := s.returnsCh
	s.mu.RUnlock()
	if ch == nil {
		return
	}
	s.drainMu.Lock()
	defer s.drainMu.Unlock()
	for {
		select {
		case ret, ok := <-ch:
			if !ok {
				return
			}
			s.handleReturn(ret)
		default:
			return
		}
	}
}

// newMessageId generates a 32-hex-char random string used as the
// amqp.Publishing.MessageId for correlating mandatory returns with the
// publish that triggered them.
func newMessageId() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Exchange returns the sender-level exchange.
func (s *RMQSender) Exchange() string {
	return s.exchange
}

// OnEvent implements bus.Subscriber. It resolves the AMQP exchange and
// routing key from the configured topic mappings, serializes the payload via
// the configured wire format, converts fields to AMQP basic-properties
// headers, and publishes via the current channel.
func (s *RMQSender) OnEvent(ctx context.Context, topic string, msg any, fields map[string]string) error {
	start := time.Now()

	s.mu.RLock()
	ch := s.ch
	s.mu.RUnlock()

	if ch == nil {
		s.metrics.RecordPublish(ctx, s.exchange, time.Since(start), "not_connected")
		return fmt.Errorf("rabbitmq sender: not yet connected")
	}

	exchange, routingKey, persistent, drop, err := s.resolveMapping(topic, msg, fields)
	if err != nil {
		s.metrics.RecordPublish(ctx, s.exchange, time.Since(start), "routing")
		return err
	}
	if drop {
		return nil // DefaultTopicIgnore — silently drop; not a publish attempt
	}

	body, err := s.wireFormat.Serialize(msg)
	if err != nil {
		s.metrics.RecordPublish(ctx, exchange, time.Since(start), "serialize")
		return fmt.Errorf("rabbitmq sender: serialize payload: %w", err)
	}

	deliveryMode := uint8(1)
	if persistent {
		deliveryMode = 2
	}

	headers := fieldsToHeaders(fields)

	// Start a producer span covering the publish (and the confirm wait, in
	// confirm mode), then inject the current trace context into the AMQP
	// headers so the consumer side can link back to it.
	tp := s.tracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	ctx, span := tp.Tracer("vinculum-rabbitmq/sender").Start(ctx, "send "+exchange,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("rabbitmq"),
			semconv.MessagingDestinationNameKey.String(exchange),
			semconv.MessagingRabbitmqDestinationRoutingKey(routingKey),
			semconv.MessagingOperationTypePublish,
			semconv.MessagingOperationNameKey.String("send"),
			attribute.String("vinculum.client.name", s.clientName),
		),
	)
	defer span.End()

	if headers == nil {
		headers = amqp.Table{}
	}
	otel.GetTextMapPropagator().Inject(ctx, carrier.New(headers))

	pub := amqp.Publishing{
		Headers:      headers,
		ContentType:  contentTypeFor(s.wireFormat, msg),
		DeliveryMode: deliveryMode,
		Body:         body,
	}

	if s.confirmMode {
		// For mandatory publishes, set a unique MessageId so the broker echoes
		// it in any Basic.Return — that lets the post-ack drain correlate the
		// return with *this* publish and surface it as an error.
		var pending *pendingEntry
		if s.mandatory {
			mid := newMessageId()
			pub.MessageId = mid
			pending = &pendingEntry{}
			s.pending.Store(mid, pending)
			defer s.pending.Delete(mid)
		}

		deferred, err := ch.publishConfirmed(ctx, exchange, routingKey, s.mandatory, false, pub)
		if err != nil {
			err = fmt.Errorf("rabbitmq sender: publish to exchange=%q key=%q: %w", exchange, routingKey, err)
			s.failPublish(ctx, span, exchange, start, "publish", err)
			return err
		}
		acked, err := deferred.WaitContext(ctx)
		if err != nil {
			err = fmt.Errorf("rabbitmq sender: wait for confirm of exchange=%q key=%q: %w", exchange, routingKey, err)
			s.failPublish(ctx, span, exchange, start, "confirm", err)
			return err
		}
		if !acked {
			err := fmt.Errorf("rabbitmq sender: broker nack'd publish to exchange=%q key=%q", exchange, routingKey)
			s.failPublish(ctx, span, exchange, start, "nack", err)
			return err
		}

		// Drain the returns channel (return precedes ack on the wire), then
		// check whether *this* publish was returned. The drain runs whether
		// or not this publish was mandatory so any stray return doesn't sit
		// in the buffer waiting on a subsequent publish that may never come.
		s.drainReturns()
		if pending != nil {
			if ret := pending.ret.Load(); ret != nil {
				err := fmt.Errorf("rabbitmq sender: mandatory message returned by broker: exchange=%q routing_key=%q reply_code=%d reply_text=%q",
					ret.Exchange, ret.RoutingKey, ret.ReplyCode, ret.ReplyText)
				s.failPublish(ctx, span, exchange, start, "returned", err)
				return err
			}
		}

		s.metrics.RecordPublish(ctx, exchange, time.Since(start), "")
		return nil
	}

	if err := ch.PublishWithContext(ctx, exchange, routingKey, s.mandatory, false, pub); err != nil {
		err = fmt.Errorf("rabbitmq sender: publish to exchange=%q key=%q: %w", exchange, routingKey, err)
		s.failPublish(ctx, span, exchange, start, "publish", err)
		return err
	}
	s.metrics.RecordPublish(ctx, exchange, time.Since(start), "")
	return nil
}

// failPublish marks the producer span as errored (error.type attribute,
// recorded exception, and Error status) and records the failed publish
// attempt (sent.messages + operation.duration carry error.type).
func (s *RMQSender) failPublish(ctx context.Context, span trace.Span, exchange string, start time.Time, errType string, err error) {
	span.SetAttributes(attribute.String("error.type", errType))
	if err != nil {
		span.RecordError(err)
	}
	span.SetStatus(codes.Error, errType)
	s.metrics.RecordPublish(ctx, exchange, time.Since(start), errType)
}

// resolveMapping iterates topicMappings in order (first match wins) and
// returns the AMQP exchange, routing key, and persistent flag for the
// outbound message. When drop is true the caller should silently discard
// the message (DefaultTopicIgnore).
func (s *RMQSender) resolveMapping(topic string, msg any, fields map[string]string) (exchange, routingKey string, persistent, drop bool, err error) {
	for _, m := range s.topicMappings {
		if !topicmatch.Matches(m.Pattern, topic) {
			continue
		}

		// Merge pattern-extracted fields with provided fields.
		// Pattern-extracted values take precedence.
		extracted := topicmatch.Extract(m.Pattern, topic)
		mergedFields := fields
		if len(extracted) > 0 {
			mergedFields = make(map[string]string, len(fields)+len(extracted))
			for k, v := range fields {
				mergedFields[k] = v
			}
			for k, v := range extracted {
				mergedFields[k] = v
			}
		}

		exchange = s.exchange
		if m.Exchange != "" {
			exchange = m.Exchange
		}

		persistent = s.persistent
		if m.Persistent != nil {
			persistent = *m.Persistent
		}

		if m.RoutingKeyFunc != nil {
			routingKey, err = m.RoutingKeyFunc(topic, msg, mergedFields)
			if err != nil {
				return "", "", false, false, fmt.Errorf("rabbitmq sender: resolve routing key for %q: %w", topic, err)
			}
		} else {
			routingKey = slashToDot(topic)
		}
		return exchange, routingKey, persistent, false, nil
	}

	// No mapping matched. Apply default transform.
	switch s.defaultXform {
	case DefaultTopicIgnore:
		return "", "", false, true, nil
	case DefaultTopicError:
		return "", "", false, false, fmt.Errorf("rabbitmq sender: no topic mapping matched for topic %q and default_topic_transform is error", topic)
	case DefaultTopicVerbatim:
		return s.exchange, topic, s.persistent, false, nil
	default: // DefaultTopicSlashToDot
		return s.exchange, slashToDot(topic), s.persistent, false, nil
	}
}

// fieldsToHeaders converts a vinculum fields map to an AMQP headers table.
// Returns nil for an empty or nil map (AMQP allows nil Headers).
func fieldsToHeaders(fields map[string]string) amqp.Table {
	if len(fields) == 0 {
		return nil
	}
	t := make(amqp.Table, len(fields))
	for k, v := range fields {
		t[k] = v
	}
	return t
}

// contentTypeFor returns the AMQP content-type that best matches what the
// wire format produced. For named formats the mapping is direct. For the
// "auto" format the content-type is derived from the input value type
// because auto serializes []byte verbatim, string verbatim, and everything
// else as JSON.
func contentTypeFor(wf wire.WireFormat, msg any) string {
	switch wf.Name() {
	case "json":
		return "application/json"
	case "string":
		return "text/plain"
	case "bytes":
		return "application/octet-stream"
	case "auto":
		switch msg.(type) {
		case []byte:
			return "application/octet-stream"
		case string:
			return "text/plain"
		default:
			return "application/json"
		}
	default:
		return ""
	}
}

// ensure RMQSender implements bus.Subscriber
var _ bus.Subscriber = (*RMQSender)(nil)
