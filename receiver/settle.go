package receiver

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	bus "github.com/tsarna/vinculum-bus"
	"go.uber.org/zap"
)

// AckMode says who settles a delivery with the broker, and when.
type AckMode int

const (
	// AckAfterHandling acknowledges a delivery once subscriber.OnEvent has
	// returned without error, and nacks it when it has not. The default, and
	// the only behaviour this receiver had before manual settle existed.
	AckAfterHandling AckMode = iota

	// AckManual settles nothing when handling returns. Each delivery carries a
	// bus.Settler on its context and something downstream — a transform, a
	// subscription several bus hops away, work running behind an async queue —
	// settles it when the work is actually finished.
	//
	// A delivery this receiver never hands to the subscriber is still settled
	// here: a message that fails to decode never reaches the configuration, so
	// the configuration cannot be the thing that answers for it.
	AckManual

	// AckNone is AMQP's own no-ack consumer mode, where the broker treats a
	// message as delivered the moment it is sent and this receiver never
	// acknowledges at all. Nothing can be settled, so deliveries carry no
	// settler.
	AckNone
)

// deliverySettleOps settles one AMQP delivery. The receiver builds one per
// delivery and puts the settler wrapping it on that delivery's context, so
// anything downstream can acknowledge the message without knowing it came from
// RabbitMQ.
type deliverySettleOps struct {
	r *RMQReceiver
	d amqp.Delivery

	// epoch is the receiver's channel generation at the time of delivery. See
	// Valid.
	epoch uint64
}

// Ack acknowledges the delivery, which is what removes it from the broker's
// unacknowledged set and frees the prefetch slot it occupies.
func (o *deliverySettleOps) Ack(_ context.Context) error {
	return o.d.Ack(false)
}

// Nack rejects the delivery without requeueing it, so a message that cannot be
// handled is dead-lettered if the queue has a dead-letter exchange and dropped
// if it does not. Requeueing is deliberately not offered: a consistently
// failing message would be redelivered immediately and burn CPU, and which of
// the two happens is the queue's configured policy rather than the caller's
// choice.
//
// The reason reaches the log and nowhere else. AMQP's basic.reject carries no
// payload, so there is nothing to annotate — a dead-lettered message arrives at
// the dead-letter exchange as itself, with the broker's own x-death header
// saying it was rejected.
func (o *deliverySettleOps) Nack(ctx context.Context, reason string) error {
	if err := o.d.Nack(false, false); err != nil {
		return err
	}
	o.r.metrics.RecordNack(ctx, o.r.queue)
	if reason != "" {
		// Empty only on this receiver's own nacks, which have already logged
		// the failure that caused them in their own words. A reason is passed
		// in from outside, and this is the whole of where it goes.
		o.r.logger.Info("rabbitmq receiver: message nacked",
			zap.String("queue", o.r.queue),
			zap.String("routing_key", o.d.RoutingKey),
			zap.String("reason", reason))
	}
	return nil
}

// Keepalive extends nothing and says so. An AMQP delivery has no per-message
// lease: it stays unacknowledged for as long as the channel lives, and the one
// clock over it is the broker's own consumer_timeout, which is server-side
// policy a consumer cannot renew. Handling that legitimately takes longer than
// the broker allows is a broker setting, not something to paper over from here.
func (o *deliverySettleOps) Keepalive(_ context.Context) (bool, error) {
	return false, nil
}

// Valid reports whether this delivery can still be settled, by comparing the
// channel generation it arrived on with the receiver's current one.
//
// A delivery tag is channel-scoped and means nothing once its channel is gone —
// AMQP re-points tags from 1 on each new channel, so a tag that outlives its
// channel names a different message on the next one. This library binds each
// delivery to the channel it came from (amqp.Delivery.Acknowledger), so a late
// settle would fail against the closed channel rather than acknowledge the
// wrong message. The epoch is what turns that into an answer: "the channel
// reconnected" says the message will be redelivered, where a raw
// channel/connection-is-not-open error from deep in the AMQP library says
// nothing about what became of the message.
func (o *deliverySettleOps) Valid() (bool, string) {
	if o.epoch != o.r.epoch.Load() {
		return false, "channel reconnected"
	}
	return true, ""
}

// newSettler returns the settler for one delivery, or nil under AckNone, where
// the broker settled the message when it sent it and there is nothing left to
// answer for. A nil settler is never put on a context, so inbound::ack() on
// such a message reports false — which is the honest answer rather than a
// successful-looking no-op.
// Under AckAfterHandling the settler is marked as settled by the framework,
// which is the same mode this receiver has always had and a different thing to
// do with it. It used to mean "acknowledge once delivery returns", which is
// exact only while delivery is synchronous — a queue or a bus hop downstream
// returns as soon as the message is enqueued. Now it means "whoever finishes
// the work settles this", and the acknowledgement follows the work however many
// hops away it happens.
func (r *RMQReceiver) newSettler(d amqp.Delivery) bus.Settler {
	if r.ackMode == AckNone {
		return nil
	}
	ops := &deliverySettleOps{r: r, d: d, epoch: r.epoch.Load()}
	if r.ackMode == AckAfterHandling {
		return bus.NewSettler(ops, bus.AutoSettle())
	}
	return bus.NewSettler(ops)
}

// ack settles a delivery this receiver is answering for itself. A nil settler
// is AckNone, where there is nothing to send.
func (r *RMQReceiver) ack(ctx context.Context, settler bus.Settler) {
	if settler == nil {
		return
	}
	if _, err := settler.Ack(ctx); err != nil {
		r.logger.Warn("rabbitmq receiver: ack failed", zap.Error(err))
	}
}

// nack settles a delivery this receiver is answering for itself. reason is
// empty because each caller has already logged the failure in its own terms;
// the reason argument exists for settles that come from outside.
func (r *RMQReceiver) nack(ctx context.Context, settler bus.Settler) {
	if settler == nil {
		return
	}
	if _, err := settler.Nack(ctx, ""); err != nil {
		r.logger.Warn("rabbitmq receiver: nack failed", zap.Error(err))
	}
}
