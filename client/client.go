package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tsarna/vinculum-rabbitmq/receiver"
	"github.com/tsarna/vinculum-rabbitmq/sender"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// Client manages a single AMQP 0-9-1 connection shared by zero or more
// senders and zero or more receivers. Each sender and each receiver gets its
// own AMQP channel on Start.
//
// On connection loss, a background reconnect goroutine fires OnDisconnect,
// walks the brokers list with exponential backoff until a new connection is
// established, re-opens all sender + receiver channels, re-runs topology
// declares, re-registers consumers, and fires OnConnect. A separate
// per-channel watcher handles channel-level recovery — one channel dying
// without the connection dropping — by re-opening just that channel.
type Client struct {
	cfg Config

	senders   []*sender.RMQSender
	receivers []*receiver.RMQReceiver

	mu       sync.Mutex
	started  bool
	stopping bool
	// drained says the client has been asked to stop consuming. It outlives
	// Drain because the reconnect and channel-recovery paths do not: they stay
	// live until Stop cancels the life context, and both of them end in
	// receiver.Start, which would register the consumer again. A shutdown would
	// then be consuming during the phase that waits for it to be finished.
	drained bool

	// connected tracks whether OnConnect has fired without a subsequent
	// OnDisconnect. Used to guarantee OnDisconnect fires exactly once per
	// connect/disconnect cycle (no double-fire when Stop runs after the
	// reconnect goroutine has already observed a drop, no missed fire when
	// Stop is the disconnect trigger).
	connected bool

	lifeCtx       context.Context
	cancelLife    context.CancelFunc
	reconnectDone chan struct{}

	conn        *amqp.Connection
	receiverChs []*amqp.Channel
	senderChs   []*amqp.Channel

	metrics *ClientMetrics
}

// NewClient returns an unstarted Client. Add senders and receivers via
// AddSender / AddReceiver, then call Start.
func NewClient(cfg Config) *Client {
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.ReconnectBackoff == nil {
		cfg.ReconnectBackoff = DefaultReconnectBackoff
	}
	var meter metric.Meter
	if cfg.MeterProvider != nil {
		meter = cfg.MeterProvider.Meter("github.com/tsarna/vinculum-rabbitmq/client")
	}
	return &Client{
		cfg:     cfg,
		metrics: NewClientMetrics(cfg.ClientName, meter),
	}
}

// AddSender registers a sender with the client. Must be called before Start.
func (c *Client) AddSender(s *sender.RMQSender) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		panic("rabbitmq client: AddSender after Start")
	}
	c.senders = append(c.senders, s)
}

// AddReceiver registers a receiver with the client. Must be called before Start.
func (c *Client) AddReceiver(r *receiver.RMQReceiver) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		panic("rabbitmq client: AddReceiver after Start")
	}
	c.receivers = append(c.receivers, r)
}

// IsConnected reports whether the client currently holds a live connection to a
// broker: OnConnect has fired without a subsequent OnDisconnect.
//
// It is a snapshot, not a guarantee — the connection may drop between this call
// and the next publish — which is what makes it useful for a health probe and
// useless as a precondition. A caller that wants to publish should publish and
// handle the error.
//
// False both before Start and after a drop the reconnect loop has not yet
// repaired, so a host reporting readiness from this correctly says "not ready"
// during a broker outage and recovers on its own.
func (c *Client) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

// Start launches the connection machinery and returns. It does not wait for a
// broker to answer.
//
// A background goroutine dials the brokers list with backoff until one accepts,
// opens one channel per sender and per receiver, declares receiver topology,
// registers consumers, fires OnConnect, and then watches for the connection to
// drop — at which point the same loop repairs it. The initial connection and a
// reconnection are therefore the same code path, and a broker that is not
// listening yet at process start is the same recoverable situation as one that
// goes away later.
//
// Start used to dial synchronously and return the failure, which meant the
// reconnect watcher was only ever spawned *after* a first success: a broker
// that was down at startup left the client dead for the life of the process,
// with the schedule its own Config described never running. It also made a
// host's boot wait on a third party — the caller could only find out by
// blocking.
//
// Errors returned here are configuration, not connectivity: no brokers, or a
// second Start. Use IsConnected to observe the connection, and OnConnect /
// OnDisconnect to react to it.
func (c *Client) Start(ctx context.Context) error {
	// Validated before any state is claimed, so a client that never launches
	// its goroutine also never leaves Stop waiting on one.
	if len(c.cfg.Brokers) == 0 {
		return errors.New("rabbitmq client: no brokers configured")
	}

	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return errors.New("rabbitmq client: already started")
	}
	c.started = true
	lifeCtx, cancel := context.WithCancel(ctx)
	c.lifeCtx = lifeCtx
	c.cancelLife = cancel
	// Created here rather than on a successful connect, so Stop can wait for
	// the goroutine to exit whether or not it ever reached a broker.
	c.reconnectDone = make(chan struct{})
	c.mu.Unlock()

	go c.connectAndWatch(lifeCtx)
	return nil
}

// connectAndWatch establishes the first connection and then keeps it, for the
// life of the client. It is the only goroutine that owns the connection.
func (c *Client) connectAndWatch(ctx context.Context) {
	defer func() {
		c.mu.Lock()
		done := c.reconnectDone
		c.mu.Unlock()
		if done != nil {
			close(done)
		}
	}()

	conn, ok := c.connect(ctx, true)
	if !ok {
		return // ctx cancelled before the first connection
	}
	c.watchConnAndReconnect(ctx, conn.NotifyClose(make(chan *amqp.Error, 1)))
}

// Stop tears down all consumers, channels, and the connection. Safe to call
// before Start or repeatedly. Cancels the reconnect goroutine and waits for
// it to exit before returning.
func (c *Client) Stop() error {
	c.mu.Lock()
	// Set before the early return, not after it. Stopping is a stronger
	// statement than draining, so it should never leave a client answering
	// "not drained" — and one of the paths through that return is a Stop
	// racing a Start, where a connect already under way is exactly the thing
	// that would otherwise go on to register a consumer.
	c.drained = true
	if c.stopping || !c.started {
		c.stopping = true
		c.mu.Unlock()
		return nil
	}
	c.stopping = true
	cancel := c.cancelLife
	reconnectDone := c.reconnectDone
	c.mu.Unlock()

	// Cancel the life context first. The reconnect goroutine watches for
	// this and will exit promptly (even mid-backoff). The receivers'
	// delivery loops also observe this and exit.
	if cancel != nil {
		cancel()
	}
	if reconnectDone != nil {
		<-reconnectDone
	}

	// Stop receivers (resets their internal cancel/loopDone state so the
	// instances can be reused if necessary). A receiver that gave up on a
	// delivery during an earlier drain says so here rather than blocking on it,
	// and the answer travels back with whatever closing the connection reports.
	var stopErrs []error
	for _, r := range c.receivers {
		stopErrs = append(stopErrs, r.Stop())
	}

	// Fire OnDisconnect exactly once if the reconnect goroutine hadn't
	// already done it for the last connection drop.
	c.mu.Lock()
	wasConnected := c.connected
	c.connected = false
	c.mu.Unlock()
	if wasConnected {
		c.metrics.SetConnected(context.Background(), false)
		if c.cfg.OnDisconnect != nil {
			c.cfg.OnDisconnect(context.Background())
		}
	}

	// Disconnect senders so further OnEvent calls return "not connected".
	for _, s := range c.senders {
		s.SetChannel(nil)
	}

	c.mu.Lock()
	conn := c.conn
	senderChs := c.senderChs
	receiverChs := c.receiverChs
	c.conn = nil
	c.senderChs = nil
	c.receiverChs = nil
	c.mu.Unlock()

	for _, ch := range receiverChs {
		_ = ch.Close()
	}
	for _, ch := range senderChs {
		_ = ch.Close()
	}
	if conn != nil {
		stopErrs = append(stopErrs, conn.Close())
	}
	return errors.Join(stopErrs...)
}

// Drain withdraws every receiver's consumer and waits for the deliveries the
// broker has already sent to be handled. It is the first phase of a graceful
// shutdown: afterwards the client takes on no new work, and everything else
// still works — the connection is up, the channels are open, and every delivery
// tag handed out is still good, so acknowledgements for work still in flight
// arrive normally. Stop is what ends that.
//
// Every receiver drains at once, under the one deadline. They are independent,
// and what is being waited for is delivery — one receiver's slow action is no
// reason to cut another's short.
//
// Safe to call on a client that was never started, or twice.
func (c *Client) Drain(ctx context.Context) error {
	c.mu.Lock()
	receivers := c.receivers
	c.drained = true
	c.mu.Unlock()

	var wg sync.WaitGroup
	errs := make([]error, len(receivers))
	for i, r := range receivers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = r.Drain(ctx)
		}()
	}
	wg.Wait()

	return errors.Join(errs...)
}

// errDrained reports a channel recovery declined because the client has stopped
// consuming. It is not a failure — there is nothing to recover to, and no
// reconnect will follow — so the caller says so in its own words rather than
// reporting a broken connection during an orderly shutdown.
var errDrained = errors.New("client drained; not recovering the receiver channel")

// isDrained reports whether the client has been asked to stop consuming, so the
// connect and recovery paths can skip the work of starting a receiver that
// would refuse anyway.
//
// It is an optimisation, not the guarantee. Reading it and then calling
// receiver.Start are two steps, and a drain can land between them; what makes
// that safe is the receiver refusing under its own lock. This only keeps a
// shutdown from opening channels and declaring topology it has no use for.
func (c *Client) isDrained() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.drained
}

// setupChannels opens one channel per sender and one per receiver on conn,
// declares each receiver's topology, and starts each receiver's delivery
// loop. On any failure, partial state is cleaned up and the error is
// returned.
func (c *Client) setupChannels(ctx context.Context, conn *amqp.Connection) error {
	senderChs := make([]*amqp.Channel, 0, len(c.senders))
	receiverChs := make([]*amqp.Channel, 0, len(c.receivers))

	cleanup := func() {
		for _, r := range c.receivers {
			// Discarded here and at every other recovery site: Stop reports a
			// delivery a drain gave up on, and no drain has run on these paths
			// — the channel died, which is a different story with its own log.
			_ = r.Stop()
		}
		for _, s := range c.senders {
			s.SetChannel(nil)
		}
		for _, ch := range receiverChs {
			_ = ch.Close()
		}
		for _, ch := range senderChs {
			_ = ch.Close()
		}
	}

	for i, s := range c.senders {
		ch, err := conn.Channel()
		if err != nil {
			cleanup()
			return fmt.Errorf("rabbitmq client %q: open channel for sender %d: %w", c.cfg.ClientName, i, err)
		}
		s.SetChannel(ch)
		senderChs = append(senderChs, ch)
	}

	for _, r := range c.receivers {
		ch, err := conn.Channel()
		if err != nil {
			cleanup()
			return fmt.Errorf("rabbitmq client %q: open channel for receiver %q: %w", c.cfg.ClientName, r.Queue(), err)
		}
		if topoErr := r.DeclareTopology(ch); topoErr != nil {
			_ = ch.Close()
			cleanup()
			return fmt.Errorf("rabbitmq client %q: %w", c.cfg.ClientName, topoErr)
		}
		// Not started once the client has been drained. This runs on every
		// connect, including one that completes *during* a shutdown — a broker
		// that was down at boot and came back, or a reconnect that landed
		// between the drain and the stop. Registering the consumer again there
		// would put the process back to consuming during the phase that waits
		// for it to have finished.
		if c.isDrained() {
			c.cfg.Logger.Info("rabbitmq client: connected while draining; not consuming",
				zap.String("client", c.cfg.ClientName),
				zap.String("queue", r.Queue()))
		} else if startErr := r.Start(ctx, ch); startErr != nil {
			_ = ch.Close()
			cleanup()
			return fmt.Errorf("rabbitmq client %q: start receiver %q: %w", c.cfg.ClientName, r.Queue(), startErr)
		}
		receiverChs = append(receiverChs, ch)
	}

	c.mu.Lock()
	c.senderChs = senderChs
	c.receiverChs = receiverChs
	c.mu.Unlock()

	// Spawn per-channel watchers. These detect channel-level errors (an
	// individual channel dying without taking the connection with it) and
	// re-open just the affected channel rather than waiting for the
	// connection-level reconnect loop to rebuild everything.
	for i, ch := range senderChs {
		go c.watchSenderChannel(ctx, i, ch)
	}
	for i, ch := range receiverChs {
		go c.watchReceiverChannel(ctx, i, ch)
	}
	return nil
}

// watchConnAndReconnect blocks on the current connection's NotifyClose
// channel; on close, fires OnDisconnect, tears down stale per-channel state,
// reconnects with backoff, and arms the next NotifyClose. Exits when the
// life context is cancelled (Stop was called).
//
// Called only from connectAndWatch, which owns the goroutine and closes
// reconnectDone when this returns.
func (c *Client) watchConnAndReconnect(ctx context.Context, closeNotif chan *amqp.Error) {
	for {
		select {
		case <-ctx.Done():
			return
		case amqpErr, ok := <-closeNotif:
			// Drain race with Stop: prefer ctx.Done if both are ready.
			if ctx.Err() != nil {
				return
			}
			if !ok {
				// closeNotif was closed without an error (graceful close
				// initiated by the library, e.g. user-Closed connection).
				return
			}
			c.cfg.Logger.Warn("rabbitmq client: connection closed; reconnecting",
				zap.String("client", c.cfg.ClientName),
				zap.Error(amqpErr))
			c.handleDisconnect(ctx)

			newConn, ok := c.connect(ctx, false)
			if !ok {
				return // ctx cancelled, or the attempt limit was reached
			}
			closeNotif = newConn.NotifyClose(make(chan *amqp.Error, 1))
		}
	}
}

// handleDisconnect stops receivers, disconnects senders, and fires
// OnDisconnect (only if we were previously in the connected state).
func (c *Client) handleDisconnect(ctx context.Context) {
	c.mu.Lock()
	wasConnected := c.connected
	c.connected = false
	c.mu.Unlock()

	for _, r := range c.receivers {
		_ = r.Stop()
	}
	for _, s := range c.senders {
		s.SetChannel(nil)
	}

	if wasConnected {
		c.metrics.SetConnected(ctx, false)
		if c.cfg.OnDisconnect != nil {
			c.cfg.OnDisconnect(ctx)
		}
	}
}

// connect loops dialing brokers (walking the list in order each attempt) with
// backoff between attempts. On a successful dial + channel setup, fires
// OnConnect and returns the new connection. Returns false if ctx is cancelled
// before a connection is established, or if the attempt limit was reached.
//
// initial distinguishes the first connection from a repair of a lost one. They
// are the same work, and deliberately the same code — what differs is only how
// each is accounted for and how long it is allowed to go on:
//
//   - MaxReconnectAttempts bounds a repair, never the first connection. It is
//     documented as governing reconnection, and a broker that is not listening
//     yet at process start is an ordinary situation rather than one to give up
//     on — the host is reporting itself not-ready throughout, which is the
//     honest thing to do about it and costs nothing to keep doing.
//   - The reconnection counter is not incremented for a connection that was
//     never lost, or every process start would record one.
func (c *Client) connect(ctx context.Context, initial bool) (*amqp.Connection, bool) {
	backoff := c.cfg.ReconnectBackoff
	if backoff == nil {
		backoff = DefaultReconnectBackoff
	}

	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return nil, false
		}

		// Giving up is checked before the attempt rather than after the backoff
		// so that MaxReconnectAttempts counts attempts made, not waits endured:
		// a limit of 3 dials the broker list three times.
		if max := c.cfg.MaxReconnectAttempts; !initial && max > 0 && attempt >= max {
			c.cfg.Logger.Error("rabbitmq client: giving up reconnection attempts",
				zap.String("client", c.cfg.ClientName),
				zap.Int("attempts", attempt),
				zap.Int("max_reconnect_attempts", max))
			return nil, false
		}

		for _, url := range c.cfg.Brokers {
			if ctx.Err() != nil {
				return nil, false
			}
			conn, dialErr := c.dialOne(url)
			if dialErr != nil {
				c.cfg.Logger.Warn("rabbitmq client: dial failed",
					zap.String("client", c.cfg.ClientName),
					zap.String("broker", redactURL(url)),
					zap.Bool("initial", initial),
					zap.Error(dialErr))
				continue
			}

			// Got a connection. Setup channels — any failure here means we
			// have a connection we can't use; close it and keep trying.
			if setupErr := c.setupChannels(ctx, conn); setupErr != nil {
				c.cfg.Logger.Warn("rabbitmq client: setup channels failed",
					zap.String("client", c.cfg.ClientName),
					zap.Bool("initial", initial),
					zap.Error(setupErr))
				_ = conn.Close()
				continue
			}

			c.mu.Lock()
			c.conn = conn
			c.connected = true
			c.mu.Unlock()

			c.metrics.SetConnected(ctx, true)
			if !initial {
				c.metrics.IncrReconnections(ctx)
			}

			msg := "rabbitmq client: reconnected"
			if initial {
				msg = "rabbitmq client: connected"
			}
			c.cfg.Logger.Info(msg,
				zap.String("client", c.cfg.ClientName),
				zap.String("broker", redactURL(url)),
				zap.Int("attempt", attempt))

			if c.cfg.OnConnect != nil {
				c.cfg.OnConnect(ctx)
			}
			return conn, true
		}

		delay := backoff(attempt)
		c.cfg.Logger.Info("rabbitmq client: all brokers failed; backing off",
			zap.String("client", c.cfg.ClientName),
			zap.Int("attempt", attempt),
			zap.Duration("delay", delay))
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(delay):
		}
	}
}

// dialOne dials a single broker URL with the configured AMQP options.
func (c *Client) dialOne(url string) (*amqp.Connection, error) {
	amqpCfg := amqp.Config{
		Heartbeat:       c.cfg.Heartbeat,
		TLSClientConfig: c.cfg.TLSClientConfig,
	}
	if c.cfg.Username != "" || c.cfg.Password != "" {
		amqpCfg.SASL = []amqp.Authentication{
			&amqp.PlainAuth{Username: c.cfg.Username, Password: c.cfg.Password},
		}
	}
	if c.cfg.ConnectionTimeout > 0 {
		amqpCfg.Dial = amqp.DefaultDial(c.cfg.ConnectionTimeout)
	}
	return amqp.DialConfig(url, amqpCfg)
}

// isConnectionLevel reports whether an AMQP error code on a channel's
// NotifyClose channel indicates that the underlying connection has died (so
// we should defer to the connection-level reconnect loop) rather than just
// the channel (so we can re-open the channel on the same connection).
//
// The library already classifies the code: for an error the broker sent, Recover
// is set only for AMQP 0-9-1's soft (channel-level) exceptions, 311–313 and
// 403–406. Every other code is a hard exception, and that includes some below
// 500 — 320 CONNECTION_FORCED, which is what a broker sends when an operator
// closes the connection or the broker shuts down, and 402 INVALID_PATH. A
// code-range test would send the watcher into a channel recovery on a
// connection that is already gone. The errors the library raises itself on a
// channel's NotifyClose — 501, 504 and 505, for a failed socket, a closed
// channel and an unexpected frame — are all hard codes too.
//
// The watchers never pass nil: a close without an error closes the
// notification channel instead, and they return on that before asking. Nil is
// still answered, as connection-level, because it has no code to recover on.
func isConnectionLevel(err *amqp.Error) bool {
	return err == nil || !err.Recover
}

// watchSenderChannel watches the close-notify for a sender's channel and
// recovers it (opens a new channel on the same connection and re-installs
// it) on channel-level errors. Connection-level errors cause the watcher to
// exit and let the connection-level reconnect loop re-establish everything.
func (c *Client) watchSenderChannel(ctx context.Context, idx int, ch *amqp.Channel) {
	for {
		closeNotif := ch.NotifyClose(make(chan *amqp.Error, 1))

		select {
		case <-ctx.Done():
			return
		case amqpErr, ok := <-closeNotif:
			if ctx.Err() != nil {
				return
			}
			if !ok {
				return
			}
			if isConnectionLevel(amqpErr) {
				c.cfg.Logger.Warn("rabbitmq client: sender channel closed (connection-level); deferring to reconnect",
					zap.String("client", c.cfg.ClientName),
					zap.Int("sender_index", idx),
					zap.Any("amqp_error", amqpErr))
				return
			}

			c.cfg.Logger.Warn("rabbitmq client: sender channel closed; attempting recovery",
				zap.String("client", c.cfg.ClientName),
				zap.Int("sender_index", idx),
				zap.Int("code", amqpErr.Code),
				zap.String("reason", amqpErr.Reason))

			newCh, err := c.recoverSenderChannel(idx)
			if err != nil {
				c.cfg.Logger.Warn("rabbitmq client: sender channel recovery failed; deferring to reconnect",
					zap.String("client", c.cfg.ClientName),
					zap.Int("sender_index", idx),
					zap.Error(err))
				return
			}
			c.metrics.IncrChannelReopens(ctx)
			c.cfg.Logger.Info("rabbitmq client: sender channel recovered",
				zap.String("client", c.cfg.ClientName),
				zap.Int("sender_index", idx))
			ch = newCh
		}
	}
}

// watchReceiverChannel mirrors watchSenderChannel for receivers. Recovery
// includes stopping the receiver (to reset its internal state), re-running
// DeclareTopology on the new channel, and Start'ing again.
func (c *Client) watchReceiverChannel(ctx context.Context, idx int, ch *amqp.Channel) {
	for {
		closeNotif := ch.NotifyClose(make(chan *amqp.Error, 1))

		select {
		case <-ctx.Done():
			return
		case amqpErr, ok := <-closeNotif:
			if ctx.Err() != nil {
				return
			}
			if !ok {
				return
			}
			if isConnectionLevel(amqpErr) {
				c.cfg.Logger.Warn("rabbitmq client: receiver channel closed (connection-level); deferring to reconnect",
					zap.String("client", c.cfg.ClientName),
					zap.Int("receiver_index", idx),
					zap.Any("amqp_error", amqpErr))
				return
			}

			c.cfg.Logger.Warn("rabbitmq client: receiver channel closed; attempting recovery",
				zap.String("client", c.cfg.ClientName),
				zap.Int("receiver_index", idx),
				zap.Int("code", amqpErr.Code),
				zap.String("reason", amqpErr.Reason))

			newCh, err := c.recoverReceiverChannel(ctx, idx)
			if errors.Is(err, errDrained) {
				// Declined, not failed, and there will be no reconnect either:
				// the process is shutting down and this receiver has stopped
				// consuming on purpose.
				c.cfg.Logger.Info("rabbitmq client: receiver channel closed after the drain; not recovering it",
					zap.String("client", c.cfg.ClientName),
					zap.Int("receiver_index", idx))
				return
			}
			if err != nil {
				c.cfg.Logger.Warn("rabbitmq client: receiver channel recovery failed; deferring to reconnect",
					zap.String("client", c.cfg.ClientName),
					zap.Int("receiver_index", idx),
					zap.Error(err))
				return
			}
			c.metrics.IncrChannelReopens(ctx)
			c.cfg.Logger.Info("rabbitmq client: receiver channel recovered",
				zap.String("client", c.cfg.ClientName),
				zap.Int("receiver_index", idx))
			ch = newCh
		}
	}
}

// recoverSenderChannel opens a new channel on the current connection and
// installs it via sender.SetChannel (which re-applies confirm-mode and
// re-arms the returns watcher). Returns an error if the connection is no
// longer available, in which case the caller should defer to the
// connection-level reconnect loop.
func (c *Client) recoverSenderChannel(idx int) (*amqp.Channel, error) {
	c.mu.Lock()
	conn := c.conn
	connected := c.connected
	c.mu.Unlock()

	if !connected || conn == nil {
		return nil, errors.New("connection is not currently established")
	}
	if idx < 0 || idx >= len(c.senders) {
		return nil, fmt.Errorf("sender index %d out of range", idx)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open new sender channel: %w", err)
	}

	c.senders[idx].SetChannel(ch)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != conn || !c.connected {
		// The connection was replaced while we were recovering. Roll back.
		c.senders[idx].SetChannel(nil)
		_ = ch.Close()
		return nil, errors.New("connection changed during recovery")
	}
	if idx < len(c.senderChs) {
		c.senderChs[idx] = ch
	}
	return ch, nil
}

// recoverReceiverChannel opens a new channel on the current connection,
// re-runs DeclareTopology, and re-starts the receiver. r.Stop is called
// first to reset the receiver's internal cancel/done state — the delivery
// loop has already exited on its own when the underlying channel closed.
func (c *Client) recoverReceiverChannel(ctx context.Context, idx int) (*amqp.Channel, error) {
	c.mu.Lock()
	conn := c.conn
	connected := c.connected
	drained := c.drained
	c.mu.Unlock()

	if !connected || conn == nil {
		return nil, errors.New("connection is not currently established")
	}
	if idx < 0 || idx >= len(c.receivers) {
		return nil, fmt.Errorf("receiver index %d out of range", idx)
	}
	r := c.receivers[idx]

	// Before the round trips, not after them. Recovering a channel for a
	// receiver that is never going to consume again costs a channel open and a
	// topology declare, both against a broker the process is disconnecting
	// from — and the receiver's own refusal, arriving later, reads in the log
	// as a recovery that failed rather than one that was declined.
	//
	// Stopped on the way out even so, because that is what retires the delivery
	// tags this channel issued. Skipping it would leave them reading valid
	// until the client stops, so a settle in that window would reach a dead
	// channel and come back with a transport error instead of the reason that
	// says what became of the message.
	if drained {
		_ = r.Stop()
		return nil, errDrained
	}

	// Reset the receiver so r.Start does not error out as "already started".
	// The delivery loop has already exited (the deliveries channel was
	// closed when the underlying AMQP channel closed); r.Stop just clears
	// the bookkeeping.
	_ = r.Stop()

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open new receiver channel: %w", err)
	}
	if topoErr := r.DeclareTopology(ch); topoErr != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("redeclare topology: %w", topoErr)
	}
	// Checked again, because the drain may have landed since. The receiver
	// refuses either way; this only avoids leaving a channel open for it.
	if c.isDrained() {
		_ = ch.Close()
		return nil, errDrained
	}
	if startErr := r.Start(ctx, ch); startErr != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("restart receiver: %w", startErr)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != conn || !c.connected {
		_ = r.Stop()
		_ = ch.Close()
		return nil, errors.New("connection changed during recovery")
	}
	if idx < len(c.receiverChs) {
		c.receiverChs[idx] = ch
	}
	return ch, nil
}

// redactURL strips userinfo (username:password@) from a broker URL for log
// output. Falls back to the raw URL if parsing fails.
func redactURL(raw string) string {
	parsed, err := amqp.ParseURI(raw)
	if err != nil {
		return raw
	}
	scheme := parsed.Scheme
	host := parsed.Host
	vhost := parsed.Vhost
	if vhost == "/" || vhost == "" {
		return fmt.Sprintf("%s://%s:%d/", scheme, host, parsed.Port)
	}
	return fmt.Sprintf("%s://%s:%d/%s", scheme, host, parsed.Port, vhost)
}
