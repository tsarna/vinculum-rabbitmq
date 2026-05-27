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

// Start dials the first reachable broker, opens one channel per sender and
// one per receiver, declares receiver topology, registers consumers, fires
// OnConnect, and spawns the reconnect watcher goroutine.
//
// Calling Start twice is an error.
func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return errors.New("rabbitmq client: already started")
	}
	c.started = true
	lifeCtx, cancel := context.WithCancel(ctx)
	c.lifeCtx = lifeCtx
	c.cancelLife = cancel
	c.mu.Unlock()

	if len(c.cfg.Brokers) == 0 {
		cancel()
		return errors.New("rabbitmq client: no brokers configured")
	}

	conn, brokerURL, err := c.dial()
	if err != nil {
		cancel()
		return err
	}
	c.cfg.Logger.Info("rabbitmq client: connected",
		zap.String("client", c.cfg.ClientName),
		zap.String("broker", redactURL(brokerURL)))

	if err := c.setupChannels(lifeCtx, conn); err != nil {
		_ = conn.Close()
		cancel()
		return err
	}

	c.mu.Lock()
	c.conn = conn
	c.connected = true
	c.reconnectDone = make(chan struct{})
	c.mu.Unlock()

	c.metrics.SetConnected(lifeCtx, true)

	closeNotif := conn.NotifyClose(make(chan *amqp.Error, 1))
	go c.watchConnAndReconnect(lifeCtx, closeNotif)

	if c.cfg.OnConnect != nil {
		c.cfg.OnConnect(lifeCtx)
	}
	return nil
}

// Stop tears down all consumers, channels, and the connection. Safe to call
// before Start or repeatedly. Cancels the reconnect goroutine and waits for
// it to exit before returning.
func (c *Client) Stop() error {
	c.mu.Lock()
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
	// instances can be reused if necessary).
	for _, r := range c.receivers {
		r.Stop()
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
		return conn.Close()
	}
	return nil
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
			r.Stop()
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
		if startErr := r.Start(ctx, ch); startErr != nil {
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
func (c *Client) watchConnAndReconnect(ctx context.Context, closeNotif chan *amqp.Error) {
	defer func() {
		c.mu.Lock()
		done := c.reconnectDone
		c.mu.Unlock()
		if done != nil {
			close(done)
		}
	}()

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

			newConn, ok := c.reconnect(ctx)
			if !ok {
				return // ctx cancelled during reconnect
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
		r.Stop()
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

// reconnect loops dialing brokers (walking the list in order each attempt)
// with backoff between attempts. On a successful dial + channel setup, fires
// OnConnect and returns the new connection. Returns false if ctx is
// cancelled before a connection is established.
func (c *Client) reconnect(ctx context.Context) (*amqp.Connection, bool) {
	backoff := c.cfg.ReconnectBackoff
	if backoff == nil {
		backoff = DefaultReconnectBackoff
	}

	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return nil, false
		}

		for _, url := range c.cfg.Brokers {
			if ctx.Err() != nil {
				return nil, false
			}
			conn, dialErr := c.dialOne(url)
			if dialErr != nil {
				c.cfg.Logger.Warn("rabbitmq client: reconnect dial failed",
					zap.String("client", c.cfg.ClientName),
					zap.String("broker", redactURL(url)),
					zap.Error(dialErr))
				continue
			}

			// Got a connection. Setup channels — any failure here means we
			// have a connection we can't use; close it and keep trying.
			if setupErr := c.setupChannels(ctx, conn); setupErr != nil {
				c.cfg.Logger.Warn("rabbitmq client: setup channels after reconnect failed",
					zap.String("client", c.cfg.ClientName),
					zap.Error(setupErr))
				_ = conn.Close()
				continue
			}

			c.mu.Lock()
			c.conn = conn
			c.connected = true
			c.mu.Unlock()

			c.metrics.SetConnected(ctx, true)
			c.metrics.IncrReconnections(ctx)

			c.cfg.Logger.Info("rabbitmq client: reconnected",
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

// dial walks the brokers list once and returns the first successful
// connection. Used by Start; reconnect drives the brokers walk itself so it
// can interleave backoff between attempts.
func (c *Client) dial() (*amqp.Connection, string, error) {
	var firstErr error
	for _, url := range c.cfg.Brokers {
		conn, err := c.dialOne(url)
		if err == nil {
			return conn, url, nil
		}
		c.cfg.Logger.Warn("rabbitmq client: dial failed",
			zap.String("client", c.cfg.ClientName),
			zap.String("broker", redactURL(url)),
			zap.Error(err))
		if firstErr == nil {
			firstErr = err
		}
	}
	return nil, "", fmt.Errorf("rabbitmq client %q: all brokers failed; first error: %w", c.cfg.ClientName, firstErr)
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
// AMQP 0-9-1 reserves codes 500+ for hard (connection-level) errors. The
// library also reports a connection close to each channel's NotifyClose; in
// some cases the AMQP error is nil there. We treat a nil error as
// connection-level too — a channel-level error always carries a code.
func isConnectionLevel(err *amqp.Error) bool {
	if err == nil {
		return true
	}
	return err.Code >= 500
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
	c.mu.Unlock()

	if !connected || conn == nil {
		return nil, errors.New("connection is not currently established")
	}
	if idx < 0 || idx >= len(c.receivers) {
		return nil, fmt.Errorf("receiver index %d out of range", idx)
	}
	r := c.receivers[idx]

	// Reset the receiver so r.Start does not error out as "already started".
	// The delivery loop has already exited (the deliveries channel was
	// closed when the underlying AMQP channel closed); r.Stop just clears
	// the bookkeeping.
	r.Stop()

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open new receiver channel: %w", err)
	}
	if topoErr := r.DeclareTopology(ch); topoErr != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("redeclare topology: %w", topoErr)
	}
	if startErr := r.Start(ctx, ch); startErr != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("restart receiver: %w", startErr)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != conn || !c.connected {
		r.Stop()
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
