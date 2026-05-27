package client

import (
	"context"
	"crypto/tls"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// Config configures a Client. Brokers must contain at least one URL.
type Config struct {
	// ClientName is the vinculum client block label (used in log messages
	// and in OTel attributes — though metric/tracer providers themselves are
	// configured on the individual senders and receivers).
	ClientName string

	// Brokers is the list of AMQP broker URLs to try on initial connect and
	// on each reconnect, in order. URLs use the amqp:// or amqps:// scheme.
	// The URL path is the virtual host; credentials should be supplied via
	// Username/Password rather than the URL.
	Brokers []string

	// Username and Password are the AMQP credentials. If empty, no SASL
	// PlainAuth is configured (the client will fall back to PLAIN with the
	// guest defaults — see amqp091-go ParseURI).
	Username string
	Password string

	// Heartbeat is the AMQP heartbeat interval. 0 means use the broker's
	// suggested value (which is also 0 — i.e. heartbeats disabled). Default
	// at higher layers should be 10s.
	Heartbeat time.Duration

	// ConnectionTimeout bounds the TCP dial + AMQP handshake. 0 = use
	// amqp091-go's default (30s).
	ConnectionTimeout time.Duration

	// TLSClientConfig is applied when the broker URL uses amqps://. nil
	// with an amqps:// URL means "system defaults".
	TLSClientConfig *tls.Config

	// Logger is the operational logger. nil means a no-op logger.
	Logger *zap.Logger

	// OnConnect runs after the connection is established and all channels
	// are open + topology declared + consumers registered. Fires on the
	// initial connect and on every successful reconnect. May be nil.
	OnConnect func(ctx context.Context)

	// OnDisconnect runs when the connection drops, before any reconnect
	// attempt. Also runs on graceful shutdown. May be nil.
	OnDisconnect func(ctx context.Context)

	// ReconnectBackoff maps the attempt number (0-based, starting from 0 on
	// the first reconnect attempt after a disconnect) to a wait duration.
	// A complete "attempt" is one full walk of the brokers list — the loop
	// only consults this function after every broker has been tried once.
	//
	// If nil, DefaultReconnectBackoff is used (1s initial, 60s max, ×2 per
	// attempt). To disable backoff, return zero.
	ReconnectBackoff func(attempt int) time.Duration

	// MeterProvider records client-level metrics (rabbitmq.client.connected,
	// .reconnections, .channel_reopens). nil disables client-level metrics;
	// sender- and receiver-level metrics are configured separately on each
	// builder.
	MeterProvider metric.MeterProvider
}

// DefaultReconnectBackoff is the backoff schedule used when Config.ReconnectBackoff
// is nil: initial delay 1s, doubling each attempt up to 60s.
func DefaultReconnectBackoff(attempt int) time.Duration {
	d := time.Second
	for i := 0; i < attempt; i++ {
		d *= 2
		if d > 60*time.Second {
			return 60 * time.Second
		}
	}
	return d
}
