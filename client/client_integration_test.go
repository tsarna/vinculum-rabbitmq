//go:build integration

package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bus "github.com/tsarna/vinculum-bus"
	"github.com/tsarna/vinculum-rabbitmq/receiver"
	"github.com/tsarna/vinculum-rabbitmq/sender"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// These tests run the client against a real RabbitMQ. ci/brokers.yml provides
// one and ci/rabbitmq.env says where it is:
//
//	RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_VHOST  the AMQP listener
//	RABBITMQ_USER, RABBITMQ_PASS                  credentials, also used for the management API
//	RABBITMQ_MGMT_URL                             management API base URL
//
// Without RABBITMQ_HOST they skip. With it, the rest are required: a test that
// skipped for want of a variable CI forgot to set would pass without running.

const (
	// awaitTimeout bounds every wait on the broker. Generous, because what it
	// guards against is a hang, and a slow runner is not one.
	awaitTimeout = 20 * time.Second
	pollInterval = 50 * time.Millisecond
)

type brokerEnv struct {
	host, port, vhost string
	user, pass        string
	mgmtURL           string
}

func loadBrokerEnv(t *testing.T) brokerEnv {
	t.Helper()
	if os.Getenv("RABBITMQ_HOST") == "" {
		t.Skip("RABBITMQ_HOST not set; skipping RabbitMQ integration test")
	}
	e := brokerEnv{host: os.Getenv("RABBITMQ_HOST")}
	for _, v := range []struct {
		name string
		dst  *string
	}{
		{"RABBITMQ_PORT", &e.port},
		{"RABBITMQ_VHOST", &e.vhost},
		{"RABBITMQ_USER", &e.user},
		{"RABBITMQ_PASS", &e.pass},
		{"RABBITMQ_MGMT_URL", &e.mgmtURL},
	} {
		*v.dst = os.Getenv(v.name)
		if *v.dst == "" {
			t.Fatalf("RABBITMQ_HOST is set but %s is not", v.name)
		}
	}
	e.mgmtURL = strings.TrimRight(e.mgmtURL, "/")
	return e
}

func (e brokerEnv) amqpURL() string {
	return fmt.Sprintf("amqp://%s:%s/%s", e.host, e.port, url.PathEscape(e.vhost))
}

// mgmt makes one management API request and returns the status and body.
func (e brokerEnv) mgmt(method, path string) (int, []byte, error) {
	req, err := http.NewRequest(method, e.mgmtURL+path, nil)
	if err != nil {
		return 0, nil, err
	}
	req.SetBasicAuth(e.user, e.pass)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	var body json.RawMessage
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body, nil
}

// vhostConnections lists the names of the broker's connections on the vhost.
func (e brokerEnv) vhostConnections() ([]string, error) {
	status, body, err := e.mgmt(http.MethodGet, "/api/vhosts/"+url.PathEscape(e.vhost)+"/connections")
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("list connections: status %d: %s", status, body)
	}
	var conns []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(body, &conns); err != nil {
		return nil, err
	}
	names := make([]string, len(conns))
	for i, c := range conns {
		names[i] = c.Name
	}
	return names, nil
}

// brokerChannel is a channel as the management API lists it. Publishes is the
// number of messages the broker has counted published on it, which trails the
// publishes themselves by up to a statistics interval.
type brokerChannel struct {
	Number       int `json:"number"`
	MessageStats struct {
		Publishes int `json:"publish"`
	} `json:"message_stats"`
}

// connectionChannels lists the channels open on the named connection.
func (e brokerEnv) connectionChannels(conn string) ([]brokerChannel, error) {
	status, body, err := e.mgmt(http.MethodGet, "/api/connections/"+url.PathEscape(conn)+"/channels")
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("list channels: status %d: %s", status, body)
	}
	var chans []brokerChannel
	if err := json.Unmarshal(body, &chans); err != nil {
		return nil, err
	}
	return chans, nil
}

// awaitChannels waits until the channels listed on the named connection satisfy
// want, and returns them. what describes want for the failure message. Like the
// connection listing, this one trails the channels themselves: a new channel
// appears with its first statistics, up to an interval after it opens.
func awaitChannels(t *testing.T, e brokerEnv, conn, what string, want func([]brokerChannel) bool) []brokerChannel {
	t.Helper()
	var chans []brokerChannel
	var lastErr error
	if !assert.Eventually(t, func() bool {
		chans, lastErr = e.connectionChannels(conn)
		return lastErr == nil && want(chans)
	}, awaitTimeout, pollInterval) {
		t.Fatalf("connection %s never listed %s; last listing %+v, error %v", conn, what, chans, lastErr)
	}
	return chans
}

// awaitVhostConnections waits until the management API lists exactly n
// connections on the vhost, and returns their names.
//
// This is how a test finds its client's connection: the broker sees the
// connection's peer through Docker's port proxy, so the client's local address
// does not identify it. Nothing but these tests uses the broker, and they run
// one at a time, so waiting for none before the client starts and for one after
// makes that one the client's. The listing trails the connections themselves,
// which is why each count is waited for rather than read.
func awaitVhostConnections(t *testing.T, e brokerEnv, n int) []string {
	t.Helper()
	var names []string
	var lastErr error
	if !assert.Eventually(t, func() bool {
		names, lastErr = e.vhostConnections()
		return lastErr == nil && len(names) == n
	}, awaitTimeout, pollInterval) {
		t.Fatalf("wanted %d connections on vhost %q; last listing %q, error %v", n, e.vhost, names, lastErr)
	}
	return names
}

// testQueue names a queue for the test and a routing key unique to it, and
// deletes the queue when the test ends. Declaring it and binding it to amq.topic
// is the receiver's job, which the client repeats on every reconnect.
func testQueue(t *testing.T, e brokerEnv) (queue, key string) {
	t.Helper()
	key = fmt.Sprintf("it.%s.%d", strings.ToLower(t.Name()), time.Now().UnixNano())
	queue = "vinculum-rabbitmq." + key
	// Registered before the client's Stop, so it runs after it.
	t.Cleanup(func() {
		status, body, err := e.mgmt(http.MethodDelete,
			"/api/queues/"+url.PathEscape(e.vhost)+"/"+url.PathEscape(queue))
		if err != nil || (status != http.StatusNoContent && status != http.StatusNotFound) {
			t.Logf("delete queue %s: status %d, error %v: %s", queue, status, err, body)
		}
	})
	return queue, key
}

// recordingSubscriber hands each delivered topic to the test.
type recordingSubscriber struct {
	bus.BaseSubscriber
	topics chan string
}

func (r *recordingSubscriber) OnEvent(_ context.Context, topic string, _ any, _ map[string]string) error {
	select {
	case r.topics <- topic:
	default:
		// Full: the test is not reading, and blocking would hold up Stop.
	}
	return nil
}

type harness struct {
	c    *Client
	s    *sender.RMQSender
	sub  *recordingSubscriber
	key  string
	logs *observer.ObservedLogs
	// connName is the management API's name for the client's first connection.
	connName string
}

// startClient starts a client with one sender, publishing to amq.topic with
// confirms, and one receiver consuming the test's queue. mappings are added to
// the sender. It returns once the client is connected and the management API
// lists its connection.
func startClient(t *testing.T, e brokerEnv, mappings ...sender.TopicMapping) *harness {
	t.Helper()
	queue, key := testQueue(t, e)
	awaitVhostConnections(t, e, 0)

	core, logs := observer.New(zap.InfoLevel)
	logger := zap.New(core)
	t.Cleanup(func() {
		if t.Failed() {
			for _, entry := range logs.All() {
				t.Logf("%s %s %v", entry.Level, entry.Message, entry.ContextMap())
			}
		}
	})

	sb := sender.NewSender().
		WithClientName("it").
		WithExchange("amq.topic").
		WithConfirmMode(true).
		WithLogger(logger)
	for _, m := range mappings {
		sb = sb.WithTopicMapping(m)
	}
	s, err := sb.Build()
	require.NoError(t, err)

	sub := &recordingSubscriber{topics: make(chan string, 16)}
	r, err := receiver.NewReceiver().
		WithClientName("it").
		WithQueue(queue).
		WithDeclare(receiver.Declare{Durable: true}).
		WithBinding(receiver.Binding{Exchange: "amq.topic", RoutingKey: key + ".#"}).
		WithSubscriber(sub).
		WithLogger(logger).
		Build()
	require.NoError(t, err)

	c := NewClient(Config{
		ClientName:       "it",
		Brokers:          []string{e.amqpURL()},
		Username:         e.user,
		Password:         e.pass,
		Logger:           logger,
		ReconnectBackoff: func(int) time.Duration { return 100 * time.Millisecond },
	})
	c.AddSender(s)
	c.AddReceiver(r)
	require.NoError(t, c.Start(context.Background()))
	t.Cleanup(func() { _ = c.Stop() })
	require.Eventually(t, c.IsConnected, awaitTimeout, pollInterval, "the client never connected")
	connName := awaitVhostConnections(t, e, 1)[0]

	return &harness{c: c, s: s, sub: sub, key: key, logs: logs, connName: connName}
}

// roundTrip publishes through the client's sender and waits for its receiver
// to deliver the same topic.
func (h *harness) roundTrip(t *testing.T, leaf string) {
	t.Helper()
	topic := strings.ReplaceAll(h.key, ".", "/") + "/" + leaf
	require.NoError(t, h.s.OnEvent(context.Background(), topic, "payload", nil), "publish %s", topic)

	timeout := time.After(awaitTimeout)
	for {
		select {
		case got := <-h.sub.topics:
			if got == topic {
				return
			}
		case <-timeout:
			t.Fatalf("%s was published but never delivered", topic)
		}
	}
}

func (h *harness) count(msg string) int { return h.logs.FilterMessage(msg).Len() }

// awaitLog waits until msg has been logged at least n times.
func (h *harness) awaitLog(t *testing.T, msg string, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return h.count(msg) >= n },
		awaitTimeout, pollInterval, "never logged %q", msg)
}

// currentConn returns the connection the client holds.
func (h *harness) currentConn() *amqp.Connection {
	h.c.mu.Lock()
	defer h.c.mu.Unlock()
	return h.c.conn
}

const (
	logConnClosed = "rabbitmq client: connection closed; reconnecting"
	logReconnect  = "rabbitmq client: reconnected"

	logSenderDeferred   = "rabbitmq client: sender channel closed (connection-level); deferring to reconnect"
	logSenderRecovering = "rabbitmq client: sender channel closed; attempting recovery"
	logSenderRecovered  = "rabbitmq client: sender channel recovered"

	logReceiverDeferred   = "rabbitmq client: receiver channel closed (connection-level); deferring to reconnect"
	logReceiverRecovering = "rabbitmq client: receiver channel closed; attempting recovery"
)

// An operator closing the connection makes RabbitMQ send 320 CONNECTION_FORCED,
// which every channel on it reports to its watcher. That is a hard exception
// despite its code, so both watchers must leave it to the reconnect loop, and
// the loop must bring the sender and receiver back.
func TestIntegration_ForcedConnectionCloseReconnects(t *testing.T) {
	e := loadBrokerEnv(t)
	h := startClient(t, e)

	status, body, err := e.mgmt(http.MethodDelete, "/api/connections/"+url.PathEscape(h.connName))
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, status, "close connection %s: %s", h.connName, body)

	// Each watcher logs one of two messages when it sees the close, and the
	// reconnect loop logs once it has rebuilt the client. Waiting for all three
	// is what makes the absence below mean something.
	require.Eventually(t, func() bool {
		return h.count(logSenderDeferred)+h.count(logSenderRecovering) > 0 &&
			h.count(logReceiverDeferred)+h.count(logReceiverRecovering) > 0 &&
			h.count(logReconnect) > 0
	}, awaitTimeout, pollInterval, "the watchers and the reconnect loop did not all see the forced close")

	assert.Zero(t, h.count(logSenderRecovering), "the sender's watcher treated a forced close as a channel error")
	assert.Zero(t, h.count(logReceiverRecovering), "the receiver's watcher treated a forced close as a channel error")
	assert.Equal(t, 1, h.count(logSenderDeferred))
	assert.Equal(t, 1, h.count(logReceiverDeferred))
	assert.Equal(t, 1, h.count(logConnClosed))
	assert.Equal(t, 1, h.count(logReconnect))
	assert.True(t, h.c.IsConnected())

	h.roundTrip(t, "after-reconnect")
}

// Publishing to an exchange that does not exist makes RabbitMQ close the
// sender's channel with 404 NOT_FOUND, a soft exception: the connection and the
// receiver's channel are unaffected. The sender's watcher must reopen its
// channel on the same connection, with no reconnect.
func TestIntegration_SoftChannelCloseRecoversOnSameConnection(t *testing.T) {
	e := loadBrokerEnv(t)
	missing := fmt.Sprintf("vinculum-rabbitmq.it.missing.%d", time.Now().UnixNano())
	h := startClient(t, e, sender.TopicMapping{Pattern: "missing/#", Exchange: missing})
	h.roundTrip(t, "before")
	conn := h.currentConn()
	var before []int
	for _, ch := range awaitChannels(t, e, h.connName, "the sender's and the receiver's channels",
		func(chans []brokerChannel) bool { return len(chans) == 2 }) {
		before = append(before, ch.Number)
	}

	// Refused, one way or another: the broker closes the channel instead of
	// confirming. What the sender makes of that is not the point here.
	assert.Error(t, h.s.OnEvent(context.Background(), "missing/x", "payload", nil))

	h.awaitLog(t, logSenderRecovered, 1)
	h.roundTrip(t, "after-recovery")

	// The sender publishes on the connection the broker already had: two channels
	// again, and the one that is new has carried the publish just made.
	// amqp091-go numbers a connection's channels from a rolling index, so the
	// reopened channel does not take the closed one's number. The client's own
	// view of its connection cannot show this, since a channel opened elsewhere
	// looks the same to it; nor can a new channel appearing here, unless it is
	// the one being published on.
	awaitChannels(t, e, h.connName, fmt.Sprintf("two channels, one of them new (not among %v) with a publish", before),
		func(chans []brokerChannel) bool {
			return len(chans) == 2 && slices.ContainsFunc(chans, func(ch brokerChannel) bool {
				return !slices.Contains(before, ch.Number) && ch.MessageStats.Publishes >= 1
			})
		})

	recovering := h.logs.FilterMessage(logSenderRecovering).AllUntimed()
	require.Len(t, recovering, 1)
	assert.EqualValues(t, amqp.NotFound, recovering[0].ContextMap()["code"])

	assert.Same(t, conn, h.currentConn(), "the client replaced its connection")
	assert.False(t, conn.IsClosed())
	assert.True(t, h.c.IsConnected())
	assert.Zero(t, h.count(logSenderDeferred))
	assert.Zero(t, h.count(logConnClosed))
	assert.Zero(t, h.count(logReconnect))
	assert.Zero(t, h.logs.FilterMessageSnippet("receiver channel closed").Len(),
		"the receiver's channel closed too")
}
