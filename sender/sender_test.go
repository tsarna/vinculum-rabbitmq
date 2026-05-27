package sender

import (
	"context"
	"errors"
	"sync"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wire "github.com/tsarna/vinculum-wire"
)

// fakeChannel captures publish calls for assertions. It also exposes hooks
// for confirm-mode behaviour: confirmsErr, publishConfirmedErr, nextAck, and
// returnsSink (set by subscribeReturns so tests can inject Return frames).
type fakeChannel struct {
	mu        sync.Mutex
	calls     int
	lastCtx   context.Context
	lastExch  string
	lastKey   string
	lastMand  bool
	lastImmed bool
	lastPub   amqp.Publishing
	returnErr error

	// confirm-mode behavior
	confirmsCalled      bool
	confirmsErr         error
	publishConfirmedErr error
	nextAck             bool // value returned by publisherConfirmation.WaitContext
	confirmCalls        int

	// returns channel installed by subscribeReturns
	returnsSink chan amqp.Return

	// When injectReturnReplyCode != 0, publishConfirmed injects a Return into
	// returnsSink with the publish's MessageId so the post-ack drain has
	// something to correlate against (mirrors the broker's behavior for
	// unroutable mandatory messages: Return frame before Ack frame).
	injectReturnReplyCode uint16
	injectReturnReplyText string
}

func (f *fakeChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastCtx = ctx
	f.lastExch = exchange
	f.lastKey = key
	f.lastMand = mandatory
	f.lastImmed = immediate
	f.lastPub = msg
	return f.returnErr
}

func (f *fakeChannel) publishConfirmed(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (publisherConfirmation, error) {
	f.mu.Lock()
	f.calls++
	f.confirmCalls++
	f.lastCtx = ctx
	f.lastExch = exchange
	f.lastKey = key
	f.lastMand = mandatory
	f.lastImmed = immediate
	f.lastPub = msg
	err := f.publishConfirmedErr
	ack := f.nextAck
	injectCode := f.injectReturnReplyCode
	injectText := f.injectReturnReplyText
	sink := f.returnsSink
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	// AMQP wire order: Return precedes Ack for an unroutable mandatory
	// message. Push the return BEFORE returning the deferred confirmation so
	// the post-ack drain finds it.
	if injectCode != 0 && sink != nil {
		select {
		case sink <- amqp.Return{
			ReplyCode:  injectCode,
			ReplyText:  injectText,
			Exchange:   exchange,
			RoutingKey: key,
			MessageId:  msg.MessageId,
		}:
		default:
		}
	}
	return &fakeConfirmation{ack: ack}, nil
}

func (f *fakeChannel) enableConfirms() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.confirmsCalled = true
	return f.confirmsErr
}

func (f *fakeChannel) subscribeReturns(c chan amqp.Return) chan amqp.Return {
	f.mu.Lock()
	f.returnsSink = c
	f.mu.Unlock()
	return c
}

// fakeConfirmation is the test analogue of *amqp.DeferredConfirmation.
type fakeConfirmation struct {
	ack bool
	err error
}

func (c *fakeConfirmation) WaitContext(ctx context.Context) (bool, error) {
	if c.err != nil {
		return false, c.err
	}
	return c.ack, nil
}

func TestOnEvent_NotConnectedReturnsError(t *testing.T) {
	s, err := NewSender().WithExchange("events").Build()
	require.NoError(t, err)

	err = s.OnEvent(context.Background(), "topic", "msg", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not yet connected")
}

func TestOnEvent_SlashToDotByDefault(t *testing.T) {
	s, err := NewSender().WithExchange("events").Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "sensor/abc/reading", "payload", nil)
	require.NoError(t, err)

	assert.Equal(t, 1, fc.calls)
	assert.Equal(t, "events", fc.lastExch)
	assert.Equal(t, "sensor.abc.reading", fc.lastKey)
	assert.False(t, fc.lastMand)
	assert.False(t, fc.lastImmed)
	// auto + string → text/plain
	assert.Equal(t, "text/plain", fc.lastPub.ContentType)
	// persistent default → delivery mode 2
	assert.Equal(t, uint8(2), fc.lastPub.DeliveryMode)
	assert.Equal(t, []byte("payload"), fc.lastPub.Body)
}

func TestOnEvent_DefaultTransformVerbatim(t *testing.T) {
	s, err := NewSender().
		WithExchange("events").
		WithDefaultTransform(DefaultTopicVerbatim).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "sensor/abc/reading", "x", nil)
	require.NoError(t, err)
	assert.Equal(t, "sensor/abc/reading", fc.lastKey)
}

func TestOnEvent_DefaultTransformError(t *testing.T) {
	s, err := NewSender().
		WithExchange("events").
		WithDefaultTransform(DefaultTopicError).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "sensor/abc/reading", "x", nil)
	require.Error(t, err)
	assert.Equal(t, 0, fc.calls)
}

func TestOnEvent_DefaultTransformIgnore(t *testing.T) {
	s, err := NewSender().
		WithExchange("events").
		WithDefaultTransform(DefaultTopicIgnore).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "sensor/abc/reading", "x", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, fc.calls)
}

func TestOnEvent_TopicMapping_FirstMatchWins(t *testing.T) {
	called := map[string]bool{}
	s, err := NewSender().
		WithExchange("events").
		WithTopicMapping(TopicMapping{
			Pattern: "sensor/+deviceId/reading",
			RoutingKeyFunc: func(topic string, msg any, fields map[string]string) (string, error) {
				called["sensor"] = true
				return "sensor." + fields["deviceId"] + ".rd", nil
			},
		}).
		WithTopicMapping(TopicMapping{
			Pattern: "#",
			RoutingKeyFunc: func(topic string, msg any, fields map[string]string) (string, error) {
				called["catchall"] = true
				return "catchall", nil
			},
		}).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "sensor/abc/reading", "x", nil)
	require.NoError(t, err)
	assert.Equal(t, "sensor.abc.rd", fc.lastKey)
	assert.True(t, called["sensor"])
	assert.False(t, called["catchall"])
}

func TestOnEvent_TopicMapping_ExchangeAndPersistentOverride(t *testing.T) {
	notPersistent := false
	s, err := NewSender().
		WithExchange("events").
		WithPersistent(true).
		WithTopicMapping(TopicMapping{
			Pattern:    "alerts/#",
			Exchange:   "alerts-exchange",
			Persistent: &notPersistent,
		}).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "alerts/cpu", "x", nil)
	require.NoError(t, err)
	assert.Equal(t, "alerts-exchange", fc.lastExch)
	assert.Equal(t, "alerts.cpu", fc.lastKey) // RoutingKeyFunc=nil → slash_to_dot
	assert.Equal(t, uint8(1), fc.lastPub.DeliveryMode)
}

func TestOnEvent_TopicMapping_RoutingKeyFuncError(t *testing.T) {
	s, err := NewSender().
		WithExchange("events").
		WithTopicMapping(TopicMapping{
			Pattern: "foo/#",
			RoutingKeyFunc: func(string, any, map[string]string) (string, error) {
				return "", errors.New("boom")
			},
		}).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "foo/bar", "x", nil)
	require.Error(t, err)
	assert.Equal(t, 0, fc.calls)
	assert.Contains(t, err.Error(), "boom")
}

func TestOnEvent_FieldsMergedWithExtracted(t *testing.T) {
	var seen map[string]string
	s, err := NewSender().
		WithExchange("events").
		WithTopicMapping(TopicMapping{
			Pattern: "sensor/+deviceId/reading",
			RoutingKeyFunc: func(topic string, msg any, fields map[string]string) (string, error) {
				seen = fields
				return "k", nil
			},
		}).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "sensor/abc/reading", "x", map[string]string{
		"source":   "test",
		"deviceId": "should-be-overridden",
	})
	require.NoError(t, err)
	assert.Equal(t, "abc", seen["deviceId"])
	assert.Equal(t, "test", seen["source"])
}

func TestOnEvent_Mandatory(t *testing.T) {
	s, err := NewSender().
		WithExchange("events").
		WithMandatory(true).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "alerts/cpu", "x", nil)
	require.NoError(t, err)
	assert.True(t, fc.lastMand)
}

func TestOnEvent_HeadersAndContentType_JSON(t *testing.T) {
	s, err := NewSender().
		WithExchange("events").
		WithWireFormat(wire.JSON).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	s.setChannel(fc)

	fields := map[string]string{"a": "1", "b": "two"}
	err = s.OnEvent(context.Background(), "topic", map[string]any{"x": 1}, fields)
	require.NoError(t, err)

	assert.Equal(t, "application/json", fc.lastPub.ContentType)
	require.NotNil(t, fc.lastPub.Headers)
	assert.Equal(t, "1", fc.lastPub.Headers["a"])
	assert.Equal(t, "two", fc.lastPub.Headers["b"])
	assert.JSONEq(t, `{"x":1}`, string(fc.lastPub.Body))
}

func TestOnEvent_ContentType_Bytes(t *testing.T) {
	s, err := NewSender().WithExchange("e").WithWireFormat(wire.Bytes).Build()
	require.NoError(t, err)
	fc := &fakeChannel{}
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "t", "hello", nil)
	require.NoError(t, err)
	assert.Equal(t, "application/octet-stream", fc.lastPub.ContentType)
}

func TestOnEvent_ContentType_AutoBytes(t *testing.T) {
	s, err := NewSender().WithExchange("e").Build()
	require.NoError(t, err)
	fc := &fakeChannel{}
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "t", []byte("raw"), nil)
	require.NoError(t, err)
	assert.Equal(t, "application/octet-stream", fc.lastPub.ContentType)
}

func TestOnEvent_ContentType_AutoMap(t *testing.T) {
	s, err := NewSender().WithExchange("e").Build()
	require.NoError(t, err)
	fc := &fakeChannel{}
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "t", map[string]any{"k": "v"}, nil)
	require.NoError(t, err)
	assert.Equal(t, "application/json", fc.lastPub.ContentType)
}

func TestOnEvent_PublishErrorPropagates(t *testing.T) {
	s, err := NewSender().WithExchange("e").Build()
	require.NoError(t, err)
	fc := &fakeChannel{returnErr: errors.New("broker down")}
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "t", "x", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "broker down")
}

func TestSetChannel_HotSwap(t *testing.T) {
	s, err := NewSender().WithExchange("e").Build()
	require.NoError(t, err)

	ch1 := &fakeChannel{}
	ch2 := &fakeChannel{}

	s.setChannel(ch1)
	require.NoError(t, s.OnEvent(context.Background(), "t", "x", nil))
	assert.Equal(t, 1, ch1.calls)

	s.setChannel(ch2)
	require.NoError(t, s.OnEvent(context.Background(), "t", "y", nil))
	assert.Equal(t, 1, ch1.calls)
	assert.Equal(t, 1, ch2.calls)
}

func TestBuilder_RejectsEmptyPattern(t *testing.T) {
	_, err := NewSender().
		WithExchange("e").
		WithTopicMapping(TopicMapping{Pattern: ""}).
		Build()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty Pattern")
}

// ─── Confirm-mode tests ─────────────────────────────────────────────────────

func TestConfirmMode_DefaultIsOff(t *testing.T) {
	// Direct Go API default is false; the VCL layer is what defaults to true.
	s, err := NewSender().WithExchange("e").Build()
	require.NoError(t, err)
	fc := &fakeChannel{}
	s.setChannel(fc)

	require.NoError(t, s.OnEvent(context.Background(), "t", "x", nil))
	assert.False(t, fc.confirmsCalled, "Confirm() must not be called when confirm mode is off")
	assert.Equal(t, 1, fc.calls)
	assert.Equal(t, 0, fc.confirmCalls)
}

func TestConfirmMode_EnabledCallsConfirmOnSetChannel(t *testing.T) {
	s, err := NewSender().WithExchange("e").WithConfirmMode(true).Build()
	require.NoError(t, err)

	fc := &fakeChannel{nextAck: true}
	s.setChannel(fc)
	assert.True(t, fc.confirmsCalled, "Confirm() must be called when confirm mode is on")
}

func TestConfirmMode_WaitsForAckAndSucceeds(t *testing.T) {
	s, err := NewSender().WithExchange("e").WithConfirmMode(true).Build()
	require.NoError(t, err)

	fc := &fakeChannel{nextAck: true}
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "alerts/cpu", "x", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, fc.confirmCalls, "confirm-mode publish must use publishConfirmed")
	assert.Equal(t, 0, fc.calls-fc.confirmCalls, "non-confirm publish path must not be used")
}

func TestConfirmMode_NackReturnsError(t *testing.T) {
	s, err := NewSender().WithExchange("e").WithConfirmMode(true).Build()
	require.NoError(t, err)

	fc := &fakeChannel{nextAck: false}
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "alerts/cpu", "x", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nack")
}

func TestConfirmMode_PublishErrorPropagates(t *testing.T) {
	s, err := NewSender().WithExchange("e").WithConfirmMode(true).Build()
	require.NoError(t, err)

	fc := &fakeChannel{publishConfirmedErr: errors.New("channel closed")}
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "t", "x", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "channel closed")
}

func TestConfirmMode_EnableConfirmsFailureLeavesSenderDisconnected(t *testing.T) {
	s, err := NewSender().WithExchange("e").WithConfirmMode(true).Build()
	require.NoError(t, err)

	fc := &fakeChannel{confirmsErr: errors.New("server refused confirm")}
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "t", "x", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not yet connected")
}

func TestSubscribeReturns_InstalledByDefault(t *testing.T) {
	s, err := NewSender().WithExchange("e").Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	s.setChannel(fc)

	fc.mu.Lock()
	sink := fc.returnsSink
	fc.mu.Unlock()
	require.NotNil(t, sink, "subscribeReturns must be called by setChannel")
}

func TestSetChannel_HotSwap_StopsPreviousReturnsWatcher(t *testing.T) {
	s, err := NewSender().WithExchange("e").Build()
	require.NoError(t, err)

	ch1 := &fakeChannel{}
	s.setChannel(ch1)
	fc1Sink := ch1.returnsSink
	require.NotNil(t, fc1Sink)

	ch2 := &fakeChannel{}
	s.setChannel(ch2)
	require.NotNil(t, ch2.returnsSink)

	// The previous returns sink is still open as far as our fake is concerned,
	// but the watcher goroutine for ch1 has been signalled to exit via the
	// stop channel. Sending on fc1Sink would not panic; nothing should consume
	// it. We close it to ensure no leaked goroutine is reading from it.
	close(fc1Sink)
}

func TestSetChannel_Nil_TearsDownReturnsWatcher(t *testing.T) {
	s, err := NewSender().WithExchange("e").Build()
	require.NoError(t, err)

	fc := &fakeChannel{}
	s.setChannel(fc)
	require.NotNil(t, fc.returnsSink)

	s.setChannel(nil)

	// After SetChannel(nil), OnEvent should report not connected.
	err = s.OnEvent(context.Background(), "t", "x", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not yet connected")
}

// Mandatory + confirm-mode: a Basic.Return for the publish in flight must
// surface as an OnEvent error, with the broker's reply code and text in the
// error string. The fake injects the return on publishConfirmed (ahead of the
// ack), mirroring the AMQP wire order.
func TestConfirmMode_MandatoryReturnSurfacesAsError(t *testing.T) {
	s, err := NewSender().
		WithExchange("e").
		WithConfirmMode(true).
		WithMandatory(true).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{
		nextAck:               true, // broker acks even when message is returned
		injectReturnReplyCode: 312,
		injectReturnReplyText: "NO_ROUTE",
	}
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "alerts/no/binding", "x", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mandatory message returned by broker")
	assert.Contains(t, err.Error(), "reply_code=312")
	assert.Contains(t, err.Error(), "NO_ROUTE")
	// The publish must have carried a MessageId so the broker (real or fake)
	// could echo it in the Return for correlation.
	assert.NotEmpty(t, fc.lastPub.MessageId, "mandatory publishes must set MessageId")
}

// Mandatory + confirm-mode without a return: routes normally; OnEvent succeeds.
func TestConfirmMode_MandatoryNotReturnedSucceeds(t *testing.T) {
	s, err := NewSender().
		WithExchange("e").
		WithConfirmMode(true).
		WithMandatory(true).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{nextAck: true} // no injectReturnReplyCode -> no return
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "alerts/cpu", "x", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, fc.lastPub.MessageId, "mandatory publishes always set MessageId")
}

// Mandatory=false + confirm-mode: no MessageId is set (no return is expected),
// and any orphan return that does arrive is logged + counted but does NOT
// produce an OnEvent error.
func TestConfirmMode_NonMandatoryDoesNotSetMessageIdNorError(t *testing.T) {
	s, err := NewSender().
		WithExchange("e").
		WithConfirmMode(true).
		Build()
	require.NoError(t, err)

	fc := &fakeChannel{
		nextAck:               true,
		injectReturnReplyCode: 312,
		injectReturnReplyText: "NO_ROUTE", // stray return; not for this publish
	}
	s.setChannel(fc)

	err = s.OnEvent(context.Background(), "alerts/cpu", "x", nil)
	require.NoError(t, err, "stray returns must not error non-mandatory publishes")
	assert.Empty(t, fc.lastPub.MessageId, "non-mandatory publishes must not set MessageId")
}
