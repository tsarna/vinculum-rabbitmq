package client

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeBroker speaks just enough AMQP 0-9-1 to take a real amqp091-go connection
// through the handshake, open channels, and close the connection from the
// broker's side. It exists so the client's goroutines can be driven through
// their real code paths without a RabbitMQ. It accepts one connection.
type fakeBroker struct {
	t  *testing.T
	ln net.Listener

	mu   sync.Mutex
	conn net.Conn
}

func newFakeBroker(t *testing.T) *fakeBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	b := &fakeBroker{t: t, ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go b.serve()
	return b
}

func (b *fakeBroker) url() string { return "amqp://" + b.ln.Addr().String() + "/" }

func (b *fakeBroker) serve() {
	conn, err := b.ln.Accept()
	if err != nil {
		return
	}
	b.mu.Lock()
	b.conn = conn
	b.mu.Unlock()
	b.t.Cleanup(func() { _ = conn.Close() })

	if _, err := io.ReadFull(conn, make([]byte, 8)); err != nil { // protocol header
		return
	}
	b.send(0, method(10, 10, // connection.start
		[]byte{0, 9},     // version 0-9
		longstr(""),      // server properties: empty table
		longstr("PLAIN"), // mechanisms
		longstr("en_US"), // locales
	))
	for {
		ch, class, id, ok := readMethod(conn)
		if !ok {
			return
		}
		switch {
		case class == 10 && id == 11: // connection.start-ok
			b.send(0, method(10, 30, u16(0), u32(131072), u16(0))) // tune: no heartbeat
		case class == 10 && id == 40: // connection.open
			b.send(0, method(10, 41, shortstr("")))
		case class == 20 && id == 10: // channel.open
			b.send(ch, method(20, 11, longstr("")))
		}
	}
}

// closeConnection closes the connection from the broker's side with code.
func (b *fakeBroker) closeConnection(code uint16) {
	b.send(0, connectionClose(code, "test"))
}

func (b *fakeBroker) send(ch uint16, payload []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn != nil {
		_, _ = b.conn.Write(frame(ch, payload))
	}
}

// readMethod reads frames until a method frame, returning its channel, class
// and method ids.
func readMethod(r io.Reader) (ch, class, id uint16, ok bool) {
	for {
		header := make([]byte, 7)
		if _, err := io.ReadFull(r, header); err != nil {
			return 0, 0, 0, false
		}
		body := make([]byte, binary.BigEndian.Uint32(header[3:])+1) // payload + frame end
		if _, err := io.ReadFull(r, body); err != nil {
			return 0, 0, 0, false
		}
		if header[0] == 1 && len(body) >= 5 {
			return binary.BigEndian.Uint16(header[1:]), binary.BigEndian.Uint16(body), binary.BigEndian.Uint16(body[2:]), true
		}
	}
}

// connectionClose is a connection.close method with no failing class or method.
func connectionClose(code uint16, reason string) []byte {
	return method(10, 50, u16(code), shortstr(reason), u16(0), u16(0))
}

func method(class, id uint16, args ...[]byte) []byte {
	payload := append(u16(class), u16(id)...)
	for _, a := range args {
		payload = append(payload, a...)
	}
	return payload
}

// frame wraps a method payload in an AMQP method frame on channel ch.
func frame(ch uint16, payload []byte) []byte {
	f := []byte{1} // type: method
	f = binary.BigEndian.AppendUint16(f, ch)
	f = binary.BigEndian.AppendUint32(f, uint32(len(payload)))
	f = append(f, payload...)
	return append(f, 0xCE) // frame end
}

func u16(v uint16) []byte { return binary.BigEndian.AppendUint16(nil, v) }
func u32(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }

func shortstr(s string) []byte { return append([]byte{byte(len(s))}, s...) }
func longstr(s string) []byte  { return append(u32(uint32(len(s))), s...) }
