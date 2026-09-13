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
// through the handshake, open and close channels, declare a queue, register a
// consumer, deliver to it, and close the connection. It exists so the client's
// goroutines can be driven through their real code paths without a RabbitMQ.
//
// Channel opens are answered as they arrive, except while holdOpens is set:
// then each one is reported on opened and answered only once release is
// closed. That is what lets a test catch a channel recovery part-way through.
type fakeBroker struct {
	t  *testing.T
	ln net.Listener

	mu        sync.Mutex
	conn      net.Conn
	consumers map[uint16]string // channel → consumer tag
	holdOpens bool
	opened    chan uint16
	release   chan struct{}
}

func newFakeBroker(t *testing.T) *fakeBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	b := &fakeBroker{
		t:         t,
		ln:        ln,
		consumers: map[uint16]string{},
		opened:    make(chan uint16, 4),
		release:   make(chan struct{}),
	}
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
		ch, class, id, args, ok := readMethod(conn)
		if !ok {
			return
		}
		switch {
		case class == 10 && id == 11: // connection.start-ok
			b.send(0, method(10, 30, u16(0), u32(131072), u16(0))) // tune: no heartbeat
		case class == 10 && id == 40: // connection.open
			b.send(0, method(10, 41, shortstr("")))
		case class == 10 && id == 50: // connection.close
			b.send(0, method(10, 51))
			return
		case class == 20 && id == 10: // channel.open
			b.mu.Lock()
			hold := b.holdOpens
			b.mu.Unlock()
			if !hold {
				b.send(ch, method(20, 11, longstr("")))
				continue
			}
			b.opened <- ch
			go func() {
				<-b.release
				b.send(ch, method(20, 11, longstr("")))
			}()
		case class == 20 && id == 40: // channel.close from the client
			b.send(ch, method(20, 41))
		case class == 50 && id == 10: // queue.declare
			b.send(ch, method(50, 11, shortstr("q"), u32(0), u32(0)))
		case class == 60 && id == 10: // basic.qos
			b.send(ch, method(60, 11))
		case class == 60 && id == 20: // basic.consume: reserved, queue, tag
			tag := readShortstr(args[2+1+int(args[2]):])
			b.mu.Lock()
			b.consumers[ch] = tag
			b.mu.Unlock()
			b.send(ch, method(60, 21, shortstr(tag)))
		case class == 60 && id == 30: // basic.cancel: tag
			b.send(ch, method(60, 31, shortstr(readShortstr(args))))
		}
	}
}

// closeChannel closes channel ch from the broker's side with code.
func (b *fakeBroker) closeChannel(ch uint16, code uint16) {
	b.send(ch, method(20, 40, u16(code), shortstr("test"), u16(0), u16(0)))
}

// closeConnection closes the connection from the broker's side with code.
func (b *fakeBroker) closeConnection(code uint16) {
	b.send(0, method(10, 50, u16(code), shortstr("test"), u16(0), u16(0)))
}

// deliver sends one message to the consumer registered on channel ch.
func (b *fakeBroker) deliver(ch uint16, routingKey, body string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	deliver := method(60, 60, shortstr(b.consumers[ch]), u64(1), []byte{0}, shortstr(""), shortstr(routingKey))
	header := append(u16(60), u16(0)...) // class basic, weight
	header = append(header, u64(uint64(len(body)))...)
	header = append(header, u16(0)...) // no properties
	_, _ = b.conn.Write(append(append(
		frame(1, ch, deliver),
		frame(2, ch, header)...),
		frame(3, ch, []byte(body))...))
}

func (b *fakeBroker) send(ch uint16, payload []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn != nil {
		_, _ = b.conn.Write(frame(1, ch, payload))
	}
}

// readMethod reads frames until a method frame, returning its channel, class
// and method ids, and its arguments.
func readMethod(r io.Reader) (ch, class, id uint16, args []byte, ok bool) {
	for {
		header := make([]byte, 7)
		if _, err := io.ReadFull(r, header); err != nil {
			return 0, 0, 0, nil, false
		}
		body := make([]byte, binary.BigEndian.Uint32(header[3:])+1) // payload + frame end
		if _, err := io.ReadFull(r, body); err != nil {
			return 0, 0, 0, nil, false
		}
		if header[0] == 1 && len(body) >= 5 {
			return binary.BigEndian.Uint16(header[1:]), binary.BigEndian.Uint16(body),
				binary.BigEndian.Uint16(body[2:]), body[4 : len(body)-1], true
		}
	}
}

func method(class, id uint16, args ...[]byte) []byte {
	payload := append(u16(class), u16(id)...)
	for _, a := range args {
		payload = append(payload, a...)
	}
	return payload
}

// frame wraps a payload in an AMQP frame of type typ (1 method, 2 content
// header, 3 body) on channel ch.
func frame(typ byte, ch uint16, payload []byte) []byte {
	f := []byte{typ}
	f = binary.BigEndian.AppendUint16(f, ch)
	f = binary.BigEndian.AppendUint32(f, uint32(len(payload)))
	f = append(f, payload...)
	return append(f, 0xCE) // frame end
}

func u16(v uint16) []byte { return binary.BigEndian.AppendUint16(nil, v) }
func u32(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }
func u64(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }

func shortstr(s string) []byte { return append([]byte{byte(len(s))}, s...) }
func longstr(s string) []byte  { return append(u32(uint32(len(s))), s...) }

func readShortstr(b []byte) string { return string(b[1 : 1+int(b[0])]) }
