package carrier

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Carrier implements propagation.TextMapCarrier backed by an amqp.Table
// (the AMQP basic-properties headers table). It is used by the sender and
// receiver packages to inject and extract W3C trace context (traceparent,
// tracestate) and baggage without importing the full propagation package.
type Carrier struct {
	table amqp.Table
}

// New wraps table. If table is nil, an empty table is allocated so Set
// can populate it; callers reading the result via Table() will get the
// populated table.
func New(table amqp.Table) *Carrier {
	if table == nil {
		table = amqp.Table{}
	}
	return &Carrier{table: table}
}

// Get returns the value for key as a string, or "" if missing. AMQP table
// values are typed `any`; strings and byte slices are returned directly,
// other types are formatted via fmt.Sprintf("%v"). The trace propagator
// only writes strings, so the verbatim path is what we exercise in practice.
func (c *Carrier) Get(key string) string {
	v, ok := c.table[key]
	if !ok {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// Set stores value under key in the underlying table.
func (c *Carrier) Set(key, value string) {
	c.table[key] = value
}

// Keys returns the keys in the underlying table. Order is unspecified
// (consistent with map iteration in Go).
func (c *Carrier) Keys() []string {
	keys := make([]string, 0, len(c.table))
	for k := range c.table {
		keys = append(keys, k)
	}
	return keys
}

// Table returns the underlying amqp.Table, including any values added via
// Set. Callers can pass the result to amqp.Publishing.Headers.
func (c *Carrier) Table() amqp.Table {
	return c.table
}
