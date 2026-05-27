// Package carrier implements propagation.TextMapCarrier over the AMQP 0-9-1
// basic-properties headers table. It is used by the sender and receiver to
// inject and extract W3C trace context headers without importing the full
// propagation package.
package carrier
