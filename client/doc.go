// Package client manages a single AMQP 0-9-1 connection shared by all senders
// and receivers within a vinculum client "rabbitmq" block. It owns the
// connection lifecycle, the reconnect loop, and per-channel recovery.
//
// # Usage
//
// Create a client, add senders and receivers, then start:
//
//	c, err := client.NewClient(client.Config{
//	    Brokers:   []string{"amqp://localhost:5672/"},
//	    Heartbeat: 10 * time.Second,
//	})
//	c.AddSender(s)
//	c.AddReceiver(r)
//	if err := c.Start(ctx); err != nil { ... }
//	defer c.Stop(ctx)
//
// Start blocks until the first connection is established and all senders and
// receivers are wired to live AMQP channels.
//
// # Connection lifecycle
//
// AMQP 0-9-1 connections carry multiple lightweight channels. The client opens
// one channel per sender and one channel per receiver. On connection loss the
// client walks the Brokers list (failover) with exponential backoff before
// reopening channels and re-declaring topology. On channel-level errors (e.g.
// a publish to a non-existent exchange) only the affected channel is reopened.
package client
