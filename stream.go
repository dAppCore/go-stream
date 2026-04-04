// SPDX-License-Identifier: EUPL-1.2

// Package stream is the transport-agnostic event and data pipe for the CoreGO
// ecosystem. It generalises WebSocket, SSE, Redis pub/sub, ZeroMQ, and raw TCP
// behind a single Stream interface. Consumers never import a specific transport —
// they call Stream. Transport adapters are wired at startup.
//
//	hub := stream.NewHub()
//	go hub.Run(ctx)
//	hub.Publish("hashrate", []byte(`{"h":123456}`))
//	unsub := hub.Subscribe("block", func(f []byte) { handleBlock(f) })
//	defer unsub()
package stream

import (
	"context"
	"iter"
	"sync"
	"time"
)

// Stream is the transport-agnostic event and data pipe.
// Consumers never import a specific adapter — they call Stream.
//
//	var s stream.Stream = hub
//	s.Publish("hashrate", frame)
//	s.Subscribe("block", handler)
type Stream interface {
	// Publish sends frame to all subscribers of channel.
	// Returns core.E if the hub is not running.
	//
	//	hub.Publish("hashrate", []byte(`{"h":123456}`))
	Publish(channel string, frame []byte) error

	// Subscribe registers handler for all frames arriving on channel.
	// Returns an unsubscribe function. Safe to call from multiple goroutines.
	//
	//	unsub := hub.Subscribe("block", func(f []byte) { ... })
	//	defer unsub()
	Subscribe(channel string, handler func([]byte)) func()

	// Broadcast sends frame to every connected peer regardless of subscriptions.
	//
	//	hub.Broadcast([]byte(`{"type":"shutdown"}`))
	Broadcast(frame []byte) error

	// Pipe connects this stream to dst: every frame published here is forwarded to dst.
	// Returns a stop function.
	//
	//	stop := hub.Pipe(remoteHub)
	//	defer stop()
	Pipe(dst Stream) func()

	// Stats returns a snapshot of current hub state.
	//
	//	s := hub.Stats()
	Stats() HubStats
}

// Frame is a raw byte payload delivered through the hub.
// Adapters and consumers define their own serialisation over Frame.
type Frame = []byte

// Channel is a named topic string used for pub/sub routing.
type Channel = string

// Peer represents one connected endpoint. Created by a transport adapter.
//
//	peer := &stream.Peer{
//	    ID:        uuid.New(),
//	    UserID:    authResult.UserID,
//	    Claims:    authResult.Claims,
//	    Transport: "ws",
//	}
type Peer struct {
	// ID is a random UUID assigned on creation.
	ID string

	// UserID is the authenticated user identifier. Empty when no auth is configured.
	UserID string

	// Claims holds arbitrary auth metadata (roles, tenant ID, scopes).
	Claims map[string]any

	// Transport identifies the adapter type for logging and metrics.
	// Values: "ws", "sse", "tcp", "zmq"
	Transport string

	send          chan []byte
	subscriptions map[string]bool
	mu            sync.RWMutex
	closeOnce     sync.Once
}

// Subscriptions returns a copy of this peer's current channel subscriptions.
//
//	channels := peer.Subscriptions()   // ["hashrate", "block"]
func (p *Peer) Subscriptions() []string {
	return nil
}

// Send enqueues frame for delivery. Non-blocking: drops and returns false if buffer full.
//
//	ok := peer.Send(frame)
func (p *Peer) Send(frame []byte) bool {
	return false
}

// Close signals the transport adapter to shut down this connection.
//
//	peer.Close()
func (p *Peer) Close() {
}

// ConnectionState represents the lifecycle state of a reconnecting client.
type ConnectionState int

const (
	StateDisconnected ConnectionState = iota
	StateConnecting
	StateConnected
)

// Envelope wraps a frame with metadata for cross-instance transport.
type Envelope struct {
	SourceID string
	Channel  string
	Frame    []byte
}

// Pipe connects src to dst: every frame published on src is forwarded to dst.
// Returns a stop function. Safe to call from multiple goroutines.
//
//	stop := stream.Pipe(zmqHub, wsHub)
//	defer stop()
func Pipe(src Stream, dst Stream) func() {
	return nil
}

// Ensure Hub satisfies Stream at compile time.
var _ Stream = (*Hub)(nil)

// Ensure unused imports are referenced.
var (
	_ context.Context
	_ iter.Seq[*Peer]
	_ sync.Once
	_ time.Duration
)
