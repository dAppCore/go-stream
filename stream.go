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
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"iter"
	"sort"
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

// NewPeer creates a peer with a generated identifier and a buffered send queue.
//
//	peer := stream.NewPeer("ws")
func NewPeer(transport string) *Peer {
	return &Peer{
		ID:            randomID(),
		Transport:     transport,
		send:          make(chan []byte, 256),
		subscriptions: map[string]bool{},
	}
}

// Subscriptions returns a copy of this peer's current channel subscriptions.
//
//	channels := peer.Subscriptions()   // ["hashrate", "block"]
func (p *Peer) Subscriptions() []string {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	channels := make([]string, 0, len(p.subscriptions))
	for channel := range p.subscriptions {
		channels = append(channels, channel)
	}
	sort.Strings(channels)
	return channels
}

// Send enqueues frame for delivery. Non-blocking: drops and returns false if buffer full.
//
//	ok := peer.Send(frame)
func (p *Peer) Send(frame []byte) bool {
	if p == nil {
		return false
	}
	defer func() {
		_ = recover()
	}()
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.send == nil {
		return false
	}
	payload := append([]byte(nil), frame...)
	select {
	case p.send <- payload:
		return true
	default:
		return false
	}
}

// Close signals the transport adapter to shut down this connection.
//
//	peer.Close()
func (p *Peer) Close() {
	if p == nil {
		return
	}
	p.closeOnce.Do(func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.send != nil {
			close(p.send)
		}
	})
}

// SendQueue returns the peer's outgoing frame queue.
//
//	for frame := range peer.SendQueue() { ... }
func (p *Peer) SendQueue() <-chan []byte {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.send
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
	if src == nil || dst == nil || src == dst {
		return func() {}
	}
	stop := src.Subscribe("*", func(frame []byte) {
		_ = dst.Broadcast(frame)
	})
	return stop
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

func randomID() string {
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	return hex.EncodeToString(raw[:4]) + "-" +
		hex.EncodeToString(raw[4:6]) + "-" +
		hex.EncodeToString(raw[6:8]) + "-" +
		hex.EncodeToString(raw[8:10]) + "-" +
		hex.EncodeToString(raw[10:])
}

func encodeTCPFrame(channel string, frame []byte) []byte {
	channelBytes := []byte(channel)
	payloadLength := uint32(4 + len(channelBytes) + len(frame))
	output := make([]byte, 4+payloadLength)
	binary.BigEndian.PutUint32(output[:4], payloadLength)
	binary.BigEndian.PutUint32(output[4:8], uint32(len(channelBytes)))
	copy(output[8:8+len(channelBytes)], channelBytes)
	copy(output[8+len(channelBytes):], frame)
	return output
}
