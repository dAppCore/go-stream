// SPDX-License-Identifier: EUPL-1.2

// Package stream is the transport-agnostic event and data pipe for the CoreGO
// ecosystem.
//
//	hub := stream.NewHub()
//	go hub.Run(ctx)
//	hub.Publish("hashrate", []byte(`{"h":123456}`))
//	unsub := hub.Subscribe("block", func(frame []byte) { handleBlock(frame) })
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
//
//	hub := stream.NewHub()
//	var streamBus stream.Stream = hub
//	streamBus.Publish("hashrate", []byte(`{"h":123456}`))
//	stop := streamBus.Pipe(remoteHub)
//	defer stop()
type Stream interface {
	// Publish sends frame to all subscribers of channel.
	//
	//	hub.Publish("hashrate", []byte(`{"h":123456}`))
	//
	Publish(channel string, frame []byte) error

	// Subscribe registers handler for all frames arriving on channel.
	//
	//	unsubscribe := hub.Subscribe("block", func(frame []byte) { handleBlock(frame) })
	//	defer unsubscribe()
	//
	Subscribe(channel string, handler func([]byte)) func()

	// Broadcast sends frame to every connected peer regardless of subscriptions.
	//
	//	hub.Broadcast([]byte(`{"type":"shutdown"}`))
	//
	Broadcast(frame []byte) error

	// Pipe forwards every published frame to destination.
	//
	//	stop := localHub.Pipe(remoteHub)
	//	defer stop()
	//
	Pipe(destination Stream) func()

	// Stats returns a snapshot of current hub state.
	//
	//	stats := hub.Stats()
	Stats() HubStats
}

// Frame is a raw byte payload delivered through the hub.
// Adapters and consumers define their own serialisation over Frame.
type Frame = []byte

// Channel is a named topic string used for pub/sub routing.
type Channel = string

// Peer represents one connected endpoint.
//
//	peer := stream.NewPeer("ws")
//	peer.UserID = authResult.UserID
//	peer.Claims = authResult.Claims
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
	closeHook     func()
	mu            sync.RWMutex
	closeOnce     sync.Once
}

// NewPeer creates a peer with a generated UUID and a buffered send queue.
//
//	peer := stream.NewPeer("ws")
func NewPeer(transport string) *Peer {
	return &Peer{
		ID:            randomUUID(),
		Transport:     transport,
		send:          make(chan []byte, 256),
		subscriptions: map[string]bool{},
	}
}

// Subscriptions returns a copy of this peer's current channel subscriptions.
//
//	channels := peer.Subscriptions() // ["hashrate", "block"]
func (peer *Peer) Subscriptions() []string {
	if peer == nil {
		return nil
	}
	peer.mu.RLock()
	defer peer.mu.RUnlock()
	channels := make([]string, 0, len(peer.subscriptions))
	for channel := range peer.subscriptions {
		channels = append(channels, channel)
	}
	sort.Strings(channels)
	return channels
}

// Send enqueues frame for delivery. Non-blocking: drops and returns false if buffer full.
//
//	ok := peer.Send(frame)
func (peer *Peer) Send(frame []byte) bool {
	if peer == nil {
		return false
	}
	defer func() {
		_ = recover()
	}()
	peer.mu.RLock()
	defer peer.mu.RUnlock()
	if peer.send == nil {
		return false
	}
	payload := append([]byte(nil), frame...)
	select {
	case peer.send <- payload:
		return true
	default:
		return false
	}
}

// Close signals the transport adapter to shut down this connection.
//
//	peer.SetCloseHook(func() { _ = conn.Close() })
//	peer.Close()
func (peer *Peer) Close() {
	if peer == nil {
		return
	}
	peer.closeOnce.Do(func() {
		peer.mu.Lock()
		send := peer.send
		closeHook := peer.closeHook
		peer.closeHook = nil
		peer.mu.Unlock()
		if send != nil {
			close(send)
		}
		if closeHook != nil {
			closeHook()
		}
	})
}

// SetCloseHook installs the transport shutdown hook invoked by Close.
//
//	peer.SetCloseHook(func() { _ = conn.Close() })
func (peer *Peer) SetCloseHook(closeHook func()) {
	if peer == nil {
		return
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	peer.closeHook = closeHook
}

// SendQueue returns the peer's outgoing frame queue.
//
//	for frame := range peer.SendQueue() { handle(frame) }
func (peer *Peer) SendQueue() <-chan []byte {
	if peer == nil {
		return nil
	}
	peer.mu.RLock()
	defer peer.mu.RUnlock()
	return peer.send
}

// ConnectionState describes a reconnecting client's lifecycle.
//
//	switch client.State() {
//	case stream.StateConnected:
//	    // send frames
//	case stream.StateConnecting:
//	    // wait for dial
//	default:
//	    // disconnected
//	}
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

// Pipe connects source to destination.
//
//	stop := stream.Pipe(zmqHub, wsHub)
//	defer stop()
//
// Published frames keep their channel. Broadcast frames stay broadcasts when the
// source exposes that hook.
func Pipe(source Stream, destination Stream) func() {
	if source == nil || destination == nil || source == destination {
		return func() {}
	}
	type publishedFrameSource interface {
		SubscribePublished(handler func(string, []byte)) func()
	}
	type broadcastFrameSource interface {
		SubscribeBroadcast(handler func([]byte)) func()
	}
	stops := make([]func(), 0, 2)
	if publisher, ok := source.(publishedFrameSource); ok {
		stops = append(stops, onceFunc(publisher.SubscribePublished(func(channel string, frame []byte) {
			_ = destination.Publish(channel, cloneFrame(frame))
		})))
	}
	if broadcaster, ok := source.(broadcastFrameSource); ok {
		stops = append(stops, onceFunc(broadcaster.SubscribeBroadcast(func(frame []byte) {
			_ = destination.Broadcast(cloneFrame(frame))
		})))
	}
	if len(stops) == 0 {
		// Generic Stream implementations do not expose channel names, so fall back
		// to publishing on the wildcard channel.
		stop := source.Subscribe("*", func(frame []byte) {
			_ = destination.Publish("*", cloneFrame(frame))
		})
		return onceFunc(stop)
	}
	return onceFunc(func() {
		for index := len(stops) - 1; index >= 0; index-- {
			stops[index]()
		}
	})
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

func randomUUID() string {
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
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

func cloneFrame(frame []byte) []byte {
	if len(frame) == 0 {
		return nil
	}
	return append([]byte(nil), frame...)
}

func onceFunc(fn func()) func() {
	if fn == nil {
		return func() {}
	}
	var once sync.Once
	return func() {
		once.Do(fn)
	}
}
