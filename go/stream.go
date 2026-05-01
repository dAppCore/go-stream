// SPDX-License-Identifier: EUPL-1.2

// Package stream wires transport-agnostic hubs and peers together.
//
//	hub := stream.NewHub()
//	go hub.Run(ctx)
//	stop := stream.Pipe(hub, remoteHub)
//	defer stop()
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

	"dappco.re/go"
)

const defaultPeerSendBufferSize = 256

// Stream is the transport-agnostic event and data pipe.
//
//	hub := stream.NewHub()
//	var bus stream.Stream = hub
//	_ = bus.Publish("hashrate", []byte(`{"h":123456}`))
//	stop := bus.Pipe(remoteHub)
//	defer stop()
type Stream interface {
	// _ = hub.Publish("hashrate", []byte(`{"h":123456}`))
	Publish(channel string, frame []byte) core.Result

	// unsubscribe := hub.Subscribe("block", func(frame []byte) { handleBlock(frame) })
	// defer unsubscribe()
	Subscribe(channel string, handler func([]byte)) func()

	// _ = hub.Broadcast([]byte(`{"type":"shutdown"}`))
	Broadcast(frame []byte) core.Result

	// stop := localHub.Pipe(remoteHub)
	// defer stop()
	Pipe(destination Stream) func()

	// stats := hub.Stats()
	Stats() HubStats
}

// frame := stream.Frame([]byte(`{"type":"event"}`))
type Frame = []byte

// channel := stream.Channel("hashrate")
type Channel = string

// peer := stream.NewPeer("ws")
// peer.UserID = authResult.UserID
// peer.Claims = authResult.Claims
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
	mutex         sync.RWMutex
	closeOnce     sync.Once
}

// peer := stream.NewPeer("ws")
// peer.UserID = "user-42"
func NewPeer(transport string) *Peer {
	return &Peer{
		ID:            randomUUID(),
		Claims:        map[string]any{},
		Transport:     transport,
		send:          make(chan []byte, defaultPeerSendBufferSize),
		subscriptions: map[string]bool{},
	}
}

// channels := peer.Subscriptions() // ["hashrate", "block"]
func (peer *Peer) Subscriptions() []string {
	if peer == nil {
		return nil
	}
	peer.mutex.RLock()
	defer peer.mutex.RUnlock()
	channels := make([]string, 0, len(peer.subscriptions))
	for channel := range peer.subscriptions {
		channels = append(channels, channel)
	}
	sort.Strings(channels)
	return channels
}

// ok := peer.Send([]byte("template"))
func (peer *Peer) Send(frame []byte) bool {
	if peer == nil {
		return false
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			return
		}
	}()
	peer.mutex.RLock()
	defer peer.mutex.RUnlock()
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

// peer := stream.NewPeer("ws")
// peer.SetCloseHook(func() { _ = conn.Close() })
// peer.Close()
func (peer *Peer) Close() {
	if peer == nil {
		return
	}
	peer.closeOnce.Do(func() {
		peer.mutex.Lock()
		send := peer.send
		closeHook := peer.closeHook
		peer.closeHook = nil
		peer.mutex.Unlock()
		if send != nil {
			close(send)
		}
		if closeHook != nil {
			closeHook()
		}
	})
}

// peer.SetCloseHook(func() { _ = conn.Close() })
func (peer *Peer) SetCloseHook(closeFunc func()) {
	if peer == nil {
		return
	}
	peer.mutex.Lock()
	defer peer.mutex.Unlock()
	peer.closeHook = closeFunc
}

// SendQueue exposes the adapter-facing outbound queue.
//
//	go func() {
//		for frame := range peer.SendQueue() {
//			_ = frame
//		}
//	}()
func (peer *Peer) SendQueue() <-chan []byte {
	if peer == nil {
		return nil
	}
	peer.mutex.RLock()
	defer peer.mutex.RUnlock()
	return peer.send
}

// switch client.State() {
// case stream.StateConnected:
//
//	_ = client.Send(stream.Message{Type: stream.TypePing})
//
// case stream.StateConnecting:
//
//	time.Sleep(100 * time.Millisecond)
//
// default:
//
//		// disconnected
//	}
type ConnectionState int

const (
	StateDisconnected ConnectionState = iota
	StateConnecting
	StateConnected
)

// state := stream.StateConnected
// core.Print(nil, "connection state=%s", state.String())
func (state ConnectionState) String() string {
	switch state {
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	default:
		return "disconnected"
	}
}

//	envelope := stream.Envelope{
//	    SourceID: "node-a",
//	    Channel:  "block",
//	    Frame:    []byte("template"),
//	}
type Envelope struct {
	SourceID string
	Channel  string
	Frame    []byte
}

// Pipe connects src to dst.
//
//	stop := stream.Pipe(zmqHub, wsHub)
//	defer stop()
//
// Published frames keep their channel. Broadcast frames stay broadcasts when the
// source exposes that hook.
func Pipe(src Stream, dst Stream) func() {
	if src == nil || dst == nil || src == dst {
		return func() {}
	}
	type publishedFrameSource interface {
		SubscribePublished(handler func(string, []byte)) func()
	}
	type broadcastFrameSource interface {
		SubscribeBroadcast(handler func([]byte)) func()
	}
	stops := make([]func(), 0, 2)
	if publisher, ok := src.(publishedFrameSource); ok {
		stops = append(stops, onceFunction(publisher.SubscribePublished(func(channel string, frame []byte) {
			if r := dst.Publish(channel, cloneFrame(frame)); !r.OK {
				return
			}
		})))
	}
	if broadcaster, ok := src.(broadcastFrameSource); ok {
		stops = append(stops, onceFunction(broadcaster.SubscribeBroadcast(func(frame []byte) {
			if r := dst.Broadcast(cloneFrame(frame)); !r.OK {
				return
			}
		})))
	}
	if len(stops) == 0 {
		// Generic Stream implementations do not expose channel names, so fall back
		// to publishing on the wildcard channel.
		stop := src.Subscribe("*", func(frame []byte) {
			if r := dst.Publish("*", cloneFrame(frame)); !r.OK {
				return
			}
		})
		return onceFunction(stop)
	}
	return onceFunction(func() {
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

// id := randomUUID() // "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5"
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

// wire := encodeTCPFrame("block", []byte("template"))
// _ = conn.Write(wire)
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

// copy := cloneFrame(original)
func cloneFrame(frame []byte) []byte {
	if len(frame) == 0 {
		return nil
	}
	return append([]byte(nil), frame...)
}

// stop := onceFunction(func() { unsubscribe() })
// stop() // executes once
// stop() // no-op
func onceFunction(handler func()) func() {
	if handler == nil {
		return func() {}
	}
	var once sync.Once
	return func() {
		once.Do(handler)
	}
}
