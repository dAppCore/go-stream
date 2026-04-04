// SPDX-License-Identifier: EUPL-1.2

package stream

import (
	"context"
	"iter"
	"sync"
)

// Hub is the central channel-based broker. Transport adapters register peers into
// the hub; the hub serialises all state mutations through Go channels.
//
//	hub := stream.NewHub()
//	go hub.Run(ctx)
//
//	wsAdapter := ws.New(ws.Config{Authenticator: auth})
//	wsAdapter.Mount(hub)
//	http.Handle("/stream/ws", wsAdapter.Handler())
type Hub struct {
	peers      map[*Peer]bool
	broadcast  chan []byte
	register   chan *Peer
	unregister chan *Peer
	channels   map[string]map[*Peer]bool
	handlers   map[string][]func([]byte)
	config     HubConfig
	done       chan struct{}
	doneOnce   sync.Once
	running    bool
	mu         sync.RWMutex
}

// NewHub creates a hub with default configuration.
//
//	hub := stream.NewHub()
//	go hub.Run(ctx)
func NewHub() *Hub {
	return nil
}

// NewHubWithConfig creates a hub with the given configuration.
//
//	hub := stream.NewHubWithConfig(stream.HubConfig{
//	    HeartbeatInterval: 30 * time.Second,
//	    OnConnect: func(p *stream.Peer) { log.Println("connected", p.ID) },
//	})
func NewHubWithConfig(config HubConfig) *Hub {
	return nil
}

// Run starts the hub's select loop. Call in a goroutine. Exits when ctx is cancelled.
//
//	go hub.Run(ctx)
func (h *Hub) Run(ctx context.Context) {
}

// SendToChannel delivers frame to all peers subscribed to channel.
// Returns nil if channel has no subscribers (not an error).
//
//	hub.SendToChannel("process:abc123", frame)
func (h *Hub) SendToChannel(channel string, frame []byte) error {
	return nil
}

// Subscribe registers a handler function invoked for every frame arriving on channel.
// Returns an unsubscribe function. Multiple handlers per channel are allowed.
// Handlers run in the hub's goroutine — keep them non-blocking.
//
//	unsub := hub.Subscribe("block", func(f []byte) { ... })
//	defer unsub()
func (h *Hub) Subscribe(channel string, handler func([]byte)) func() {
	return nil
}

// SubscribePeer adds peer to a named channel. Used by transport adapters when
// a peer requests channel subscription (WebSocket TypeSubscribe message, etc.).
//
//	hub.SubscribePeer(peer, "hashrate")
func (h *Hub) SubscribePeer(peer *Peer, channel string) error {
	return nil
}

// UnsubscribePeer removes peer from a named channel.
//
//	hub.UnsubscribePeer(peer, "hashrate")
func (h *Hub) UnsubscribePeer(peer *Peer, channel string) {
}

// Publish sends frame to all subscribers of channel. Satisfies Stream interface.
//
//	hub.Publish("hashrate", frame)
func (h *Hub) Publish(channel string, frame []byte) error {
	return nil
}

// Broadcast sends frame to every connected peer regardless of subscriptions.
// Satisfies Stream interface.
//
//	hub.Broadcast([]byte(`{"type":"shutdown"}`))
func (h *Hub) Broadcast(frame []byte) error {
	return nil
}

// Pipe connects this hub to dst: every frame published here is forwarded to dst.
// Returns a stop function. Satisfies Stream interface.
//
//	stop := hub.Pipe(remoteHub)
//	defer stop()
func (h *Hub) Pipe(dst Stream) func() {
	return nil
}

// Stats returns a snapshot of current hub state.
//
//	s := hub.Stats()
//	core.Print("stream", "peers=%d channels=%d", s.Peers, s.Channels)
func (h *Hub) Stats() HubStats {
	return HubStats{}
}

// PeerCount returns the number of connected peers.
//
//	n := hub.PeerCount()
func (h *Hub) PeerCount() int {
	return 0
}

// ChannelCount returns the number of active channels.
//
//	n := hub.ChannelCount()
func (h *Hub) ChannelCount() int {
	return 0
}

// ChannelSubscriberCount returns the subscriber count for a channel.
// Returns 0 if the channel has no subscribers.
//
//	n := hub.ChannelSubscriberCount("hashrate")
func (h *Hub) ChannelSubscriberCount(channel string) int {
	return 0
}

// AllPeers returns an iterator for all connected peers.
//
//	for peer := range hub.AllPeers() { log.Println(peer.UserID) }
func (h *Hub) AllPeers() iter.Seq[*Peer] {
	return nil
}

// AllChannels returns an iterator for all active channels.
//
//	for ch := range hub.AllChannels() { log.Println(ch) }
func (h *Hub) AllChannels() iter.Seq[string] {
	return nil
}
