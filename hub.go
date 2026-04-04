// SPDX-License-Identifier: EUPL-1.2

package stream

import (
	"context"
	"iter"
	"sync"

	"dappco.re/go/core"
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
	handlers   map[string]map[uint64]func([]byte)
	publishers map[uint64]func(string, []byte)
	nextID     uint64
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
	return NewHubWithConfig(DefaultHubConfig())
}

// NewHubWithConfig creates a hub with the given configuration.
//
//	hub := stream.NewHubWithConfig(stream.HubConfig{
//	    HeartbeatInterval: 30 * time.Second,
//	    OnConnect: func(p *stream.Peer) { log.Println("connected", p.ID) },
//	})
func NewHubWithConfig(config HubConfig) *Hub {
	config = normalizeHubConfig(config)
	return &Hub{
		peers:      map[*Peer]bool{},
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Peer, 256),
		unregister: make(chan *Peer, 256),
		channels:   map[string]map[*Peer]bool{},
		handlers:   map[string]map[uint64]func([]byte){},
		publishers: map[uint64]func(string, []byte){},
		config:     config,
		done:       make(chan struct{}),
	}
}

// Config returns a normalised copy of the hub configuration.
//
//	cfg := hub.Config()
func (h *Hub) Config() HubConfig {
	if h == nil {
		return DefaultHubConfig()
	}
	h.mu.RLock()
	config := h.config
	h.mu.RUnlock()
	return normalizeHubConfig(config)
}

// Run starts the hub's select loop. Call in a goroutine. Exits when ctx is cancelled.
//
//	go hub.Run(ctx)
func (h *Hub) Run(ctx context.Context) {
	if h == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	h.mu.Lock()
	if h.running {
		h.mu.Unlock()
		<-ctx.Done()
		return
	}
	h.running = true
	h.mu.Unlock()

	<-ctx.Done()

	h.mu.Lock()
	peers := make([]*Peer, 0, len(h.peers))
	for peer := range h.peers {
		peers = append(peers, peer)
	}
	h.running = false
	h.mu.Unlock()

	for _, peer := range peers {
		h.RemovePeer(peer)
	}

	h.doneOnce.Do(func() {
		close(h.done)
	})
}

// SendToChannel delivers frame to all peers subscribed to channel.
// Returns nil if channel has no subscribers (not an error).
//
//	hub.SendToChannel("process:abc123", frame)
func (h *Hub) SendToChannel(channel string, frame []byte) error {
	if h == nil {
		return core.E("stream.hub", "nil hub", nil)
	}
	h.mu.RLock()
	running := h.running
	peers := h.channels[channel]
	wildcardPeers := h.channels["*"]
	if channel == "*" {
		wildcardPeers = nil
	}
	handlers := cloneHandlers(h.handlers[channel])
	wildcardHandlers := cloneHandlers(h.handlers["*"])
	publishers := clonePublishHandlers(h.publishers)
	h.mu.RUnlock()
	if !running {
		return ErrHubNotRunning
	}
	if len(peers) == 0 && len(handlers) == 0 && len(wildcardHandlers) == 0 && len(publishers) == 0 {
		return nil
	}
	for peer := range peers {
		h.sendToPeer(peer, channel, frame)
	}
	for peer := range wildcardPeers {
		h.sendToPeer(peer, channel, frame)
	}
	h.invokeHandlers(handlers, frame)
	h.invokeHandlers(wildcardHandlers, frame)
	h.invokePublishHandlers(publishers, channel, frame)
	return nil
}

// Subscribe registers a handler function invoked for every frame arriving on channel.
// Returns an unsubscribe function. Multiple handlers per channel are allowed.
// Handlers run in the hub's goroutine — keep them non-blocking.
//
//	unsub := hub.Subscribe("block", func(f []byte) { ... })
//	defer unsub()
func (h *Hub) Subscribe(channel string, handler func([]byte)) func() {
	if h == nil || channel == "" || handler == nil {
		return func() {}
	}
	h.mu.Lock()
	if h.handlers == nil {
		h.handlers = map[string]map[uint64]func([]byte){}
	}
	if h.channels == nil {
		h.channels = map[string]map[*Peer]bool{}
	}
	h.nextID++
	id := h.nextID
	if h.handlers[channel] == nil {
		h.handlers[channel] = map[uint64]func([]byte){}
	}
	h.handlers[channel][id] = handler
	h.mu.Unlock()

	return func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if handlers := h.handlers[channel]; handlers != nil {
			delete(handlers, id)
			if len(handlers) == 0 {
				delete(h.handlers, channel)
			}
		}
	}
}

// SubscribePeer adds peer to a named channel. Used by transport adapters when
// a peer requests channel subscription (WebSocket TypeSubscribe message, etc.).
//
//	hub.SubscribePeer(peer, "hashrate")
func (h *Hub) SubscribePeer(peer *Peer, channel string) error {
	if h == nil {
		return core.E("stream.hub", "nil hub", nil)
	}
	if peer == nil {
		return core.E("stream.hub", "nil peer", nil)
	}
	if channel == "" {
		return ErrEmptyChannel
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.config.ChannelAuthoriser != nil && channel != "*" && !h.config.ChannelAuthoriser(peer, channel) {
		return ErrAuthRejected
	}
	if peer.send == nil {
		peer.send = make(chan []byte, 256)
	}
	if peer.subscriptions == nil {
		peer.subscriptions = map[string]bool{}
	}
	peer.subscriptions[channel] = true
	if h.channels[channel] == nil {
		h.channels[channel] = map[*Peer]bool{}
	}
	h.channels[channel][peer] = true
	return nil
}

// UnsubscribePeer removes peer from a named channel.
//
//	hub.UnsubscribePeer(peer, "hashrate")
func (h *Hub) UnsubscribePeer(peer *Peer, channel string) {
	if h == nil || peer == nil || channel == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(peer.subscriptions, channel)
	if peers := h.channels[channel]; peers != nil {
		delete(peers, peer)
		if len(peers) == 0 {
			delete(h.channels, channel)
		}
	}
}

// Publish sends frame to all subscribers of channel. Satisfies Stream interface.
//
//	hub.Publish("hashrate", frame)
func (h *Hub) Publish(channel string, frame []byte) error {
	return h.SendToChannel(channel, frame)
}

// Broadcast sends frame to every connected peer regardless of subscriptions.
// Satisfies Stream interface.
//
//	hub.Broadcast([]byte(`{"type":"shutdown"}`))
func (h *Hub) Broadcast(frame []byte) error {
	if h == nil {
		return core.E("stream.hub", "nil hub", nil)
	}
	h.mu.RLock()
	running := h.running
	peers := make([]*Peer, 0, len(h.peers))
	for peer := range h.peers {
		peers = append(peers, peer)
	}
	handlers := cloneHandlers(h.handlers["*"])
	h.mu.RUnlock()
	if !running {
		return ErrHubNotRunning
	}
	for _, peer := range peers {
		h.sendBroadcastToPeer(peer, frame)
	}
	h.invokeHandlers(handlers, frame)
	return nil
}

// Pipe connects this hub to dst: every frame published here is forwarded to dst.
// Returns a stop function. Satisfies Stream interface.
//
//	stop := hub.Pipe(remoteHub)
//	defer stop()
func (h *Hub) Pipe(dst Stream) func() {
	return Pipe(h, dst)
}

// Stats returns a snapshot of current hub state.
//
//	s := hub.Stats()
//	core.Print("stream", "peers=%d channels=%d", s.Peers, s.Channels)
func (h *Hub) Stats() HubStats {
	if h == nil {
		return HubStats{}
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	subscriberCount := map[string]int{}
	for channel, peers := range h.channels {
		if channel == "*" {
			continue
		}
		subscriberCount[channel] = len(peers)
	}
	return HubStats{
		Peers:           len(h.peers),
		Channels:        len(subscriberCount),
		SubscriberCount: subscriberCount,
	}
}

// PeerCount returns the number of connected peers.
//
//	n := hub.PeerCount()
func (h *Hub) PeerCount() int {
	if h == nil {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.peers)
}

// ChannelCount returns the number of active channels.
//
//	n := hub.ChannelCount()
func (h *Hub) ChannelCount() int {
	if h == nil {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	count := 0
	for channel, peers := range h.channels {
		if channel == "*" || len(peers) == 0 {
			continue
		}
		count++
	}
	return count
}

// ChannelSubscriberCount returns the subscriber count for a channel.
// Returns 0 if the channel has no subscribers.
//
//	n := hub.ChannelSubscriberCount("hashrate")
func (h *Hub) ChannelSubscriberCount(channel string) int {
	if h == nil {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.channels[channel])
}

// AllPeers returns an iterator for all connected peers.
//
//	for peer := range hub.AllPeers() { log.Println(peer.UserID) }
func (h *Hub) AllPeers() iter.Seq[*Peer] {
	if h == nil {
		return func(yield func(*Peer) bool) {}
	}
	h.mu.RLock()
	peers := make([]*Peer, 0, len(h.peers))
	for peer := range h.peers {
		peers = append(peers, peer)
	}
	h.mu.RUnlock()
	return func(yield func(*Peer) bool) {
		for _, peer := range peers {
			if !yield(peer) {
				return
			}
		}
	}
}

// AllChannels returns an iterator for all active channels.
//
//	for ch := range hub.AllChannels() { log.Println(ch) }
func (h *Hub) AllChannels() iter.Seq[string] {
	if h == nil {
		return func(yield func(string) bool) {}
	}
	h.mu.RLock()
	channels := make([]string, 0, len(h.channels))
	for channel, peers := range h.channels {
		if channel == "*" || len(peers) == 0 {
			continue
		}
		channels = append(channels, channel)
	}
	h.mu.RUnlock()
	return func(yield func(string) bool) {
		for _, channel := range channels {
			if !yield(channel) {
				return
			}
		}
	}
}

// AddPeer registers a peer with the hub and invokes OnConnect.
//
//	hub.AddPeer(stream.NewPeer("ws"))
func (h *Hub) AddPeer(peer *Peer) error {
	if h == nil {
		return core.E("stream.hub", "nil hub", nil)
	}
	if peer == nil {
		return core.E("stream.hub", "nil peer", nil)
	}
	if peer.send == nil {
		peer.send = make(chan []byte, 256)
	}
	if peer.subscriptions == nil {
		peer.subscriptions = map[string]bool{}
	}
	h.mu.Lock()
	if h.peers == nil {
		h.peers = map[*Peer]bool{}
	}
	if h.peers[peer] {
		h.mu.Unlock()
		return nil
	}
	h.peers[peer] = true
	onConnect := h.config.OnConnect
	h.mu.Unlock()
	if onConnect != nil {
		onConnect(peer)
	}
	return nil
}

// RemovePeer unregisters a peer from the hub and invokes OnDisconnect.
//
//	hub.RemovePeer(peer)
func (h *Hub) RemovePeer(peer *Peer) {
	if h == nil || peer == nil {
		return
	}
	h.mu.Lock()
	if !h.peers[peer] {
		h.mu.Unlock()
		return
	}
	delete(h.peers, peer)
	for channel, peers := range h.channels {
		delete(peers, peer)
		if len(peers) == 0 {
			delete(h.channels, channel)
		}
	}
	peer.mu.Lock()
	peer.subscriptions = map[string]bool{}
	peer.mu.Unlock()
	onDisconnect := h.config.OnDisconnect
	h.mu.Unlock()
	peer.Close()
	if onDisconnect != nil {
		onDisconnect(peer)
	}
}

func (h *Hub) sendToPeer(peer *Peer, channel string, frame []byte) {
	if peer == nil {
		return
	}
	if peer.Transport == "tcp" {
		_ = peer.Send(encodeTCPFrame(channel, frame))
		return
	}
	_ = peer.Send(frame)
}

func (h *Hub) sendBroadcastToPeer(peer *Peer, frame []byte) {
	if peer == nil {
		return
	}
	if peer.Transport == "tcp" {
		_ = peer.Send(encodeTCPFrame("", frame))
		return
	}
	_ = peer.Send(frame)
}

func (h *Hub) invokeHandlers(handlers []func([]byte), frame []byte) {
	for _, handler := range handlers {
		func(fn func([]byte)) {
			defer func() {
				_ = recover()
			}()
			fn(frame)
		}(handler)
	}
}

func (h *Hub) subscribePublished(handler func(string, []byte)) func() {
	if h == nil || handler == nil {
		return func() {}
	}
	h.mu.Lock()
	if h.publishers == nil {
		h.publishers = map[uint64]func(string, []byte){}
	}
	h.nextID++
	id := h.nextID
	h.publishers[id] = handler
	h.mu.Unlock()

	return func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.publishers, id)
	}
}

func (h *Hub) invokePublishHandlers(handlers []func(string, []byte), channel string, frame []byte) {
	for _, handler := range handlers {
		func(fn func(string, []byte)) {
			defer func() {
				_ = recover()
			}()
			fn(channel, frame)
		}(handler)
	}
}

func cloneHandlers(handlers map[uint64]func([]byte)) []func([]byte) {
	if len(handlers) == 0 {
		return nil
	}
	cloned := make([]func([]byte), 0, len(handlers))
	for _, handler := range handlers {
		cloned = append(cloned, handler)
	}
	return cloned
}

func clonePublishHandlers(handlers map[uint64]func(string, []byte)) []func(string, []byte) {
	if len(handlers) == 0 {
		return nil
	}
	cloned := make([]func(string, []byte), 0, len(handlers))
	for _, handler := range handlers {
		cloned = append(cloned, handler)
	}
	return cloned
}
