// SPDX-License-Identifier: EUPL-1.2

package stream

import (
	"context"
	"iter"
	"sort"
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
	peers             map[*Peer]bool
	broadcastQueue    chan broadcastDelivery
	deliver           chan delivery
	register          chan *Peer
	unregister        chan *Peer
	channels          map[string]map[*Peer]bool
	channelHandlers   map[string]map[uint64]func([]byte)
	broadcastHandlers map[uint64]func([]byte)
	publishHandlers   map[uint64]func(string, []byte)
	nextID            uint64
	config            HubConfig
	done              chan struct{}
	doneOnce          sync.Once
	running           bool
	mu                sync.RWMutex
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
		peers:             map[*Peer]bool{},
		broadcastQueue:    make(chan broadcastDelivery, 256),
		deliver:           make(chan delivery, 256),
		register:          make(chan *Peer, 256),
		unregister:        make(chan *Peer, 256),
		channels:          map[string]map[*Peer]bool{},
		channelHandlers:   map[string]map[uint64]func([]byte){},
		broadcastHandlers: map[uint64]func([]byte){},
		publishHandlers:   map[uint64]func(string, []byte){},
		config:            config,
		done:              make(chan struct{}),
	}
}

// Config returns a normalised copy of the hub configuration.
//
//	cfg := hub.Config()
//	writeTimeout := cfg.WriteTimeout
func (hub *Hub) Config() HubConfig {
	if hub == nil {
		return DefaultHubConfig()
	}
	hub.mu.RLock()
	config := hub.config
	hub.mu.RUnlock()
	return normalizeHubConfig(config)
}

// Run starts the hub's select loop. Call in a goroutine. Exits when ctx is cancelled.
//
//	go hub.Run(ctx)
func (hub *Hub) Run(ctx context.Context) {
	if hub == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	hub.mu.Lock()
	if hub.running {
		hub.mu.Unlock()
		return
	}
	hub.running = true
	hub.mu.Unlock()

	defer func() {
		hub.mu.Lock()
		peers := make([]*Peer, 0, len(hub.peers))
		for peer := range hub.peers {
			peers = append(peers, peer)
		}
		hub.running = false
		hub.mu.Unlock()

		for _, peer := range peers {
			hub.removePeer(peer)
		}

		hub.doneOnce.Do(func() {
			close(hub.done)
		})
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case peer := <-hub.register:
			hub.addPeer(peer)
		case peer := <-hub.unregister:
			hub.removePeer(peer)
		case item := <-hub.broadcastQueue:
			hub.broadcastToPeers(item.source, item.frame, item.notifyBroadcastSubscribers)
		case item := <-hub.deliver:
			hub.processDelivery(item.channel, item.frame, item.notifyPublishSubscribers)
		}
	}
}

// SendToChannel delivers frame to all peers subscribed to channel.
// Returns nil if channel has no subscribers (not an error).
//
//	hub.SendToChannel("process:abc123", frame)
func (hub *Hub) SendToChannel(channel string, frame []byte) error {
	return hub.sendToChannel(channel, frame, true)
}

// PublishFromPeer delivers a channel frame while excluding the source peer from fan-out.
//
//	_ = hub.PublishFromPeer(peer, "block", frame)
func (hub *Hub) PublishFromPeer(source *Peer, channel string, frame []byte) error {
	return hub.sendToChannelFromPeer(source, channel, frame, true)
}

// PublishFromBridge delivers frame to subscribers without notifying publish hooks.
//
//	_ = hub.PublishFromBridge("block", frame)
func (hub *Hub) PublishFromBridge(channel string, frame []byte) error {
	return hub.sendToChannel(channel, frame, false)
}

func (hub *Hub) sendToChannel(channel string, frame []byte, notifyPublishSubscribers bool) error {
	return hub.sendToChannelFromPeer(nil, channel, frame, notifyPublishSubscribers)
}

func (hub *Hub) sendToChannelFromPeer(source *Peer, channel string, frame []byte, notifyPublishSubscribers bool) error {
	if hub == nil {
		return core.E("stream.hub", "nil hub", nil)
	}
	hub.mu.RLock()
	running := hub.running
	peersToSend := hub.collectChannelPeersLocked(channel, source)
	hasHandlers := len(hub.channelHandlers[channel]) > 0
	hasWildcardHandlers := len(hub.channelHandlers["*"]) > 0 && channel != "*"
	hasPublishers := notifyPublishSubscribers && len(hub.publishHandlers) > 0
	hub.mu.RUnlock()
	if !running {
		return ErrHubNotRunning
	}
	if len(peersToSend) == 0 && !hasHandlers && !hasWildcardHandlers && !hasPublishers {
		return nil
	}
	for _, peer := range peersToSend {
		hub.sendToPeer(peer, channel, frame)
	}
	hub.enqueueDelivery(channel, frame, notifyPublishSubscribers)
	return nil
}

// SubscribeWithError registers a handler function invoked for every frame arriving
// on channel. Returns an unsubscribe function and an error for invalid input.
// Multiple handlers per channel are allowed. Handlers run in the hub's goroutine —
// keep them non-blocking.
//
//	unsub, err := hub.SubscribeWithError("block", func(frame []byte) { ... })
//	if err != nil { return err }
//	defer unsub()
func (hub *Hub) SubscribeWithError(channel string, handler func([]byte)) (func(), error) {
	if hub == nil {
		return func() {}, core.E("stream.hub", "nil hub", nil)
	}
	if channel == "" {
		return func() {}, ErrEmptyChannel
	}
	if handler == nil {
		return func() {}, core.E("stream.hub", "nil handler", nil)
	}
	hub.mu.Lock()
	if hub.channelHandlers == nil {
		hub.channelHandlers = map[string]map[uint64]func([]byte){}
	}
	if hub.channels == nil {
		hub.channels = map[string]map[*Peer]bool{}
	}
	hub.nextID++
	id := hub.nextID
	if hub.channelHandlers[channel] == nil {
		hub.channelHandlers[channel] = map[uint64]func([]byte){}
	}
	hub.channelHandlers[channel][id] = handler
	hub.mu.Unlock()

	return onceFunc(func() {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		if handlers := hub.channelHandlers[channel]; handlers != nil {
			delete(handlers, id)
			if len(handlers) == 0 {
				delete(hub.channelHandlers, channel)
			}
		}
	}), nil
}

// SubscribeE is a compatibility alias for SubscribeWithError.
//
//	unsub, err := hub.SubscribeE("block", func(frame []byte) { ... })
func (hub *Hub) SubscribeE(channel string, handler func([]byte)) (func(), error) {
	return hub.SubscribeWithError(channel, handler)
}

// Subscribe registers a handler function invoked for every frame arriving on channel.
// Returns an unsubscribe function. Multiple handlers per channel are allowed.
// Handlers run in the hub's goroutine — keep them non-blocking.
//
//	unsub := hub.Subscribe("block", func(f []byte) { ... })
//	defer unsub()
func (hub *Hub) Subscribe(channel string, handler func([]byte)) func() {
	unsub, _ := hub.SubscribeWithError(channel, handler)
	return unsub
}

// SubscribePeer adds peer to a named channel. Used by transport adapters when
// a peer requests channel subscription (WebSocket TypeSubscribe message, etc.).
//
//	hub.SubscribePeer(peer, "hashrate")
func (hub *Hub) SubscribePeer(peer *Peer, channel string) error {
	if hub == nil {
		return core.E("stream.hub", "nil hub", nil)
	}
	if peer == nil {
		return core.E("stream.hub", "nil peer", nil)
	}
	if channel == "" {
		return ErrEmptyChannel
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.config.ChannelAuthoriser != nil && channel != "*" && !hub.config.ChannelAuthoriser(peer, channel) {
		return ErrAuthRejected
	}
	if peer.send == nil {
		peer.send = make(chan []byte, 256)
	}
	if peer.subscriptions == nil {
		peer.subscriptions = map[string]bool{}
	}
	peer.subscriptions[channel] = true
	if hub.channels[channel] == nil {
		hub.channels[channel] = map[*Peer]bool{}
	}
	hub.channels[channel][peer] = true
	return nil
}

// CanSubscribePeer reports whether peer may subscribe to channel.
//
//	err := hub.CanSubscribePeer(peer, "hashrate")
//	if err == stream.ErrAuthRejected { return }
func (hub *Hub) CanSubscribePeer(peer *Peer, channel string) error {
	if hub == nil {
		return core.E("stream.hub", "nil hub", nil)
	}
	if peer == nil {
		return core.E("stream.hub", "nil peer", nil)
	}
	if channel == "" {
		return ErrEmptyChannel
	}
	hub.mu.RLock()
	defer hub.mu.RUnlock()
	if hub.config.ChannelAuthoriser != nil && channel != "*" && !hub.config.ChannelAuthoriser(peer, channel) {
		return ErrAuthRejected
	}
	return nil
}

// UnsubscribePeer removes peer from a named channel.
//
//	hub.UnsubscribePeer(peer, "hashrate")
func (hub *Hub) UnsubscribePeer(peer *Peer, channel string) {
	if hub == nil || peer == nil || channel == "" {
		return
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	delete(peer.subscriptions, channel)
	if peers := hub.channels[channel]; peers != nil {
		delete(peers, peer)
		if len(peers) == 0 {
			delete(hub.channels, channel)
		}
	}
}

// Publish sends frame to all subscribers of channel. Satisfies Stream interface.
//
//	hub.Publish("hashrate", frame)
func (hub *Hub) Publish(channel string, frame []byte) error {
	return hub.sendToChannel(channel, frame, true)
}

// Broadcast sends frame to every connected peer regardless of subscriptions.
// Satisfies Stream interface.
//
//	hub.Broadcast([]byte(`{"type":"shutdown"}`))
func (hub *Hub) Broadcast(frame []byte) error {
	return hub.broadcastFrame(frame, true)
}

// BroadcastFromPeer delivers a broadcast frame while excluding the source peer from fan-out.
//
//	_ = hub.BroadcastFromPeer(peer, []byte("shutdown"))
func (hub *Hub) BroadcastFromPeer(source *Peer, frame []byte) error {
	return hub.broadcastFrameFromPeer(source, frame, true)
}

// BroadcastFromBridge delivers frame to peers without notifying broadcast hooks.
//
//	_ = hub.BroadcastFromBridge([]byte("shutdown"))
func (hub *Hub) BroadcastFromBridge(frame []byte) error {
	return hub.broadcastFrame(frame, false)
}

func (hub *Hub) broadcastFrame(frame []byte, notifyBroadcastSubscribers bool) error {
	return hub.broadcastFrameFromPeer(nil, frame, notifyBroadcastSubscribers)
}

func (hub *Hub) broadcastFrameFromPeer(source *Peer, frame []byte, notifyBroadcastSubscribers bool) error {
	if hub == nil {
		return core.E("stream.hub", "nil hub", nil)
	}
	hub.mu.RLock()
	running := hub.running
	hub.mu.RUnlock()
	if !running {
		return ErrHubNotRunning
	}
	select {
	case hub.broadcastQueue <- broadcastDelivery{
		source:                     source,
		frame:                      append([]byte(nil), frame...),
		notifyBroadcastSubscribers: notifyBroadcastSubscribers,
	}:
		return nil
	default:
		go hub.enqueueBroadcast(broadcastDelivery{
			source:                     source,
			frame:                      append([]byte(nil), frame...),
			notifyBroadcastSubscribers: notifyBroadcastSubscribers,
		})
	}
	return nil
}

// Pipe connects this hub to destination: every frame published here is forwarded to destination.
// Returns a stop function. Satisfies Stream interface.
//
//	stop := hub.Pipe(remoteHub)
//	defer stop()
func (hub *Hub) Pipe(destination Stream) func() {
	return Pipe(hub, destination)
}

// Stats returns a snapshot of current hub state.
//
//	s := hub.Stats()
//	core.Print("stream", "peers=%d channels=%d", s.Peers, s.Channels)
func (hub *Hub) Stats() HubStats {
	if hub == nil {
		return HubStats{}
	}
	hub.mu.RLock()
	defer hub.mu.RUnlock()
	subscriberCount := map[string]int{}
	for channel, peers := range hub.channels {
		if channel == "*" {
			continue
		}
		subscriberCount[channel] = len(peers)
	}
	return HubStats{
		Peers:           len(hub.peers),
		Channels:        len(subscriberCount),
		SubscriberCount: subscriberCount,
	}
}

// SubscribePublished registers a handler invoked for each published channel frame.
//
//	stop := hub.SubscribePublished(func(channel string, frame []byte) {
//	    _ = channel
//	    _ = frame
//	})
func (hub *Hub) SubscribePublished(handler func(string, []byte)) func() {
	return hub.subscribePublished(handler)
}

// SubscribeBroadcast registers a handler invoked for each broadcast frame.
//
//	stop := hub.SubscribeBroadcast(func(frame []byte) {
//	    _ = frame
//	})
func (hub *Hub) SubscribeBroadcast(handler func([]byte)) func() {
	if hub == nil || handler == nil {
		return func() {}
	}
	hub.mu.Lock()
	if hub.broadcastHandlers == nil {
		hub.broadcastHandlers = map[uint64]func([]byte){}
	}
	hub.nextID++
	id := hub.nextID
	hub.broadcastHandlers[id] = handler
	hub.mu.Unlock()

	return onceFunc(func() {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		delete(hub.broadcastHandlers, id)
	})
}

// PeerCount returns the number of connected peers.
//
//	n := hub.PeerCount()
func (hub *Hub) PeerCount() int {
	if hub == nil {
		return 0
	}
	hub.mu.RLock()
	defer hub.mu.RUnlock()
	return len(hub.peers)
}

// ChannelCount returns the number of active channels.
//
//	n := hub.ChannelCount()
func (hub *Hub) ChannelCount() int {
	if hub == nil {
		return 0
	}
	hub.mu.RLock()
	defer hub.mu.RUnlock()
	count := 0
	for channel, peers := range hub.channels {
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
func (hub *Hub) ChannelSubscriberCount(channel string) int {
	if hub == nil {
		return 0
	}
	hub.mu.RLock()
	defer hub.mu.RUnlock()
	return len(hub.channels[channel])
}

// AllPeers returns an iterator for all connected peers.
//
//	for peer := range hub.AllPeers() { log.Println(peer.UserID) }
func (hub *Hub) AllPeers() iter.Seq[*Peer] {
	if hub == nil {
		return func(yield func(*Peer) bool) {}
	}
	hub.mu.RLock()
	peers := make([]*Peer, 0, len(hub.peers))
	for peer := range hub.peers {
		peers = append(peers, peer)
	}
	hub.mu.RUnlock()
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
func (hub *Hub) AllChannels() iter.Seq[string] {
	if hub == nil {
		return func(yield func(string) bool) {}
	}
	hub.mu.RLock()
	channels := make([]string, 0, len(hub.channels))
	for channel, peers := range hub.channels {
		if channel == "*" || len(peers) == 0 {
			continue
		}
		channels = append(channels, channel)
	}
	hub.mu.RUnlock()
	sort.Strings(channels)
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
//	peer := stream.NewPeer("ws")
//	peer.UserID = "user-42"
//	_ = hub.AddPeer(peer)
func (hub *Hub) AddPeer(peer *Peer) error {
	if hub == nil {
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
	hub.mu.RLock()
	running := hub.running
	hub.mu.RUnlock()
	if running {
		select {
		case hub.register <- peer:
			return nil
		default:
		}
	}
	hub.addPeer(peer)
	return nil
}

// RemovePeer unregisters a peer from the hub and invokes OnDisconnect.
//
//	hub.RemovePeer(peer)
func (hub *Hub) RemovePeer(peer *Peer) {
	if hub == nil || peer == nil {
		return
	}
	hub.mu.RLock()
	running := hub.running
	hub.mu.RUnlock()
	if running {
		select {
		case hub.unregister <- peer:
			return
		default:
		}
	}
	hub.removePeer(peer)
}

func (hub *Hub) sendToPeer(peer *Peer, channel string, frame []byte) {
	if peer == nil {
		return
	}
	if peer.Transport == "tcp" {
		_ = peer.Send(encodeTCPFrame(channel, frame))
		return
	}
	_ = peer.Send(frame)
}

func (hub *Hub) sendBroadcastToPeer(peer *Peer, frame []byte) {
	if peer == nil {
		return
	}
	if peer.Transport == "tcp" {
		_ = peer.Send(encodeTCPFrame("", frame))
		return
	}
	_ = peer.Send(frame)
}

func (hub *Hub) invokeHandlers(handlers []func([]byte), frame []byte) {
	for _, handler := range handlers {
		func(fn func([]byte)) {
			defer func() {
				_ = recover()
			}()
			fn(frame)
		}(handler)
	}
}

func (hub *Hub) addPeer(peer *Peer) {
	if hub == nil || peer == nil {
		return
	}
	hub.mu.Lock()
	if hub.peers == nil {
		hub.peers = map[*Peer]bool{}
	}
	if hub.peers[peer] {
		hub.mu.Unlock()
		return
	}
	hub.peers[peer] = true
	onConnect := hub.config.OnConnect
	hub.mu.Unlock()
	if onConnect != nil {
		onConnect(peer)
	}
}

func (hub *Hub) removePeer(peer *Peer) {
	if hub == nil || peer == nil {
		return
	}
	hub.mu.Lock()
	if !hub.peers[peer] {
		hub.mu.Unlock()
		return
	}
	delete(hub.peers, peer)
	for channel, peers := range hub.channels {
		delete(peers, peer)
		if len(peers) == 0 {
			delete(hub.channels, channel)
		}
	}
	peer.mu.Lock()
	peer.subscriptions = map[string]bool{}
	peer.mu.Unlock()
	onDisconnect := hub.config.OnDisconnect
	hub.mu.Unlock()
	peer.Close()
	if onDisconnect != nil {
		onDisconnect(peer)
	}
}

func (hub *Hub) broadcastToPeers(source *Peer, frame []byte, notifyBroadcastSubscribers bool) {
	if hub == nil {
		return
	}
	hub.mu.RLock()
	peers := make([]*Peer, 0, len(hub.peers))
	for peer := range hub.peers {
		if peer == source {
			continue
		}
		peers = append(peers, peer)
	}
	handlers := cloneChannelHandlers(hub.channelHandlers["*"])
	broadcastHandlers := cloneBroadcastHandlers(hub.broadcastHandlers)
	hub.mu.RUnlock()
	for _, peer := range peers {
		hub.sendBroadcastToPeer(peer, frame)
	}
	hub.invokeHandlers(handlers, frame)
	if notifyBroadcastSubscribers {
		hub.invokeBroadcastHandlers(broadcastHandlers, frame)
	}
}

type delivery struct {
	channel                  string
	frame                    []byte
	notifyPublishSubscribers bool
}

type broadcastDelivery struct {
	source                     *Peer
	frame                      []byte
	notifyBroadcastSubscribers bool
}

func (hub *Hub) enqueueDelivery(channel string, frame []byte, notifyPublishSubscribers bool) {
	if hub == nil {
		return
	}
	item := delivery{
		channel:                  channel,
		frame:                    append([]byte(nil), frame...),
		notifyPublishSubscribers: notifyPublishSubscribers,
	}
	select {
	case hub.deliver <- item:
	default:
		go hub.enqueueDeliveryAsync(item)
	}
}

func (hub *Hub) enqueueBroadcast(item broadcastDelivery) {
	if hub == nil {
		return
	}
	select {
	case hub.broadcastQueue <- item:
	case <-hub.done:
	}
}

func (hub *Hub) enqueueDeliveryAsync(item delivery) {
	if hub == nil {
		return
	}
	select {
	case hub.deliver <- item:
	case <-hub.done:
	}
}

func (hub *Hub) processDelivery(channel string, frame []byte, notifyPublishSubscribers bool) {
	if hub == nil {
		return
	}
	hub.mu.RLock()
	handlers := cloneChannelHandlers(hub.channelHandlers[channel])
	wildcardHandlers := cloneChannelHandlers(hub.channelHandlers["*"])
	publishHandlers := clonePublishHandlers(hub.publishHandlers)
	hub.mu.RUnlock()

	hub.invokeHandlers(handlers, frame)
	if channel != "*" {
		hub.invokeHandlers(wildcardHandlers, frame)
	}
	if notifyPublishSubscribers {
		hub.invokePublishHandlers(publishHandlers, channel, frame)
	}
}

func (hub *Hub) subscribePublished(handler func(string, []byte)) func() {
	if hub == nil || handler == nil {
		return func() {}
	}
	hub.mu.Lock()
	if hub.publishHandlers == nil {
		hub.publishHandlers = map[uint64]func(string, []byte){}
	}
	hub.nextID++
	id := hub.nextID
	hub.publishHandlers[id] = handler
	hub.mu.Unlock()

	return onceFunc(func() {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		delete(hub.publishHandlers, id)
	})
}

func (hub *Hub) invokeBroadcastHandlers(handlers []func([]byte), frame []byte) {
	for _, handler := range handlers {
		func(fn func([]byte)) {
			defer func() {
				_ = recover()
			}()
			fn(frame)
		}(handler)
	}
}

func (hub *Hub) invokePublishHandlers(handlers []func(string, []byte), channel string, frame []byte) {
	for _, handler := range handlers {
		func(fn func(string, []byte)) {
			defer func() {
				_ = recover()
			}()
			fn(channel, frame)
		}(handler)
	}
}

func (hub *Hub) collectChannelPeersLocked(channel string, source *Peer) []*Peer {
	combined := map[*Peer]struct{}{}
	for peer := range hub.channels[channel] {
		if peer == source {
			continue
		}
		combined[peer] = struct{}{}
	}
	if channel != "*" {
		for peer := range hub.channels["*"] {
			if peer == source {
				continue
			}
			combined[peer] = struct{}{}
		}
	}
	peers := make([]*Peer, 0, len(combined))
	for peer := range combined {
		peers = append(peers, peer)
	}
	return peers
}

func cloneChannelHandlers(handlers map[uint64]func([]byte)) []func([]byte) {
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

func cloneBroadcastHandlers(handlers map[uint64]func([]byte)) []func([]byte) {
	if len(handlers) == 0 {
		return nil
	}
	cloned := make([]func([]byte), 0, len(handlers))
	for _, handler := range handlers {
		cloned = append(cloned, handler)
	}
	return cloned
}

func clonePeers(peers map[*Peer]bool) []*Peer {
	if len(peers) == 0 {
		return nil
	}
	cloned := make([]*Peer, 0, len(peers))
	for peer := range peers {
		cloned = append(cloned, peer)
	}
	return cloned
}
