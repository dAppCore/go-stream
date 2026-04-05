// SPDX-License-Identifier: EUPL-1.2

package stream

import (
	"context"
	"iter"
	"sort"
	"sync"

	"dappco.re/go/core"
)

const defaultHubQueueSize = 256

// hub := stream.NewHub()
// go hub.Run(ctx)
//
// wsAdapter := ws.New(ws.Config{Authenticator: auth})
// wsAdapter.Mount(hub)
// http.Handle("/stream/ws", wsAdapter.Handler())
type Hub struct {
	peers             map[*Peer]bool
	broadcastQueue    chan broadcastDelivery
	publishQueue      chan publishDelivery
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

// hub := stream.NewHub()
// go hub.Run(ctx)
func NewHub() *Hub {
	return NewHubWithConfig(DefaultHubConfig())
}

//	hub := stream.NewHubWithConfig(stream.HubConfig{
//		HeartbeatInterval: 30 * time.Second,
//		OnConnect: func(peer *stream.Peer) { log.Println("connected", peer.ID) },
//	})
func NewHubWithConfig(config HubConfig) *Hub {
	config = normalizeHubConfig(config)
	return &Hub{
		peers:             map[*Peer]bool{},
		broadcastQueue:    make(chan broadcastDelivery, defaultHubQueueSize),
		publishQueue:      make(chan publishDelivery, defaultHubQueueSize),
		register:          make(chan *Peer, defaultHubQueueSize),
		unregister:        make(chan *Peer, defaultHubQueueSize),
		channels:          map[string]map[*Peer]bool{},
		channelHandlers:   map[string]map[uint64]func([]byte){},
		broadcastHandlers: map[uint64]func([]byte){},
		publishHandlers:   map[uint64]func(string, []byte){},
		config:            config,
		done:              make(chan struct{}),
	}
}

// config := hub.Config()
// writeTimeout := config.WriteTimeout
func (hub *Hub) Config() HubConfig {
	if hub == nil {
		return DefaultHubConfig()
	}
	hub.mu.RLock()
	config := hub.config
	hub.mu.RUnlock()
	return normalizeHubConfig(config)
}

// running := hub.Running()
func (hub *Hub) Running() bool {
	if hub == nil {
		return false
	}
	hub.mu.RLock()
	defer hub.mu.RUnlock()
	return hub.running
}

// go hub.Run(ctx)
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
		case item := <-hub.publishQueue:
			hub.processPublishDelivery(item.channel, item.frame, item.notifyPublishSubscribers)
		}
	}
}

// _ = hub.SendToChannel("hashrate", []byte(`{"h":123456}`))
func (hub *Hub) SendToChannel(channel string, frame []byte) error {
	return hub.sendToChannel(channel, frame, true)
}

// _ = hub.PublishFromPeer(peer, "block", []byte("template"))
func (hub *Hub) PublishFromPeer(source *Peer, channel string, frame []byte) error {
	return hub.sendToChannelFromPeer(source, channel, frame, true)
}

// _ = hub.PublishFromBridge("block", []byte("template"))
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
	hub.enqueuePublishDelivery(channel, frame, notifyPublishSubscribers)
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

	return onceFunction(func() {
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

// Subscribe a handler for one channel.
//
//	unsubscribe := hub.Subscribe("block", func(frame []byte) { handleBlock(frame) })
//	defer unsubscribe()
func (hub *Hub) Subscribe(channel string, handler func([]byte)) func() {
	unsub, _ := hub.SubscribeWithError(channel, handler)
	return unsub
}

// _ = hub.SubscribePeer(peer, "hashrate")
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
	if peer.sendQueue == nil {
		peer.sendQueue = make(chan []byte, defaultPeerSendBufferSize)
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

// err := hub.CanSubscribePeer(peer, "hashrate")
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

// hub.UnsubscribePeer(peer, "hashrate")
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

// _ = hub.Publish("hashrate", []byte(`{"h":123456}`))
func (hub *Hub) Publish(channel string, frame []byte) error {
	return hub.sendToChannel(channel, frame, true)
}

// _ = hub.Broadcast([]byte(`{"type":"shutdown"}`))
func (hub *Hub) Broadcast(frame []byte) error {
	return hub.broadcastFrame(frame, true)
}

// _ = hub.BroadcastFromPeer(peer, []byte("shutdown"))
func (hub *Hub) BroadcastFromPeer(source *Peer, frame []byte) error {
	return hub.broadcastFrameFromPeer(source, frame, true)
}

// _ = hub.BroadcastFromBridge([]byte("shutdown"))
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

// stop := hub.Pipe(remoteHub)
func (hub *Hub) Pipe(destination Stream) func() {
	return Pipe(hub, destination)
}

// stats := hub.Stats()
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

//	stop := hub.SubscribePublished(func(channel string, frame []byte) {
//		_ = channel
//		_ = frame
//	})
func (hub *Hub) SubscribePublished(handler func(string, []byte)) func() {
	return hub.subscribePublished(handler)
}

//	stop := hub.SubscribeBroadcast(func(frame []byte) {
//		_ = frame
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

	return onceFunction(func() {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		delete(hub.broadcastHandlers, id)
	})
}

// n := hub.PeerCount()
func (hub *Hub) PeerCount() int {
	if hub == nil {
		return 0
	}
	hub.mu.RLock()
	defer hub.mu.RUnlock()
	return len(hub.peers)
}

// n := hub.ChannelCount()
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

// n := hub.ChannelSubscriberCount("hashrate")
func (hub *Hub) ChannelSubscriberCount(channel string) int {
	if hub == nil {
		return 0
	}
	hub.mu.RLock()
	defer hub.mu.RUnlock()
	return len(hub.channels[channel])
}

// for peer := range hub.AllPeers() { _ = peer.UserID }
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
	sort.SliceStable(peers, func(left, right int) bool {
		if peers[left] == nil {
			return false
		}
		if peers[right] == nil {
			return true
		}
		return peers[left].ID < peers[right].ID
	})
	return func(yield func(*Peer) bool) {
		for _, peer := range peers {
			if !yield(peer) {
				return
			}
		}
	}
}

// for channel := range hub.AllChannels() { _ = channel }
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

// peer := stream.NewPeer("ws")
// _ = hub.AddPeer(peer)
func (hub *Hub) AddPeer(peer *Peer) error {
	if hub == nil {
		return core.E("stream.hub", "nil hub", nil)
	}
	if peer == nil {
		return core.E("stream.hub", "nil peer", nil)
	}
	if peer.sendQueue == nil {
		peer.sendQueue = make(chan []byte, defaultPeerSendBufferSize)
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

// hub.RemovePeer(peer)
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
		func(handlerFunction func([]byte)) {
			defer func() {
				_ = recover()
			}()
			handlerFunction(frame)
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

func (hub *Hub) broadcastToPeers(_ *Peer, frame []byte, notifyBroadcastSubscribers bool) {
	if hub == nil {
		return
	}
	hub.mu.RLock()
	peers := make([]*Peer, 0, len(hub.peers))
	for peer := range hub.peers {
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

type publishDelivery struct {
	channel                  string
	frame                    []byte
	notifyPublishSubscribers bool
}

type broadcastDelivery struct {
	source                     *Peer
	frame                      []byte
	notifyBroadcastSubscribers bool
}

func (hub *Hub) enqueuePublishDelivery(channel string, frame []byte, notifyPublishSubscribers bool) {
	if hub == nil {
		return
	}
	item := publishDelivery{
		channel:                  channel,
		frame:                    append([]byte(nil), frame...),
		notifyPublishSubscribers: notifyPublishSubscribers,
	}
	select {
	case hub.publishQueue <- item:
	default:
		go hub.enqueuePublishDeliveryAsync(item)
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

func (hub *Hub) enqueuePublishDeliveryAsync(item publishDelivery) {
	if hub == nil {
		return
	}
	select {
	case hub.publishQueue <- item:
	case <-hub.done:
	}
}

func (hub *Hub) processPublishDelivery(channel string, frame []byte, notifyPublishSubscribers bool) {
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

	return onceFunction(func() {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		delete(hub.publishHandlers, id)
	})
}

func (hub *Hub) invokeBroadcastHandlers(handlers []func([]byte), frame []byte) {
	for _, handler := range handlers {
		func(handlerFunction func([]byte)) {
			defer func() {
				_ = recover()
			}()
			handlerFunction(frame)
		}(handler)
	}
}

func (hub *Hub) invokePublishHandlers(handlers []func(string, []byte), channel string, frame []byte) {
	for _, handler := range handlers {
		func(handlerFunction func(string, []byte)) {
			defer func() {
				_ = recover()
			}()
			handlerFunction(channel, frame)
		}(handler)
	}
}

func (hub *Hub) collectChannelPeersLocked(channel string, _ *Peer) []*Peer {
	combined := map[*Peer]struct{}{}
	for peer := range hub.channels[channel] {
		combined[peer] = struct{}{}
	}
	if channel != "*" {
		for peer := range hub.channels["*"] {
			combined[peer] = struct{}{}
		}
	}
	peers := make([]*Peer, 0, len(combined))
	for peer := range combined {
		peers = append(peers, peer)
	}
	sort.SliceStable(peers, func(left, right int) bool {
		if peers[left] == nil {
			return false
		}
		if peers[right] == nil {
			return true
		}
		return peers[left].ID < peers[right].ID
	})
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
