// SPDX-License-Identifier: EUPL-1.2

package stream

import (
	"context"
	"sync"
	"testing"
	"time"
)

type testStream struct {
	mutex       sync.Mutex
	subscribers map[string]map[int]func([]byte)
	nextID      int
	published   []publishedFrame
	broadcasts  [][]byte
}

type publishedFrame struct {
	channel string
	frame   []byte
}

func newTestStream() *testStream {
	return &testStream{
		subscribers: map[string]map[int]func([]byte){},
	}
}

func TestHub_NewPeer_DefaultClaims_Good(t *testing.T) {
	peer := NewPeer("ws")
	if peer == nil {
		t.Fatal("NewPeer() = nil")
	}
	if peer.Claims == nil {
		t.Fatal("NewPeer().Claims = nil, want empty map")
	}
	if len(peer.Claims) != 0 {
		t.Fatalf("len(NewPeer().Claims) = %d, want 0", len(peer.Claims))
	}
	peer.Claims["role"] = "worker"
	if role := peer.Claims["role"]; role != "worker" {
		t.Fatalf("Claims[role] = %v, want %q", role, "worker")
	}
}

func (streamValue *testStream) Publish(channel string, frame []byte) error {
	streamValue.mutex.Lock()
	streamValue.published = append(streamValue.published, publishedFrame{
		channel: channel,
		frame:   append([]byte(nil), frame...),
	})
	handlers := streamValue.cloneHandlersLocked(channel)
	wildcardHandlers := streamValue.cloneHandlersLocked("*")
	streamValue.mutex.Unlock()

	for _, handler := range handlers {
		handler(frame)
	}
	if channel != "*" {
		for _, handler := range wildcardHandlers {
			handler(frame)
		}
	}
	return nil
}

func (streamValue *testStream) Subscribe(channel string, handler func([]byte)) func() {
	streamValue.mutex.Lock()
	defer streamValue.mutex.Unlock()
	streamValue.nextID++
	id := streamValue.nextID
	if streamValue.subscribers[channel] == nil {
		streamValue.subscribers[channel] = map[int]func([]byte){}
	}
	streamValue.subscribers[channel][id] = handler
	return func() {
		streamValue.mutex.Lock()
		defer streamValue.mutex.Unlock()
		delete(streamValue.subscribers[channel], id)
		if len(streamValue.subscribers[channel]) == 0 {
			delete(streamValue.subscribers, channel)
		}
	}
}

func (streamValue *testStream) Broadcast(frame []byte) error {
	streamValue.mutex.Lock()
	defer streamValue.mutex.Unlock()
	streamValue.broadcasts = append(streamValue.broadcasts, append([]byte(nil), frame...))
	return nil
}

func (streamValue *testStream) Pipe(dst Stream) func() {
	return Pipe(streamValue, dst)
}

func (streamValue *testStream) Stats() HubStats {
	return HubStats{}
}

func (streamValue *testStream) cloneHandlersLocked(channel string) []func([]byte) {
	handlers := streamValue.subscribers[channel]
	if len(handlers) == 0 {
		return nil
	}
	cloned := make([]func([]byte), 0, len(handlers))
	for _, handler := range handlers {
		cloned = append(cloned, handler)
	}
	return cloned
}

func TestAX7_Hub_Pipe_Good(t *testing.T) {
	sourceHub := NewHub()
	destinationHub := NewHub()

	sourceContext, sourceCancel := context.WithCancel(context.Background())
	defer sourceCancel()
	destinationContext, destinationCancel := context.WithCancel(context.Background())
	defer destinationCancel()

	go sourceHub.Run(sourceContext)
	go destinationHub.Run(destinationContext)
	waitForRunningHub(t, sourceHub)
	waitForRunningHub(t, destinationHub)

	stop := Pipe(sourceHub, destinationHub)
	defer stop()

	received := make(chan []byte, 1)
	unsubscribe := destinationHub.Subscribe("hashrate", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	if err := sourceHub.Publish("hashrate", []byte("123456")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case frame := <-received:
		if string(frame) != "123456" {
			t.Fatalf("received frame = %q, want %q", string(frame), "123456")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for forwarded frame")
	}
}

func TestHub_Pipe_Broadcast_Good(t *testing.T) {
	sourceHub := NewHub()
	destinationHub := NewHub()

	sourceContext, sourceCancel := context.WithCancel(context.Background())
	defer sourceCancel()
	destinationContext, destinationCancel := context.WithCancel(context.Background())
	defer destinationCancel()

	go sourceHub.Run(sourceContext)
	go destinationHub.Run(destinationContext)
	waitForRunningHub(t, sourceHub)
	waitForRunningHub(t, destinationHub)

	stop := Pipe(sourceHub, destinationHub)
	defer stop()

	received := make(chan []byte, 1)
	unsubscribe := destinationHub.SubscribeBroadcast(func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	if err := sourceHub.Broadcast([]byte("shutdown")); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}

	select {
	case frame := <-received:
		if string(frame) != "shutdown" {
			t.Fatalf("received broadcast frame = %q, want %q", string(frame), "shutdown")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for broadcast frame")
	}
}

func TestAX7_Hub_Pipe_Bad(t *testing.T) {
	sourceHub := NewHub()
	destinationHub := NewHub()

	sourceContext, sourceCancel := context.WithCancel(context.Background())
	defer sourceCancel()
	destinationContext, destinationCancel := context.WithCancel(context.Background())
	defer destinationCancel()

	go sourceHub.Run(sourceContext)
	go destinationHub.Run(destinationContext)
	waitForRunningHub(t, sourceHub)
	waitForRunningHub(t, destinationHub)

	stop := Pipe(sourceHub, destinationHub)
	received := make(chan []byte, 1)
	unsubscribe := destinationHub.Subscribe("block", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	var stopWG sync.WaitGroup
	for i := 0; i < 8; i++ {
		stopWG.Add(1)
		go func() {
			defer stopWG.Done()
			stop()
		}()
	}
	stopWG.Wait()

	if err := sourceHub.Publish("block", []byte("template")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case frame := <-received:
		t.Fatalf("received unexpected frame after stop: %q", string(frame))
	case <-time.After(200 * time.Millisecond):
	}
}

func TestAX7_Hub_Pipe_Ugly(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()

	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	stop := Pipe(hub, hub)
	defer stop()

	received := make(chan []byte, 1)
	unsubscribe := hub.Subscribe("agent", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	if err := hub.Publish("agent", []byte("event")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case frame := <-received:
		if string(frame) != "event" {
			t.Fatalf("received frame = %q, want %q", string(frame), "event")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for local frame")
	}
}

func TestHub_Pipe_GenericPublishFallback_Good(t *testing.T) {
	sourceStream := newTestStream()
	destinationStream := newTestStream()

	stop := Pipe(sourceStream, destinationStream)
	defer stop()

	if err := sourceStream.Publish("hashrate", []byte("123456")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	destinationStream.mutex.Lock()
	defer destinationStream.mutex.Unlock()
	if len(destinationStream.published) != 1 {
		t.Fatalf("len(published) = %d, want %d", len(destinationStream.published), 1)
	}
	if destinationStream.published[0].channel != "*" {
		t.Fatalf("published channel = %q, want %q", destinationStream.published[0].channel, "*")
	}
	if string(destinationStream.published[0].frame) != "123456" {
		t.Fatalf("published frame = %q, want %q", string(destinationStream.published[0].frame), "123456")
	}
	if len(destinationStream.broadcasts) != 0 {
		t.Fatalf("len(broadcasts) = %d, want %d", len(destinationStream.broadcasts), 0)
	}
}

func TestAX7_Hub_Publish_Good(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()

	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	peer := NewPeer("ws")
	if err := hub.AddPeer(peer); err != nil {
		t.Fatalf("AddPeer() error = %v", err)
	}
	defer hub.RemovePeer(peer)
	waitForPeerCount(t, hub, 1)

	if err := hub.SubscribePeer(peer, "hashrate"); err != nil {
		t.Fatalf("SubscribePeer(channel) error = %v", err)
	}
	if err := hub.SubscribePeer(peer, "*"); err != nil {
		t.Fatalf("SubscribePeer(wildcard) error = %v", err)
	}

	if err := hub.Publish("hashrate", []byte("123456")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case frame := <-peer.SendQueue():
		if string(frame) != "123456" {
			t.Fatalf("received frame = %q, want %q", string(frame), "123456")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for published frame")
	}

	select {
	case frame := <-peer.SendQueue():
		t.Fatalf("received duplicate frame = %q", string(frame))
	case <-time.After(200 * time.Millisecond):
	}
}

func TestAX7_Hub_Publish_Bad(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()

	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	if err := hub.Publish("hashrate", []byte("123456")); err != nil {
		t.Fatalf("Publish() error = %v, want nil", err)
	}
}

func TestAX7_Hub_PublishFromPeer_Good(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()

	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	peer := NewPeer("ws")
	if err := hub.AddPeer(peer); err != nil {
		t.Fatalf("AddPeer() error = %v", err)
	}
	defer hub.RemovePeer(peer)

	if err := hub.SubscribePeer(peer, "hashrate"); err != nil {
		t.Fatalf("SubscribePeer() error = %v", err)
	}

	if err := hub.PublishFromPeer(peer, "hashrate", []byte("123456")); err != nil {
		t.Fatalf("PublishFromPeer() error = %v", err)
	}

	select {
	case frame := <-peer.SendQueue():
		if string(frame) != "123456" {
			t.Fatalf("received frame = %q, want %q", string(frame), "123456")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for published frame")
	}
}

func TestAX7_Hub_BroadcastFromPeer_Good(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()

	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	peer := NewPeer("ws")
	if err := hub.AddPeer(peer); err != nil {
		t.Fatalf("AddPeer() error = %v", err)
	}
	defer hub.RemovePeer(peer)

	if err := hub.BroadcastFromPeer(peer, []byte("shutdown")); err != nil {
		t.Fatalf("BroadcastFromPeer() error = %v", err)
	}

	select {
	case frame := <-peer.SendQueue():
		if string(frame) != "shutdown" {
			t.Fatalf("received frame = %q, want %q", string(frame), "shutdown")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for broadcast frame")
	}
}

func TestAX7_Hub_Publish_Ugly(t *testing.T) {
	hub := NewHub()

	if err := hub.Publish("hashrate", []byte("123456")); err != ErrHubNotRunning {
		t.Fatalf("Publish() error = %v, want %v", err, ErrHubNotRunning)
	}
}

func TestAX7_Hub_Running_Good(t *testing.T) {
	hub := NewHub()
	if hub.Running() {
		t.Fatal("Running() = true before Run()")
	}

	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()

	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	if !hub.Running() {
		t.Fatal("Running() = false while Run() is active")
	}

	hubCancel()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !hub.Running() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("Running() stayed true after context cancellation")
}

func TestAX7_Hub_Running_Bad(t *testing.T) {
	var hub *Hub
	if hub.Running() {
		t.Fatal("nil hub Running() = true, want false")
	}
}

func TestAX7_Hub_Running_Ugly(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()

	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	observed := make(chan bool, 1)
	go func() {
		observed <- hub.Running()
	}()

	select {
	case running := <-observed:
		if !running {
			t.Fatal("Running() = false while hub is active")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for concurrent Running() read")
	}

	hubCancel()
}

func TestAX7_Hub_Broadcast_Good(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()

	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	peer := NewPeer("ws")
	if err := hub.AddPeer(peer); err != nil {
		t.Fatalf("AddPeer() error = %v", err)
	}
	defer hub.RemovePeer(peer)
	waitForPeerCount(t, hub, 1)

	if err := hub.Broadcast([]byte("123456")); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}

	select {
	case frame := <-peer.SendQueue():
		if string(frame) != "123456" {
			t.Fatalf("received frame = %q, want %q", string(frame), "123456")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for broadcast frame")
	}
}

func TestAX7_Hub_Broadcast_Bad(t *testing.T) {
	hub := NewHub()

	if err := hub.Broadcast([]byte("123456")); err != ErrHubNotRunning {
		t.Fatalf("Broadcast() error = %v, want %v", err, ErrHubNotRunning)
	}
}

func TestAX7_Hub_Broadcast_Ugly(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()

	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	peer := NewPeer("ws")
	if err := hub.AddPeer(peer); err != nil {
		t.Fatalf("AddPeer() error = %v", err)
	}
	defer hub.RemovePeer(peer)
	waitForPeerCount(t, hub, 1)

	received := make(chan []byte, 1)
	unsubscribe := hub.Subscribe("*", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	if err := hub.Broadcast([]byte("event")); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}

	select {
	case frame := <-received:
		if string(frame) != "event" {
			t.Fatalf("received handler frame = %q, want %q", string(frame), "event")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for broadcast handler")
	}

	select {
	case frame := <-peer.SendQueue():
		if string(frame) != "event" {
			t.Fatalf("received peer frame = %q, want %q", string(frame), "event")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for broadcast peer")
	}

	hubCancel()
	waitForPeerCount(t, hub, 0)
}

func TestAX7_Hub_SubscribeE_Good(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()

	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	received := make(chan []byte, 1)
	unsubscribe, err := hub.SubscribeE("block", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	if err != nil {
		t.Fatalf("SubscribeE() error = %v", err)
	}
	defer unsubscribe()

	if err := hub.Publish("block", []byte("template")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case frame := <-received:
		if string(frame) != "template" {
			t.Fatalf("received frame = %q, want %q", string(frame), "template")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for subscribed frame")
	}
}

func TestAX7_Hub_SubscribeWithError_Good(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()

	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	received := make(chan []byte, 1)
	unsubscribe, err := hub.SubscribeWithError("block", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	if err != nil {
		t.Fatalf("SubscribeWithError() error = %v", err)
	}
	defer unsubscribe()

	if err := hub.Publish("block", []byte("template")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case frame := <-received:
		if string(frame) != "template" {
			t.Fatalf("received frame = %q, want %q", string(frame), "template")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for subscribed frame")
	}
}

func TestHub_Stats_IncludeHandlerOnlyChannels_Good(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()

	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	unsubscribe := hub.Subscribe("events", func(frame []byte) {})
	defer unsubscribe()

	stats := hub.Stats()
	if stats.Peers != 0 {
		t.Fatalf("Stats().Peers = %d, want %d", stats.Peers, 0)
	}
	if stats.Channels != 1 {
		t.Fatalf("Stats().Channels = %d, want %d", stats.Channels, 1)
	}
	if stats.SubscriberCount["events"] != 1 {
		t.Fatalf("Stats().SubscriberCount[events] = %d, want %d", stats.SubscriberCount["events"], 1)
	}
	if hub.ChannelCount() != 1 {
		t.Fatalf("ChannelCount() = %d, want %d", hub.ChannelCount(), 1)
	}
	if hub.ChannelSubscriberCount("events") != 1 {
		t.Fatalf("ChannelSubscriberCount(events) = %d, want %d", hub.ChannelSubscriberCount("events"), 1)
	}

	channels := make([]string, 0, 1)
	for channel := range hub.AllChannels() {
		channels = append(channels, channel)
	}
	if len(channels) != 1 || channels[0] != "events" {
		t.Fatalf("AllChannels() = %v, want [events]", channels)
	}
}

func TestAX7_Hub_SubscribeE_Bad(t *testing.T) {
	hub := NewHub()

	unsubscribe, err := hub.SubscribeE("", func(frame []byte) {})
	if err != ErrEmptyChannel {
		t.Fatalf("SubscribeE() error = %v, want %v", err, ErrEmptyChannel)
	}
	if unsubscribe == nil {
		t.Fatal("SubscribeE() unsubscribe = nil")
	}
	unsubscribe()
}

func TestAX7_Hub_SubscribeE_Ugly(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()

	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	panicked := 0
	unsubscribe, err := hub.SubscribeE("event", func(frame []byte) {
		panicked++
		panic("boom")
	})
	if err != nil {
		t.Fatalf("SubscribeE() error = %v", err)
	}
	defer unsubscribe()

	received := make(chan []byte, 1)
	safeUnsubscribe := hub.Subscribe("event", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer safeUnsubscribe()

	if err := hub.Publish("event", []byte("payload")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case frame := <-received:
		if string(frame) != "payload" {
			t.Fatalf("received frame = %q, want %q", string(frame), "payload")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for safe handler")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if panicked == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("SubscribeE panic handler count = %d, want 1", panicked)
}

func TestAX7_Hub_CanSubscribePeer_Bad(t *testing.T) {
	hub := NewHubWithConfig(HubConfig{
		ChannelAuthoriser: func(peer *Peer, channel string) bool {
			return channel == "public"
		},
	})

	peer := NewPeer("ws")
	if err := hub.CanSubscribePeer(peer, "private"); err != ErrAuthRejected {
		t.Fatalf("CanSubscribePeer() error = %v, want %v", err, ErrAuthRejected)
	}
	if err := hub.CanSubscribePeer(peer, "public"); err != nil {
		t.Fatalf("CanSubscribePeer() error = %v, want nil", err)
	}
}

func TestAX7_Peer_Subscriptions_Good(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()

	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	peer := NewPeer("ws")
	if err := hub.AddPeer(peer); err != nil {
		t.Fatalf("AddPeer() error = %v", err)
	}
	defer hub.RemovePeer(peer)

	if err := hub.SubscribePeer(peer, "hashrate"); err != nil {
		t.Fatalf("SubscribePeer(hashrate) error = %v", err)
	}
	if err := hub.SubscribePeer(peer, "block"); err != nil {
		t.Fatalf("SubscribePeer(block) error = %v", err)
	}

	subscriptions := peer.Subscriptions()
	if len(subscriptions) != 2 {
		t.Fatalf("len(Subscriptions()) = %d, want %d", len(subscriptions), 2)
	}
	if subscriptions[0] != "block" || subscriptions[1] != "hashrate" {
		t.Fatalf("Subscriptions() = %v, want [block hashrate]", subscriptions)
	}

	hub.UnsubscribePeer(peer, "block")
	subscriptions = peer.Subscriptions()
	if len(subscriptions) != 1 || subscriptions[0] != "hashrate" {
		t.Fatalf("Subscriptions() after unsubscribe = %v, want [hashrate]", subscriptions)
	}
}

func TestAX7_Peer_Subscriptions_Bad(t *testing.T) {
	var peer *Peer

	if subscriptions := peer.Subscriptions(); subscriptions != nil {
		t.Fatalf("Subscriptions() = %v, want nil", subscriptions)
	}
}

func TestAX7_Peer_Subscriptions_Ugly(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()

	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	peer := NewPeer("ws")
	if err := hub.AddPeer(peer); err != nil {
		t.Fatalf("AddPeer() error = %v", err)
	}
	defer hub.RemovePeer(peer)

	if err := hub.SubscribePeer(peer, "hashrate"); err != nil {
		t.Fatalf("SubscribePeer() error = %v", err)
	}

	subscriptions := peer.Subscriptions()
	subscriptions[0] = "tampered"

	current := peer.Subscriptions()
	if len(current) != 1 || current[0] != "hashrate" {
		t.Fatalf("Subscriptions() after caller mutation = %v, want [hashrate]", current)
	}
}

func TestHub_SendToChannel_Wildcard_Good(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()

	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	count := 0
	unsubscribe := hub.Subscribe("*", func(frame []byte) {
		if string(frame) == "event" {
			count++
		}
	})
	defer unsubscribe()

	if err := hub.Publish("*", []byte("event")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if count == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("wildcard handler count = %d, want 1", count)
}

func TestAX7_Peer_Close_Good(t *testing.T) {
	peer := NewPeer("ws")
	closed := make(chan struct{}, 1)

	peer.SetCloseHook(func() {
		closed <- struct{}{}
	})
	peer.Close()
	peer.Close()

	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for close hook")
	}

	select {
	case <-closed:
		t.Fatal("close hook ran more than once")
	case <-time.After(200 * time.Millisecond):
	}

	select {
	case _, ok := <-peer.SendQueue():
		if ok {
			t.Fatal("SendQueue() channel still open after Close()")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for closed SendQueue()")
	}
}

func TestAX7_Hub_Run_Good(t *testing.T) {
	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go hub.Run(ctx)
	waitForRunningHub(t, hub)

	peer := NewPeer("ws")
	if err := hub.AddPeer(peer); err != nil {
		t.Fatalf("AddPeer() error = %v", err)
	}
	waitForPeerCount(t, hub, 1)

	cancel()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !hub.Running() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if hub.Running() {
		t.Fatal("hub still running after context cancellation")
	}
	if hub.PeerCount() != 0 {
		t.Fatalf("PeerCount() = %d after shutdown, want 0", hub.PeerCount())
	}
}

func TestAX7_Hub_Run_Bad(t *testing.T) {
	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go hub.Run(ctx)
	waitForRunningHub(t, hub)

	// Second Run call is a no-op — hub remains running with the original context.
	secondDone := make(chan struct{})
	go func() {
		hub.Run(ctx)
		close(secondDone)
	}()

	select {
	case <-secondDone:
	case <-time.After(2 * time.Second):
		t.Fatal("second Run() did not return immediately")
	}

	if !hub.Running() {
		t.Fatal("hub stopped after second Run() call")
	}
}

func TestAX7_Hub_Run_Ugly(t *testing.T) {
	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())

	go hub.Run(ctx)
	waitForRunningHub(t, hub)

	peer := NewPeer("ws")
	if err := hub.AddPeer(peer); err != nil {
		t.Fatalf("AddPeer() error = %v", err)
	}
	waitForPeerCount(t, hub, 1)

	// Cancel context while a broadcast is in flight.
	go func() {
		for i := 0; i < 100; i++ {
			_ = hub.Broadcast([]byte("inflight"))
		}
	}()
	cancel()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !hub.Running() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if hub.Running() {
		t.Fatal("hub still running after context cancellation during broadcast")
	}
	if hub.PeerCount() != 0 {
		t.Fatalf("PeerCount() = %d after shutdown, want 0 (goroutine leak)", hub.PeerCount())
	}
}

func TestAX7_Hub_Subscribe_Good(t *testing.T) {
	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go hub.Run(ctx)
	waitForRunningHub(t, hub)

	received := make(chan []byte, 1)
	unsubscribe := hub.Subscribe("hashrate", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	if err := hub.Publish("hashrate", []byte("123456")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case frame := <-received:
		if string(frame) != "123456" {
			t.Fatalf("received frame = %q, want %q", string(frame), "123456")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for subscribed frame")
	}
}

func TestAX7_Hub_Subscribe_Bad(t *testing.T) {
	hub := NewHub()

	unsubscribe := hub.Subscribe("", func(frame []byte) {})
	if unsubscribe == nil {
		t.Fatal("Subscribe() with empty channel returned nil unsubscribe")
	}
	unsubscribe()
}

func TestAX7_Hub_Subscribe_Ugly(t *testing.T) {
	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go hub.Run(ctx)
	waitForRunningHub(t, hub)

	panicked := 0
	_ = hub.Subscribe("event", func(frame []byte) {
		panicked++
		panic("handler panic")
	})

	received := make(chan []byte, 1)
	unsubscribe := hub.Subscribe("event", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	if err := hub.Publish("event", []byte("payload")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case frame := <-received:
		if string(frame) != "payload" {
			t.Fatalf("received frame = %q, want %q", string(frame), "payload")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for safe handler after panic")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if panicked == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("panic handler count = %d, want 1", panicked)
}

func waitForRunningHub(t *testing.T, hub *Hub) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hub.mutex.RLock()
		running := hub.running
		hub.mutex.RUnlock()
		if running {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for hub to start")
}

func waitForPeerCount(t *testing.T, hub *Hub, expected int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hub.PeerCount() == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("PeerCount() = %d, want %d", hub.PeerCount(), expected)
}
