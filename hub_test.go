// SPDX-License-Identifier: EUPL-1.2

package stream

import (
	"context"
	"testing"
	"time"
)

func TestHub_Pipe_Good(t *testing.T) {
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

func TestHub_Pipe_Bad(t *testing.T) {
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

	stop()

	if err := sourceHub.Publish("block", []byte("template")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case frame := <-received:
		t.Fatalf("received unexpected frame after stop: %q", string(frame))
	case <-time.After(200 * time.Millisecond):
	}
}

func TestHub_Pipe_Ugly(t *testing.T) {
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

func TestHub_Publish_Good(t *testing.T) {
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

func TestHub_Publish_Bad(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()

	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	if err := hub.Publish("hashrate", []byte("123456")); err != nil {
		t.Fatalf("Publish() error = %v, want nil", err)
	}
}

func TestHub_Publish_Ugly(t *testing.T) {
	hub := NewHub()

	if err := hub.Publish("hashrate", []byte("123456")); err != ErrHubNotRunning {
		t.Fatalf("Publish() error = %v, want %v", err, ErrHubNotRunning)
	}
}

func TestHub_Broadcast_Good(t *testing.T) {
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

func TestHub_Broadcast_Bad(t *testing.T) {
	hub := NewHub()

	if err := hub.Broadcast([]byte("123456")); err != ErrHubNotRunning {
		t.Fatalf("Broadcast() error = %v, want %v", err, ErrHubNotRunning)
	}
}

func TestHub_Broadcast_Ugly(t *testing.T) {
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

func waitForRunningHub(t *testing.T, hub *Hub) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hub.mu.RLock()
		running := hub.running
		hub.mu.RUnlock()
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
