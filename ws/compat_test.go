// SPDX-License-Identifier: EUPL-1.2

package ws

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestCompat_LegacySurface_Good(t *testing.T) {
	auth := NewAPIKeyAuth(map[string]string{"valid-key": "user-1"})
	if auth == nil {
		t.Fatal("NewAPIKeyAuth() = nil")
	}

	var frame Frame = []byte("payload")
	if string(frame) != "payload" {
		t.Fatalf("Frame alias produced %q, want %q", string(frame), "payload")
	}

	var channel Channel = "hashrate"
	if channel != "hashrate" {
		t.Fatalf("Channel alias produced %q, want %q", channel, "hashrate")
	}

	if StateDisconnected != 0 || StateConnecting != 1 || StateConnected != 2 {
		t.Fatalf("unexpected connection states: %d %d %d", StateDisconnected, StateConnecting, StateConnected)
	}

	if ErrMissingAuthHeader == nil || ErrMalformedAuthHeader == nil || ErrInvalidAPIKey == nil {
		t.Fatal("expected auth sentinel errors to be re-exported")
	}
	if ErrHandshakeTimeout == nil || ErrAuthRejected == nil || ErrHubNotRunning == nil || ErrEmptyChannel == nil {
		t.Fatal("expected transport sentinel errors to be re-exported")
	}

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

	received := make(chan []byte, 1)
	unsubscribe := destinationHub.Subscribe("hashrate", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	stop := Pipe(sourceHub, destinationHub)
	defer stop()

	if err := sourceHub.Publish("hashrate", []byte("123456")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case frame := <-received:
		if string(frame) != "123456" {
			t.Fatalf("received frame = %q, want %q", string(frame), "123456")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for piped frame")
	}

	peer := NewPeer("ws")
	if peer == nil {
		t.Fatal("NewPeer() = nil")
	}
	if peer.Transport != "ws" {
		t.Fatalf("peer.Transport = %q, want %q", peer.Transport, "ws")
	}

	stats := destinationHub.Stats()
	var _ HubStats = stats
}

func TestCompat_LegacySurface_Bad(t *testing.T) {
	hub := NewHub()

	if err := hub.Publish("hashrate", []byte("123456")); err != ErrHubNotRunning {
		t.Fatalf("Publish() error = %v, want %v", err, ErrHubNotRunning)
	}

	peer := NewPeer("ws")
	if err := hub.SubscribePeer(peer, ""); err != ErrEmptyChannel {
		t.Fatalf("SubscribePeer() error = %v, want %v", err, ErrEmptyChannel)
	}
}

func TestCompat_LegacySurface_Ugly(t *testing.T) {
	var source Stream
	stop := Pipe(source, source)
	if stop == nil {
		t.Fatal("Pipe(nil, nil) returned nil stop function")
	}
	stop()
}

func TestCompat_HubHandler_Good(t *testing.T) {
	hub := NewHub()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)
	waitForRunningHub(t, hub)

	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	url := "ws" + server.URL[len("http"):] + "?channel=hashrate"
	connection, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer connection.Close()

	payload := []byte(`{"type":"event","channel":"hashrate","data":{"h":123456},"timestamp":"2026-01-01T00:00:00Z"}`)
	if err := hub.Publish("hashrate", payload); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	_, frame, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage() error = %v", err)
	}
	if string(frame) != string(payload) {
		t.Fatalf("ReadMessage() frame = %q, want %q", string(frame), string(payload))
	}
}

func waitForRunningHub(t *testing.T, hub *Hub) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hub.Publish("health", nil) == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for hub to start")
}
