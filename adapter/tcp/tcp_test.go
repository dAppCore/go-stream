// SPDX-License-Identifier: EUPL-1.2

package tcp

import (
	"context"
	"net"
	"testing"
	"time"

	"dappco.re/go/stream"
)

func TestTCP_Listen_Good(t *testing.T) {
	hub := stream.NewHubWithConfig(stream.HubConfig{
		OnConnect: func(peer *stream.Peer) {
			if peer.UserID != "user-42" {
				t.Errorf("peer.UserID = %q, want %q", peer.UserID, "user-42")
			}
			if peer.Claims["role"] != "admin" {
				t.Errorf("peer.Claims[role] = %v, want %q", peer.Claims["role"], "admin")
			}
		},
	})

	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{
		Addr: "127.0.0.1:0",
		ConnAuthenticator: stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
			if string(handshake) != "hello" {
				return stream.AuthResult{Valid: false}
			}
			return stream.AuthResult{
				Valid:  true,
				UserID: "user-42",
				Claims: map[string]any{"role": "admin"},
			}
		}),
	})
	adapter.Mount(hub)

	listenContext, listenCancel := context.WithCancel(context.Background())
	defer listenCancel()
	go func() {
		_ = adapter.Listen(listenContext)
	}()

	address := waitForListenerAddress(t, adapter)
	connection, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer connection.Close()

	if _, err := connection.Write(encodeFrame("", []byte("hello"))); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	waitForPeerCount(t, hub, 1)
}

func TestTCP_Listen_NoAuthenticator_Good(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{
		Addr: "127.0.0.1:0",
	})
	adapter.Mount(hub)

	listenContext, listenCancel := context.WithCancel(context.Background())
	defer listenCancel()
	go func() {
		_ = adapter.Listen(listenContext)
	}()

	address := waitForListenerAddress(t, adapter)
	connection, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer connection.Close()

	received := make(chan []byte, 1)
	unsubscribe := hub.Subscribe("block", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	if _, err := connection.Write(encodeFrame("block", []byte("template"))); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	select {
	case frame := <-received:
		if string(frame) != "template" {
			t.Fatalf("received frame = %q, want %q", string(frame), "template")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for unauthenticated frame")
	}
}

func TestTCP_Listen_Bad(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{
		Addr: "127.0.0.1:0",
		ConnAuthenticator: stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
			return stream.AuthResult{Valid: false}
		}),
	})
	adapter.Mount(hub)

	listenContext, listenCancel := context.WithCancel(context.Background())
	defer listenCancel()
	go func() {
		_ = adapter.Listen(listenContext)
	}()

	address := waitForListenerAddress(t, adapter)
	connection, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer connection.Close()

	if _, err := connection.Write(encodeFrame("", []byte("nope"))); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	waitForPeerCount(t, hub, 0)
}

func TestTCP_Listen_Ugly(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{
		Addr: "127.0.0.1:0",
		ConnAuthenticator: stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
			return stream.AuthResult{Valid: true}
		}),
		HandshakeTimeout: 50 * time.Millisecond,
	})
	adapter.Mount(hub)

	listenContext, listenCancel := context.WithCancel(context.Background())
	defer listenCancel()
	go func() {
		_ = adapter.Listen(listenContext)
	}()

	address := waitForListenerAddress(t, adapter)
	connection, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer connection.Close()

	time.Sleep(120 * time.Millisecond)
	waitForPeerCount(t, hub, 0)
}

func TestTCP_Listen_HandshakeTooLarge_Good(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{
		Addr: "127.0.0.1:0",
	})
	adapter.Mount(hub)

	listenContext, listenCancel := context.WithCancel(context.Background())
	defer listenCancel()
	go func() {
		_ = adapter.Listen(listenContext)
	}()

	address := waitForListenerAddress(t, adapter)
	connection, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer connection.Close()

	tooLargeHandshake := make([]byte, maxHandshakeFrameSize+1)
	if _, err := connection.Write(encodeFrame("", tooLargeHandshake)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	waitForPeerCount(t, hub, 0)
}

func waitForListenerAddress(t *testing.T, adapter *Adapter) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		adapter.mu.Lock()
		listener := adapter.listener
		adapter.mu.Unlock()
		if listener != nil {
			return listener.Addr().String()
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for listener")
	return ""
}

func waitForPeerCount(t *testing.T, hub *stream.Hub, expected int) {
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
