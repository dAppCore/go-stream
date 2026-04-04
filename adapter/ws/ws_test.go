// SPDX-License-Identifier: EUPL-1.2

package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"dappco.re/go/stream"
)

func TestAdapter_Handler_Good(t *testing.T) {
	hub := stream.NewHubWithConfig(stream.HubConfig{
		HeartbeatInterval: 20 * time.Millisecond,
		PongTimeout:       100 * time.Millisecond,
		WriteTimeout:      100 * time.Millisecond,
	})

	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{})
	adapter.Mount(hub)

	server := httptest.NewServer(http.HandlerFunc(adapter.Handler()))
	defer server.Close()

	conn := dialWebSocket(t, server.URL, nil)
	defer conn.Close()

	receivedPing := make(chan struct{}, 1)
	receivedFrame := make(chan []byte, 1)
	conn.SetPingHandler(func(appData string) error {
		select {
		case receivedPing <- struct{}{}:
		default:
		}
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(time.Second))
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			messageType, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if messageType == websocket.TextMessage {
				receivedFrame <- append([]byte(nil), payload...)
			}
		}
	}()

	if err := conn.WriteJSON(stream.Message{
		Type:    stream.TypeSubscribe,
		Channel: "hashrate",
	}); err != nil {
		t.Fatalf("WriteJSON() error = %v", err)
	}

	waitForChannelSubscriberCount(t, hub, "hashrate", 1)

	if err := hub.Publish("hashrate", []byte("123456")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case frame := <-receivedFrame:
		if string(frame) != "123456" {
			t.Fatalf("received frame = %q, want %q", string(frame), "123456")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for published frame")
	}

	select {
	case <-receivedPing:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for heartbeat ping")
	}

	_ = conn.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for client reader to exit")
	}
}

func TestAdapter_Handler_Bad(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{
		Authenticator: stream.NewAPIKeyAuth(map[string]string{"valid-key": "user-1"}),
	})
	adapter.Mount(hub)

	server := httptest.NewServer(http.HandlerFunc(adapter.Handler()))
	defer server.Close()

	_, resp, err := websocket.DefaultDialer.Dial(websocketURL(server.URL), nil)
	if err == nil {
		t.Fatal("Dial() error = nil, want auth failure")
	}
	if resp == nil {
		t.Fatal("Dial() response = nil, want 401 response")
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestAdapter_Handler_Ugly(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{})
	adapter.Mount(hub)

	server := httptest.NewServer(http.HandlerFunc(adapter.Handler()))
	defer server.Close()

	conn := dialWebSocket(t, server.URL, nil)
	if err := conn.WriteJSON(stream.Message{
		Type:    stream.TypeSubscribe,
		Channel: "block",
	}); err != nil {
		t.Fatalf("WriteJSON() error = %v", err)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	waitForPeerCount(t, hub, 0)
}

func TestAdapter_ServeHTTP_Good(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{})
	adapter.Mount(hub)

	server := httptest.NewServer(adapter)
	defer server.Close()

	conn := dialWebSocket(t, server.URL, nil)
	defer conn.Close()

	if err := conn.WriteJSON(stream.Message{
		Type:    stream.TypeSubscribe,
		Channel: "serve-http",
	}); err != nil {
		t.Fatalf("WriteJSON() error = %v", err)
	}

	waitForChannelSubscriberCount(t, hub, "serve-http", 1)

	if err := hub.Publish("serve-http", []byte("ok")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	messageType, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage() error = %v", err)
	}
	if messageType != websocket.TextMessage {
		t.Fatalf("messageType = %d, want %d", messageType, websocket.TextMessage)
	}
	if string(payload) != "ok" {
		t.Fatalf("payload = %q, want %q", string(payload), "ok")
	}
}

func dialWebSocket(t *testing.T, serverURL string, header http.Header) *websocket.Conn {
	t.Helper()
	conn, resp, err := websocket.DefaultDialer.Dial(websocketURL(serverURL), header)
	if err != nil {
		if resp != nil {
			t.Fatalf("Dial() error = %v, status = %s", err, resp.Status)
		}
		t.Fatalf("Dial() error = %v", err)
	}
	return conn
}

func websocketURL(serverURL string) string {
	parsed, err := url.Parse(serverURL)
	if err != nil {
		return serverURL
	}
	switch parsed.Scheme {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	}
	return parsed.String()
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

func waitForChannelSubscriberCount(t *testing.T, hub *stream.Hub, channel string, expected int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hub.ChannelSubscriberCount(channel) == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("ChannelSubscriberCount(%q) = %d, want %d", channel, hub.ChannelSubscriberCount(channel), expected)
}
