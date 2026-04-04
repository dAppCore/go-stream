// SPDX-License-Identifier: EUPL-1.2

package sse

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dappco.re/go/stream"
)

func TestAdapter_Handler_Good(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{HeartbeatInterval: 20 * time.Millisecond})
	adapter.Mount(hub)

	server := httptest.NewServer(http.HandlerFunc(adapter.Handler()))
	defer server.Close()

	response, err := http.Get(server.URL + "?channel=hashrate")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	defer response.Body.Close()

	waitForPeerCount(t, hub, 1)
	if err := hub.Publish("hashrate", []byte("123456")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	reader := bufio.NewReader(response.Body)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("ReadString() error = %v", err)
		}
		if strings.TrimSpace(line) == "data: 123456" {
			return
		}
	}
}

func TestAdapter_Handler_ZeroValueConfig_Good(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := &Adapter{}
	adapter.Mount(hub)

	server := httptest.NewServer(http.HandlerFunc(adapter.Handler()))
	defer server.Close()

	response, err := http.Get(server.URL + "?channel=hashrate")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	defer response.Body.Close()

	waitForPeerCount(t, hub, 1)
	if err := hub.Publish("hashrate", []byte("123456")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	reader := bufio.NewReader(response.Body)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("ReadString() error = %v", err)
		}
		if strings.TrimSpace(line) == "data: 123456" {
			return
		}
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

	response, err := http.Get(server.URL + "?channel=hashrate")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("StatusCode = %d, want %d", response.StatusCode, http.StatusUnauthorized)
	}
}

func TestAdapter_Handler_Ugly(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{HeartbeatInterval: 20 * time.Millisecond})
	adapter.Mount(hub)

	server := httptest.NewServer(http.HandlerFunc(adapter.Handler()))
	defer server.Close()

	requestContext, requestCancel := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, server.URL+"?channel=hashrate", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}

	waitForPeerCount(t, hub, 1)
	requestCancel()
	_ = response.Body.Close()

	waitForPeerCount(t, hub, 0)
}

func TestAdapter_ServeHTTP_Good(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{HeartbeatInterval: 20 * time.Millisecond})
	adapter.Mount(hub)

	server := httptest.NewServer(adapter)
	defer server.Close()

	response, err := http.Get(server.URL + "?channel=serve-http")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	defer response.Body.Close()

	waitForPeerCount(t, hub, 1)
	if err := hub.Publish("serve-http", []byte("ok")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	reader := bufio.NewReader(response.Body)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("ReadString() error = %v", err)
		}
		if strings.TrimSpace(line) == "data: ok" {
			return
		}
	}
}

func TestAdapter_HandlerForChannel_Good(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{HeartbeatInterval: 20 * time.Millisecond})
	adapter.Mount(hub)

	server := httptest.NewServer(http.HandlerFunc(adapter.HandlerForChannel("hashrate")))
	defer server.Close()

	response, err := http.Get(server.URL)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	defer response.Body.Close()

	waitForPeerCount(t, hub, 1)
	if err := hub.Publish("hashrate", []byte("654321")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	reader := bufio.NewReader(response.Body)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("ReadString() error = %v", err)
		}
		if strings.TrimSpace(line) == "data: 654321" {
			return
		}
	}
}

func TestAdapter_Handler_RetryMs_Good(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{RetryMs: 1234, HeartbeatInterval: time.Second})
	adapter.Mount(hub)

	server := httptest.NewServer(http.HandlerFunc(adapter.Handler()))
	defer server.Close()

	response, err := http.Get(server.URL + "?channel=hashrate")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	defer response.Body.Close()

	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("ReadString() error = %v", err)
	}
	if strings.TrimSpace(line) != "retry: 1234" {
		t.Fatalf("first line = %q, want %q", strings.TrimSpace(line), "retry: 1234")
	}
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
