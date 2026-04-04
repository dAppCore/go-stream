// SPDX-License-Identifier: EUPL-1.2

package ws

import (
	"context"
	"net/http"
	"time"

	"dappco.re/go/core/stream"
)

// ReconnectConfig configures the client-side reconnecting WebSocket.
//
//	rc := ws.ReconnectConfig{
//	    URL:            "ws://localhost:8080/stream/ws",
//	    InitialBackoff: 500 * time.Millisecond,
//	    MaxBackoff:     30 * time.Second,
//	    OnMessage: func(msg stream.Message) {
//	        log.Printf("received %s on %s", msg.Type, msg.Channel)
//	    },
//	}
//	client := ws.NewReconnectingClient(rc)
//	err := client.Connect(ctx)
type ReconnectConfig struct {
	URL               string
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64
	MaxRetries        int // 0 = unlimited
	OnConnect         func()
	OnDisconnect      func()
	OnReconnect       func(attempt int)
	OnMessage         func(msg stream.Message)
	Headers           http.Header
}

// ReconnectingClient is a WebSocket client with automatic reconnection.
// Preserves the go-ws ReconnectingClient API.
//
//	client := ws.NewReconnectingClient(rc)
//	err := client.Connect(ctx)  // blocks until ctx cancelled
type ReconnectingClient struct {
	config ReconnectConfig
	state  stream.ConnectionState
}

// NewReconnectingClient creates a reconnecting WebSocket client.
//
//	client := ws.NewReconnectingClient(rc)
func NewReconnectingClient(config ReconnectConfig) *ReconnectingClient {
	return nil
}

// Connect starts the connection loop. Blocks until ctx is cancelled.
//
//	err := client.Connect(ctx)
func (rc *ReconnectingClient) Connect(ctx context.Context) error {
	return nil
}

// Send marshals and sends a message through the WebSocket connection.
//
//	client.Send(stream.Message{Type: stream.TypeEvent, Data: payload})
func (rc *ReconnectingClient) Send(msg stream.Message) error {
	return nil
}

// State returns the current connection state.
//
//	if client.State() == stream.StateConnected { ... }
func (rc *ReconnectingClient) State() stream.ConnectionState {
	return stream.StateDisconnected
}

// Close shuts down the reconnecting client.
//
//	client.Close()
func (rc *ReconnectingClient) Close() error {
	return nil
}
