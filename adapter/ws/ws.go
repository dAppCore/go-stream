// SPDX-License-Identifier: EUPL-1.2

// Package ws is the WebSocket transport adapter for stream.Hub.
// It wires gorilla/websocket onto the hub, handling HTTP upgrade,
// per-client read/write pumps, and authentication.
//
//	adapter := ws.New(ws.Config{Authenticator: auth})
//	adapter.Mount(hub)
//	http.Handle("/stream/ws", adapter.Handler())
package ws

import (
	"net/http"

	"dappco.re/go/core/stream"
)

// Config configures the WebSocket adapter.
//
//	cfg := ws.Config{
//	    Authenticator: stream.NewAPIKeyAuth(keys),
//	    OnAuthFailure: func(r *http.Request, res stream.AuthResult) {
//	        log.Printf("ws auth fail from %s", r.RemoteAddr)
//	    },
//	}
type Config struct {
	// Authenticator is called during HTTP upgrade. When nil, all connections accepted.
	Authenticator stream.Authenticator

	// OnAuthFailure is called when Authenticator rejects a connection.
	OnAuthFailure func(r *http.Request, result stream.AuthResult)

	// ReadBufferSize and WriteBufferSize are passed to the gorilla upgrader.
	// Default: 1024 each.
	ReadBufferSize  int
	WriteBufferSize int

	// CheckOrigin overrides the upgrader's origin check. When nil, all origins accepted.
	CheckOrigin func(r *http.Request) bool
}

// Adapter is the WebSocket transport adapter for a stream.Hub.
//
//	adapter := ws.New(ws.Config{...})
//	adapter.Mount(hub)
//	http.Handle("/ws", adapter.Handler())
type Adapter struct {
	hub    *stream.Hub
	config Config
}

// New creates a WebSocket adapter. Call Mount before serving requests.
//
//	adapter := ws.New(ws.Config{Authenticator: auth})
func New(config Config) *Adapter {
	return nil
}

// Mount wires the adapter to a hub. Must be called before Handler().
//
//	adapter.Mount(hub)
func (a *Adapter) Mount(hub *stream.Hub) {
}

// Handler returns an http.HandlerFunc for WebSocket connections.
// Compatible with net/http and gin (use gin.WrapF).
//
//	http.Handle("/stream/ws", adapter.Handler())
//
//	// Gin:
//	r.GET("/stream/ws", gin.WrapF(adapter.Handler()))
func (a *Adapter) Handler() http.HandlerFunc {
	return nil
}
