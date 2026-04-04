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
	"time"

	"github.com/gorilla/websocket"

	"dappco.re/go/core"
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
	if config.ReadBufferSize == 0 {
		config.ReadBufferSize = 1024
	}
	if config.WriteBufferSize == 0 {
		config.WriteBufferSize = 1024
	}
	return &Adapter{config: config}
}

// Mount wires the adapter to a hub. Must be called before Handler().
//
//	adapter.Mount(hub)
func (a *Adapter) Mount(hub *stream.Hub) {
	a.hub = hub
}

// Handler returns an http.HandlerFunc for WebSocket connections.
// Compatible with net/http and gin (use gin.WrapF).
//
//	http.Handle("/stream/ws", adapter.Handler())
//
//	// Gin:
//	r.GET("/stream/ws", gin.WrapF(adapter.Handler()))
func (a *Adapter) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.hub == nil {
			http.Error(w, "stream hub not mounted", http.StatusInternalServerError)
			return
		}

		result := stream.AuthResult{Valid: true}
		if a.config.Authenticator != nil {
			result = a.config.Authenticator.Authenticate(r)
			if !result.Valid {
				if a.config.OnAuthFailure != nil {
					a.config.OnAuthFailure(r, result)
				}
				http.Error(w, "unauthorised", http.StatusUnauthorized)
				return
			}
		}

		upgrader := websocket.Upgrader{
			ReadBufferSize:  a.config.ReadBufferSize,
			WriteBufferSize: a.config.WriteBufferSize,
			CheckOrigin: func(r *http.Request) bool {
				if a.config.CheckOrigin != nil {
					return a.config.CheckOrigin(r)
				}
				return true
			},
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		peer := stream.NewPeer("ws")
		peer.UserID = result.UserID
		peer.Claims = result.Claims
		_ = a.hub.AddPeer(peer)
		defer a.hub.RemovePeer(peer)
		defer conn.Close()

		go func() {
			for frame := range peer.SendQueue() {
				if err := conn.WriteMessage(websocket.TextMessage, frame); err != nil {
					return
				}
			}
		}()

		conn.SetReadLimit(1 << 20)
		for {
			messageType, payload, err := conn.ReadMessage()
			if err != nil {
				break
			}
			if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
				continue
			}
			var message stream.Message
			if !core.JSONUnmarshal(payload, &message).OK {
				continue
			}
			switch message.Type {
			case stream.TypeSubscribe:
				_ = a.hub.SubscribePeer(peer, message.Channel)
			case stream.TypeUnsubscribe:
				a.hub.UnsubscribePeer(peer, message.Channel)
			case stream.TypePing:
				_ = peer.Send([]byte(core.JSONMarshalString(stream.Message{
					Type:      stream.TypePong,
					Channel:   message.Channel,
					ProcessID: message.ProcessID,
					Timestamp: time.Now().UTC(),
				})))
			}
		}

		peer.Close()
	}
}
