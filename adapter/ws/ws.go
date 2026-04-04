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
	"dappco.re/go/stream"
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

// New creates a WebSocket adapter.
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

// Mount wires the adapter to a hub.
//
//	adapter.Mount(hub)
func (adapter *Adapter) Mount(hub *stream.Hub) {
	adapter.hub = hub
}

// ServeHTTP upgrades the request to WebSocket and binds the connection to the mounted hub.
//
//	http.Handle("/stream/ws", adapter.Handler())
//
//	// Gin:
//	r.GET("/stream/ws", gin.WrapF(adapter.Handler()))
func (adapter *Adapter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	adapter.serveHTTP(w, r, r.URL.Query()["channel"])
}

// HandlerForChannel returns a handler that auto-subscribes every connection to one channel.
//
//	http.Handle("/stream/hashrate", adapter.HandlerForChannel("hashrate"))
func (adapter *Adapter) HandlerForChannel(channel string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		adapter.serveHTTP(w, r, []string{channel})
	}
}

func (adapter *Adapter) serveHTTP(w http.ResponseWriter, r *http.Request, channels []string) {
	if adapter.hub == nil {
		http.Error(w, "stream hub not mounted", http.StatusInternalServerError)
		return
	}

	result := stream.AuthResult{Valid: true}
	if adapter.config.Authenticator != nil {
		result = adapter.config.Authenticator.Authenticate(r)
		if !result.Valid {
			if adapter.config.OnAuthFailure != nil {
				adapter.config.OnAuthFailure(r, result)
			}
			http.Error(w, "unauthorised", http.StatusUnauthorized)
			return
		}
	}

	upgrader := websocket.Upgrader{
		ReadBufferSize:  adapter.config.ReadBufferSize,
		WriteBufferSize: adapter.config.WriteBufferSize,
		CheckOrigin: func(r *http.Request) bool {
			if adapter.config.CheckOrigin != nil {
				return adapter.config.CheckOrigin(r)
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
	peer.SetCloseHook(func() {
		_ = conn.Close()
	})
	_ = adapter.hub.AddPeer(peer)
	defer adapter.hub.RemovePeer(peer)
	for _, channel := range channels {
		if channel == "" {
			continue
		}
		_ = adapter.hub.SubscribePeer(peer, channel)
	}
	defer conn.Close()

	hubConfig := adapter.hub.Config()
	if hubConfig.PongTimeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(hubConfig.PongTimeout))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(hubConfig.PongTimeout))
		})
	}

	go adapter.writePump(conn, peer, hubConfig.WriteTimeout, hubConfig.HeartbeatInterval)

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
			_ = adapter.hub.SubscribePeer(peer, message.Channel)
		case stream.TypeUnsubscribe:
			adapter.hub.UnsubscribePeer(peer, message.Channel)
		case stream.TypePing:
			_ = peer.Send([]byte(core.JSONMarshalString(stream.Message{
				Type:      stream.TypePong,
				Channel:   message.Channel,
				ProcessID: message.ProcessID,
				Timestamp: time.Now().UTC(),
			})))
		default:
			if message.Channel == "" {
				_ = adapter.hub.BroadcastFromPeer(peer, payload)
				continue
			}
			_ = adapter.hub.PublishFromPeer(peer, message.Channel, payload)
		}
	}

	peer.Close()
}

// Handler returns an http.HandlerFunc for WebSocket connections.
// Compatible with net/http and gin (use gin.WrapF).
//
//	http.Handle("/stream/ws", adapter.Handler())
//
//	// Gin:
//	r.GET("/stream/ws", gin.WrapF(adapter.Handler()))
func (adapter *Adapter) Handler() http.HandlerFunc {
	return adapter.ServeHTTP
}

func (adapter *Adapter) writePump(conn *websocket.Conn, peer *stream.Peer, writeTimeout, heartbeatInterval time.Duration) {
	var ticker *time.Ticker
	var heartbeat <-chan time.Time
	if heartbeatInterval > 0 {
		ticker = time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		heartbeat = ticker.C
	}
	for {
		select {
		case <-heartbeat:
			if writeTimeout > 0 {
				_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			}
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case frame, ok := <-peer.SendQueue():
			if !ok {
				return
			}
			if writeTimeout > 0 {
				_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			}
			if err := conn.WriteMessage(websocket.TextMessage, frame); err != nil {
				return
			}
		}
	}
}
