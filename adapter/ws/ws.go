// SPDX-License-Identifier: EUPL-1.2

// adapter := ws.New(ws.Config{Authenticator: auth})
// adapter.Mount(hub)
// http.Handle("/stream/ws", adapter.Handler())
package ws

import (
	"context"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"dappco.re/go"
	"dappco.re/go/stream"
)

//	config := ws.Config{
//	    Authenticator: stream.NewAPIKeyAuth(keys),
//	    OnAuthFailure: func(r *http.Request, res stream.AuthResult) {
//	        core.Print("stream", "ws auth fail from %s", r.RemoteAddr)
//	    },
//	}
type Config struct {
	// ws.New(ws.Config{Authenticator: stream.NewAPIKeyAuth(keys)})
	Authenticator stream.Authenticator

	// ws.New(ws.Config{OnAuthFailure: func(r *http.Request, result stream.AuthResult) { ... }})
	OnAuthFailure func(r *http.Request, result stream.AuthResult)

	// ws.New(ws.Config{ReadBufferSize: 1024, WriteBufferSize: 1024})
	ReadBufferSize  int
	WriteBufferSize int

	// ws.New(ws.Config{CheckOrigin: func(r *http.Request) bool { return true }})
	CheckOrigin func(r *http.Request) bool
}

// adapter := ws.New(ws.Config{Authenticator: auth})
// adapter.Mount(hub)
// http.Handle("/ws", adapter.Handler())
type Adapter struct {
	hub    *stream.Hub
	config Config
}

// adapter := ws.New(ws.Config{Authenticator: auth})
func New(config Config) *Adapter {
	if config.ReadBufferSize == 0 {
		config.ReadBufferSize = 1024
	}
	if config.WriteBufferSize == 0 {
		config.WriteBufferSize = 1024
	}
	return &Adapter{config: config}
}

// adapter.Mount(hub)
func (adapter *Adapter) Mount(hub *stream.Hub) {
	adapter.hub = hub
}

// http.Handle("/stream/ws", adapter.Handler())
//
// Gin:
// r.GET("/stream/ws", gin.WrapF(adapter.Handler()))
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

	authResult := stream.AuthResult{Valid: true}
	if adapter.config.Authenticator != nil {
		authResult = adapter.config.Authenticator.Authenticate(r)
		if !authResult.Valid {
			if adapter.config.OnAuthFailure != nil {
				adapter.config.OnAuthFailure(r, authResult)
			}
			http.Error(w, "unauthorised", http.StatusUnauthorized)
			return
		}
	}

	peer := stream.NewPeer("ws")
	peer.UserID = authResult.UserID
	if authResult.Claims != nil {
		peer.Claims = authResult.Claims
	}
	for _, channel := range channels {
		if channel == "" {
			continue
		}
		if err := adapter.hub.CanSubscribePeer(peer, channel); err != nil {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
	}

	if !adapter.hub.Running() {
		http.Error(w, "stream hub not running", http.StatusInternalServerError)
		return
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

	if err := adapter.hub.AddPeer(peer); err != nil {
		if closeErr := conn.Close(); closeErr != nil {
			return
		}
		http.Error(w, "stream hub not running", http.StatusInternalServerError)
		return
	}
	defer adapter.hub.RemovePeer(peer)

	peer.SetCloseHook(func() {
		if err := conn.Close(); err != nil {
			return
		}
	})
	for _, channel := range channels {
		if channel == "" {
			continue
		}
		if err := adapter.hub.SubscribePeer(peer, channel); err != nil {
			peer.Close()
			return
		}
	}
	defer conn.Close()
	stopClose := context.AfterFunc(r.Context(), func() {
		if err := conn.Close(); err != nil {
			return
		}
	})
	defer stopClose()

	hubConfig := adapter.hub.Config()
	if hubConfig.PongTimeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(hubConfig.PongTimeout)); err != nil {
			peer.Close()
			return
		}
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
			if err := adapter.hub.SubscribePeer(peer, message.Channel); err != nil {
				if ok := peer.Send(marshalMessage(stream.Message{
					Type:      stream.TypeError,
					Channel:   message.Channel,
					Data:      errorPayload(err),
					Timestamp: time.Now().UTC(),
				})); !ok {
					return
				}
			}
		case stream.TypeUnsubscribe:
			adapter.hub.UnsubscribePeer(peer, message.Channel)
		case stream.TypePing:
			if ok := peer.Send([]byte(core.JSONMarshalString(stream.Message{
				Type:      stream.TypePong,
				Channel:   message.Channel,
				ProcessID: message.ProcessID,
				Timestamp: time.Now().UTC(),
			}))); !ok {
				return
			}
		default:
			if message.Channel == "" {
				if err := adapter.hub.BroadcastFromPeer(peer, payload); err != nil {
					return
				}
				continue
			}
			if err := adapter.hub.PublishFromPeer(peer, message.Channel, payload); err != nil {
				return
			}
		}
	}

	peer.Close()
}

// http.Handle("/stream/ws", adapter.Handler())
// r.GET("/stream/ws", gin.WrapF(adapter.Handler()))
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
				if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
					return
				}
			}
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case frame, ok := <-peer.SendQueue():
			if !ok {
				return
			}
			if writeTimeout > 0 {
				if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
					return
				}
			}
			if err := conn.WriteMessage(websocket.TextMessage, frame); err != nil {
				return
			}
		}
	}
}

func marshalMessage(message stream.Message) []byte {
	return []byte(core.JSONMarshalString(message))
}

func errorPayload(err error) map[string]any {
	if err == nil {
		return nil
	}
	return map[string]any{"message": err.Error()}
}
