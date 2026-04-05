// SPDX-License-Identifier: EUPL-1.2

// Package sse streams hub frames over Server-Sent Events.
//
//	adapter := sse.New(sse.Config{HeartbeatInterval: 15 * time.Second})
//	adapter.Mount(hub)
//	http.Handle("/stream/events", adapter.Handler())
package sse

import (
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"dappco.re/go/stream"
)

//	config := sse.Config{
//	    Authenticator:     stream.NewAPIKeyAuth(map[string]string{"sk-live": "user-42"}),
//	    HeartbeatInterval: 15 * time.Second,
//	    RetryMs:           3000,
//	}
type Config struct {
	// sse.New(sse.Config{Authenticator: stream.NewAPIKeyAuth(keys)})
	Authenticator stream.Authenticator

	// sse.New(sse.Config{OnAuthFailure: func(r *http.Request, result stream.AuthResult) { ... }})
	OnAuthFailure func(r *http.Request, result stream.AuthResult)

	// sse.New(sse.Config{HeartbeatInterval: 15 * time.Second})
	HeartbeatInterval time.Duration

	// sse.New(sse.Config{RetryMs: 3000})
	RetryMs int
}

// adapter := sse.New(sse.Config{})
// adapter.Mount(hub)
// http.Handle("/stream/events", adapter.Handler())
type Adapter struct {
	hub    *stream.Hub
	config Config
}

// adapter := sse.New(sse.Config{HeartbeatInterval: 15 * time.Second})
func New(config Config) *Adapter {
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 15 * time.Second
	}
	if config.RetryMs == 0 {
		config.RetryMs = 3000
	}
	return &Adapter{config: config}
}

// adapter.Mount(hub)
func (adapter *Adapter) Mount(hub *stream.Hub) {
	adapter.hub = hub
}

// http.Handle("/stream/events", adapter.Handler())
// http.Get("http://127.0.0.1:8080/stream/events?channel=hashrate")
func (adapter *Adapter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	adapter.serve(w, r, r.URL.Query()["channel"])
}

// http.Handle("/stream/events", adapter.Handler())
// http.Get("http://127.0.0.1:8080/stream/events?channel=hashrate")
func (adapter *Adapter) Handler() http.HandlerFunc {
	return adapter.ServeHTTP
}

// http.Handle("/stream/hashrate", adapter.HandlerForChannel("hashrate"))
func (adapter *Adapter) HandlerForChannel(channel string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		adapter.serve(w, r, []string{channel})
	}
}

func (adapter *Adapter) serve(w http.ResponseWriter, r *http.Request, channels []string) {
	if adapter.hub == nil {
		http.Error(w, "stream hub not mounted", http.StatusInternalServerError)
		return
	}

	config := adapter.config
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 15 * time.Second
	}
	if config.RetryMs == 0 {
		config.RetryMs = 3000
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

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	header := w.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	header.Set("X-Accel-Buffering", "no")

	peer := stream.NewPeer("sse")
	peer.UserID = authResult.UserID
	peer.Claims = authResult.Claims
	done := make(chan struct{})
	var doneOnce sync.Once
	peer.SetCloseHook(func() {
		doneOnce.Do(func() {
			close(done)
		})
	})

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

	if err := adapter.hub.AddPeer(peer); err != nil {
		http.Error(w, "stream hub not running", http.StatusInternalServerError)
		return
	}
	defer adapter.hub.RemovePeer(peer)

	for _, channel := range channels {
		if channel == "" {
			continue
		}
		if err := adapter.hub.SubscribePeer(peer, channel); err != nil {
			http.Error(w, "stream hub not running", http.StatusInternalServerError)
			return
		}
	}

	_, _ = io.WriteString(w, "retry: "+strconv.Itoa(config.RetryMs)+"\n\n")
	flusher.Flush()

	ticker := time.NewTicker(config.HeartbeatInterval)
	defer ticker.Stop()

	requestDone := r.Context().Done()
	for {
		select {
		case <-done:
			return
		case <-requestDone:
			return
		case frame, ok := <-peer.SendQueue():
			if !ok {
				return
			}
			_, _ = io.WriteString(w, "data: ")
			_, _ = w.Write(frame)
			_, _ = io.WriteString(w, "\n\n")
			flusher.Flush()
		case <-ticker.C:
			_, _ = io.WriteString(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}
