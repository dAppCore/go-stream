// SPDX-License-Identifier: EUPL-1.2

// Package sse is the Server-Sent Events transport adapter for stream.Hub.
// Lightweight server-push over HTTP/1.1 - no upgrade required.
// Used by core/api for live stats, agent event streams, and /live_stats endpoints.
package sse

import (
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"dappco.re/go/stream"
)

// Config configures the SSE adapter.
type Config struct {
	Authenticator     stream.Authenticator
	HeartbeatInterval time.Duration
	RetryMs           int
}

// Adapter is the SSE transport adapter for a stream.Hub.
type Adapter struct {
	hub    *stream.Hub
	config Config
}

// New creates an SSE adapter.
func New(config Config) *Adapter {
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 15 * time.Second
	}
	if config.RetryMs == 0 {
		config.RetryMs = 3000
	}
	return &Adapter{config: config}
}

// Mount wires the adapter to a hub.
func (adapter *Adapter) Mount(hub *stream.Hub) {
	adapter.hub = hub
}

// ServeHTTP accepts an SSE connection and subscribes it using the channel query params.
//
//	http.Handle("/stream/events", adapter.Handler())
func (adapter *Adapter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	adapter.serve(w, r, r.URL.Query()["channel"])
}

// Handler returns an http.HandlerFunc that accepts SSE connections.
//
//	http.Handle("/stream/events", adapter.Handler())
func (adapter *Adapter) Handler() http.HandlerFunc {
	return adapter.ServeHTTP
}

// HandlerForChannel returns a handler that auto-subscribes all connections to channel.
//
//	http.Handle("/stream/hashrate", adapter.HandlerForChannel("hashrate"))
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

	result := stream.AuthResult{Valid: true}
	if adapter.config.Authenticator != nil {
		result = adapter.config.Authenticator.Authenticate(r)
		if !result.Valid {
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
	peer.UserID = result.UserID
	peer.Claims = result.Claims
	done := make(chan struct{})
	var doneOnce sync.Once
	peer.SetCloseHook(func() {
		doneOnce.Do(func() {
			close(done)
		})
	})
	_ = adapter.hub.AddPeer(peer)
	defer adapter.hub.RemovePeer(peer)

	for _, channel := range channels {
		if channel == "" {
			continue
		}
		_ = adapter.hub.SubscribePeer(peer, channel)
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
