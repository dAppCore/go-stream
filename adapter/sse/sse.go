// SPDX-License-Identifier: EUPL-1.2

// Package sse is the Server-Sent Events transport adapter for stream.Hub.
// Lightweight server-push over HTTP/1.1 - no upgrade required.
// Used by core/api for live stats, agent event streams, and /live_stats endpoints.
package sse

import (
	"fmt"
	"net/http"
	"time"

	"dappco.re/go/core"
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

// New creates an SSE adapter. Call Mount before serving requests.
func New(config Config) *Adapter {
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 15 * time.Second
	}
	if config.RetryMs == 0 {
		config.RetryMs = 3000
	}
	return &Adapter{config: config}
}

// Mount wires the adapter to a hub. Must be called before Handler().
func (a *Adapter) Mount(hub *stream.Hub) {
	a.hub = hub
}

// Handler returns an http.HandlerFunc that accepts SSE connections.
func (a *Adapter) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		a.serve(w, r, r.URL.Query()["channel"])
	}
}

// HandlerForChannel returns a handler that auto-subscribes all connections to channel.
func (a *Adapter) HandlerForChannel(channel string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		a.serve(w, r, []string{channel})
	}
}

func (a *Adapter) serve(w http.ResponseWriter, r *http.Request, channels []string) {
	if a.hub == nil {
		http.Error(w, "stream hub not mounted", http.StatusInternalServerError)
		return
	}

	result := stream.AuthResult{Valid: true}
	if a.config.Authenticator != nil {
		result = a.config.Authenticator.Authenticate(r)
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
	_ = a.hub.AddPeer(peer)
	defer a.hub.RemovePeer(peer)

	for _, channel := range channels {
		if channel == "" {
			continue
		}
		_ = a.hub.SubscribePeer(peer, channel)
	}

	_, _ = fmt.Fprintf(w, "retry: %d\n\n", a.config.RetryMs)
	flusher.Flush()

	ticker := time.NewTicker(a.config.HeartbeatInterval)
	defer ticker.Stop()

	done := r.Context().Done()
	for {
		select {
		case <-done:
			return
		case frame, ok := <-peer.SendQueue():
			if !ok {
				return
			}
			_, _ = fmt.Fprintf(w, "data: %s\n\n", frame)
			flusher.Flush()
		case <-ticker.C:
			_, _ = fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

var _ time.Duration
var _ = core.E
