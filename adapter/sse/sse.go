// SPDX-License-Identifier: EUPL-1.2

// Package sse is the Server-Sent Events transport adapter for stream.Hub.
// Lightweight server-push over HTTP/1.1 — no upgrade required.
// Used by core/api for live stats, agent event streams, and /live_stats endpoints.
//
//	adapter := sse.New(sse.Config{})
//	adapter.Mount(hub)
//	http.Handle("/stream/events", adapter.Handler())
package sse

import (
	"net/http"
	"time"

	"dappco.re/go/core/stream"
)

// Config configures the SSE adapter.
//
//	cfg := sse.Config{
//	    Authenticator:     stream.NewAPIKeyAuth(keys),
//	    HeartbeatInterval: 15 * time.Second,
//	}
type Config struct {
	// Authenticator is checked before accepting the SSE connection.
	// When nil, all connections accepted.
	Authenticator stream.Authenticator

	// HeartbeatInterval is the interval between SSE comment heartbeats (": ping").
	// Defaults to 15 seconds. Keeps the connection alive through proxies.
	HeartbeatInterval time.Duration

	// RetryMs is the SSE retry field sent to the client in milliseconds.
	// Instructs the browser how long to wait before reconnecting.
	// Defaults to 3000.
	RetryMs int
}

// Adapter is the SSE transport adapter for a stream.Hub.
//
//	adapter := sse.New(sse.Config{})
//	adapter.Mount(hub)
//	http.Handle("/stream/events", adapter.Handler())
type Adapter struct {
	hub    *stream.Hub
	config Config
}

// New creates an SSE adapter. Call Mount before serving requests.
//
//	adapter := sse.New(sse.Config{HeartbeatInterval: 15 * time.Second})
func New(config Config) *Adapter {
	return nil
}

// Mount wires the adapter to a hub. Must be called before Handler().
//
//	adapter.Mount(hub)
func (a *Adapter) Mount(hub *stream.Hub) {
}

// Handler returns an http.HandlerFunc that accepts SSE connections.
// Response: Content-Type: text/event-stream, Cache-Control: no-cache
//
// Subscription is controlled via query parameter:
//
//	GET /stream/events?channel=hashrate
//	GET /stream/events?channel=hashrate&channel=block
//
//	http.Handle("/stream/events", adapter.Handler())
func (a *Adapter) Handler() http.HandlerFunc {
	return nil
}

// HandlerForChannel returns a handler that auto-subscribes all connections to channel.
// Used when a dedicated endpoint per channel is preferred.
//
//	http.Handle("/stream/hashrate", adapter.HandlerForChannel("hashrate"))
func (a *Adapter) HandlerForChannel(channel string) http.HandlerFunc {
	return nil
}

// Ensure unused imports are referenced.
var _ time.Duration
