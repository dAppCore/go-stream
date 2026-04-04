// SPDX-License-Identifier: EUPL-1.2

// Package zmq is the ZeroMQ transport adapter for stream.Hub.
// High-throughput IPC for daemon block notifications and inter-process job broadcasts.
// Uses PUB/SUB and PUSH/PULL socket patterns.
//
//	adapter := zmq.New(zmq.Config{
//	    Mode:     zmq.ModePubSub,
//	    Endpoint: "tcp://127.0.0.1:5555",
//	    Role:     zmq.RoleSubscriber,
//	    Topics:   []string{"block"},
//	})
//	adapter.Mount(hub)
//	go adapter.Start(ctx)
package zmq

import (
	"context"

	"dappco.re/go/core/stream"
)

// Mode selects the ZMQ socket pattern.
type Mode int

const (
	// ModePubSub uses PUB/SUB sockets.
	// Publisher calls Publish(channel, frame) — topic = channel name.
	// Subscriber receives frames for subscribed topics.
	ModePubSub Mode = iota

	// ModePushPull uses PUSH/PULL sockets.
	// Pusher sends frames; Puller receives and forwards to hub.
	// Useful for daemon → pool block notification (single consumer).
	ModePushPull
)

// Role is the ZMQ socket role.
type Role int

const (
	RolePublisher  Role = iota // PubSub: sends frames
	RoleSubscriber             // PubSub: receives frames → hub dispatch
	RolePusher                 // PushPull: sends frames
	RolePuller                 // PushPull: receives frames → hub dispatch
)

// Config configures the ZMQ adapter.
//
//	cfg := zmq.Config{
//	    Mode:     zmq.ModePubSub,
//	    Endpoint: "tcp://127.0.0.1:5555",
//	    Role:     zmq.RoleSubscriber,
//	}
type Config struct {
	Mode     Mode
	Endpoint string   // ZMQ endpoint, e.g. "tcp://127.0.0.1:5555" or "ipc:///tmp/pool.sock"
	Role     Role     // Publisher/Subscriber or Pusher/Puller
	Topics   []string // PubSub subscriber: topics to subscribe. Empty = all.
}

// Adapter is the ZMQ transport adapter.
//
//	adapter := zmq.New(zmq.Config{
//	    Mode:     zmq.ModePubSub,
//	    Endpoint: "tcp://127.0.0.1:5555",
//	    Role:     zmq.RoleSubscriber,
//	    Topics:   []string{"block"},
//	})
//	adapter.Mount(hub)
//	go adapter.Start(ctx)
type Adapter struct {
	hub    *stream.Hub
	config Config
}

// New creates a ZMQ adapter. Call Mount and Start before use.
//
//	adapter := zmq.New(zmq.Config{Mode: zmq.ModePubSub, Endpoint: "tcp://127.0.0.1:5555"})
func New(config Config) *Adapter {
	return nil
}

// Mount wires the adapter to a hub.
//
//	adapter.Mount(hub)
func (a *Adapter) Mount(hub *stream.Hub) {
}

// Start opens the ZMQ socket and begins receive/dispatch. Blocks until ctx cancelled.
// For publisher/pusher roles, Start is still required to open the socket.
//
//	go adapter.Start(ctx)
func (a *Adapter) Start(ctx context.Context) error {
	return nil
}

// Publish sends frame with topic (channel name) via the ZMQ socket.
// Only valid when Role is RolePublisher or RolePusher.
//
//	adapter.Publish("block", templateBytes)
func (a *Adapter) Publish(channel string, frame []byte) error {
	return nil
}
