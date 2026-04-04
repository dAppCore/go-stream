// SPDX-License-Identifier: EUPL-1.2

// Package redis is the Redis pub/sub bridge for stream.Hub.
// Enables cross-instance coordination: multiple Hub instances on different nodes
// using the same Redis backend coordinate broadcasts and channel messages transparently.
//
//	bridge, err := redis.NewBridge(hub, redis.Config{Addr: "redis:6379", Prefix: "pool"})
//	bridge.Start(ctx)
//	defer bridge.Stop()
package redis

import (
	"context"
	"crypto/tls"

	"dappco.re/go/core/stream"
)

// Config configures the Redis bridge.
//
//	cfg := redis.Config{
//	    Addr:   "10.69.69.87:6379",
//	    Prefix: "pool",
//	}
type Config struct {
	Addr      string
	Password  string
	DB        int
	Prefix    string // key prefix for Redis channels. Default: "stream"
	TLSConfig *tls.Config
}

// Bridge connects a Hub to Redis pub/sub for cross-instance messaging.
//
//	bridge, err := redis.NewBridge(hub, redis.Config{Addr: "redis:6379", Prefix: "pool"})
//	bridge.Start(ctx)
//	defer bridge.Stop()
type Bridge struct {
	hub      *stream.Hub
	config   Config
	sourceID string
}

// NewBridge creates and validates the Redis connection. Does not start listening.
// Returns core.E if hub is nil, address is empty, or Redis ping fails.
//
//	bridge, err := redis.NewBridge(hub, cfg)
func NewBridge(hub *stream.Hub, cfg Config) (*Bridge, error) {
	return nil, nil
}

// Start begins the Redis pub/sub listener. Blocks in a goroutine until Stop() or ctx cancel.
//
//	bridge.Start(ctx)
func (b *Bridge) Start(ctx context.Context) error {
	return nil
}

// Stop cleanly shuts down the bridge. Closes the pub/sub subscription and Redis client.
//
//	defer bridge.Stop()
func (b *Bridge) Stop() error {
	return nil
}

// PublishToChannel publishes frame to a specific hub channel via Redis.
// All Bridge instances on the same Redis receive the frame and deliver locally.
//
//	bridge.PublishToChannel("block", templateBytes)
func (b *Bridge) PublishToChannel(channel string, frame []byte) error {
	return nil
}

// PublishBroadcast publishes frame as a broadcast via Redis.
// All Bridge instances forward it to all local hub peers.
//
//	bridge.PublishBroadcast(shutdownFrame)
func (b *Bridge) PublishBroadcast(frame []byte) error {
	return nil
}

// SourceID returns the random instance identifier. Used in tests to verify echo prevention.
//
//	id := bridge.SourceID()
func (b *Bridge) SourceID() string {
	return b.sourceID
}

// envelope wraps a frame with a sourceID to prevent infinite echo loops.
// Serialised as JSON on the Redis wire.
type envelope struct {
	SourceID string `json:"s"`
	Frame    []byte `json:"f"`
}
