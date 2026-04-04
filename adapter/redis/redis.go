// SPDX-License-Identifier: EUPL-1.2

// Package redis is the Redis pub/sub bridge for stream.Hub.
// Enables cross-instance coordination: multiple Hub instances on different nodes
// using the same Redis backend coordinate broadcasts and channel messages transparently.
package redis

import (
	"context"
	"crypto/tls"
	"strconv"
	"sync"

	"dappco.re/go/core"
	"dappco.re/go/core/stream"
)

// Config configures the Redis bridge.
type Config struct {
	Addr      string
	Password  string
	DB        int
	Prefix    string
	TLSConfig *tls.Config
}

// Bridge connects a Hub to Redis pub/sub for cross-instance messaging.
type Bridge struct {
	hub      *stream.Hub
	config   Config
	sourceID string

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
}

type bridgeRegistry struct {
	mu      sync.RWMutex
	bridges map[string]map[*Bridge]struct{}
}

var registry = bridgeRegistry{bridges: map[string]map[*Bridge]struct{}{}}

// NewBridge creates and validates the Redis connection. Does not start listening.
func NewBridge(hub *stream.Hub, cfg Config) (*Bridge, error) {
	if hub == nil {
		return nil, core.E("stream.redis", "nil hub", nil)
	}
	if cfg.Addr == "" {
		return nil, core.E("stream.redis", "empty address", nil)
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "stream"
	}
	return &Bridge{
		hub:      hub,
		config:   cfg,
		sourceID: stream.NewPeer("redis").ID,
		stopCh:   make(chan struct{}),
	}, nil
}

// Start begins the Redis pub/sub listener. Blocks in a goroutine until Stop() or ctx cancel.
func (b *Bridge) Start(ctx context.Context) error {
	if b == nil {
		return core.E("stream.redis", "nil bridge", nil)
	}
	b.mu.Lock()
	if b.running {
		b.mu.Unlock()
		<-ctx.Done()
		return nil
	}
	b.running = true
	stopCh := b.stopCh
	key := b.registryKey()
	b.mu.Unlock()

	registry.add(key, b)
	defer registry.remove(key, b)

	select {
	case <-ctx.Done():
	case <-stopCh:
	}

	b.mu.Lock()
	b.running = false
	b.mu.Unlock()
	return nil
}

// Stop cleanly shuts down the bridge. Closes the pub/sub subscription and Redis client.
func (b *Bridge) Stop() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	if !b.running {
		b.mu.Unlock()
		return nil
	}
	close(b.stopCh)
	b.stopCh = make(chan struct{})
	b.mu.Unlock()
	return nil
}

// PublishToChannel publishes frame to a specific hub channel via Redis.
func (b *Bridge) PublishToChannel(channel string, frame []byte) error {
	if b == nil {
		return core.E("stream.redis", "nil bridge", nil)
	}
	if !b.isRunning() {
		return core.E("stream.redis", "bridge not started", nil)
	}
	registry.publish(b.registryKey(), channel, envelope{
		SourceID: b.sourceID,
		Frame:    append([]byte(nil), frame...),
	})
	return nil
}

// PublishBroadcast publishes frame as a broadcast via Redis.
func (b *Bridge) PublishBroadcast(frame []byte) error {
	if b == nil {
		return core.E("stream.redis", "nil bridge", nil)
	}
	if !b.isRunning() {
		return core.E("stream.redis", "bridge not started", nil)
	}
	registry.publish(b.registryKey(), "", envelope{
		SourceID: b.sourceID,
		Frame:    append([]byte(nil), frame...),
	})
	return nil
}

// SourceID returns the random instance identifier.
func (b *Bridge) SourceID() string {
	if b == nil {
		return ""
	}
	return b.sourceID
}

func (b *Bridge) registryKey() string {
	return b.config.Addr + "|" + strconv.Itoa(b.config.DB) + "|" + b.config.Prefix
}

func (b *Bridge) isRunning() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.running
}

type envelope struct {
	SourceID string `json:"s"`
	Frame    []byte `json:"f"`
}

func (r *bridgeRegistry) add(key string, bridge *Bridge) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.bridges[key] == nil {
		r.bridges[key] = map[*Bridge]struct{}{}
	}
	r.bridges[key][bridge] = struct{}{}
}

func (r *bridgeRegistry) remove(key string, bridge *Bridge) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if bridges := r.bridges[key]; bridges != nil {
		delete(bridges, bridge)
		if len(bridges) == 0 {
			delete(r.bridges, key)
		}
	}
}

func (r *bridgeRegistry) publish(key, channel string, message envelope) {
	r.mu.RLock()
	bridges := r.bridges[key]
	targets := make([]*Bridge, 0, len(bridges))
	for bridge := range bridges {
		targets = append(targets, bridge)
	}
	r.mu.RUnlock()

	for _, bridge := range targets {
		if bridge == nil || bridge.sourceID == message.SourceID {
			continue
		}
		if channel == "" {
			_ = bridge.hub.Broadcast(message.Frame)
			continue
		}
		_ = bridge.hub.Publish(channel, message.Frame)
	}
}
