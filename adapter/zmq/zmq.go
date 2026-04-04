// SPDX-License-Identifier: EUPL-1.2

// Package zmq is the ZeroMQ transport adapter for stream.Hub.
// High-throughput IPC for daemon block notifications and inter-process job broadcasts.
package zmq

import (
	"context"
	"strconv"
	"sync"

	"dappco.re/go/core"
	"dappco.re/go/stream"
)

// Mode selects the ZMQ socket pattern.
type Mode int

const (
	ModePubSub Mode = iota
	ModePushPull
)

// Role is the ZMQ socket role.
type Role int

const (
	RolePublisher Role = iota
	RoleSubscriber
	RolePusher
	RolePuller
)

// Config configures the ZMQ adapter.
type Config struct {
	Mode     Mode
	Endpoint string
	Role     Role
	Topics   []string
}

// Adapter is the ZMQ transport adapter.
type Adapter struct {
	hub    *stream.Hub
	config Config
	source string

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
}

type zmqRegistry struct {
	mu       sync.RWMutex
	adapters map[string]map[*Adapter]struct{}
}

var registry = zmqRegistry{adapters: map[string]map[*Adapter]struct{}{}}

// New creates a ZMQ adapter. Call Mount and Start before use.
func New(config Config) *Adapter {
	return &Adapter{config: config, source: stream.NewPeer("zmq").ID, stopCh: make(chan struct{})}
}

// Mount wires the adapter to a hub.
func (a *Adapter) Mount(hub *stream.Hub) {
	a.hub = hub
}

// Start opens the ZMQ socket and begins receive/dispatch. Blocks until ctx cancelled.
func (a *Adapter) Start(ctx context.Context) error {
	if a == nil {
		return core.E("stream.zmq", "nil adapter", nil)
	}
	if a.config.Endpoint == "" {
		return core.E("stream.zmq", "empty endpoint", nil)
	}
	if a.hub == nil {
		return core.E("stream.zmq", "stream hub not mounted", nil)
	}

	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		<-ctx.Done()
		return nil
	}
	a.running = true
	stopCh := a.stopCh
	key := a.registryKey()
	a.mu.Unlock()

	registry.add(key, a)
	defer registry.remove(key, a)

	select {
	case <-ctx.Done():
	case <-stopCh:
	}

	a.mu.Lock()
	a.running = false
	a.mu.Unlock()
	return nil
}

// Publish sends frame with topic (channel name) via the ZMQ socket.
func (a *Adapter) Publish(channel string, frame []byte) error {
	if a == nil {
		return core.E("stream.zmq", "nil adapter", nil)
	}
	if a.config.Role != RolePublisher && a.config.Role != RolePusher {
		return core.E("stream.zmq", "publish not supported for this role", nil)
	}
	if !a.isRunning() {
		return core.E("stream.zmq", "adapter not started", nil)
	}
	registry.publish(a.registryKey(), message{
		SourceID: a.sourceID(),
		Channel:  channel,
		Frame:    append([]byte(nil), frame...),
	})
	return nil
}

// Stop shuts down the adapter.
func (a *Adapter) Stop() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return nil
	}
	close(a.stopCh)
	a.stopCh = make(chan struct{})
	a.mu.Unlock()
	return nil
}

type message struct {
	SourceID string
	Channel  string
	Frame    []byte
}

func (a *Adapter) registryKey() string {
	return a.config.Endpoint + "|" + strconv.Itoa(int(a.config.Mode))
}

func (a *Adapter) sourceID() string {
	return a.source
}

func (a *Adapter) isRunning() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.running
}

func (r *zmqRegistry) add(key string, adapter *Adapter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.adapters[key] == nil {
		r.adapters[key] = map[*Adapter]struct{}{}
	}
	r.adapters[key][adapter] = struct{}{}
}

func (r *zmqRegistry) remove(key string, adapter *Adapter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if adapters := r.adapters[key]; adapters != nil {
		delete(adapters, adapter)
		if len(adapters) == 0 {
			delete(r.adapters, key)
		}
	}
}

func (r *zmqRegistry) publish(key string, message message) {
	r.mu.RLock()
	adapters := r.adapters[key]
	targets := make([]*Adapter, 0, len(adapters))
	for adapter := range adapters {
		targets = append(targets, adapter)
	}
	r.mu.RUnlock()

	for _, adapter := range targets {
		if adapter == nil || adapter.sourceID() == message.SourceID {
			continue
		}
		if adapter.config.Role != RoleSubscriber && adapter.config.Role != RolePuller {
			continue
		}
		if len(adapter.config.Topics) > 0 && message.Channel != "" {
			allowed := false
			for _, topic := range adapter.config.Topics {
				if topic == message.Channel {
					allowed = true
					break
				}
			}
			if !allowed {
				continue
			}
		}
		if adapter.hub == nil {
			continue
		}
		if message.Channel == "" {
			_ = adapter.hub.Broadcast(message.Frame)
			continue
		}
		_ = adapter.hub.Publish(message.Channel, message.Frame)
	}
}
