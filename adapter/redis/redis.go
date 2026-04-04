// SPDX-License-Identifier: EUPL-1.2

// Package redis is the Redis pub/sub bridge for stream.Hub.
// Enables cross-instance coordination: multiple Hub instances on different nodes
// using the same Redis backend coordinate broadcasts and channel messages transparently.
package redis

import (
	"context"
	"crypto/tls"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"dappco.re/go/core"
	"dappco.re/go/stream"
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

	mu     sync.RWMutex
	cancel context.CancelFunc
	pubsub *redis.PubSub
	client *redis.Client
}

type envelope struct {
	SourceID string `json:"s"`
	Frame    []byte `json:"f"`
}

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
	client := newRedisClient(cfg)
	defer client.Close()

	pingContext, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := client.Ping(pingContext).Err(); err != nil {
		return nil, core.E("stream.redis", "redis ping failed", err)
	}

	return &Bridge{
		hub:      hub,
		config:   cfg,
		sourceID: stream.NewPeer("redis").ID,
	}, nil
}

// Start begins the Redis pub/sub listener. Blocks in a goroutine until Stop() or ctx cancel.
func (b *Bridge) Start(ctx context.Context) error {
	if b == nil {
		return core.E("stream.redis", "nil bridge", nil)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	runContext, runCancel := context.WithCancel(ctx)
	client := newRedisClient(b.config)
	pubsub := client.PSubscribe(runContext, b.broadcastChannel(), b.channelPattern())

	b.mu.Lock()
	b.cancel = runCancel
	b.client = client
	b.pubsub = pubsub
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		b.cancel = nil
		b.client = nil
		b.pubsub = nil
		b.mu.Unlock()
		runCancel()
		_ = pubsub.Close()
		_ = client.Close()
	}()

	for {
		message, err := pubsub.ReceiveMessage(runContext)
		if err != nil {
			if runContext.Err() != nil {
				return nil
			}
			return err
		}

		var decoded envelope
		if !core.JSONUnmarshal([]byte(message.Payload), &decoded).OK {
			continue
		}
		if decoded.SourceID == b.sourceID {
			continue
		}

		channel := b.channelFromRedis(message.Channel)
		if channel == "" {
			_ = b.hub.Broadcast(decoded.Frame)
			continue
		}
		_ = b.hub.Publish(channel, decoded.Frame)
	}
}

// Stop cleanly shuts down the bridge. Closes the pub/sub subscription and Redis client.
func (b *Bridge) Stop() error {
	if b == nil {
		return nil
	}

	b.mu.RLock()
	cancel := b.cancel
	pubsub := b.pubsub
	client := b.client
	b.mu.RUnlock()

	if cancel != nil {
		cancel()
	}
	if pubsub != nil {
		_ = pubsub.Close()
	}
	if client != nil {
		return client.Close()
	}
	return nil
}

// PublishToChannel publishes frame to a specific hub channel via Redis.
func (b *Bridge) PublishToChannel(channel string, frame []byte) error {
	if b == nil {
		return core.E("stream.redis", "nil bridge", nil)
	}
	if channel == "" {
		return core.E("stream.redis", "empty channel", nil)
	}

	return b.publish(b.channelKey(channel), frame)
}

// PublishBroadcast publishes frame as a broadcast via Redis.
func (b *Bridge) PublishBroadcast(frame []byte) error {
	if b == nil {
		return core.E("stream.redis", "nil bridge", nil)
	}

	return b.publish(b.broadcastChannel(), frame)
}

// SourceID returns the random instance identifier.
func (b *Bridge) SourceID() string {
	if b == nil {
		return ""
	}
	return b.sourceID
}

func (b *Bridge) publish(channel string, frame []byte) error {
	client := newRedisClient(b.config)
	defer client.Close()

	payload := envelope{
		SourceID: b.sourceID,
		Frame:    append([]byte(nil), frame...),
	}
	encoded := core.JSONMarshal(payload)
	if !encoded.OK {
		if err, ok := encoded.Value.(error); ok {
			return err
		}
		return core.E("stream.redis", "failed to marshal envelope", nil)
	}

	publishContext, publishCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer publishCancel()
	return client.Publish(publishContext, channel, encoded.Value).Err()
}

func (b *Bridge) broadcastChannel() string {
	return b.config.Prefix + ":broadcast"
}

func (b *Bridge) channelKey(channel string) string {
	return b.config.Prefix + ":channel:" + channel
}

func (b *Bridge) channelPattern() string {
	return b.config.Prefix + ":channel:*"
}

func (b *Bridge) channelFromRedis(channel string) string {
	if channel == b.broadcastChannel() {
		return ""
	}
	return core.TrimPrefix(channel, b.config.Prefix+":channel:")
}

func newRedisClient(cfg Config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:      cfg.Addr,
		Password:  cfg.Password,
		DB:        cfg.DB,
		TLSConfig: cfg.TLSConfig,
	})
}
