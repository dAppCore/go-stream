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

	mu            sync.RWMutex
	cancel        context.CancelFunc
	pubsub        *redis.PubSub
	client        *redis.Client
	publishStop   func()
	broadcastStop func()
}

type envelope struct {
	SourceID string `json:"s"`
	Frame    []byte `json:"f"`
}

// NewBridge creates and validates the Redis connection. Does not start listening.
func NewBridge(hub *stream.Hub, config Config) (*Bridge, error) {
	if hub == nil {
		return nil, core.E("stream.redis", "nil hub", nil)
	}
	if config.Addr == "" {
		return nil, core.E("stream.redis", "empty address", nil)
	}
	if config.Prefix == "" {
		config.Prefix = "stream"
	}
	client := newRedisClient(config)
	defer client.Close()

	pingContext, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := client.Ping(pingContext).Err(); err != nil {
		return nil, core.E("stream.redis", "redis ping failed", err)
	}

	return &Bridge{
		hub:      hub,
		config:   config,
		sourceID: stream.NewPeer("redis").ID,
	}, nil
}

// Start begins the Redis pub/sub listener. Blocks in a goroutine until Stop() or ctx cancel.
func (bridge *Bridge) Start(ctx context.Context) error {
	if bridge == nil {
		return core.E("stream.redis", "nil bridge", nil)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	runContext, runCancel := context.WithCancel(ctx)
	client := newRedisClient(bridge.config)
	pubsub := client.PSubscribe(runContext, bridge.broadcastChannel(), bridge.channelPattern())
	publishStop := bridge.hub.SubscribePublished(func(channel string, frame []byte) {
		if channel == "" {
			return
		}
		_ = bridge.publishWithClient(client, bridge.channelKey(channel), frame)
	})
	broadcastStop := bridge.hub.SubscribeBroadcast(func(frame []byte) {
		_ = bridge.publishWithClient(client, bridge.broadcastChannel(), frame)
	})

	bridge.mu.Lock()
	bridge.cancel = runCancel
	bridge.client = client
	bridge.pubsub = pubsub
	bridge.publishStop = publishStop
	bridge.broadcastStop = broadcastStop
	bridge.mu.Unlock()

	defer func() {
		bridge.mu.Lock()
		publishStop := bridge.publishStop
		broadcastStop := bridge.broadcastStop
		bridge.cancel = nil
		bridge.client = nil
		bridge.pubsub = nil
		bridge.publishStop = nil
		bridge.broadcastStop = nil
		bridge.mu.Unlock()
		if publishStop != nil {
			publishStop()
		}
		if broadcastStop != nil {
			broadcastStop()
		}
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
		if decoded.SourceID == bridge.sourceID {
			continue
		}

		channel := bridge.channelFromRedis(message.Channel)
		if channel == "" {
			_ = bridge.hub.BroadcastFromBridge(decoded.Frame)
			continue
		}
		_ = bridge.hub.PublishFromBridge(channel, decoded.Frame)
	}
}

// Stop cleanly shuts down the bridge. Closes the pub/sub subscription and Redis client.
func (bridge *Bridge) Stop() error {
	if bridge == nil {
		return nil
	}

	bridge.mu.RLock()
	cancel := bridge.cancel
	pubsub := bridge.pubsub
	client := bridge.client
	publishStop := bridge.publishStop
	broadcastStop := bridge.broadcastStop
	bridge.mu.RUnlock()

	if cancel != nil {
		cancel()
	}
	if publishStop != nil {
		publishStop()
	}
	if broadcastStop != nil {
		broadcastStop()
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
func (bridge *Bridge) PublishToChannel(channel string, frame []byte) error {
	if bridge == nil {
		return core.E("stream.redis", "nil bridge", nil)
	}
	if channel == "" {
		return core.E("stream.redis", "empty channel", nil)
	}

	return bridge.publish(bridge.channelKey(channel), frame)
}

// PublishBroadcast publishes frame as a broadcast via Redis.
func (bridge *Bridge) PublishBroadcast(frame []byte) error {
	if bridge == nil {
		return core.E("stream.redis", "nil bridge", nil)
	}

	return bridge.publish(bridge.broadcastChannel(), frame)
}

// SourceID returns the random instance identifier.
func (bridge *Bridge) SourceID() string {
	if bridge == nil {
		return ""
	}
	return bridge.sourceID
}

func (bridge *Bridge) publish(channel string, frame []byte) error {
	bridge.mu.RLock()
	client := bridge.client
	bridge.mu.RUnlock()
	if client == nil {
		client = newRedisClient(bridge.config)
		defer client.Close()
	}

	return bridge.publishWithClient(client, channel, frame)
}

func (bridge *Bridge) publishWithClient(client *redis.Client, channel string, frame []byte) error {
	if client == nil {
		return core.E("stream.redis", "nil redis client", nil)
	}

	payload := envelope{
		SourceID: bridge.sourceID,
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

func (bridge *Bridge) broadcastChannel() string {
	return bridge.config.Prefix + ":broadcast"
}

func (bridge *Bridge) channelKey(channel string) string {
	return bridge.config.Prefix + ":channel:" + channel
}

func (bridge *Bridge) channelPattern() string {
	return bridge.config.Prefix + ":channel:*"
}

func (bridge *Bridge) channelFromRedis(channel string) string {
	if channel == bridge.broadcastChannel() {
		return ""
	}
	return core.TrimPrefix(channel, bridge.config.Prefix+":channel:")
}

func newRedisClient(cfg Config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:      cfg.Addr,
		Password:  cfg.Password,
		DB:        cfg.DB,
		TLSConfig: cfg.TLSConfig,
	})
}
