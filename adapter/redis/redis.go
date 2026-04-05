// SPDX-License-Identifier: EUPL-1.2

// Package redis bridges a hub through Redis pub/sub.
//
//	bridge, err := redis.NewBridge(hub, redis.Config{Addr: "redis:6379", Prefix: "pool"})
//	if err != nil {
//	    return err
//	}
//	go bridge.Start(ctx)
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

// config := redis.Config{Addr: "127.0.0.1:6379", Prefix: "pool"}
type Config struct {
	Addr      string
	Password  string
	DB        int
	Prefix    string
	TLSConfig *tls.Config
}

// bridge, err := redis.NewBridge(hub, redis.Config{Addr: "127.0.0.1:6379", Prefix: "pool"})
// if err != nil {
//     return err
// }
// go bridge.Start(ctx)
// defer bridge.Stop()
type Bridge struct {
	hub      *stream.Hub
	config   Config
	sourceID string

	mu            sync.RWMutex
	running       bool
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

// bridge, err := redis.NewBridge(hub, config)
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

// go bridge.Start(ctx)
//
// The bridge installs publish and broadcast hooks on the hub, then relays those
// frames through Redis pub/sub until the context is cancelled or Stop is called.
func (bridge *Bridge) Start(ctx context.Context) error {
	if bridge == nil {
		return core.E("stream.redis", "nil bridge", nil)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	bridge.mu.Lock()
	if bridge.running {
		bridge.mu.Unlock()
		return nil
	}
	bridge.running = true
	bridge.mu.Unlock()

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
		bridge.running = false
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

// defer bridge.Stop()
//
// Stop cancels the running bridge, removes the Redis hooks, and closes the
// underlying client and pub/sub session.
func (bridge *Bridge) Stop() error {
	if bridge == nil {
		return nil
	}

	bridge.mu.RLock()
	running := bridge.running
	cancel := bridge.cancel
	pubsub := bridge.pubsub
	client := bridge.client
	publishStop := bridge.publishStop
	broadcastStop := bridge.broadcastStop
	bridge.mu.RUnlock()

	if !running {
		return nil
	}

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

// _ = bridge.PublishToChannel("block", templateBytes)
//
// PublishToChannel preserves the channel name so all subscribers on other
// instances receive the frame on the same logical route.
func (bridge *Bridge) PublishToChannel(channel string, frame []byte) error {
	if bridge == nil {
		return core.E("stream.redis", "nil bridge", nil)
	}
	if channel == "" {
		return core.E("stream.redis", "empty channel", nil)
	}

	return bridge.publish(bridge.channelKey(channel), frame)
}

// _ = bridge.PublishBroadcast(shutdownFrame)
//
// PublishBroadcast delivers a frame to every bridge instance without channel
// filtering.
func (bridge *Bridge) PublishBroadcast(frame []byte) error {
	if bridge == nil {
		return core.E("stream.redis", "nil bridge", nil)
	}

	return bridge.publish(bridge.broadcastChannel(), frame)
}

// SourceID exposes the bridge instance identifier used for echo prevention.
//
//	id := bridge.SourceID()
func (bridge *Bridge) SourceID() string {
	if bridge == nil {
		return ""
	}
	return bridge.sourceID
}

func (bridge *Bridge) publish(channel string, frame []byte) error {
	bridge.mu.RLock()
	running := bridge.running
	client := bridge.client
	bridge.mu.RUnlock()
	if !running {
		return core.E("stream.redis", "bridge not started", nil)
	}
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

func newRedisClient(config Config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:      config.Addr,
		Password:  config.Password,
		DB:        config.DB,
		TLSConfig: config.TLSConfig,
	})
}
