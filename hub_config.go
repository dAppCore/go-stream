// SPDX-License-Identifier: EUPL-1.2

package stream

import "time"

//	cfg := stream.HubConfig{
//	    HeartbeatInterval: 30 * time.Second,
//	    PongTimeout:       60 * time.Second,
//	    WriteTimeout:      10 * time.Second,
//	    OnConnect: func(peer *stream.Peer) {
//	        metrics.Inc("peers")
//	    },
//	}
type HubConfig struct {
	// stream.NewHubWithConfig(stream.HubConfig{HeartbeatInterval: 30 * time.Second})
	// Keeps WebSocket peers alive. SSE and TCP adapters ignore it.
	HeartbeatInterval time.Duration

	// stream.NewHubWithConfig(stream.HubConfig{PongTimeout: 60 * time.Second})
	// Closes stale WebSocket peers after a ping. Keep it above HeartbeatInterval.
	PongTimeout time.Duration

	// stream.NewHubWithConfig(stream.HubConfig{WriteTimeout: 10 * time.Second})
	// Bounds each WebSocket or TCP write.
	WriteTimeout time.Duration

	// stream.NewHubWithConfig(stream.HubConfig{
	//     OnConnect: func(peer *stream.Peer) { metrics.Inc("peers") },
	// })
	OnConnect func(peer *Peer)

	// stream.NewHubWithConfig(stream.HubConfig{
	//     OnDisconnect: func(peer *stream.Peer) { metrics.Dec("peers") },
	// })
	OnDisconnect func(peer *Peer)

	// stream.NewHubWithConfig(stream.HubConfig{
	//     ChannelAuthoriser: func(peer *stream.Peer, channel string) bool {
	//         return peer.Claims["role"] == "admin" || channel == "public"
	//     },
	// })
	// When nil, all subscriptions are allowed.
	ChannelAuthoriser func(peer *Peer, channel string) bool
}

// DefaultHubConfig returns sensible defaults.
//
//	cfg := stream.DefaultHubConfig()
func DefaultHubConfig() HubConfig {
	return HubConfig{
		HeartbeatInterval: 30 * time.Second,
		PongTimeout:       60 * time.Second,
		WriteTimeout:      10 * time.Second,
	}
}

func normalizeHubConfig(config HubConfig) HubConfig {
	defaults := DefaultHubConfig()
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = defaults.HeartbeatInterval
	}
	if config.PongTimeout == 0 {
		config.PongTimeout = defaults.PongTimeout
	}
	if config.PongTimeout <= config.HeartbeatInterval {
		config.PongTimeout = config.HeartbeatInterval * 2
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = defaults.WriteTimeout
	}
	return config
}
