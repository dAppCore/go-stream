// SPDX-License-Identifier: EUPL-1.2

package stream

import "time"

//	cfg := stream.HubConfig{
//	    HeartbeatInterval: 30 * time.Second,
//	    OnConnect: func(peer *stream.Peer) {
//	        metrics.Inc("peers")
//	    },
//	    ChannelAuthoriser: func(peer *stream.Peer, channel string) bool {
//	        return peer.Claims["role"] == "admin" || channel == "public"
//	    },
//	}
type HubConfig struct {
	// HeartbeatInterval: 30 * time.Second keeps WebSocket clients alive.
	// Ignored by SSE and TCP adapters.
	HeartbeatInterval time.Duration

	// PongTimeout: 60 * time.Second closes stale WebSocket peers after a ping.
	// Must be greater than HeartbeatInterval.
	PongTimeout time.Duration

	// WriteTimeout: 10 * time.Second bounds each WebSocket or TCP write.
	WriteTimeout time.Duration

	// OnConnect: func(peer *stream.Peer) { metrics.Inc("peers") }.
	//
	//		OnConnect: func(peer *stream.Peer) { metrics.Inc("peers") },
	OnConnect func(peer *Peer)

	// OnDisconnect: func(peer *stream.Peer) { metrics.Dec("peers") }.
	OnDisconnect func(peer *Peer)

	// ChannelAuthoriser: func(peer *stream.Peer, channel string) bool {
	//     return peer.Claims["role"] == "admin" || channel == "public"
	// }.
	//
	//		ChannelAuthoriser: func(peer *stream.Peer, channel string) bool {
	//		    return peer.Claims["role"] == "admin" || channel == "public"
	//		},
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
