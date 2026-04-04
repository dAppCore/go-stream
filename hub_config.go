// SPDX-License-Identifier: EUPL-1.2

package stream

import "time"

//	config := stream.HubConfig{
//	    HeartbeatInterval: 30 * time.Second,
//	    OnConnect:         func(peer *stream.Peer) { metrics.Inc("peers") },
//	    ChannelAuthoriser: func(peer *stream.Peer, channel string) bool {
//	        return peer.Claims["role"] == "admin" || channel == "public"
//	    },
//	}
type HubConfig struct {
	// HeartbeatInterval is the server-side ping interval for WebSocket peers.
	// Example: `HeartbeatInterval: 30 * time.Second`.
	// Defaults to 30 seconds. Ignored by SSE and TCP adapters.
	HeartbeatInterval time.Duration

	// PongTimeout is the deadline after a ping before the WS connection is closed.
	// Example: `PongTimeout: 60 * time.Second`.
	// Must be greater than HeartbeatInterval. Defaults to 60 seconds.
	PongTimeout time.Duration

	// WriteTimeout is the per-write deadline for WS and TCP adapters.
	// Example: `WriteTimeout: 10 * time.Second`.
	// Defaults to 10 seconds.
	WriteTimeout time.Duration

	// OnConnect runs when a peer registers.
	//
	//		OnConnect: func(peer *stream.Peer) { metrics.Inc("peers") },
	OnConnect func(peer *Peer)

	// OnDisconnect runs when a peer unregisters.
	OnDisconnect func(peer *Peer)

	// ChannelAuthoriser decides whether a peer may subscribe to a channel.
	//
	//		ChannelAuthoriser: func(peer *stream.Peer, channel string) bool {
	//		    return peer.Claims["role"] == "admin" || channel == "public"
	//		},
	// When nil, all subscriptions are allowed.
	ChannelAuthoriser func(peer *Peer, channel string) bool
}

// DefaultHubConfig returns sensible defaults.
//
//	config := stream.DefaultHubConfig()
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
