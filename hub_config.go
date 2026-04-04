// SPDX-License-Identifier: EUPL-1.2

package stream

import "time"

// HubConfig controls hub behaviour and lifecycle callbacks.
//
//	cfg := stream.HubConfig{
//	    HeartbeatInterval: 30 * time.Second,
//	    OnConnect:         func(p *stream.Peer) { metrics.Inc("peers") },
//	    ChannelAuthoriser: func(p *stream.Peer, ch string) bool {
//	        return p.Claims["role"] == "admin" || ch == "public"
//	    },
//	}
type HubConfig struct {
	// HeartbeatInterval is the server-side ping interval for WebSocket peers.
	// Defaults to 30 seconds. Ignored by SSE and TCP adapters.
	HeartbeatInterval time.Duration

	// PongTimeout is the deadline after a ping before the WS connection is closed.
	// Must be greater than HeartbeatInterval. Defaults to 60 seconds.
	PongTimeout time.Duration

	// WriteTimeout is the per-write deadline for WS and TCP adapters.
	// Defaults to 10 seconds.
	WriteTimeout time.Duration

	// OnConnect is called when a peer registers. Optional.
	//
	//	OnConnect: func(p *stream.Peer) { metrics.Inc("peers") },
	OnConnect func(peer *Peer)

	// OnDisconnect is called when a peer unregisters. Optional.
	OnDisconnect func(peer *Peer)

	// ChannelAuthoriser optionally decides whether a peer may subscribe to a channel.
	// Return true to allow. When nil, all subscriptions are allowed.
	//
	//	ChannelAuthoriser: func(p *stream.Peer, ch string) bool {
	//	    return p.Claims["role"] == "admin" || ch == "public"
	//	},
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
