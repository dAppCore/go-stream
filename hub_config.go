// SPDX-License-Identifier: EUPL-1.2

package stream

import "time"

//	authoriser := stream.ChannelAuthoriser(func(peer *stream.Peer, channel string) bool {
//	    return peer.Claims["role"] == "admin" || channel == "public"
//	})
type ChannelAuthoriser func(peer *Peer, channel string) bool

//	config := stream.HubConfig{
//	    HeartbeatInterval: 30 * time.Second,
//	    PongTimeout:       60 * time.Second,
//	    WriteTimeout:      10 * time.Second,
//	    OnConnect: func(peer *stream.Peer) {
//	        metrics.Inc("peers")
//	    },
//	}
type HubConfig struct {
	// config := stream.HubConfig{HeartbeatInterval: 30 * time.Second}
	HeartbeatInterval time.Duration

	// config := stream.HubConfig{PongTimeout: 60 * time.Second}
	PongTimeout time.Duration

	// config := stream.HubConfig{WriteTimeout: 10 * time.Second}
	WriteTimeout time.Duration

	// config := stream.HubConfig{OnConnect: func(peer *stream.Peer) { metrics.Inc("peers") }}
	OnConnect func(peer *Peer)

	// config := stream.HubConfig{OnDisconnect: func(peer *stream.Peer) { metrics.Dec("peers") }}
	OnDisconnect func(peer *Peer)

	// config := stream.HubConfig{ChannelAuthoriser: func(peer *stream.Peer, channel string) bool {
	//     return peer.Claims["role"] == "admin" || channel == "public"
	// }}
	ChannelAuthoriser ChannelAuthoriser
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
