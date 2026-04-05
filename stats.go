// SPDX-License-Identifier: EUPL-1.2

package stream

// stats := hub.Stats()
// core.Print(nil, "peers=%d channels=%d", stats.Peers, stats.Channels)
// // Example: peers=12 channels=4
type HubStats struct {
	// Peers is the number of currently connected peers across all transports.
	//
	// Example: peers=12
	Peers int `json:"peers"`

	// Channels is the number of active named channels with at least one subscriber.
	//
	// Example: channels=4
	Channels int `json:"channels"`

	// SubscriberCount maps channel name to subscriber count.
	//
	// Example: {"hashrate": 3, "block": 2}
	SubscriberCount map[string]int `json:"subscriber_count"`
}
