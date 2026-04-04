// SPDX-License-Identifier: EUPL-1.2

package stream

// HubStats captures a hub snapshot at a point in time.
//
//	stats := hub.Stats()
//	core.Print(nil, "peers=%d channels=%d", stats.Peers, stats.Channels)
type HubStats struct {
	// Peers is the number of currently connected peers across all transports.
	Peers int `json:"peers"`

	// Channels is the number of active named channels with at least one subscriber.
	Channels int `json:"channels"`

	// SubscriberCount maps channel name to subscriber count.
	SubscriberCount map[string]int `json:"subscriber_count"`
}
