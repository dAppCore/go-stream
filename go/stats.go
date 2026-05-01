// SPDX-License-Identifier: EUPL-1.2

package stream

// stats := hub.Stats()
// core.Print("stream", "peers=%d channels=%d", stats.Peers, stats.Channels)
type HubStats struct {
	// stats := hub.Stats()
	// core.Print("stream", "peers=%d", stats.Peers)
	Peers int `json:"peers"`

	// stats := hub.Stats()
	// core.Print("stream", "channels=%d", stats.Channels)
	Channels int `json:"channels"`

	// stats := hub.Stats()
	// count := stats.SubscriberCount["hashrate"]
	SubscriberCount map[string]int `json:"subscriber_count"`
}
