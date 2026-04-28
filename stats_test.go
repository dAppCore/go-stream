// SPDX-License-Identifier: EUPL-1.2

package stream

import (
	"context"
	"testing"

	"dappco.re/go"
)

func TestStats_HubStats_Good(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	peer := NewPeer("ws")
	if err := hub.AddPeer(peer); err != nil {
		t.Fatalf("AddPeer() error = %v", err)
	}
	defer hub.RemovePeer(peer)
	waitForPeerCount(t, hub, 1)

	if err := hub.SubscribePeer(peer, "hashrate"); err != nil {
		t.Fatalf("SubscribePeer() error = %v", err)
	}

	stats := hub.Stats()
	if stats.Peers != 1 {
		t.Fatalf("Stats().Peers = %d, want %d", stats.Peers, 1)
	}
	if stats.Channels != 1 {
		t.Fatalf("Stats().Channels = %d, want %d", stats.Channels, 1)
	}
	if stats.SubscriberCount["hashrate"] != 1 {
		t.Fatalf("Stats().SubscriberCount[hashrate] = %d, want %d", stats.SubscriberCount["hashrate"], 1)
	}
}

func TestStats_HubStats_Bad(t *testing.T) {
	// Stats on a nil hub returns zero values.
	var hub *Hub
	stats := hub.Stats()
	if stats.Peers != 0 {
		t.Fatalf("nil hub Stats().Peers = %d, want %d", stats.Peers, 0)
	}
	if stats.Channels != 0 {
		t.Fatalf("nil hub Stats().Channels = %d, want %d", stats.Channels, 0)
	}
}

func TestStats_HubStats_Ugly(t *testing.T) {
	// Stats called after all peers are removed returns zero peers.
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	peer := NewPeer("ws")
	if err := hub.AddPeer(peer); err != nil {
		t.Fatalf("AddPeer() error = %v", err)
	}
	if err := hub.SubscribePeer(peer, "hashrate"); err != nil {
		t.Fatalf("SubscribePeer() error = %v", err)
	}
	hub.RemovePeer(peer)
	waitForPeerCount(t, hub, 0)

	stats := hub.Stats()
	if stats.Peers != 0 {
		t.Fatalf("Stats().Peers after remove = %d, want %d", stats.Peers, 0)
	}
}

func TestStats_HubStats_JSONTags_Good(t *testing.T) {
	// Verify HubStats serialises with the expected JSON field names.
	stats := HubStats{
		Peers:           3,
		Channels:        2,
		SubscriberCount: map[string]int{"hashrate": 2, "block": 1},
	}
	result := core.JSONMarshal(stats)
	if !result.OK {
		t.Fatalf("JSONMarshal(HubStats) failed: %v", result.Value)
	}
	serialised := string(result.Value.([]byte))
	if !core.Contains(serialised, `"peers":3`) {
		t.Fatalf("JSON missing peers field: %s", serialised)
	}
	if !core.Contains(serialised, `"channels":2`) {
		t.Fatalf("JSON missing channels field: %s", serialised)
	}
	if !core.Contains(serialised, `"subscriber_count"`) {
		t.Fatalf("JSON missing subscriber_count field: %s", serialised)
	}
}

func TestStats_PeerCount_Good(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	if hub.PeerCount() != 0 {
		t.Fatalf("PeerCount() = %d, want %d", hub.PeerCount(), 0)
	}

	peer := NewPeer("ws")
	if err := hub.AddPeer(peer); err != nil {
		t.Fatalf("AddPeer() error = %v", err)
	}
	waitForPeerCount(t, hub, 1)

	if hub.PeerCount() != 1 {
		t.Fatalf("PeerCount() = %d, want %d", hub.PeerCount(), 1)
	}
	hub.RemovePeer(peer)
}

func TestStats_PeerCount_Bad(t *testing.T) {
	// PeerCount on nil hub returns 0.
	var hub *Hub
	if hub.PeerCount() != 0 {
		t.Fatalf("nil hub PeerCount() = %d, want %d", hub.PeerCount(), 0)
	}
}

func TestStats_ChannelCount_Good(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	unsubscribe := hub.Subscribe("events", func([]byte) {})
	defer unsubscribe()

	if hub.ChannelCount() != 1 {
		t.Fatalf("ChannelCount() = %d, want %d", hub.ChannelCount(), 1)
	}
}

func TestStats_ChannelCount_Bad(t *testing.T) {
	// ChannelCount on nil hub returns 0.
	var hub *Hub
	if hub.ChannelCount() != 0 {
		t.Fatalf("nil hub ChannelCount() = %d, want %d", hub.ChannelCount(), 0)
	}
}

func TestStats_ChannelSubscriberCount_Good(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	// Peer subscriber.
	peer := NewPeer("ws")
	if err := hub.AddPeer(peer); err != nil {
		t.Fatalf("AddPeer() error = %v", err)
	}
	defer hub.RemovePeer(peer)

	if err := hub.SubscribePeer(peer, "hashrate"); err != nil {
		t.Fatalf("SubscribePeer() error = %v", err)
	}

	// Handler subscriber.
	unsubscribe := hub.Subscribe("hashrate", func([]byte) {})
	defer unsubscribe()

	count := hub.ChannelSubscriberCount("hashrate")
	if count != 2 {
		t.Fatalf("ChannelSubscriberCount(hashrate) = %d, want %d", count, 2)
	}
}

func TestStats_ChannelSubscriberCount_Bad(t *testing.T) {
	hub := NewHub()
	// Channel with no subscribers returns 0.
	count := hub.ChannelSubscriberCount("nonexistent")
	if count != 0 {
		t.Fatalf("ChannelSubscriberCount(nonexistent) = %d, want %d", count, 0)
	}
}

func TestStats_AllPeers_Good(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	peer1 := NewPeer("ws")
	peer2 := NewPeer("sse")
	_ = hub.AddPeer(peer1)
	_ = hub.AddPeer(peer2)
	defer hub.RemovePeer(peer1)
	defer hub.RemovePeer(peer2)
	waitForPeerCount(t, hub, 2)

	count := 0
	for range hub.AllPeers() {
		count++
	}
	if count != 2 {
		t.Fatalf("AllPeers() count = %d, want %d", count, 2)
	}
}

func TestStats_AllPeers_Bad(t *testing.T) {
	// AllPeers on nil hub yields no peers.
	var hub *Hub
	count := 0
	for range hub.AllPeers() {
		count++
	}
	if count != 0 {
		t.Fatalf("nil hub AllPeers() count = %d, want %d", count, 0)
	}
}

func TestStats_AllChannels_Good(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	unsub1 := hub.Subscribe("block", func([]byte) {})
	unsub2 := hub.Subscribe("hashrate", func([]byte) {})
	defer unsub1()
	defer unsub2()

	channels := make([]string, 0, 2)
	for channel := range hub.AllChannels() {
		channels = append(channels, channel)
	}
	if len(channels) != 2 {
		t.Fatalf("AllChannels() count = %d, want %d", len(channels), 2)
	}
	// Channels should be sorted.
	if channels[0] != "block" || channels[1] != "hashrate" {
		t.Fatalf("AllChannels() = %v, want [block, hashrate]", channels)
	}
}

func TestStats_AllChannels_Bad(t *testing.T) {
	// AllChannels on nil hub yields no channels.
	var hub *Hub
	count := 0
	for range hub.AllChannels() {
		count++
	}
	if count != 0 {
		t.Fatalf("nil hub AllChannels() count = %d, want %d", count, 0)
	}
}
