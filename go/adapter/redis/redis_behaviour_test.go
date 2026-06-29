// SPDX-License-Identifier: EUPL-1.2

// Behaviour-level tests for the redis bridge that do not require a live
// redis server: construction guards, channel-key derivation, source-id
// generation, the broadcast/channel round-trip naming, and nil-bridge
// safety on every exported method. The Start/publish paths that touch a
// real redis are left to integration coverage.
package redis

import (
	"context"

	core "dappco.re/go"
	"dappco.re/go/stream"
)

func TestRedis_NewBridge_Guards_Behaviour(t *core.T) {
	core.AssertFalse(t, NewBridge(nil, Config{Addr: "127.0.0.1:6379"}).OK, "nil hub rejected")
	core.AssertFalse(t, NewBridge(stream.NewHub(), Config{}).OK, "empty addr rejected")

	// A non-routable address fails the ping inside NewBridge, exercising
	// the connect-failure branch without a live server.
	r := NewBridge(stream.NewHub(), Config{Addr: "127.0.0.1:1"})
	core.AssertFalse(t, r.OK, "unreachable redis fails the ping")
}

func TestRedis_ChannelNaming_Behaviour(t *core.T) {
	bridge := &Bridge{config: Config{Prefix: "pool"}, sourceID: "node-a"}

	core.AssertEqual(t, "pool:broadcast", bridge.broadcastChannel(), "broadcast channel name")
	core.AssertEqual(t, "pool:channel:block", bridge.channelKey("block"), "channel key name")
	core.AssertEqual(t, "pool:channel:*", bridge.channelPattern(), "channel pattern")

	// channelFromRedis reverses channelKey and recognises broadcast.
	core.AssertEqual(t, "block", bridge.channelFromRedis("pool:channel:block"), "channel decoded")
	core.AssertEqual(t, "", bridge.channelFromRedis("pool:broadcast"), "broadcast maps to empty channel")
}

func TestRedis_SourceID_Behaviour(t *core.T) {
	idA := randomSourceID()
	idB := randomSourceID()
	core.AssertNotEqual(t, idA, idB, "source ids are unique")
	core.AssertEqual(t, 36, len(idA), "source id is a UUID")

	bridge := &Bridge{sourceID: idA}
	core.AssertEqual(t, idA, bridge.SourceID(), "SourceID returns the assigned id")

	var nilBridge *Bridge
	core.AssertEqual(t, "", nilBridge.SourceID(), "nil bridge source id is empty")
}

func TestRedis_PublishWithNilClient_Behaviour(t *core.T) {
	bridge := &Bridge{config: Config{Prefix: "pool"}, sourceID: "node-a"}
	core.AssertFalse(t, bridge.publishWithClient(nil, "pool:channel:block", []byte("x")).OK, "nil client rejected")
}

func TestRedis_PublishBeforeStart_Behaviour(t *core.T) {
	bridge := &Bridge{config: Config{Prefix: "pool"}, sourceID: "node-a"}
	core.AssertFalse(t, bridge.PublishToChannel("block", []byte("x")).OK, "publish before start fails")
	core.AssertFalse(t, bridge.PublishToChannel("", []byte("x")).OK, "empty channel rejected")
	core.AssertFalse(t, bridge.PublishBroadcast([]byte("x")).OK, "broadcast before start fails")
}

func TestRedis_NilBridge_Behaviour(t *core.T) {
	var bridge *Bridge
	core.AssertFalse(t, bridge.Start(context.Background()).OK, "nil bridge start fails")
	core.AssertTrue(t, bridge.Stop().OK, "nil bridge stop is a no-op success")
	core.AssertFalse(t, bridge.PublishToChannel("block", []byte("x")).OK, "nil bridge publish fails")
	core.AssertFalse(t, bridge.PublishBroadcast([]byte("x")).OK, "nil bridge broadcast fails")
}

func TestRedis_StopWhenNotRunning_Behaviour(t *core.T) {
	bridge := &Bridge{config: Config{Prefix: "pool"}, sourceID: "node-a"}
	core.AssertTrue(t, bridge.Stop().OK, "stop on a never-started bridge is a no-op success")
}

func TestRedis_NewRedisClient_Behaviour(t *core.T) {
	client := newRedisClient(Config{Addr: "127.0.0.1:6379", DB: 3})
	core.AssertNotNil(t, client, "client constructed")
	_ = client.Close()
}
