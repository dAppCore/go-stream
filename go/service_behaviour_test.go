// SPDX-License-Identifier: EUPL-1.2

// Behaviour-level tests for the stream Service action handlers and the
// remaining Hub convenience methods (bridge/peer publish variants, the
// TCP framing path, and the async enqueue fall-backs).
package stream

import (
	"context"
	"sync"
	"time"

	core "dappco.re/go"
)

// startedService builds a Service, runs its hub, and returns it with a
// stop function.
//
//	svc, stop := startedService(t)
//	defer stop()
func startedService(t *core.T) (*Service, func()) {
	t.Helper()
	c := core.New()
	r := NewService(DefaultHubConfig())(c)
	core.RequireTrue(t, r.OK)
	svc := r.Value.(*Service)
	svc.OnStartup(context.Background())
	waitFor(t, func() bool { return svc.Hub.Running() })
	return svc, func() { svc.OnShutdown(context.Background()) }
}

func TestService_HandlePublish_Behaviour(t *core.T) {
	svc, stop := startedService(t)
	defer stop()

	var mutex sync.Mutex
	var got string
	unsub := svc.Hub.Subscribe("alerts", func(frame []byte) {
		mutex.Lock()
		got = string(frame)
		mutex.Unlock()
	})
	defer unsub()

	r := svc.handlePublish(context.Background(), core.NewOptions(
		core.Option{Key: "channel", Value: "alerts"},
		core.Option{Key: "frame", Value: []byte("msg")},
	))
	core.AssertTrue(t, r.OK, "publish handler succeeds")
	waitFor(t, func() bool { mutex.Lock(); defer mutex.Unlock(); return got == "msg" })

	// String frames are coerced to bytes.
	r = svc.handlePublish(context.Background(), core.NewOptions(
		core.Option{Key: "channel", Value: "alerts"},
		core.Option{Key: "frame", Value: "stringframe"},
	))
	core.AssertTrue(t, r.OK, "string frame coerced")

	// Missing channel rejected.
	r = svc.handlePublish(context.Background(), core.NewOptions(
		core.Option{Key: "frame", Value: []byte("x")},
	))
	core.AssertFalse(t, r.OK, "missing channel rejected")

	// Missing frame rejected.
	r = svc.handlePublish(context.Background(), core.NewOptions(
		core.Option{Key: "channel", Value: "alerts"},
	))
	core.AssertFalse(t, r.OK, "missing frame rejected")

	// Wrong frame type rejected.
	r = svc.handlePublish(context.Background(), core.NewOptions(
		core.Option{Key: "channel", Value: "alerts"},
		core.Option{Key: "frame", Value: 42},
	))
	core.AssertFalse(t, r.OK, "non-bytes/string frame rejected")
}

func TestService_HandleBroadcast_Behaviour(t *core.T) {
	svc, stop := startedService(t)
	defer stop()

	peer := NewPeer("ws")
	svc.Hub.AddPeer(peer)
	waitFor(t, func() bool { return svc.Hub.PeerCount() == 1 })

	r := svc.handleBroadcast(context.Background(), core.NewOptions(
		core.Option{Key: "frame", Value: []byte("ping")},
	))
	core.AssertTrue(t, r.OK, "broadcast handler succeeds")
	core.AssertEqual(t, "ping", string(drainNext(t, peer)), "peer received broadcast")

	r = svc.handleBroadcast(context.Background(), core.NewOptions())
	core.AssertFalse(t, r.OK, "missing frame rejected")
}

func TestService_HandleSendChannel_Behaviour(t *core.T) {
	svc, stop := startedService(t)
	defer stop()

	peer := NewPeer("ws")
	svc.Hub.AddPeer(peer)
	svc.Hub.SubscribePeer(peer, "alerts")
	waitFor(t, func() bool { return svc.Hub.PeerCount() == 1 })

	r := svc.handleSendChannel(context.Background(), core.NewOptions(
		core.Option{Key: "channel", Value: "alerts"},
		core.Option{Key: "frame", Value: []byte("hello")},
	))
	core.AssertTrue(t, r.OK, "send_channel handler succeeds")
	core.AssertEqual(t, "hello", string(drainNext(t, peer)), "peer received frame")

	r = svc.handleSendChannel(context.Background(), core.NewOptions(
		core.Option{Key: "frame", Value: []byte("x")},
	))
	core.AssertFalse(t, r.OK, "missing channel rejected")
}

func TestService_HandleStatsRunningConfig_Behaviour(t *core.T) {
	svc, stop := startedService(t)
	defer stop()

	statsR := svc.handleStats(context.Background(), core.NewOptions())
	core.AssertTrue(t, statsR.OK, "stats handler succeeds")
	_, ok := statsR.Value.(HubStats)
	core.AssertTrue(t, ok, "stats value is HubStats")

	runningR := svc.handleRunning(context.Background(), core.NewOptions())
	core.AssertTrue(t, runningR.OK, "running handler succeeds")
	core.AssertEqual(t, true, runningR.Value.(bool), "hub reports running")

	configR := svc.handleConfig(context.Background(), core.NewOptions())
	core.AssertTrue(t, configR.OK, "config handler succeeds")
	_, ok = configR.Value.(HubConfig)
	core.AssertTrue(t, ok, "config value is HubConfig")
}

func TestService_HandlersNilSafe_Behaviour(t *core.T) {
	var s *Service
	core.AssertFalse(t, s.handlePublish(context.Background(), core.NewOptions()).OK, "nil publish")
	core.AssertFalse(t, s.handleBroadcast(context.Background(), core.NewOptions()).OK, "nil broadcast")
	core.AssertFalse(t, s.handleSendChannel(context.Background(), core.NewOptions()).OK, "nil send_channel")
	core.AssertFalse(t, s.handleStats(context.Background(), core.NewOptions()).OK, "nil stats")
	core.AssertFalse(t, s.handleRunning(context.Background(), core.NewOptions()).OK, "nil running")
	core.AssertFalse(t, s.handleConfig(context.Background(), core.NewOptions()).OK, "nil config")
}

func TestService_FrameBytes_Behaviour(t *core.T) {
	core.AssertTrue(t, frameBytes(core.Ok([]byte("x"))).OK, "bytes accepted")
	core.AssertTrue(t, frameBytes(core.Ok("x")).OK, "string accepted")
	core.AssertFalse(t, frameBytes(core.Fail(core.E("t", "missing", nil))).OK, "failed result propagates")
	core.AssertFalse(t, frameBytes(core.Ok(42)).OK, "wrong type rejected")
}

func TestStream_Hub_BridgeAndPeerVariants_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	var mutex sync.Mutex
	var published int
	stopPub := hub.SubscribePublished(func(string, []byte) {
		mutex.Lock()
		published++
		mutex.Unlock()
	})
	defer stopPub()

	// PublishFromBridge does NOT notify publish subscribers.
	core.AssertTrue(t, hub.PublishFromBridge("block", []byte("b")).OK, "bridge publish ok")
	// SendToChannel DOES notify publish subscribers.
	core.AssertTrue(t, hub.SendToChannel("block", []byte("c")).OK, "send to channel ok")
	waitFor(t, func() bool { mutex.Lock(); defer mutex.Unlock(); return published == 1 })

	time.Sleep(20 * time.Millisecond)
	mutex.Lock()
	core.AssertEqual(t, 1, published, "bridge publish skipped subscriber notification")
	mutex.Unlock()

	source := NewPeer("ws")
	core.AssertTrue(t, hub.PublishFromPeer(source, "block", []byte("d")).OK, "publish from peer ok")
	core.AssertTrue(t, hub.BroadcastFromPeer(source, []byte("e")).OK, "broadcast from peer ok")
	core.AssertTrue(t, hub.BroadcastFromBridge([]byte("f")).OK, "broadcast from bridge ok")

	// SubscribeE alias mirrors SubscribeWithError.
	r := hub.SubscribeE("chan", func([]byte) {})
	core.AssertTrue(t, r.OK, "SubscribeE ok")
	r.Value.(func())()
}

func TestStream_Hub_UnsubscribePeerMethod_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()
	peer := NewPeer("ws")
	hub.AddPeer(peer)
	hub.SubscribePeer(peer, "block")
	core.AssertEqual(t, 1, hub.ChannelSubscriberCount("block"), "subscribed")
	hub.UnsubscribePeer(peer, "block")
	core.AssertEqual(t, 0, hub.ChannelSubscriberCount("block"), "unsubscribed")
	// nil/empty guards do not panic.
	hub.UnsubscribePeer(nil, "block")
	hub.UnsubscribePeer(peer, "")
}

func TestStream_Hub_PipeMethod_Behaviour(t *core.T) {
	src, stopSrc := runHub(t)
	defer stopSrc()
	dst, stopDst := runHub(t)
	defer stopDst()

	var mutex sync.Mutex
	var got string
	unsub := dst.Subscribe("block", func(frame []byte) {
		mutex.Lock()
		got = string(frame)
		mutex.Unlock()
	})
	defer unsub()

	stopPipe := src.Pipe(dst)
	defer stopPipe()
	src.Publish("block", []byte("via-method"))
	waitFor(t, func() bool { mutex.Lock(); defer mutex.Unlock(); return got == "via-method" })
}

func TestStream_Hub_TCPTransportFraming_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	peer := NewPeer("tcp")
	hub.AddPeer(peer)
	hub.SubscribePeer(peer, "block")
	waitFor(t, func() bool { return hub.PeerCount() == 1 })

	hub.SendToChannel("block", []byte("payload"))
	frame := drainNext(t, peer)
	// TCP frames are length-prefixed: 4-byte total length, then channel
	// length, channel bytes, payload.
	core.AssertGreater(t, len(frame), len("payload"), "tcp frame is wrapped with length prefix")
	core.AssertContains(t, string(frame), "block", "channel encoded in tcp frame")
	core.AssertContains(t, string(frame), "payload", "payload encoded in tcp frame")
}

func TestStream_Hub_SubscribeBroadcastHandler_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	var mutex sync.Mutex
	var got string
	stopSub := hub.SubscribeBroadcast(func(frame []byte) {
		mutex.Lock()
		got = string(frame)
		mutex.Unlock()
	})
	defer stopSub()

	hub.Broadcast([]byte("everyone"))
	waitFor(t, func() bool { mutex.Lock(); defer mutex.Unlock(); return got == "everyone" })

	// nil hub / nil handler return a safe no-op stopper.
	var nilHub *Hub
	nilHub.SubscribeBroadcast(func([]byte) {})()
	hub.SubscribeBroadcast(nil)()
}

func TestStream_Hub_BroadcastToTCPPeer_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	peer := NewPeer("tcp")
	hub.AddPeer(peer)
	waitFor(t, func() bool { return hub.PeerCount() == 1 })

	hub.Broadcast([]byte("payload"))
	frame := drainNext(t, peer)
	// Broadcasts to tcp peers are wrapped with an empty channel.
	core.AssertGreater(t, len(frame), len("payload"), "tcp broadcast frame is length-prefixed")
	core.AssertContains(t, string(frame), "payload", "payload present in tcp broadcast")
}

func TestStream_EncodeTCPFrame_Behaviour(t *core.T) {
	frame := encodeTCPFrame("block", []byte("data"))
	// 4 (len) + 4 (channel len) + 5 (channel) + 4 (data) = 17
	core.AssertEqual(t, 4+4+len("block")+len("data"), len(frame), "tcp frame length is deterministic")

	empty := encodeTCPFrame("", []byte(""))
	core.AssertEqual(t, 8, len(empty), "empty frame is header-only")
}
