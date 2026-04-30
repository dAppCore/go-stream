// SPDX-License-Identifier: EUPL-1.2

package redis

import (
	"github.com/alicebob/miniredis/v2"

	core "dappco.re/go"
	"dappco.re/go/stream"
)

func ax7StartedBridge(t *core.T) (*Bridge, core.CancelFunc) {
	redisServer := miniredis.RunT(t)
	hub := stream.NewHub()
	ctx, cancel := core.WithCancel(core.Background())
	go hub.Run(ctx)

	bridge, err := NewBridge(hub, Config{Addr: redisServer.Addr(), Prefix: "pool"})
	core.RequireNoError(t, err)
	go func() {
		if err := bridge.Start(ctx); err != nil {
			t.Errorf("Start() error = %v", err)
		}
	}()
	core.Sleep(100 * core.Millisecond)
	return bridge, cancel
}

func TestAX7_NewBridge_Good(t *core.T) {
	redisServer := miniredis.RunT(t)
	hub := stream.NewHub()

	bridge, err := NewBridge(hub, Config{Addr: redisServer.Addr()})
	core.AssertNoError(t, err)
	core.AssertEqual(t, hub, bridge.hub)
	core.AssertEqual(t, "stream", bridge.config.Prefix)
}

func TestAX7_NewBridge_Bad(t *core.T) {
	redisServer := miniredis.RunT(t)

	bridge, err := NewBridge(nil, Config{Addr: redisServer.Addr()})
	core.AssertError(t, err)
	core.AssertNil(t, bridge)
}

func TestAX7_NewBridge_Ugly(t *core.T) {
	redisServer := miniredis.RunT(t)
	hub := stream.NewHub()

	left, err := NewBridge(hub, Config{Addr: redisServer.Addr(), Prefix: "pool"})
	core.RequireNoError(t, err)
	right, err := NewBridge(hub, Config{Addr: redisServer.Addr(), Prefix: "pool"})
	core.RequireNoError(t, err)
	core.AssertNotEqual(t, left.SourceID(), right.SourceID())
}

func TestAX7_Bridge_SourceID_Good(t *core.T) {
	redisServer := miniredis.RunT(t)
	bridge, err := NewBridge(stream.NewHub(), Config{Addr: redisServer.Addr(), Prefix: "pool"})
	core.RequireNoError(t, err)

	core.AssertNotEmpty(t, bridge.SourceID())
	core.AssertEqual(t, 36, core.RuneCount(bridge.SourceID()))
}

func TestAX7_Bridge_SourceID_Bad(t *core.T) {
	var bridge *Bridge

	core.AssertEqual(t, "", bridge.SourceID())
	core.AssertNil(t, bridge)
}

func TestAX7_Bridge_SourceID_Ugly(t *core.T) {
	redisServer := miniredis.RunT(t)
	bridge, err := NewBridge(stream.NewHub(), Config{Addr: redisServer.Addr(), Prefix: "pool"})
	core.RequireNoError(t, err)

	sourceID := bridge.SourceID()
	core.AssertEqual(t, sourceID, bridge.SourceID())
	core.AssertNotEmpty(t, sourceID)
}

func TestAX7_Bridge_Start_Good(t *core.T) {
	bridge, cancel := ax7StartedBridge(t)
	defer cancel()

	bridge.mutex.RLock()
	running := bridge.running
	bridge.mutex.RUnlock()
	core.AssertTrue(t, running)
}

func TestAX7_Bridge_Start_Bad(t *core.T) {
	var bridge *Bridge

	err := bridge.Start(core.Background())
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "nil bridge")
}

func TestAX7_Bridge_Start_Ugly(t *core.T) {
	bridge, cancel := ax7StartedBridge(t)
	defer cancel()

	err := bridge.Start(core.Background())
	core.AssertNoError(t, err)
	core.AssertNotEmpty(t, bridge.SourceID())
}

func TestAX7_Bridge_Stop_Good(t *core.T) {
	bridge, cancel := ax7StartedBridge(t)
	defer cancel()

	core.AssertNoError(t, bridge.Stop())
	core.Sleep(50 * core.Millisecond)
	bridge.mutex.RLock()
	running := bridge.running
	bridge.mutex.RUnlock()
	core.AssertFalse(t, running)
}

func TestAX7_Bridge_Stop_Bad(t *core.T) {
	var bridge *Bridge

	core.AssertNoError(t, bridge.Stop())
	core.AssertNil(t, bridge)
}

func TestAX7_Bridge_Stop_Ugly(t *core.T) {
	redisServer := miniredis.RunT(t)
	bridge, err := NewBridge(stream.NewHub(), Config{Addr: redisServer.Addr(), Prefix: "pool"})
	core.RequireNoError(t, err)

	core.AssertNoError(t, bridge.Stop())
	core.AssertNotEmpty(t, bridge.SourceID())
}

func TestAX7_Bridge_PublishToChannel_Good(t *core.T) {
	redisServer := miniredis.RunT(t)
	hub1 := stream.NewHub()
	hub2 := stream.NewHub()
	ctx, cancel := core.WithCancel(core.Background())
	defer cancel()
	go hub1.Run(ctx)
	go hub2.Run(ctx)
	bridge1, err := NewBridge(hub1, Config{Addr: redisServer.Addr(), Prefix: "pool"})
	core.RequireNoError(t, err)
	bridge2, err := NewBridge(hub2, Config{Addr: redisServer.Addr(), Prefix: "pool"})
	core.RequireNoError(t, err)
	go func() { core.AssertNoError(t, bridge1.Start(ctx)) }()
	go func() { core.AssertNoError(t, bridge2.Start(ctx)) }()
	core.Sleep(100 * core.Millisecond)

	received := make(chan []byte, 1)
	stop := hub2.Subscribe("block", func(frame []byte) { received <- append([]byte(nil), frame...) })
	defer stop()
	core.AssertNoError(t, bridge1.PublishToChannel("block", []byte("template")))
	core.AssertEqual(t, "template", string(<-received))
}

func TestAX7_Bridge_PublishToChannel_Bad(t *core.T) {
	redisServer := miniredis.RunT(t)
	bridge, err := NewBridge(stream.NewHub(), Config{Addr: redisServer.Addr(), Prefix: "pool"})
	core.RequireNoError(t, err)

	err = bridge.PublishToChannel("block", []byte("template"))
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "not started")
}

func TestAX7_Bridge_PublishToChannel_Ugly(t *core.T) {
	bridge, cancel := ax7StartedBridge(t)
	defer cancel()

	err := bridge.PublishToChannel("", []byte("template"))
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "empty channel")
}

func TestAX7_Bridge_PublishBroadcast_Bad(t *core.T) {
	redisServer := miniredis.RunT(t)
	bridge, err := NewBridge(stream.NewHub(), Config{Addr: redisServer.Addr(), Prefix: "pool"})
	core.RequireNoError(t, err)

	err = bridge.PublishBroadcast([]byte("shutdown"))
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "not started")
}

func TestAX7_Bridge_PublishBroadcast_Ugly(t *core.T) {
	var bridge *Bridge

	err := bridge.PublishBroadcast([]byte("shutdown"))
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "nil bridge")
}
