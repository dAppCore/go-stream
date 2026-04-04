// SPDX-License-Identifier: EUPL-1.2

package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"dappco.re/go/stream"
)

func TestBridge_Publish_Good(t *testing.T) {
	redisServer := miniredis.RunT(t)

	hub1 := stream.NewHub()
	hub2 := stream.NewHub()

	hub1Context, hub1Cancel := context.WithCancel(context.Background())
	defer hub1Cancel()
	hub2Context, hub2Cancel := context.WithCancel(context.Background())
	defer hub2Cancel()

	go hub1.Run(hub1Context)
	go hub2.Run(hub2Context)

	bridge1, err := NewBridge(hub1, Config{Addr: redisServer.Addr(), Prefix: "pool"})
	if err != nil {
		t.Fatalf("NewBridge(hub1) error = %v", err)
	}
	bridge2, err := NewBridge(hub2, Config{Addr: redisServer.Addr(), Prefix: "pool"})
	if err != nil {
		t.Fatalf("NewBridge(hub2) error = %v", err)
	}

	bridgeContext, bridgeCancel := context.WithCancel(context.Background())
	defer bridgeCancel()
	go func() { _ = bridge1.Start(bridgeContext) }()
	go func() { _ = bridge2.Start(bridgeContext) }()
	time.Sleep(100 * time.Millisecond)

	received := make(chan []byte, 1)
	unsubscribe := hub2.Subscribe("block", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	if err := bridge1.PublishToChannel("block", []byte("template")); err != nil {
		t.Fatalf("PublishToChannel() error = %v", err)
	}

	select {
	case frame := <-received:
		if string(frame) != "template" {
			t.Fatalf("received frame = %q, want %q", string(frame), "template")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for bridged frame")
	}
}

func TestBridge_Publish_Bad(t *testing.T) {
	hub := stream.NewHub()
	_, err := NewBridge(hub, Config{})
	if err == nil {
		t.Fatal("NewBridge() error = nil, want empty address error")
	}
}

func TestBridge_Publish_Ugly(t *testing.T) {
	redisServer := miniredis.RunT(t)

	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	bridge, err := NewBridge(hub, Config{Addr: redisServer.Addr(), Prefix: "pool"})
	if err != nil {
		t.Fatalf("NewBridge() error = %v", err)
	}

	bridgeContext, bridgeCancel := context.WithCancel(context.Background())
	defer bridgeCancel()
	go func() { _ = bridge.Start(bridgeContext) }()
	time.Sleep(100 * time.Millisecond)

	received := make(chan []byte, 1)
	unsubscribe := hub.Subscribe("block", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	if err := bridge.PublishToChannel("block", []byte("template")); err != nil {
		t.Fatalf("PublishToChannel() error = %v", err)
	}

	select {
	case frame := <-received:
		t.Fatalf("received unexpected self-echo frame = %q", string(frame))
	case <-time.After(200 * time.Millisecond):
	}
}

func TestBridge_PublishBroadcast_Good(t *testing.T) {
	redisServer := miniredis.RunT(t)

	hub1 := stream.NewHub()
	hub2 := stream.NewHub()

	hub1Context, hub1Cancel := context.WithCancel(context.Background())
	defer hub1Cancel()
	hub2Context, hub2Cancel := context.WithCancel(context.Background())
	defer hub2Cancel()

	go hub1.Run(hub1Context)
	go hub2.Run(hub2Context)

	bridge1, err := NewBridge(hub1, Config{Addr: redisServer.Addr(), Prefix: "pool"})
	if err != nil {
		t.Fatalf("NewBridge(hub1) error = %v", err)
	}
	bridge2, err := NewBridge(hub2, Config{Addr: redisServer.Addr(), Prefix: "pool"})
	if err != nil {
		t.Fatalf("NewBridge(hub2) error = %v", err)
	}

	bridgeContext, bridgeCancel := context.WithCancel(context.Background())
	defer bridgeCancel()
	go func() { _ = bridge1.Start(bridgeContext) }()
	go func() { _ = bridge2.Start(bridgeContext) }()
	time.Sleep(100 * time.Millisecond)

	peer := stream.NewPeer("ws")
	if err := hub2.AddPeer(peer); err != nil {
		t.Fatalf("AddPeer() error = %v", err)
	}
	defer hub2.RemovePeer(peer)

	if err := bridge1.PublishBroadcast([]byte("shutdown")); err != nil {
		t.Fatalf("PublishBroadcast() error = %v", err)
	}

	select {
	case frame := <-peer.SendQueue():
		if string(frame) != "shutdown" {
			t.Fatalf("received frame = %q, want %q", string(frame), "shutdown")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for bridged broadcast")
	}
}
