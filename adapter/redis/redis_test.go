// SPDX-License-Identifier: EUPL-1.2

package redis

import (
	"context"
	"testing"
	"time"

	"dappco.re/go/stream"
)

func TestBridge_Publish_Good(t *testing.T) {
	hub1 := stream.NewHub()
	hub2 := stream.NewHub()

	hub1Context, hub1Cancel := context.WithCancel(context.Background())
	defer hub1Cancel()
	hub2Context, hub2Cancel := context.WithCancel(context.Background())
	defer hub2Cancel()

	go hub1.Run(hub1Context)
	go hub2.Run(hub2Context)

	bridge1, err := NewBridge(hub1, Config{Addr: "redis:6379", Prefix: "pool"})
	if err != nil {
		t.Fatalf("NewBridge(hub1) error = %v", err)
	}
	bridge2, err := NewBridge(hub2, Config{Addr: "redis:6379", Prefix: "pool"})
	if err != nil {
		t.Fatalf("NewBridge(hub2) error = %v", err)
	}

	bridgeContext, bridgeCancel := context.WithCancel(context.Background())
	defer bridgeCancel()
	go func() { _ = bridge1.Start(bridgeContext) }()
	go func() { _ = bridge2.Start(bridgeContext) }()

	received := make(chan []byte, 1)
	unsubscribe := hub2.Subscribe("block", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	waitForBridgeRunning(t, bridge1)
	waitForBridgeRunning(t, bridge2)

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
	bridge, err := NewBridge(hub, Config{Addr: "redis:6379", Prefix: "pool"})
	if err != nil {
		t.Fatalf("NewBridge() error = %v", err)
	}

	if err := bridge.PublishToChannel("block", []byte("template")); err == nil {
		t.Fatal("PublishToChannel() error = nil, want bridge not started error")
	}
}

func TestBridge_Publish_Ugly(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	bridge, err := NewBridge(hub, Config{Addr: "redis:6379", Prefix: "pool"})
	if err != nil {
		t.Fatalf("NewBridge() error = %v", err)
	}

	bridgeContext, bridgeCancel := context.WithCancel(context.Background())
	defer bridgeCancel()
	go func() { _ = bridge.Start(bridgeContext) }()
	waitForBridgeRunning(t, bridge)

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

func waitForBridgeRunning(t *testing.T, bridge *Bridge) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if bridge.isRunning() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for bridge to start")
}
