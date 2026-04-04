// SPDX-License-Identifier: EUPL-1.2

package zmq

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"dappco.re/go/stream"
)

func TestAdapter_Publish_Good(t *testing.T) {
	publisherHub := stream.NewHub()
	subscriberHub := stream.NewHub()

	publisherContext, publisherCancel := context.WithCancel(context.Background())
	defer publisherCancel()
	subscriberContext, subscriberCancel := context.WithCancel(context.Background())
	defer subscriberCancel()

	go publisherHub.Run(publisherContext)
	go subscriberHub.Run(subscriberContext)

	endpoint := randomTCPEndpoint(t)
	publisher := New(Config{
		Mode:     ModePubSub,
		Endpoint: endpoint,
		Role:     RolePublisher,
	})
	publisher.Mount(publisherHub)

	subscriber := New(Config{
		Mode:     ModePubSub,
		Endpoint: endpoint,
		Role:     RoleSubscriber,
		Topics:   []string{"block"},
	})
	subscriber.Mount(subscriberHub)

	runContext, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	go func() { _ = publisher.Start(runContext) }()
	go func() { _ = subscriber.Start(runContext) }()
	waitForAdapterRunning(t, publisher)
	waitForAdapterRunning(t, subscriber)

	received := make(chan []byte, 1)
	unsubscribe := subscriberHub.Subscribe("block", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := publisher.Publish("block", []byte("template")); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		select {
		case frame := <-received:
			if string(frame) != "template" {
				t.Fatalf("received frame = %q, want %q", string(frame), "template")
			}
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatal("timed out waiting for zmq frame")
}

func TestAdapter_Publish_Bad(t *testing.T) {
	hub := stream.NewHub()
	adapter := New(Config{
		Mode:     ModePubSub,
		Endpoint: randomTCPEndpoint(t),
		Role:     RoleSubscriber,
	})
	adapter.Mount(hub)

	if err := adapter.Publish("block", []byte("template")); err == nil {
		t.Fatal("Publish() error = nil, want publish not supported error")
	}
}

func TestAdapter_Start_Ugly(t *testing.T) {
	pusherHub := stream.NewHub()
	pullerHub := stream.NewHub()

	pusherContext, pusherCancel := context.WithCancel(context.Background())
	defer pusherCancel()
	pullerContext, pullerCancel := context.WithCancel(context.Background())
	defer pullerCancel()

	go pusherHub.Run(pusherContext)
	go pullerHub.Run(pullerContext)

	endpoint := randomTCPEndpoint(t)
	puller := New(Config{
		Mode:     ModePushPull,
		Endpoint: endpoint,
		Role:     RolePuller,
	})
	puller.Mount(pullerHub)

	pusher := New(Config{
		Mode:     ModePushPull,
		Endpoint: endpoint,
		Role:     RolePusher,
	})
	pusher.Mount(pusherHub)

	runContext, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	go func() { _ = puller.Start(runContext) }()
	go func() { _ = pusher.Start(runContext) }()
	waitForAdapterRunning(t, puller)
	waitForAdapterRunning(t, pusher)

	received := make(chan []byte, 1)
	unsubscribe := pullerHub.Subscribe("job", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := pusher.Publish("job", []byte("work")); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		select {
		case frame := <-received:
			if string(frame) != "work" {
				t.Fatalf("received frame = %q, want %q", string(frame), "work")
			}
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatal("timed out waiting for push/pull frame")
}

func randomTCPEndpoint(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()

	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("Addr() type = %T, want *net.TCPAddr", listener.Addr())
	}
	return "tcp://127.0.0.1:" + strconv.Itoa(address.Port)
}

func waitForAdapterRunning(t *testing.T, adapter *Adapter) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		adapter.mu.RLock()
		running := adapter.running
		adapter.mu.RUnlock()
		if running {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for adapter to start")
}
