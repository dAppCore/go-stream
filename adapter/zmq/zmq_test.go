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

func TestAdapter_Start_Auth_Good(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	endpoint := randomTCPEndpoint(t)
	subscriber := New(Config{
		Mode:     ModePubSub,
		Endpoint: endpoint,
		Role:     RoleSubscriber,
		Topics:   []string{"block"},
		ConnAuthenticator: stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
			if string(handshake) != "block\x00hello" {
				return stream.AuthResult{Valid: false}
			}
			return stream.AuthResult{Valid: true}
		}),
	})
	subscriber.Mount(hub)

	publisher := New(Config{
		Mode:     ModePubSub,
		Endpoint: endpoint,
		Role:     RolePublisher,
	})
	publisher.Mount(stream.NewHub())

	runContext, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	go func() { _ = subscriber.Start(runContext) }()
	go func() { _ = publisher.Start(runContext) }()
	waitForAdapterRunning(t, subscriber)
	waitForAdapterRunning(t, publisher)

	received := make(chan []byte, 1)
	unsubscribe := hub.Subscribe("block", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := publisher.Publish("block", []byte("hello")); err != nil {
			t.Fatalf("handshake Publish() error = %v", err)
		}
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

	t.Fatal("timed out waiting for authenticated zmq frame")
}

func TestAdapter_Start_RegistersPeer_Good(t *testing.T) {
	connected := make(chan *stream.Peer, 1)
	hub := stream.NewHubWithConfig(stream.HubConfig{
		OnConnect: func(peer *stream.Peer) {
			select {
			case connected <- peer:
			default:
			}
		},
	})
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	endpoint := randomTCPEndpoint(t)
	subscriber := New(Config{
		Mode:     ModePubSub,
		Endpoint: endpoint,
		Role:     RoleSubscriber,
		Topics:   []string{"block"},
		ConnAuthenticator: stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
			if string(handshake) != "block\x00hello" {
				return stream.AuthResult{Valid: false}
			}
			return stream.AuthResult{
				Valid:  true,
				UserID: "node-42",
				Claims: map[string]any{"role": "worker"},
			}
		}),
	})
	subscriber.Mount(hub)

	publisher := New(Config{
		Mode:     ModePubSub,
		Endpoint: endpoint,
		Role:     RolePublisher,
	})
	publisher.Mount(stream.NewHub())

	runContext, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	go func() { _ = subscriber.Start(runContext) }()
	go func() { _ = publisher.Start(runContext) }()
	waitForAdapterRunning(t, subscriber)
	waitForAdapterRunning(t, publisher)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := publisher.Publish("block", []byte("hello")); err != nil {
			t.Fatalf("handshake Publish() error = %v", err)
		}
		select {
		case peer := <-connected:
			if peer.Transport != "zmq" {
				t.Fatalf("connected peer transport = %q, want %q", peer.Transport, "zmq")
			}
			if peer.UserID != "node-42" {
				t.Fatalf("connected peer userID = %q, want %q", peer.UserID, "node-42")
			}
			if role, _ := peer.Claims["role"].(string); role != "worker" {
				t.Fatalf("connected peer role = %q, want %q", role, "worker")
			}
			if peers := hub.PeerCount(); peers != 1 {
				t.Fatalf("PeerCount() = %d, want %d", peers, 1)
			}
			return
		case <-time.After(100 * time.Millisecond):
		}
	}

	t.Fatal("timed out waiting for zmq peer registration")
}

func TestAdapter_Start_Auth_Ugly(t *testing.T) {
	endpoint := randomTCPEndpoint(t)
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	subscriber := New(Config{
		Mode:     ModePubSub,
		Endpoint: endpoint,
		Role:     RoleSubscriber,
		ConnAuthenticator: stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
			return stream.AuthResult{Valid: false}
		}),
		HandshakeTimeout: 500 * time.Millisecond,
	})
	subscriber.Mount(hub)

	publisher := New(Config{
		Mode:     ModePubSub,
		Endpoint: endpoint,
		Role:     RolePublisher,
	})
	publisher.Mount(stream.NewHub())

	runContext, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	errs := make(chan error, 1)
	go func() { errs <- subscriber.Start(runContext) }()
	go func() { _ = publisher.Start(runContext) }()
	waitForAdapterRunning(t, subscriber)
	waitForAdapterRunning(t, publisher)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := publisher.Publish("block", []byte("hello")); err != nil {
			t.Fatalf("handshake Publish() error = %v", err)
		}

		select {
		case err := <-errs:
			if err != stream.ErrAuthRejected {
				t.Fatalf("Start() error = %v, want %v", err, stream.ErrAuthRejected)
			}
			return
		case <-time.After(100 * time.Millisecond):
		}
	}

	t.Fatal("timed out waiting for auth rejection")
}

func TestAdapter_Start_Auth_Timeout(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	subscriber := New(Config{
		Mode:     ModePubSub,
		Endpoint: randomTCPEndpoint(t),
		Role:     RoleSubscriber,
		ConnAuthenticator: stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
			return stream.AuthResult{Valid: true}
		}),
		HandshakeTimeout: 500 * time.Millisecond,
	})
	subscriber.Mount(hub)

	publisher := New(Config{
		Mode:     ModePubSub,
		Endpoint: subscriber.config.Endpoint,
		Role:     RolePublisher,
	})
	publisher.Mount(stream.NewHub())

	runContext, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	errs := make(chan error, 1)
	go func() { errs <- subscriber.Start(runContext) }()
	go func() { _ = publisher.Start(runContext) }()
	waitForAdapterRunning(t, subscriber)
	waitForAdapterRunning(t, publisher)

	select {
	case err := <-errs:
		if err != stream.ErrHandshakeTimeout {
			t.Fatalf("Start() error = %v, want %v", err, stream.ErrHandshakeTimeout)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for handshake timeout")
	}
}

func TestAdapter_Start_Auth_HandshakeTooLarge_Good(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	endpoint := randomTCPEndpoint(t)
	subscriber := New(Config{
		Mode:     ModePubSub,
		Endpoint: endpoint,
		Role:     RoleSubscriber,
		ConnAuthenticator: stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
			return stream.AuthResult{Valid: true}
		}),
		HandshakeTimeout: 500 * time.Millisecond,
	})
	subscriber.Mount(hub)

	publisher := New(Config{
		Mode:     ModePubSub,
		Endpoint: endpoint,
		Role:     RolePublisher,
	})
	publisher.Mount(stream.NewHub())

	runContext, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	errs := make(chan error, 1)
	go func() { errs <- subscriber.Start(runContext) }()
	go func() { _ = publisher.Start(runContext) }()
	waitForAdapterRunning(t, subscriber)
	waitForAdapterRunning(t, publisher)

	tooLargeHandshake := make([]byte, maxHandshakeFrameSize+1)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := publisher.Publish("block", tooLargeHandshake); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}

		select {
		case err := <-errs:
			if err != stream.ErrAuthRejected {
				t.Fatalf("Start() error = %v, want %v", err, stream.ErrAuthRejected)
			}
			return
		case <-time.After(100 * time.Millisecond):
		}
	}

	t.Fatal("timed out waiting for handshake rejection")
}

func TestModeAndRole_String_Good(t *testing.T) {
	if ModePubSub.String() != "pubsub" {
		t.Fatalf("ModePubSub.String() = %q, want %q", ModePubSub.String(), "pubsub")
	}
	if ModePushPull.String() != "pushpull" {
		t.Fatalf("ModePushPull.String() = %q, want %q", ModePushPull.String(), "pushpull")
	}
	if RolePublisher.String() != "publisher" {
		t.Fatalf("RolePublisher.String() = %q, want %q", RolePublisher.String(), "publisher")
	}
	if RoleSubscriber.String() != "subscriber" {
		t.Fatalf("RoleSubscriber.String() = %q, want %q", RoleSubscriber.String(), "subscriber")
	}
	if RolePusher.String() != "pusher" {
		t.Fatalf("RolePusher.String() = %q, want %q", RolePusher.String(), "pusher")
	}
	if RolePuller.String() != "puller" {
		t.Fatalf("RolePuller.String() = %q, want %q", RolePuller.String(), "puller")
	}
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
		adapter.mutex.RLock()
		running := adapter.running
		adapter.mutex.RUnlock()
		if running {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for adapter to start")
}
