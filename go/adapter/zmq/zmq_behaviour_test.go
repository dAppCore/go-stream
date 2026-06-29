// SPDX-License-Identifier: EUPL-1.2

// Behaviour-level tests for the ZeroMQ adapter: mode/role stringers,
// role validation, frame encode/decode symmetry, listen-endpoint
// rewriting, sender/receiver/listen predicates, construction guards,
// and a real publisher -> subscriber round-trip over loopback tcp.
package zmq

import (
	"context"
	"time"

	"github.com/go-zeromq/zmq4"

	core "dappco.re/go"
	"dappco.re/go/stream"
)

// runHub starts a hub and returns it with a stop function.
//
//	hub, stop := runHub(t)
//	defer stop()
func runHub(t *core.T) (*stream.Hub, func()) {
	t.Helper()
	hub := stream.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	go hub.Run(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for !hub.Running() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !hub.Running() {
		t.Fatalf("hub did not start")
	}
	return hub, func() {
		cancel()
		stopDeadline := time.Now().Add(2 * time.Second)
		for hub.Running() && time.Now().Before(stopDeadline) {
			time.Sleep(time.Millisecond)
		}
	}
}

func TestZmq_ModeStringer_Behaviour(t *core.T) {
	core.AssertEqual(t, "pubsub", ModePubSub.String(), "pubsub mode")
	core.AssertEqual(t, "pushpull", ModePushPull.String(), "pushpull mode")
	core.AssertEqual(t, "unknown", Mode(99).String(), "unknown mode")
}

func TestZmq_RoleStringer_Behaviour(t *core.T) {
	core.AssertEqual(t, "publisher", RolePublisher.String(), "publisher role")
	core.AssertEqual(t, "subscriber", RoleSubscriber.String(), "subscriber role")
	core.AssertEqual(t, "pusher", RolePusher.String(), "pusher role")
	core.AssertEqual(t, "puller", RolePuller.String(), "puller role")
	core.AssertEqual(t, "unknown", Role(99).String(), "unknown role")
}

func TestZmq_NewDefaults_Behaviour(t *core.T) {
	adapter := New(Config{})
	core.AssertEqual(t, 5*time.Second, adapter.config.HandshakeTimeout, "handshake timeout defaulted")
}

func TestZmq_ValidateRole_Behaviour(t *core.T) {
	core.AssertTrue(t, New(Config{Mode: ModePubSub, Role: RolePublisher}).validateRole().OK, "pubsub publisher valid")
	core.AssertTrue(t, New(Config{Mode: ModePubSub, Role: RoleSubscriber}).validateRole().OK, "pubsub subscriber valid")
	core.AssertFalse(t, New(Config{Mode: ModePubSub, Role: RolePusher}).validateRole().OK, "pubsub pusher invalid")

	core.AssertTrue(t, New(Config{Mode: ModePushPull, Role: RolePusher}).validateRole().OK, "pushpull pusher valid")
	core.AssertTrue(t, New(Config{Mode: ModePushPull, Role: RolePuller}).validateRole().OK, "pushpull puller valid")
	core.AssertFalse(t, New(Config{Mode: ModePushPull, Role: RolePublisher}).validateRole().OK, "pushpull publisher invalid")

	core.AssertFalse(t, New(Config{Mode: Mode(99)}).validateRole().OK, "invalid mode rejected")
}

func TestZmq_Predicates_Behaviour(t *core.T) {
	core.AssertTrue(t, New(Config{Role: RolePublisher}).isSender(), "publisher sends")
	core.AssertTrue(t, New(Config{Role: RolePusher}).isSender(), "pusher sends")
	core.AssertFalse(t, New(Config{Role: RoleSubscriber}).isSender(), "subscriber does not send")

	core.AssertTrue(t, New(Config{Role: RoleSubscriber}).isReceiver(), "subscriber receives")
	core.AssertTrue(t, New(Config{Role: RolePuller}).isReceiver(), "puller receives")
	core.AssertFalse(t, New(Config{Role: RolePublisher}).isReceiver(), "publisher does not receive")

	core.AssertTrue(t, New(Config{Mode: ModePubSub, Role: RolePublisher}).shouldListen(), "pubsub publisher listens")
	core.AssertFalse(t, New(Config{Mode: ModePubSub, Role: RoleSubscriber}).shouldListen(), "pubsub subscriber dials")
	core.AssertTrue(t, New(Config{Mode: ModePushPull, Role: RolePusher}).shouldListen(), "pushpull pusher listens")
	core.AssertFalse(t, New(Config{Mode: ModePushPull, Role: RolePuller}).shouldListen(), "pushpull puller dials")
}

func TestZmq_EncodeDecodeSymmetry_Behaviour(t *core.T) {
	wire := encodeMessage("block", []byte("payload"))
	channel, frame, ok := decodeMessage(zmq4.NewMsg(wire))
	core.AssertTrue(t, ok, "message decodes")
	core.AssertEqual(t, "block", channel, "channel round-trips")
	core.AssertEqual(t, "payload", string(frame), "payload round-trips")

	// Broadcast frames carry an empty channel.
	bcast := encodeMessage("", []byte("everyone"))
	channel, frame, ok = decodeMessage(zmq4.NewMsg(bcast))
	core.AssertTrue(t, ok, "broadcast decodes")
	core.AssertEqual(t, "", channel, "broadcast channel empty")
	core.AssertEqual(t, "everyone", string(frame), "broadcast payload round-trips")

	// A payload without the null separator is rejected.
	_, _, ok = decodeMessage(zmq4.NewMsg([]byte{0x41, 0x42, 0x43}))
	core.AssertFalse(t, ok, "message without separator rejected")
}

func TestZmq_ListenEndpoint_Behaviour(t *core.T) {
	core.AssertEqual(t, "tcp://*:5555", listenEndpoint("tcp://127.0.0.1:5555"), "host rewritten to wildcard")
	core.AssertEqual(t, "tcp://*:5555", listenEndpoint("tcp://*:5555"), "wildcard host kept")
	core.AssertEqual(t, "ipc:///tmp/sock", listenEndpoint("ipc:///tmp/sock"), "non-tcp scheme untouched")
}

func TestZmq_Guards_Behaviour(t *core.T) {
	var nilAdapter *Adapter
	core.AssertFalse(t, nilAdapter.Start(context.Background()).OK, "nil adapter start fails")
	core.AssertTrue(t, nilAdapter.Stop().OK, "nil adapter stop is a no-op success")
	core.AssertFalse(t, nilAdapter.Publish("block", []byte("x")).OK, "nil adapter publish fails")

	noEndpoint := New(Config{Mode: ModePubSub, Role: RolePublisher})
	noEndpoint.Mount(stream.NewHub())
	core.AssertFalse(t, noEndpoint.Start(context.Background()).OK, "empty endpoint fails")

	unmounted := New(Config{Mode: ModePubSub, Role: RolePublisher, Endpoint: "tcp://127.0.0.1:0"})
	core.AssertFalse(t, unmounted.Start(context.Background()).OK, "unmounted hub fails")

	badRole := New(Config{Mode: ModePubSub, Role: RolePusher, Endpoint: "tcp://127.0.0.1:0"})
	badRole.Mount(stream.NewHub())
	core.AssertFalse(t, badRole.Start(context.Background()).OK, "invalid role fails")
}

func TestZmq_PublishBeforeStart_Behaviour(t *core.T) {
	adapter := New(Config{Mode: ModePubSub, Role: RolePublisher, Endpoint: "tcp://127.0.0.1:5599"})
	core.AssertFalse(t, adapter.Publish("block", []byte("x")).OK, "publish before start fails")

	subscriber := New(Config{Mode: ModePubSub, Role: RoleSubscriber, Endpoint: "tcp://127.0.0.1:5599"})
	core.AssertFalse(t, subscriber.Publish("block", []byte("x")).OK, "subscriber cannot publish")
}

func TestZmq_RecvWithTimeout_Behaviour(t *core.T) {
	adapter := New(Config{Mode: ModePubSub, Role: RoleSubscriber, Endpoint: "tcp://127.0.0.1:0"})
	socket := zmq4.NewSub(context.Background())
	defer socket.Close()
	// No peer connected, so Recv never delivers — the timeout branch fires.
	r := adapter.recvWithTimeout(context.Background(), socket, 50*time.Millisecond)
	core.AssertFalse(t, r.OK, "timed-out recv fails")
	core.AssertErrorIs(t, r.Value.(error), stream.ErrHandshakeTimeout, "carries ErrHandshakeTimeout")
}

func TestZmq_PubSubRoundTrip_Behaviour(t *core.T) {
	subscriberHub, stopSub := runHub(t)
	defer stopSub()

	endpoint := "tcp://127.0.0.1:5598"

	publisher := New(Config{Mode: ModePubSub, Role: RolePublisher, Endpoint: endpoint})
	publisher.Mount(stream.NewHub())
	pubCtx, pubCancel := context.WithCancel(context.Background())
	defer pubCancel()
	go publisher.Start(pubCtx)
	// Wait for the publisher socket to be live.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		publisher.mutex.RLock()
		live := publisher.running
		publisher.mutex.RUnlock()
		if live {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	subscriber := New(Config{Mode: ModePubSub, Role: RoleSubscriber, Endpoint: endpoint})
	subscriber.Mount(subscriberHub)
	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()
	go subscriber.Start(subCtx)

	// Give the subscriber time to connect, then publish repeatedly to
	// absorb zmq's slow-joiner behaviour.
	got := make(chan []byte, 1)
	subscriberHub.Subscribe("block", func(frame []byte) {
		select {
		case got <- append([]byte(nil), frame...):
		default:
		}
	})

	time.Sleep(200 * time.Millisecond)
	deadline = time.Now().Add(4 * time.Second)
	var received []byte
	for time.Now().Before(deadline) && received == nil {
		publisher.Publish("block", []byte("template"))
		select {
		case received = <-got:
		case <-time.After(50 * time.Millisecond):
		}
	}
	core.AssertNotNil(t, received, "subscriber hub received the published frame over zmq")
	core.AssertEqual(t, "template", string(received), "payload survived the zmq round-trip")
}
