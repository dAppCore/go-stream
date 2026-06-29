// SPDX-License-Identifier: EUPL-1.2

// Behaviour-level tests for the TCP adapter: listen/dial round-trip,
// frame encode/decode symmetry, malformed/oversized frame rejection,
// guard rails (nil adapter, unmounted hub, empty addr), connection
// authentication, and the reconnecting client's lifecycle + backoff.
package tcp

import (
	"context"
	"net"
	"sync"
	"time"

	core "dappco.re/go"
	"dappco.re/go/stream"
)

// startHub runs a hub and returns it with a stop function.
//
//	hub, stop := startHub(t)
//	defer stop()
func startHub(t *core.T) (*stream.Hub, func()) {
	t.Helper()
	hub := stream.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	go hub.Run(ctx)
	waitFor(t, func() bool { return hub.Running() })
	return hub, func() {
		cancel()
		deadline := time.Now().Add(2 * time.Second)
		for hub.Running() && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
	}
}

// waitFor polls condition until true or the deadline elapses.
//
//	waitFor(t, func() bool { return hub.PeerCount() == 1 })
func waitFor(t *core.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !condition() {
		t.Fatalf("condition not met within deadline")
	}
}

func TestTcp_Guards_Behaviour(t *core.T) {
	var nilAdapter *Adapter
	core.AssertFalse(t, nilAdapter.Listen(context.Background()).OK, "nil adapter Listen fails")
	core.AssertFalse(t, nilAdapter.Dial(context.Background(), nil).OK, "nil adapter Dial fails")

	unmounted := New(Config{Addr: ":0"})
	core.AssertFalse(t, unmounted.Listen(context.Background()).OK, "unmounted hub Listen fails")

	noAddr := New(Config{})
	hub := stream.NewHub()
	noAddr.Mount(hub)
	core.AssertFalse(t, noAddr.Listen(context.Background()).OK, "empty addr Listen fails")

	dialNoHub := New(Config{Addr: "127.0.0.1:0"})
	core.AssertFalse(t, dialNoHub.Dial(context.Background(), nil).OK, "Dial without hub fails")
}

func TestTcp_EncodeDecodeSymmetry_Behaviour(t *core.T) {
	wire := encodeTCPFrame("block", []byte("payload"))
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	go func() {
		_ = writeAll(client, wire)
	}()

	r := readTCPFrame(server, time.Second, MaxFrameSize)
	core.AssertTrue(t, r.OK, "frame decodes")
	decoded := r.Value.(tcpFrame)
	core.AssertEqual(t, "block", decoded.channel, "channel round-trips")
	core.AssertEqual(t, "payload", string(decoded.frame), "payload round-trips")
}

func TestTcp_ReadFrame_OversizedRejected_Behaviour(t *core.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	go func() {
		// Length prefix claims a frame far larger than the cap.
		oversized := []byte{0x00, 0x10, 0x00, 0x00} // 1,048,576 bytes
		_, _ = client.Write(oversized)
	}()

	r := readTCPFrame(server, time.Second, 1024)
	core.AssertFalse(t, r.OK, "oversized frame rejected")
}

func TestTcp_ReadFrame_InvalidChannelLength_Behaviour(t *core.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	go func() {
		// payloadLength=5, then channelLength=99 which exceeds payload-4.
		buffer := []byte{
			0x00, 0x00, 0x00, 0x05, // total payload length = 5
			0x00, 0x00, 0x00, 0x63, // channel length = 99 (invalid)
			0x41, // one stray byte
		}
		_, _ = client.Write(buffer)
	}()

	r := readTCPFrame(server, time.Second, MaxFrameSize)
	core.AssertFalse(t, r.OK, "invalid channel length rejected")
}

func TestTcp_ReadFrame_Timeout_Behaviour(t *core.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	// No writer — the read should time out and surface ErrHandshakeTimeout.
	r := readTCPFrame(server, 50*time.Millisecond, MaxFrameSize)
	core.AssertFalse(t, r.OK, "timed-out read fails")
	core.AssertErrorIs(t, r.Value.(error), stream.ErrHandshakeTimeout, "carries ErrHandshakeTimeout")
}

func TestTcp_ListenDialRoundTrip_Behaviour(t *core.T) {
	serverHub, stopServer := startHub(t)
	defer stopServer()

	listener := New(Config{Addr: "127.0.0.1:0"})
	listener.Mount(serverHub)
	// Bind the listener explicitly so we know the address before Listen blocks.
	bindR := listener.listen()
	core.AssertTrue(t, bindR.OK, "listener binds")
	addr := bindR.Value.(net.Listener).Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go listener.Listen(ctx)

	clientHub, stopClient := startHub(t)
	defer stopClient()

	// The server's handleConn blocks on a first frame before it registers
	// the peer, so the dialer announces itself with a handshake frame.
	dialer := New(Config{Addr: addr, HandshakeChannel: "hello", HandshakeFrame: []byte("hi")})
	var mutex sync.Mutex
	var received string
	dialer.Mount(clientHub)
	clientHub.Subscribe("*", func(frame []byte) {
		mutex.Lock()
		received = string(frame)
		mutex.Unlock()
	})

	dialR := dialer.Dial(ctx, clientHub)
	core.AssertTrue(t, dialR.OK, "dial succeeds")

	// Server-side peer should register and subscribe to the wildcard.
	waitFor(t, func() bool { return serverHub.ChannelSubscriberCount("*") == 1 })

	// Server publishes on the wildcard; the dialing client receives the
	// framed payload and re-publishes it into its own hub. Retry the
	// publish a few times to absorb the cross-goroutine pipe latency.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		serverHub.SendToChannel("*", []byte("downstream"))
		mutex.Lock()
		done := received == "downstream"
		mutex.Unlock()
		if done {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mutex.Lock()
	core.AssertEqual(t, "downstream", received, "client received the framed payload over TCP")
	mutex.Unlock()
}

func TestTcp_ConnAuthenticatorRejects_Behaviour(t *core.T) {
	serverHub, stopServer := startHub(t)
	defer stopServer()

	listener := New(Config{
		Addr: "127.0.0.1:0",
		ConnAuthenticator: stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
			if string(handshake) == "trusted" {
				return stream.AuthResult{Valid: true, UserID: "peer-1"}
			}
			return stream.AuthResult{Valid: false}
		}),
	})
	listener.Mount(serverHub)
	bindR := listener.listen()
	core.AssertTrue(t, bindR.OK, "listener binds")
	addr := bindR.Value.(net.Listener).Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go listener.Listen(ctx)

	// Dial raw and send a bad handshake — the server must drop us.
	conn, err := net.Dial("tcp", addr)
	core.AssertNoError(t, err, "raw dial connects")
	defer conn.Close()
	_ = writeAll(conn, encodeTCPFrame("auth", []byte("intruder")))

	// Peer count stays zero because auth rejected.
	time.Sleep(100 * time.Millisecond)
	core.AssertEqual(t, 0, serverHub.PeerCount(), "rejected handshake adds no peer")
}

func TestTcp_Reconnect_Defaults_Behaviour(t *core.T) {
	client := NewReconnectingTCP(ReconnectConfig{Addr: "127.0.0.1:1"})
	core.AssertEqual(t, time.Second, client.config.InitialBackoff, "initial backoff defaulted")
	core.AssertEqual(t, 30*time.Second, client.config.MaxBackoff, "max backoff defaulted")
	core.AssertEqual(t, float64(2), client.config.BackoffMultiplier, "multiplier defaulted")
	core.AssertEqual(t, stream.StateDisconnected, client.State(), "starts disconnected")
}

func TestTcp_Reconnect_SendBeforeConnect_Behaviour(t *core.T) {
	client := NewReconnectingTCP(ReconnectConfig{Addr: "127.0.0.1:1"})
	r := client.Send("vpn", []byte("packet"))
	core.AssertFalse(t, r.OK, "send before connect fails")

	var nilClient *ReconnectingTCP
	core.AssertFalse(t, nilClient.Send("x", []byte("y")).OK, "nil client send fails")
	core.AssertEqual(t, stream.StateDisconnected, nilClient.State(), "nil client disconnected")
	core.AssertTrue(t, nilClient.Close().OK, "nil client close ok")
}

func TestTcp_Reconnect_MaxRetriesGivesUp_Behaviour(t *core.T) {
	// Dial a closed port; with MaxRetries=1 and a tiny backoff Connect
	// returns the dial failure rather than looping forever.
	client := NewReconnectingTCP(ReconnectConfig{
		Addr:           "127.0.0.1:1",
		InitialBackoff: time.Millisecond,
		MaxRetries:     1,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r := client.Connect(ctx)
	core.AssertFalse(t, r.OK, "exhausted retries returns failure")
}

func TestTcp_Reconnect_CloseStopsConnect_Behaviour(t *core.T) {
	client := NewReconnectingTCP(ReconnectConfig{
		Addr:           "127.0.0.1:1",
		InitialBackoff: 10 * time.Millisecond,
	})
	go func() {
		time.Sleep(20 * time.Millisecond)
		client.Close()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r := client.Connect(ctx)
	core.AssertTrue(t, r.OK, "Close ends the reconnect loop cleanly")
	core.AssertTrue(t, client.isClosed(), "client marked closed")
}

func TestTcp_Reconnect_ConnectAndReceive_Behaviour(t *core.T) {
	serverHub, stopServer := startHub(t)
	defer stopServer()

	listener := New(Config{Addr: "127.0.0.1:0"})
	listener.Mount(serverHub)
	bindR := listener.listen()
	core.AssertTrue(t, bindR.OK, "listener binds")
	addr := bindR.Value.(net.Listener).Addr().String()

	listenCtx, listenCancel := context.WithCancel(context.Background())
	defer listenCancel()
	go listener.Listen(listenCtx)

	var mutex sync.Mutex
	var connected bool
	var messages []string
	client := NewReconnectingTCP(ReconnectConfig{
		Addr:             addr,
		HandshakeChannel: "auth",
		HandshakeFrame:   []byte("hello"),
		InitialBackoff:   10 * time.Millisecond,
		OnConnect:        func() { mutex.Lock(); connected = true; mutex.Unlock() },
		OnMessage: func(channel string, frame []byte) {
			mutex.Lock()
			messages = append(messages, channel+":"+string(frame))
			mutex.Unlock()
		},
	})

	connectCtx, connectCancel := context.WithCancel(context.Background())
	go client.Connect(connectCtx)

	// Wait for connect, then the server pushes a frame on the wildcard.
	waitFor(t, func() bool { mutex.Lock(); defer mutex.Unlock(); return connected })
	waitFor(t, func() bool { return client.State() == stream.StateConnected })
	waitFor(t, func() bool { return serverHub.ChannelSubscriberCount("*") == 1 })

	seen := func() bool {
		mutex.Lock()
		defer mutex.Unlock()
		for _, m := range messages {
			if core.Contains(m, "frommaster") {
				return true
			}
		}
		return false
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		serverHub.SendToChannel("*", []byte("frommaster"))
		if seen() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	core.AssertTrue(t, seen(), "client OnMessage received the framed payload through readLoop")

	connectCancel()
	client.Close()
}

func TestTcp_NextBackoff_Behaviour(t *core.T) {
	core.AssertEqual(t, 2*time.Second, nextTCPBackoff(time.Second, 2, 30*time.Second), "doubles")
	core.AssertEqual(t, 30*time.Second, nextTCPBackoff(20*time.Second, 2, 30*time.Second), "caps at max")
	core.AssertEqual(t, time.Second, nextTCPBackoff(time.Second, 0, 30*time.Second), "non-positive multiplier holds")
}

func TestTcp_SleepContext_Behaviour(t *core.T) {
	core.AssertTrue(t, sleepContext(context.Background(), 0).OK, "zero duration returns immediately")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	core.AssertFalse(t, sleepContext(ctx, time.Second).OK, "cancelled context aborts sleep")
}

func TestTcp_IsClosedNetworkError_Behaviour(t *core.T) {
	core.AssertFalse(t, isClosedNetworkError(nil), "nil is not closed-network")
	core.AssertTrue(t, isClosedNetworkError(net.ErrClosed), "ErrClosed recognised")
}
