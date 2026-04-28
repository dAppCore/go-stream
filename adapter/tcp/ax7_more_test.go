// SPDX-License-Identifier: EUPL-1.2

package tcp

import (
	core "dappco.re/go"
	"dappco.re/go/stream"
)

func ax7TCPHub(t *core.T) (*stream.Hub, core.Context, core.CancelFunc) {
	hub := stream.NewHub()
	ctx, cancel := core.WithCancel(core.Background())
	go hub.Run(ctx)
	deadline := core.Now().Add(2 * core.Second)
	for core.Now().Before(deadline) {
		if hub.Running() {
			return hub, ctx, cancel
		}
		core.Sleep(10 * core.Millisecond)
	}
	t.Fatal("timed out waiting for hub")
	return nil, nil, nil
}

func TestAX7_New_Good(t *core.T) {
	adapter := New(Config{Addr: "127.0.0.1:0", HandshakeTimeout: core.Second})

	core.AssertNotNil(t, adapter)
	core.AssertEqual(t, "127.0.0.1:0", adapter.config.Addr)
	core.AssertEqual(t, core.Second, adapter.config.HandshakeTimeout)
}

func TestAX7_New_Bad(t *core.T) {
	adapter := New(Config{})

	core.AssertNotNil(t, adapter)
	core.AssertEqual(t, "", adapter.config.Addr)
	core.AssertEqual(t, 5*core.Second, adapter.config.HandshakeTimeout)
}

func TestAX7_New_Ugly(t *core.T) {
	adapter := New(Config{HandshakeChannel: "auth", HandshakeFrame: []byte("token")})

	core.AssertEqual(t, "auth", adapter.config.HandshakeChannel)
	core.AssertEqual(t, "token", string(adapter.config.HandshakeFrame))
	core.AssertEqual(t, 5*core.Second, adapter.config.HandshakeTimeout)
}

func TestAX7_Adapter_Mount_Good(t *core.T) {
	adapter := New(Config{})
	hub := stream.NewHub()

	adapter.Mount(hub)
	core.AssertEqual(t, hub, adapter.hub)
	core.AssertFalse(t, adapter.hub.Running())
}

func TestAX7_Adapter_Mount_Bad(t *core.T) {
	adapter := New(Config{})

	adapter.Mount(nil)
	core.AssertNil(t, adapter.hub)
	core.AssertNotNil(t, adapter)
}

func TestAX7_Adapter_Mount_Ugly(t *core.T) {
	adapter := New(Config{})
	first := stream.NewHub()
	second := stream.NewHub()

	adapter.Mount(first)
	adapter.Mount(second)
	core.AssertEqual(t, second, adapter.hub)
}

func TestAX7_Adapter_Listen_Good(t *core.T) {
	hub, ctx, cancel := ax7TCPHub(t)
	defer cancel()
	adapter := New(Config{Addr: "127.0.0.1:0"})
	adapter.Mount(hub)

	go func() { core.AssertNoError(t, adapter.Listen(ctx)) }()
	addr := waitForListenerAddress(t, adapter)
	core.AssertNotEmpty(t, addr)
}

func TestAX7_Adapter_Listen_Bad(t *core.T) {
	var adapter *Adapter

	err := adapter.Listen(core.Background())
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "nil adapter")
}

func TestAX7_Adapter_Listen_Ugly(t *core.T) {
	adapter := New(Config{})
	adapter.Mount(stream.NewHub())

	err := adapter.Listen(core.Background())
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "empty address")
}

func TestAX7_Adapter_Dial_Good(t *core.T) {
	hub, ctx, cancel := ax7TCPHub(t)
	defer cancel()
	server := New(Config{Addr: "127.0.0.1:0"})
	server.Mount(hub)
	go func() { core.AssertNoError(t, server.Listen(ctx)) }()
	addr := waitForListenerAddress(t, server)

	client := New(Config{Addr: addr, HandshakeChannel: "auth", HandshakeFrame: []byte("token")})
	peer, err := client.Dial(ctx, hub)
	core.AssertNoError(t, err)
	core.AssertNotNil(t, peer)
	core.AssertEqual(t, "tcp", peer.Transport)
}

func TestAX7_Adapter_Dial_Bad(t *core.T) {
	var adapter *Adapter

	peer, err := adapter.Dial(core.Background(), nil)
	core.AssertError(t, err)
	core.AssertNil(t, peer)
}

func TestAX7_Adapter_Dial_Ugly(t *core.T) {
	adapter := New(Config{Addr: "127.0.0.1:1"})

	peer, err := adapter.Dial(core.Background(), nil)
	core.AssertError(t, err)
	core.AssertNil(t, peer)
}

func TestAX7_NewReconnectingTCP_Good(t *core.T) {
	client := NewReconnectingTCP(ReconnectConfig{Addr: "127.0.0.1:9000"})

	core.AssertNotNil(t, client)
	core.AssertEqual(t, "127.0.0.1:9000", client.config.Addr)
	core.AssertEqual(t, stream.StateDisconnected, client.State())
}

func TestAX7_NewReconnectingTCP_Bad(t *core.T) {
	client := NewReconnectingTCP(ReconnectConfig{})

	core.AssertEqual(t, core.Second, client.config.InitialBackoff)
	core.AssertEqual(t, 30*core.Second, client.config.MaxBackoff)
	core.AssertEqual(t, 2.0, client.config.BackoffMultiplier)
}

func TestAX7_NewReconnectingTCP_Ugly(t *core.T) {
	client := NewReconnectingTCP(ReconnectConfig{InitialBackoff: core.Millisecond, MaxBackoff: core.Second, BackoffMultiplier: 3})

	core.AssertEqual(t, core.Millisecond, client.config.InitialBackoff)
	core.AssertEqual(t, core.Second, client.config.MaxBackoff)
	core.AssertEqual(t, 3.0, client.config.BackoffMultiplier)
}

func TestAX7_ReconnectingTCP_Connect_Good(t *core.T) {
	hub, ctx, cancel := ax7TCPHub(t)
	defer cancel()
	server := New(Config{Addr: "127.0.0.1:0"})
	server.Mount(hub)
	go func() { core.AssertNoError(t, server.Listen(ctx)) }()
	addr := waitForListenerAddress(t, server)

	client := NewReconnectingTCP(ReconnectConfig{Addr: addr})
	go func() { core.AssertNoError(t, client.Connect(ctx)) }()
	deadline := core.Now().Add(2 * core.Second)
	for core.Now().Before(deadline) {
		if client.State() == stream.StateConnected {
			core.AssertEqual(t, stream.StateConnected, client.State())
			return
		}
		core.Sleep(10 * core.Millisecond)
	}
	t.Fatal("timed out waiting for connected state")
}

func TestAX7_ReconnectingTCP_Connect_Bad(t *core.T) {
	var client *ReconnectingTCP

	err := client.Connect(core.Background())
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "nil reconnecting tcp")
}

func TestAX7_ReconnectingTCP_Connect_Ugly(t *core.T) {
	client := NewReconnectingTCP(ReconnectConfig{Addr: "127.0.0.1:1", MaxRetries: 1, InitialBackoff: core.Millisecond})

	err := client.Connect(core.Background())
	core.AssertError(t, err)
	core.AssertEqual(t, stream.StateDisconnected, client.State())
}

func TestAX7_ReconnectingTCP_Send_Good(t *core.T) {
	left, right := core.NetPipe()
	defer left.Close()
	defer right.Close()
	client := NewReconnectingTCP(ReconnectConfig{})
	client.setConn(left)
	done := make(chan error, 1)

	go func() { done <- client.Send("block", []byte("template")) }()
	channel, frame, err := readTCPFrame(right, 0, MaxFrameSize)
	core.AssertNoError(t, err)
	core.AssertEqual(t, "block", channel)
	core.AssertEqual(t, "template", string(frame))
	core.AssertNoError(t, <-done)
}

func TestAX7_ReconnectingTCP_Send_Bad(t *core.T) {
	var client *ReconnectingTCP

	err := client.Send("block", []byte("template"))
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "nil reconnecting tcp")
}

func TestAX7_ReconnectingTCP_Send_Ugly(t *core.T) {
	client := NewReconnectingTCP(ReconnectConfig{})

	err := client.Send("block", []byte("template"))
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "not connected")
}

func TestAX7_ReconnectingTCP_State_Bad(t *core.T) {
	var client *ReconnectingTCP

	core.AssertEqual(t, stream.StateDisconnected, client.State())
	core.AssertNil(t, client)
}

func TestAX7_ReconnectingTCP_State_Ugly(t *core.T) {
	client := NewReconnectingTCP(ReconnectConfig{})
	client.setState(stream.StateConnecting)

	core.AssertEqual(t, stream.StateConnecting, client.State())
	client.setState(stream.StateDisconnected)
	core.AssertEqual(t, stream.StateDisconnected, client.State())
}

func TestAX7_ReconnectingTCP_Close_Good(t *core.T) {
	left, right := core.NetPipe()
	defer right.Close()
	client := NewReconnectingTCP(ReconnectConfig{})
	client.setConn(left)

	core.AssertNoError(t, client.Close())
	core.AssertEqual(t, stream.StateDisconnected, client.State())
	core.AssertTrue(t, client.closed)
}

func TestAX7_ReconnectingTCP_Close_Bad(t *core.T) {
	var client *ReconnectingTCP

	core.AssertNoError(t, client.Close())
	core.AssertNil(t, client)
}

func TestAX7_ReconnectingTCP_Close_Ugly(t *core.T) {
	client := NewReconnectingTCP(ReconnectConfig{})

	core.AssertNoError(t, client.Close())
	core.AssertNoError(t, client.Close())
	core.AssertEqual(t, stream.StateDisconnected, client.State())
}
