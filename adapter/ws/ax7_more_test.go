// SPDX-License-Identifier: EUPL-1.2

package ws

import (
	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"

	core "dappco.re/go"
	"dappco.re/go/stream"
	adapterredis "dappco.re/go/stream/adapter/redis"
)

func ax7WebSocketPair(t *core.T) (*websocket.Conn, *websocket.Conn, func()) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*core.Request) bool { return true }}
	serverConn := make(chan *websocket.Conn, 1)
	server := core.NewHTTPTestServer(core.HandlerFunc(func(w core.ResponseWriter, r *core.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("Upgrade() error = %v", err)
			return
		}
		serverConn <- conn
	}))
	clientConn, _, err := websocket.DefaultDialer.Dial("ws"+server.URL[len("http"):], nil)
	core.RequireNoError(t, err)
	conn := <-serverConn
	cleanup := func() {
		clientConn.Close()
		conn.Close()
		server.Close()
	}
	return clientConn, conn, cleanup
}

func TestAX7_New_Good(t *core.T) {
	adapter := New(Config{ReadBufferSize: 2048, WriteBufferSize: 4096})

	core.AssertNotNil(t, adapter)
	core.AssertEqual(t, 2048, adapter.config.ReadBufferSize)
	core.AssertEqual(t, 4096, adapter.config.WriteBufferSize)
}

func TestAX7_New_Bad(t *core.T) {
	adapter := New(Config{})

	core.AssertNotNil(t, adapter)
	core.AssertEqual(t, 1024, adapter.config.ReadBufferSize)
	core.AssertEqual(t, 1024, adapter.config.WriteBufferSize)
}

func TestAX7_New_Ugly(t *core.T) {
	allowed := false
	adapter := New(Config{CheckOrigin: func(*core.Request) bool { allowed = true; return true }})

	core.AssertTrue(t, adapter.config.CheckOrigin(nil))
	core.AssertTrue(t, allowed)
}

func TestAX7_Adapter_Mount_Good(t *core.T) {
	adapter := New(Config{})
	hub := stream.NewHub()

	adapter.Mount(hub)
	core.AssertEqual(t, hub, adapter.hub)
	core.AssertNotNil(t, adapter.Handler())
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

func TestAX7_Adapter_ServeHTTP_Bad(t *core.T) {
	adapter := New(Config{})
	recorder := core.NewHTTPTestRecorder()
	request := core.NewHTTPTestRequest("GET", "/stream/ws", nil)

	adapter.ServeHTTP(recorder, request)
	core.AssertEqual(t, 500, recorder.Code)
	core.AssertContains(t, recorder.Body.String(), "not mounted")
}

func TestAX7_Adapter_ServeHTTP_Ugly(t *core.T) {
	adapter := New(Config{})
	adapter.Mount(stream.NewHub())
	recorder := core.NewHTTPTestRecorder()
	request := core.NewHTTPTestRequest("GET", "/stream/ws?channel=hashrate", nil)

	adapter.ServeHTTP(recorder, request)
	core.AssertEqual(t, 500, recorder.Code)
	core.AssertContains(t, recorder.Body.String(), "not running")
}

func TestAX7_Adapter_HandlerForChannel_Bad(t *core.T) {
	adapter := New(Config{})
	handler := adapter.HandlerForChannel("hashrate")
	recorder := core.NewHTTPTestRecorder()

	handler.ServeHTTP(recorder, core.NewHTTPTestRequest("GET", "/stream/ws", nil))
	core.AssertEqual(t, 500, recorder.Code)
	core.AssertContains(t, recorder.Body.String(), "not mounted")
}

func TestAX7_Adapter_HandlerForChannel_Ugly(t *core.T) {
	adapter := New(Config{})
	adapter.Mount(stream.NewHub())
	handler := adapter.HandlerForChannel("hashrate")
	recorder := core.NewHTTPTestRecorder()

	handler.ServeHTTP(recorder, core.NewHTTPTestRequest("GET", "/stream/ws", nil))
	core.AssertEqual(t, 500, recorder.Code)
	core.AssertContains(t, recorder.Body.String(), "not running")
}

func TestAX7_DefaultHubConfig_Good(t *core.T) {
	config := DefaultHubConfig()

	core.AssertEqual(t, 30*core.Second, config.HeartbeatInterval)
	core.AssertEqual(t, 60*core.Second, config.PongTimeout)
	core.AssertEqual(t, 10*core.Second, config.WriteTimeout)
}

func TestAX7_DefaultHubConfig_Bad(t *core.T) {
	config := DefaultHubConfig()

	core.AssertNil(t, config.OnConnect)
	core.AssertNil(t, config.OnDisconnect)
	core.AssertNil(t, config.ChannelAuthoriser)
}

func TestAX7_DefaultHubConfig_Ugly(t *core.T) {
	config := DefaultHubConfig()

	core.AssertGreater(t, config.PongTimeout, config.HeartbeatInterval)
	core.AssertGreater(t, config.WriteTimeout, core.Duration(0))
}

func TestAX7_NewAPIKeyAuth_Good(t *core.T) {
	authenticator := NewAPIKeyAuth(map[string]string{"sk": "user"})

	core.AssertNotNil(t, authenticator)
	core.AssertEqual(t, "user", authenticator.Keys["sk"])
}

func TestAX7_NewAPIKeyAuth_Bad(t *core.T) {
	authenticator := NewAPIKeyAuth(nil)

	core.AssertNotNil(t, authenticator)
	core.AssertEqual(t, 0, len(authenticator.Keys))
}

func TestAX7_NewAPIKeyAuth_Ugly(t *core.T) {
	keys := map[string]string{"sk": "user"}
	authenticator := NewAPIKeyAuth(keys)
	keys["sk"] = "mutated"

	core.AssertEqual(t, "user", authenticator.Keys["sk"])
	core.AssertEqual(t, "mutated", keys["sk"])
}

func TestAX7_NewHub_Good(t *core.T) {
	hub := NewHub()

	core.AssertNotNil(t, hub)
	core.AssertFalse(t, hub.Running())
	core.AssertEqual(t, 0, hub.PeerCount())
}

func TestAX7_NewHub_Bad(t *core.T) {
	hub := NewHub()

	core.AssertEqual(t, 30*core.Second, hub.Config().HeartbeatInterval)
	core.AssertEqual(t, 0, hub.ChannelCount())
}

func TestAX7_NewHub_Ugly(t *core.T) {
	left := NewHub()
	right := NewHub()

	core.AssertNotEqual(t, left, right)
	core.AssertNoError(t, left.AddPeer(stream.NewPeer("ws")))
}

func TestAX7_NewHubWithConfig_Good(t *core.T) {
	hub := NewHubWithConfig(stream.HubConfig{HeartbeatInterval: core.Second, PongTimeout: 3 * core.Second})

	core.AssertEqual(t, core.Second, hub.Config().HeartbeatInterval)
	core.AssertEqual(t, 3*core.Second, hub.Config().PongTimeout)
}

func TestAX7_NewHubWithConfig_Bad(t *core.T) {
	hub := NewHubWithConfig(stream.HubConfig{})

	core.AssertEqual(t, 30*core.Second, hub.Config().HeartbeatInterval)
	core.AssertEqual(t, 60*core.Second, hub.Config().PongTimeout)
}

func TestAX7_NewHubWithConfig_Ugly(t *core.T) {
	called := false
	hub := NewHubWithConfig(stream.HubConfig{OnConnect: func(*stream.Peer) { called = true }})

	core.AssertNoError(t, hub.AddPeer(stream.NewPeer("ws")))
	core.AssertTrue(t, called)
}

func TestAX7_NewPeer_Good(t *core.T) {
	peer := NewPeer("ws")

	core.AssertNotNil(t, peer)
	core.AssertEqual(t, "ws", peer.Transport)
	core.AssertNotEmpty(t, peer.ID)
}

func TestAX7_NewPeer_Bad(t *core.T) {
	peer := NewPeer("")

	core.AssertNotNil(t, peer)
	core.AssertEqual(t, "", peer.Transport)
	core.AssertNotNil(t, peer.SendQueue())
}

func TestAX7_NewPeer_Ugly(t *core.T) {
	left := NewPeer("ws")
	right := NewPeer("ws")

	core.AssertNotEqual(t, left.ID, right.ID)
	core.AssertEqual(t, "ws", right.Transport)
}

func TestAX7_Pipe_Good(t *core.T) {
	source := stream.NewHub()
	destination := stream.NewHub()

	stop := Pipe(source, destination)
	core.AssertNotNil(t, stop)
	stop()
}

func TestAX7_Pipe_Bad(t *core.T) {
	stop := Pipe(nil, stream.NewHub())

	core.AssertNotNil(t, stop)
	core.AssertNotPanics(t, stop)
}

func TestAX7_Pipe_Ugly(t *core.T) {
	hub := stream.NewHub()
	stop := Pipe(hub, hub)

	core.AssertNotNil(t, stop)
	core.AssertNotPanics(t, stop)
}

func TestAX7_NewRedisBridge_Good(t *core.T) {
	redisServer := miniredis.RunT(t)
	bridge, err := NewRedisBridge(stream.NewHub(), adapterredis.Config{Addr: redisServer.Addr(), Prefix: "pool"})

	core.AssertNoError(t, err)
	core.AssertNotNil(t, bridge)
	core.AssertNotEmpty(t, bridge.SourceID())
}

func TestAX7_NewRedisBridge_Bad(t *core.T) {
	redisServer := miniredis.RunT(t)
	bridge, err := NewRedisBridge(nil, adapterredis.Config{Addr: redisServer.Addr(), Prefix: "pool"})

	core.AssertError(t, err)
	core.AssertNil(t, bridge)
}

func TestAX7_NewRedisBridge_Ugly(t *core.T) {
	bridge, err := NewRedisBridge(stream.NewHub(), adapterredis.Config{})

	core.AssertError(t, err)
	core.AssertNil(t, bridge)
}

func TestAX7_NewReconnectingClient_Good(t *core.T) {
	client := NewReconnectingClient(ReconnectConfig{URL: "ws://127.0.0.1/stream/ws"})

	core.AssertNotNil(t, client)
	core.AssertEqual(t, stream.StateDisconnected, client.State())
	core.AssertNoError(t, client.Close())
}

func TestAX7_NewReconnectingClient_Bad(t *core.T) {
	client := NewReconnectingClient(ReconnectConfig{})

	core.AssertEqual(t, 500*core.Millisecond, client.config.InitialBackoff)
	core.AssertEqual(t, 30*core.Second, client.config.MaxBackoff)
	core.AssertEqual(t, 2.0, client.config.BackoffMultiplier)
}

func TestAX7_NewReconnectingClient_Ugly(t *core.T) {
	client := NewReconnectingClient(ReconnectConfig{InitialBackoff: core.Millisecond, MaxBackoff: core.Second, BackoffMultiplier: 3})

	core.AssertEqual(t, core.Millisecond, client.config.InitialBackoff)
	core.AssertEqual(t, core.Second, client.config.MaxBackoff)
	core.AssertEqual(t, 3.0, client.config.BackoffMultiplier)
}

func TestAX7_ReconnectingClient_Connect_Good(t *core.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*core.Request) bool { return true }}
	connected := make(chan struct{}, 1)
	server := core.NewHTTPTestServer(core.HandlerFunc(func(w core.ResponseWriter, r *core.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err == nil {
			defer conn.Close()
			<-r.Context().Done()
		}
	}))
	defer server.Close()
	ctx, cancel := core.WithCancel(core.Background())
	defer cancel()
	client := NewReconnectingClient(ReconnectConfig{
		URL:       "ws" + server.URL[len("http"):],
		OnConnect: func() { connected <- struct{}{} },
	})
	errs := make(chan error, 1)

	go func() { errs <- client.Connect(ctx) }()
	<-connected
	core.AssertEqual(t, stream.StateConnected, client.State())
	core.AssertNoError(t, client.Close())
	cancel()
	core.AssertNoError(t, <-errs)
}

func TestAX7_ReconnectingClient_Connect_Bad(t *core.T) {
	var client *ReconnectingClient

	err := client.Connect(core.Background())
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "nil reconnecting client")
}

func TestAX7_ReconnectingClient_Connect_Ugly(t *core.T) {
	client := NewReconnectingClient(ReconnectConfig{URL: "://bad-url", MaxRetries: 1, InitialBackoff: core.Millisecond})

	err := client.Connect(core.Background())
	core.AssertError(t, err)
	core.AssertEqual(t, stream.StateDisconnected, client.State())
}

func TestAX7_ReconnectingClient_Send_Good(t *core.T) {
	clientConn, serverConn, cleanup := ax7WebSocketPair(t)
	defer cleanup()
	client := NewReconnectingClient(ReconnectConfig{})
	client.mutex.Lock()
	client.conn = clientConn
	client.state = stream.StateConnected
	client.mutex.Unlock()

	core.AssertNoError(t, client.Send(stream.Message{Type: stream.TypePing, Channel: "health"}))
	_, payload, err := serverConn.ReadMessage()
	core.AssertNoError(t, err)
	core.AssertContains(t, string(payload), `"type":"ping"`)
}

func TestAX7_ReconnectingClient_Send_Bad(t *core.T) {
	var client *ReconnectingClient

	err := client.Send(stream.Message{Type: stream.TypePing})
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "nil reconnecting client")
}

func TestAX7_ReconnectingClient_Send_Ugly(t *core.T) {
	client := NewReconnectingClient(ReconnectConfig{})

	err := client.Send(stream.Message{Type: stream.TypePing})
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "not connected")
}

func TestAX7_ReconnectingClient_State_Good(t *core.T) {
	client := NewReconnectingClient(ReconnectConfig{})
	client.mutex.Lock()
	client.state = stream.StateConnected
	client.mutex.Unlock()

	core.AssertEqual(t, stream.StateConnected, client.State())
	core.AssertNoError(t, client.Close())
}

func TestAX7_ReconnectingClient_State_Bad(t *core.T) {
	var client *ReconnectingClient

	core.AssertEqual(t, stream.StateDisconnected, client.State())
	core.AssertNil(t, client)
}

func TestAX7_ReconnectingClient_State_Ugly(t *core.T) {
	client := NewReconnectingClient(ReconnectConfig{})

	core.AssertNoError(t, client.Close())
	core.AssertEqual(t, stream.StateDisconnected, client.State())
	core.AssertTrue(t, client.closed)
}

func TestAX7_ReconnectingClient_Close_Good(t *core.T) {
	clientConn, _, cleanup := ax7WebSocketPair(t)
	defer cleanup()
	client := NewReconnectingClient(ReconnectConfig{})
	client.mutex.Lock()
	client.conn = clientConn
	client.state = stream.StateConnected
	client.mutex.Unlock()

	core.AssertNoError(t, client.Close())
	core.AssertEqual(t, stream.StateDisconnected, client.State())
	core.AssertTrue(t, client.closed)
}

func TestAX7_ReconnectingClient_Close_Bad(t *core.T) {
	var client *ReconnectingClient

	core.AssertNoError(t, client.Close())
	core.AssertNil(t, client)
}

func TestAX7_ReconnectingClient_Close_Ugly(t *core.T) {
	client := NewReconnectingClient(ReconnectConfig{})

	core.AssertNoError(t, client.Close())
	core.AssertNoError(t, client.Close())
	core.AssertEqual(t, stream.StateDisconnected, client.State())
}
