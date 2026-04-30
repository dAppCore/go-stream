// SPDX-License-Identifier: EUPL-1.2

package ws

import (
	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"

	core "dappco.re/go"
	"dappco.re/go/stream"
	adapterredis "dappco.re/go/stream/adapter/redis"
)

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

func TestAX7_New_Good(t *core.T) {
	adapter := New(Config{ReadBufferSize: 2048, WriteBufferSize: 4096})

	core.AssertNotNil(t, adapter)
	core.AssertNotNil(t, adapter.Handler())
}

func TestAX7_New_Bad(t *core.T) {
	adapter := New(Config{})

	core.AssertNotNil(t, adapter)
	core.AssertNotNil(t, adapter.HandlerForChannel("events"))
}

func TestAX7_New_Ugly(t *core.T) {
	called := false
	adapter := New(Config{CheckOrigin: func(*core.Request) bool { called = true; return true }})

	core.AssertTrue(t, adapter.Handler() != nil)
	core.AssertTrue(t, adapter.HandlerForChannel("x") != nil)
	core.AssertFalse(t, called)
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
	core.AssertNotNil(t, hub.Hub)
	core.AssertFalse(t, hub.Running())
}

func TestAX7_NewHub_Bad(t *core.T) {
	hub := NewHub()

	core.AssertEqual(t, 30*core.Second, hub.Config().HeartbeatInterval)
	core.AssertEqual(t, 0, hub.PeerCount())
}

func TestAX7_NewHub_Ugly(t *core.T) {
	left := NewHub()
	right := NewHub()

	core.AssertNotEqual(t, left, right)
	core.AssertNotEqual(t, left.Hub, right.Hub)
}

func TestAX7_NewHubWithConfig_Good(t *core.T) {
	hub := NewHubWithConfig(HubConfig{HeartbeatInterval: core.Second, PongTimeout: 3 * core.Second})

	core.AssertEqual(t, core.Second, hub.Config().HeartbeatInterval)
	core.AssertEqual(t, 3*core.Second, hub.Config().PongTimeout)
}

func TestAX7_NewHubWithConfig_Bad(t *core.T) {
	hub := NewHubWithConfig(HubConfig{})

	core.AssertEqual(t, 30*core.Second, hub.Config().HeartbeatInterval)
	core.AssertEqual(t, 60*core.Second, hub.Config().PongTimeout)
}

func TestAX7_NewHubWithConfig_Ugly(t *core.T) {
	called := false
	hub := NewHubWithConfig(HubConfig{OnConnect: func(*Peer) { called = true }})

	core.AssertNoError(t, hub.AddPeer(NewPeer("ws")))
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

func TestAX7_NewReconnectingClient_Good(t *core.T) {
	client := NewReconnectingClient(ReconnectConfig{URL: "ws://127.0.0.1/stream/ws"})

	core.AssertNotNil(t, client)
	core.AssertEqual(t, stream.StateDisconnected, client.State())
	core.AssertNoError(t, client.Close())
}

func TestAX7_NewReconnectingClient_Bad(t *core.T) {
	client := NewReconnectingClient(ReconnectConfig{})

	core.AssertNotNil(t, client)
	core.AssertEqual(t, StateDisconnected, client.State())
}

func TestAX7_NewReconnectingClient_Ugly(t *core.T) {
	client := NewReconnectingClient(ReconnectConfig{URL: "://bad-url", MaxRetries: 1, InitialBackoff: core.Millisecond})

	err := client.Connect(core.Background())
	core.AssertError(t, err)
	core.AssertEqual(t, StateDisconnected, client.State())
}

func TestAX7_NewRedisBridge_Good(t *core.T) {
	redisServer := miniredis.RunT(t)
	bridge, err := NewRedisBridge(NewHub(), adapterredis.Config{Addr: redisServer.Addr(), Prefix: "pool"})

	core.AssertNoError(t, err)
	core.AssertNotNil(t, bridge)
	core.AssertNotEmpty(t, bridge.SourceID())
}

func TestAX7_NewRedisBridge_Bad(t *core.T) {
	bridge, err := NewRedisBridge("unsupported", adapterredis.Config{})

	core.AssertError(t, err)
	core.AssertNil(t, bridge)
}

func TestAX7_NewRedisBridge_Ugly(t *core.T) {
	var hub *Hub
	redisServer := miniredis.RunT(t)

	bridge, err := NewRedisBridge(hub, adapterredis.Config{Addr: redisServer.Addr(), Prefix: "pool"})
	core.AssertError(t, err)
	core.AssertNil(t, bridge)
}

func TestAX7_Pipe_Good(t *core.T) {
	source := NewHub()
	destination := NewHub()

	stop := Pipe(source, destination)
	core.AssertNotNil(t, stop)
	stop()
}

func TestAX7_Pipe_Bad(t *core.T) {
	stop := Pipe(nil, NewHub())

	core.AssertNotNil(t, stop)
	core.AssertNotPanics(t, stop)
}

func TestAX7_Pipe_Ugly(t *core.T) {
	hub := NewHub()
	stop := Pipe(hub, hub)

	core.AssertNotNil(t, stop)
	core.AssertNotPanics(t, stop)
}

func TestAX7_Hub_Handler_Good(t *core.T) {
	hub := NewHub()
	ctx, cancel := core.WithCancel(core.Background())
	defer cancel()
	go hub.Run(ctx)
	waitForRunningHub(t, hub)
	server := core.NewHTTPTestServer(hub.Handler())
	defer server.Close()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+server.URL[len("http"):], nil)
	core.AssertNoError(t, err)
	core.AssertNoError(t, conn.Close())
}

func TestAX7_Hub_Handler_Bad(t *core.T) {
	var hub *Hub
	handler := hub.Handler()
	recorder := core.NewHTTPTestRecorder()

	handler.ServeHTTP(recorder, core.NewHTTPTestRequest("GET", "/stream/ws", nil))
	core.AssertEqual(t, 500, recorder.Code)
	core.AssertContains(t, recorder.Body.String(), "not mounted")
}

func TestAX7_Hub_Handler_Ugly(t *core.T) {
	hub := NewHub()
	handler := hub.Handler()
	recorder := core.NewHTTPTestRecorder()

	handler.ServeHTTP(recorder, core.NewHTTPTestRequest("GET", "/stream/ws", nil))
	core.AssertEqual(t, 500, recorder.Code)
	core.AssertContains(t, recorder.Body.String(), "not running")
}

func TestAX7_Hub_HandlerForChannel_Good(t *core.T) {
	hub := NewHub()
	ctx, cancel := core.WithCancel(core.Background())
	defer cancel()
	go hub.Run(ctx)
	waitForRunningHub(t, hub)
	server := core.NewHTTPTestServer(hub.HandlerForChannel("hashrate"))
	defer server.Close()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+server.URL[len("http"):], nil)
	core.AssertNoError(t, err)
	core.AssertNoError(t, conn.Close())
}

func TestAX7_Hub_HandlerForChannel_Bad(t *core.T) {
	var hub *Hub
	handler := hub.HandlerForChannel("hashrate")
	recorder := core.NewHTTPTestRecorder()

	handler.ServeHTTP(recorder, core.NewHTTPTestRequest("GET", "/stream/ws", nil))
	core.AssertEqual(t, 500, recorder.Code)
	core.AssertContains(t, recorder.Body.String(), "not mounted")
}

func TestAX7_Hub_HandlerForChannel_Ugly(t *core.T) {
	hub := NewHub()
	handler := hub.HandlerForChannel("hashrate")
	recorder := core.NewHTTPTestRecorder()

	handler.ServeHTTP(recorder, core.NewHTTPTestRequest("GET", "/stream/ws", nil))
	core.AssertEqual(t, 500, recorder.Code)
	core.AssertContains(t, recorder.Body.String(), "not running")
}
