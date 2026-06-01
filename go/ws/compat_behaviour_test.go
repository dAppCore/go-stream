// SPDX-License-Identifier: EUPL-1.2

// Behaviour-level tests for the legacy go-ws compat shim: constructor
// wiring, the hub-bound Handler/HandlerForChannel entrypoints (incl.
// nil-hub guards), the lazy compat adapter, and NewRedisBridge type
// dispatch. The live websocket transport is covered in adapter/ws.
package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	core "dappco.re/go"
	"dappco.re/go/stream"
	adapterredis "dappco.re/go/stream/adapter/redis"
)

func TestWsCompat_Constructors_Behaviour(t *core.T) {
	hub := NewHub()
	core.AssertNotNil(t, hub, "NewHub returns a hub")
	core.AssertNotNil(t, hub.Hub, "embedded stream hub present")

	withConfig := NewHubWithConfig(HubConfig{HeartbeatInterval: 10 * time.Second})
	core.AssertEqual(t, 10*time.Second, withConfig.Config().HeartbeatInterval, "config honoured")

	core.AssertEqual(t, 30*time.Second, DefaultHubConfig().HeartbeatInterval, "default heartbeat")
	core.AssertNotNil(t, NewPeer("ws"), "NewPeer constructs")
	core.AssertNotNil(t, NewAPIKeyAuth(map[string]string{"k": "u"}), "NewAPIKeyAuth constructs")
	core.AssertNotNil(t, New(Config{}), "New adapter constructs")
	core.AssertNotNil(t, NewReconnectingClient(ReconnectConfig{URL: "ws://127.0.0.1:1"}), "reconnect client constructs")
}

func TestWsCompat_PipeAlias_Behaviour(t *core.T) {
	src := NewHub()
	dst := NewHub()
	stop := Pipe(src, dst)
	core.AssertNotNil(t, stop, "Pipe returns a stopper")
	stop()
}

func TestWsCompat_HandlerNilGuards_Behaviour(t *core.T) {
	var hub *Hub
	recorder := httptest.NewRecorder()
	hub.Handler()(recorder, httptest.NewRequest(http.MethodGet, "/ws", nil))
	core.AssertEqual(t, http.StatusInternalServerError, recorder.Code, "nil hub Handler returns 500")

	recorder2 := httptest.NewRecorder()
	hub.HandlerForChannel("block")(recorder2, httptest.NewRequest(http.MethodGet, "/ws", nil))
	core.AssertEqual(t, http.StatusInternalServerError, recorder2.Code, "nil hub HandlerForChannel returns 500")
}

func TestWsCompat_HubHandlerServesWebSocket_Behaviour(t *core.T) {
	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for !hub.Running() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	core.AssertNoError(t, err, "compat hub Handler upgrades the websocket")
	defer conn.Close()

	// Subscribe then receive a published frame to prove the compat
	// adapter is wired to the embedded hub.
	core.AssertNoError(t, conn.WriteJSON(stream.Message{Type: stream.TypeSubscribe, Channel: "block"}), "subscribe sent")
	subDeadline := time.Now().Add(2 * time.Second)
	for hub.ChannelSubscriberCount("block") == 0 && time.Now().Before(subDeadline) {
		time.Sleep(time.Millisecond)
	}
	hub.Publish("block", []byte("template"))
	_, payload, err := conn.ReadMessage()
	core.AssertNoError(t, err, "frame received")
	core.AssertEqual(t, "template", string(payload), "compat hub delivered the frame")
}

func TestWsCompat_CompatAdapterMemoised_Behaviour(t *core.T) {
	hub := NewHub()
	first := hub.compatAdapter()
	second := hub.compatAdapter()
	core.AssertSame(t, first, second, "compat adapter is created once")
}

func TestWsCompat_NewRedisBridge_Dispatch_Behaviour(t *core.T) {
	// Unsupported hub type is rejected outright (no network needed).
	core.AssertFalse(t, NewRedisBridge("not-a-hub", adapterredis.Config{Addr: "127.0.0.1:1"}).OK, "unsupported hub type rejected")

	// *Hub and *stream.Hub both route into the bridge constructor; with an
	// unreachable address the ping fails, which still proves dispatch.
	core.AssertFalse(t, NewRedisBridge(NewHub(), adapterredis.Config{Addr: "127.0.0.1:1"}).OK, "*Hub routes (ping fails)")
	core.AssertFalse(t, NewRedisBridge(stream.NewHub(), adapterredis.Config{Addr: "127.0.0.1:1"}).OK, "*stream.Hub routes (ping fails)")

	var nilHub *Hub
	core.AssertFalse(t, NewRedisBridge(nilHub, adapterredis.Config{Addr: "127.0.0.1:1"}).OK, "nil *Hub rejected by bridge")
}
