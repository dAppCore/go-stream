// SPDX-License-Identifier: EUPL-1.2

// Behaviour-level tests for the WebSocket adapter and reconnecting
// client: upgrade handshake, auth gating, subscribe/unsubscribe/ping
// control frames, publish + broadcast from a peer, hub-not-mounted /
// not-running guards, and the reconnecting client lifecycle.
package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

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

// wsURL converts an http test server URL to a ws:// URL.
//
//	url := wsURL(server.URL)
func wsURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

func TestWs_NewDefaults_Behaviour(t *core.T) {
	adapter := New(Config{})
	core.AssertEqual(t, 1024, adapter.config.ReadBufferSize, "read buffer defaulted")
	core.AssertEqual(t, 1024, adapter.config.WriteBufferSize, "write buffer defaulted")
}

func TestWs_HubNotMounted_Behaviour(t *core.T) {
	adapter := New(Config{})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/stream/ws", nil)
	adapter.ServeHTTP(recorder, request)
	core.AssertEqual(t, http.StatusInternalServerError, recorder.Code, "no hub mounted returns 500")
}

func TestWs_AuthRejected_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	var failureSeen bool
	adapter := New(Config{
		Authenticator: stream.NewAPIKeyAuth(map[string]string{"sk-live": "user-42"}),
		OnAuthFailure: func(*http.Request, stream.AuthResult) { failureSeen = true },
	})
	adapter.Mount(hub)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/stream/ws", nil)
	adapter.ServeHTTP(recorder, request)
	core.AssertEqual(t, http.StatusUnauthorized, recorder.Code, "missing key returns 401")
	core.AssertTrue(t, failureSeen, "OnAuthFailure invoked")
}

func TestWs_ForbiddenChannel_Behaviour(t *core.T) {
	hub := stream.NewHubWithConfig(stream.HubConfig{
		ChannelAuthoriser: func(*stream.Peer, string) bool { return false },
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)
	waitFor(t, func() bool { return hub.Running() })

	adapter := New(Config{})
	adapter.Mount(hub)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/stream/ws?channel=secret", nil)
	adapter.ServeHTTP(recorder, request)
	core.AssertEqual(t, http.StatusForbidden, recorder.Code, "unauthorised channel returns 403")
}

func TestWs_SubscribeReceivePublish_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	adapter := New(Config{})
	adapter.Mount(hub)
	server := httptest.NewServer(adapter.Handler())
	defer server.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(server.URL), nil)
	core.AssertNoError(t, err, "ws connection upgrades")
	defer conn.Close()

	// Subscribe via a control message.
	core.AssertNoError(t, conn.WriteJSON(stream.Message{Type: stream.TypeSubscribe, Channel: "block"}), "subscribe sent")
	waitFor(t, func() bool { return hub.ChannelSubscriberCount("block") == 1 })

	// Hub publishes; the client reads the frame.
	hub.Publish("block", []byte("template"))
	_, payload, err := conn.ReadMessage()
	core.AssertNoError(t, err, "frame received")
	core.AssertEqual(t, "template", string(payload), "published frame delivered to ws client")

	// Unsubscribe removes the subscription.
	core.AssertNoError(t, conn.WriteJSON(stream.Message{Type: stream.TypeUnsubscribe, Channel: "block"}), "unsubscribe sent")
	waitFor(t, func() bool { return hub.ChannelSubscriberCount("block") == 0 })
}

func TestWs_PingPong_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	adapter := New(Config{})
	adapter.Mount(hub)
	server := httptest.NewServer(adapter.Handler())
	defer server.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(server.URL), nil)
	core.AssertNoError(t, err, "ws connection upgrades")
	defer conn.Close()

	core.AssertNoError(t, conn.WriteJSON(stream.Message{Type: stream.TypePing, ProcessID: "client-1"}), "ping sent")
	_, payload, err := conn.ReadMessage()
	core.AssertNoError(t, err, "pong received")
	var reply stream.Message
	core.AssertTrue(t, core.JSONUnmarshal(payload, &reply).OK, "pong decodes")
	core.AssertEqual(t, stream.TypePong, reply.Type, "type is pong")
	core.AssertEqual(t, "client-1", reply.ProcessID, "pong echoes process id")
}

func TestWs_PublishFromClient_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	var mutex sync.Mutex
	var got string
	hub.Subscribe("uplink", func(frame []byte) {
		mutex.Lock()
		got = string(frame)
		mutex.Unlock()
	})

	adapter := New(Config{})
	adapter.Mount(hub)
	server := httptest.NewServer(adapter.Handler())
	defer server.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(server.URL), nil)
	core.AssertNoError(t, err, "ws connection upgrades")
	defer conn.Close()
	waitFor(t, func() bool { return hub.PeerCount() == 1 })

	core.AssertNoError(t, conn.WriteJSON(stream.Message{Type: stream.TypeEvent, Channel: "uplink"}), "publish sent")
	waitFor(t, func() bool { mutex.Lock(); defer mutex.Unlock(); return got != "" })
	mutex.Lock()
	core.AssertContains(t, got, "uplink", "client publish reached the hub subscriber")
	mutex.Unlock()
}

func TestWs_MarshalAndErrorPayload_Behaviour(t *core.T) {
	frame := marshalMessage(stream.Message{Type: stream.TypeEvent, Channel: "x"})
	core.AssertContains(t, string(frame), "event", "marshalMessage produces JSON")

	core.AssertNil(t, errorPayload(nil), "nil value yields nil payload")
	withErr := errorPayload(core.E("scope", "boom", nil))
	core.AssertContains(t, withErr["message"].(string), "boom", "error payload carries message")
	withVal := errorPayload(42)
	core.AssertEqual(t, "42", withVal["message"], "non-error value stringified")
}

func TestWs_Reconnect_Defaults_Behaviour(t *core.T) {
	client := NewReconnectingClient(ReconnectConfig{URL: "ws://127.0.0.1:1"})
	core.AssertEqual(t, 500*time.Millisecond, client.config.InitialBackoff, "initial backoff defaulted")
	core.AssertEqual(t, 30*time.Second, client.config.MaxBackoff, "max backoff defaulted")
	core.AssertEqual(t, float64(2), client.config.BackoffMultiplier, "multiplier defaulted")
	core.AssertEqual(t, stream.StateDisconnected, client.State(), "starts disconnected")
}

func TestWs_Reconnect_SendBeforeConnect_Behaviour(t *core.T) {
	client := NewReconnectingClient(ReconnectConfig{URL: "ws://127.0.0.1:1"})
	core.AssertFalse(t, client.Send(stream.Message{Type: stream.TypeEvent}).OK, "send before connect fails")

	var nilClient *ReconnectingClient
	core.AssertFalse(t, nilClient.Send(stream.Message{}).OK, "nil client send fails")
	core.AssertEqual(t, stream.StateDisconnected, nilClient.State(), "nil client disconnected")
	core.AssertTrue(t, nilClient.Close().OK, "nil client close ok")
	core.AssertFalse(t, nilClient.Connect(context.Background()).OK, "nil client connect fails")
}

func TestWs_Reconnect_ConnectReceiveClose_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	adapter := New(Config{})
	adapter.Mount(hub)
	server := httptest.NewServer(adapter.Handler())
	defer server.Close()

	var mutex sync.Mutex
	var connected bool
	var messages []stream.Message
	client := NewReconnectingClient(ReconnectConfig{
		URL:            wsURL(server.URL),
		InitialBackoff: 10 * time.Millisecond,
		OnConnect:      func() { mutex.Lock(); connected = true; mutex.Unlock() },
		OnMessage: func(msg stream.Message) {
			mutex.Lock()
			messages = append(messages, msg)
			mutex.Unlock()
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	go client.Connect(ctx)
	waitFor(t, func() bool { mutex.Lock(); defer mutex.Unlock(); return connected })
	waitFor(t, func() bool { return client.State() == stream.StateConnected })
	waitFor(t, func() bool { return hub.PeerCount() == 1 })

	// Subscribe the client, then publish from the hub.
	client.Send(stream.Message{Type: stream.TypeSubscribe, Channel: "block"})
	waitFor(t, func() bool { return hub.ChannelSubscriberCount("block") == 1 })

	hub.Publish("block", []byte(core.JSONMarshalString(stream.Message{Type: stream.TypeEvent, Channel: "block"})))
	waitFor(t, func() bool { mutex.Lock(); defer mutex.Unlock(); return len(messages) > 0 })

	cancel()
	core.AssertTrue(t, client.Close().OK, "close succeeds")
	core.AssertEqual(t, stream.StateDisconnected, client.State(), "disconnected after close")
}

func TestWs_Reconnect_MaxRetriesGivesUp_Behaviour(t *core.T) {
	client := NewReconnectingClient(ReconnectConfig{
		URL:            "ws://127.0.0.1:1",
		InitialBackoff: time.Millisecond,
		MaxRetries:     1,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	core.AssertFalse(t, client.Connect(ctx).OK, "exhausted retries returns failure")
}

func TestWs_NextBackoff_Behaviour(t *core.T) {
	core.AssertEqual(t, 2*time.Second, nextBackoff(time.Second, 2, 30*time.Second), "doubles")
	core.AssertEqual(t, 30*time.Second, nextBackoff(20*time.Second, 2, 30*time.Second), "caps at max")
	core.AssertEqual(t, time.Second, nextBackoff(time.Second, 0, 30*time.Second), "non-positive multiplier holds")
}

func TestWs_SleepContext_Behaviour(t *core.T) {
	core.AssertTrue(t, sleepContext(context.Background(), 0).OK, "zero duration returns immediately")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	core.AssertFalse(t, sleepContext(ctx, time.Second).OK, "cancelled context aborts sleep")
}
