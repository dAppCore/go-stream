// SPDX-License-Identifier: EUPL-1.2

// Behaviour-level tests for the stream package. The sibling *_test.go
// files assert callable shape (signature smoke tests); these exercise
// real hub lifecycle, pub/sub delivery, broadcast fan-out, peer
// registration, backpressure, authentication, and pipe bridging.
package stream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	core "dappco.re/go"
)

// runHub starts a hub event loop and returns a stop function that
// cancels it and waits for the loop to drain.
//
//	hub, stop := runHub(t)
//	defer stop()
func runHub(t *core.T) (*Hub, func()) {
	t.Helper()
	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	go hub.Run(ctx)
	waitFor(t, func() bool { return hub.Running() })
	return hub, func() {
		cancel()
		<-hub.done
	}
}

// waitFor polls condition until true or the deadline elapses.
//
//	waitFor(t, func() bool { return hub.PeerCount() == 1 })
func waitFor(t *core.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
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

// drainNext reads one frame from a peer's send queue or fails on timeout.
//
//	frame := drainNext(t, peer)
func drainNext(t *core.T, peer *Peer) []byte {
	t.Helper()
	select {
	case frame := <-peer.SendQueue():
		return frame
	case <-time.After(2 * time.Second):
		t.Fatalf("no frame delivered within deadline")
		return nil
	}
}

func TestStream_Hub_PublishBeforeRun_Behaviour(t *core.T) {
	hub := NewHub()
	r := hub.Publish("hashrate", []byte("123"))
	core.AssertFalse(t, r.OK, "publish before Run must fail")
	core.AssertErrorIs(t, r.Value.(error), ErrHubNotRunning, "carries ErrHubNotRunning")
}

func TestStream_Hub_BroadcastBeforeRun_Behaviour(t *core.T) {
	hub := NewHub()
	r := hub.Broadcast([]byte("ping"))
	core.AssertFalse(t, r.OK, "broadcast before Run must fail")
	core.AssertErrorIs(t, r.Value.(error), ErrHubNotRunning, "carries ErrHubNotRunning")
}

func TestStream_Hub_NilReceiver_Behaviour(t *core.T) {
	var hub *Hub
	core.AssertFalse(t, hub.Running(), "nil hub not running")
	core.AssertEqual(t, 0, hub.PeerCount(), "nil hub has no peers")
	core.AssertEqual(t, 0, hub.ChannelCount(), "nil hub has no channels")
	r := hub.Publish("x", []byte("y"))
	core.AssertFalse(t, r.OK, "nil hub publish fails")
	core.AssertNotNil(t, hub.Config(), "nil hub returns default config")
}

func TestStream_Hub_SubscribeReceivesPublish_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	var mutex sync.Mutex
	var received [][]byte
	unsub := hub.Subscribe("block", func(frame []byte) {
		mutex.Lock()
		received = append(received, append([]byte(nil), frame...))
		mutex.Unlock()
	})
	defer unsub()

	r := hub.Publish("block", []byte("template"))
	core.AssertTrue(t, r.OK, "publish to running hub succeeds")

	waitFor(t, func() bool {
		mutex.Lock()
		defer mutex.Unlock()
		return len(received) == 1
	})
	mutex.Lock()
	core.AssertEqual(t, "template", string(received[0]), "handler sees published frame")
	mutex.Unlock()
}

func TestStream_Hub_UnsubscribeStopsDelivery_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	var count int
	var mutex sync.Mutex
	unsub := hub.Subscribe("block", func([]byte) {
		mutex.Lock()
		count++
		mutex.Unlock()
	})
	hub.Publish("block", []byte("one"))
	waitFor(t, func() bool { mutex.Lock(); defer mutex.Unlock(); return count == 1 })

	unsub()
	unsub() // idempotent — onceFunction guard

	core.AssertEqual(t, 0, hub.ChannelSubscriberCount("block"), "subscriber removed after unsub")
	hub.Publish("block", []byte("two"))
	time.Sleep(20 * time.Millisecond)
	mutex.Lock()
	core.AssertEqual(t, 1, count, "no delivery after unsubscribe")
	mutex.Unlock()
}

func TestStream_Hub_SubscribeWithErrorRejectsEmptyChannel_Behaviour(t *core.T) {
	hub := NewHub()
	r := hub.SubscribeWithError("", func([]byte) {})
	core.AssertFalse(t, r.OK, "empty channel rejected")
	core.AssertErrorIs(t, r.Value.(error), ErrEmptyChannel, "carries ErrEmptyChannel")

	r = hub.SubscribeWithError("block", nil)
	core.AssertFalse(t, r.OK, "nil handler rejected")
}

func TestStream_Hub_WildcardSubscriberSeesEveryChannel_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	var mutex sync.Mutex
	var seen int
	unsub := hub.Subscribe("*", func([]byte) {
		mutex.Lock()
		seen++
		mutex.Unlock()
	})
	defer unsub()

	hub.Publish("a", []byte("1"))
	hub.Publish("b", []byte("2"))
	waitFor(t, func() bool { mutex.Lock(); defer mutex.Unlock(); return seen == 2 })
}

func TestStream_Hub_SubscribePublishedSeesChannel_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	var mutex sync.Mutex
	var gotChannel string
	stopSub := hub.SubscribePublished(func(channel string, _ []byte) {
		mutex.Lock()
		gotChannel = channel
		mutex.Unlock()
	})
	defer stopSub()

	hub.Publish("hashrate", []byte("x"))
	waitFor(t, func() bool { mutex.Lock(); defer mutex.Unlock(); return gotChannel == "hashrate" })
}

func TestStream_Hub_PeerReceivesPublishedFrame_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	peer := NewPeer("ws")
	core.AssertTrue(t, hub.AddPeer(peer).OK, "add peer succeeds")
	core.AssertTrue(t, hub.SubscribePeer(peer, "block").OK, "peer subscribes")

	hub.Publish("block", []byte("template"))
	frame := drainNext(t, peer)
	core.AssertEqual(t, "template", string(frame), "peer received the frame")
}

func TestStream_Hub_BroadcastReachesAllPeers_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	peerA := NewPeer("ws")
	peerB := NewPeer("ws")
	hub.AddPeer(peerA)
	hub.AddPeer(peerB)
	waitFor(t, func() bool { return hub.PeerCount() == 2 })

	r := hub.Broadcast([]byte("shutdown"))
	core.AssertTrue(t, r.OK, "broadcast succeeds")

	core.AssertEqual(t, "shutdown", string(drainNext(t, peerA)), "peer A got broadcast")
	core.AssertEqual(t, "shutdown", string(drainNext(t, peerB)), "peer B got broadcast")
}

func TestStream_Hub_RemovePeerClosesAndUnsubscribes_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	peer := NewPeer("ws")
	hub.AddPeer(peer)
	hub.SubscribePeer(peer, "block")
	waitFor(t, func() bool { return hub.PeerCount() == 1 })

	hub.RemovePeer(peer)
	waitFor(t, func() bool { return hub.PeerCount() == 0 })
	core.AssertEqual(t, 0, hub.ChannelSubscriberCount("block"), "peer removed from channel")
}

func TestStream_Hub_OnConnectDisconnectFire_Behaviour(t *core.T) {
	var mutex sync.Mutex
	var connects, disconnects int
	config := HubConfig{
		OnConnect:    func(*Peer) { mutex.Lock(); connects++; mutex.Unlock() },
		OnDisconnect: func(*Peer) { mutex.Lock(); disconnects++; mutex.Unlock() },
	}
	hub := NewHubWithConfig(config)
	ctx, cancel := context.WithCancel(context.Background())
	go hub.Run(ctx)
	waitFor(t, func() bool { return hub.Running() })

	peer := NewPeer("ws")
	hub.AddPeer(peer)
	waitFor(t, func() bool { mutex.Lock(); defer mutex.Unlock(); return connects == 1 })
	hub.RemovePeer(peer)
	waitFor(t, func() bool { mutex.Lock(); defer mutex.Unlock(); return disconnects == 1 })

	cancel()
	<-hub.done
}

func TestStream_Hub_ChannelAuthoriserRejects_Behaviour(t *core.T) {
	config := HubConfig{
		ChannelAuthoriser: func(peer *Peer, channel string) bool {
			return peer.Claims["role"] == "admin" || channel == "public"
		},
	}
	hub := NewHubWithConfig(config)

	guest := NewPeer("ws")
	r := hub.SubscribePeer(guest, "secret")
	core.AssertFalse(t, r.OK, "guest rejected on secret channel")
	core.AssertErrorIs(t, r.Value.(error), ErrAuthRejected, "carries ErrAuthRejected")

	core.AssertTrue(t, hub.SubscribePeer(guest, "public").OK, "public channel allowed")

	admin := NewPeer("ws")
	admin.Claims["role"] = "admin"
	core.AssertTrue(t, hub.SubscribePeer(admin, "secret").OK, "admin allowed on secret")

	core.AssertFalse(t, hub.CanSubscribePeer(guest, "secret").OK, "CanSubscribePeer mirrors decision")
	core.AssertTrue(t, hub.CanSubscribePeer(admin, "secret").OK, "CanSubscribePeer allows admin")
}

func TestStream_Hub_StatsAndCounts_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	unsubA := hub.Subscribe("a", func([]byte) {})
	defer unsubA()
	unsubB := hub.Subscribe("b", func([]byte) {})
	defer unsubB()

	stats := hub.Stats()
	core.AssertEqual(t, 2, stats.Channels, "two channels reported")
	core.AssertEqual(t, 1, stats.SubscriberCount["a"], "channel a has one subscriber")
	core.AssertEqual(t, 2, hub.ChannelCount(), "ChannelCount matches")

	channels := make([]string, 0)
	for channel := range hub.AllChannels() {
		channels = append(channels, channel)
	}
	core.AssertEqual(t, 2, len(channels), "AllChannels yields both")
	core.AssertEqual(t, "a", channels[0], "AllChannels is sorted")
}

func TestStream_Hub_AllPeersSorted_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	peer := NewPeer("ws")
	hub.AddPeer(peer)
	waitFor(t, func() bool { return hub.PeerCount() == 1 })

	count := 0
	for p := range hub.AllPeers() {
		core.AssertEqual(t, peer.ID, p.ID, "AllPeers yields the added peer")
		count++
	}
	core.AssertEqual(t, 1, count, "AllPeers yields exactly one")
}

func TestStream_Peer_SendBackpressureAndClose_Behaviour(t *core.T) {
	peer := NewPeer("ws")
	// Fill the buffer (defaultPeerSendBufferSize) then expect a drop.
	for i := 0; i < defaultPeerSendBufferSize; i++ {
		core.AssertTrue(t, peer.Send([]byte("x")), "send within buffer succeeds")
	}
	core.AssertFalse(t, peer.Send([]byte("overflow")), "send fails when buffer full (backpressure)")

	peer.Close()
	peer.Close() // idempotent
	core.AssertFalse(t, peer.Send([]byte("after close")), "send after close fails safely")
}

func TestStream_Peer_CloseHookFires_Behaviour(t *core.T) {
	peer := NewPeer("ws")
	var fired bool
	peer.SetCloseHook(func() { fired = true })
	peer.Close()
	core.AssertTrue(t, fired, "close hook fired once")
}

func TestStream_Peer_SubscriptionsSorted_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()
	peer := NewPeer("ws")
	hub.AddPeer(peer)
	hub.SubscribePeer(peer, "zeta")
	hub.SubscribePeer(peer, "alpha")
	subs := peer.Subscriptions()
	core.AssertEqual(t, 2, len(subs), "two subscriptions")
	core.AssertEqual(t, "alpha", subs[0], "subscriptions sorted")
}

func TestStream_Peer_NilReceiver_Behaviour(t *core.T) {
	var peer *Peer
	core.AssertFalse(t, peer.Send([]byte("x")), "nil peer send fails")
	core.AssertNil(t, peer.Subscriptions(), "nil peer has no subscriptions")
	core.AssertNil(t, peer.SendQueue(), "nil peer has no queue")
	peer.Close()        // no panic
	peer.SetCloseHook(func() {}) // no panic
}

func TestStream_ConnectionState_String_Behaviour(t *core.T) {
	core.AssertEqual(t, "disconnected", StateDisconnected.String(), "disconnected")
	core.AssertEqual(t, "connecting", StateConnecting.String(), "connecting")
	core.AssertEqual(t, "connected", StateConnected.String(), "connected")
	core.AssertEqual(t, "disconnected", ConnectionState(99).String(), "unknown falls back")
}

func TestStream_Pipe_ForwardsPublishedFrames_Behaviour(t *core.T) {
	src, stopSrc := runHub(t)
	defer stopSrc()
	dst, stopDst := runHub(t)
	defer stopDst()

	var mutex sync.Mutex
	var got string
	unsub := dst.Subscribe("block", func(frame []byte) {
		mutex.Lock()
		got = string(frame)
		mutex.Unlock()
	})
	defer unsub()

	stopPipe := Pipe(src, dst)
	defer stopPipe()

	src.Publish("block", []byte("relayed"))
	waitFor(t, func() bool { mutex.Lock(); defer mutex.Unlock(); return got == "relayed" })
}

func TestStream_Pipe_NilAndSelf_Behaviour(t *core.T) {
	hub := NewHub()
	core.AssertNotNil(t, Pipe(nil, hub), "nil src returns no-op stopper")
	core.AssertNotNil(t, Pipe(hub, nil), "nil dst returns no-op stopper")
	core.AssertNotNil(t, Pipe(hub, hub), "self pipe returns no-op stopper")
	// The no-op stoppers must not panic when called.
	Pipe(nil, nil)()
}

func TestStream_HubConfig_Normalise_Behaviour(t *core.T) {
	cfg := normalizeHubConfig(HubConfig{})
	def := DefaultHubConfig()
	core.AssertEqual(t, def.HeartbeatInterval, cfg.HeartbeatInterval, "heartbeat defaulted")
	core.AssertEqual(t, def.WriteTimeout, cfg.WriteTimeout, "write timeout defaulted")
	core.AssertGreater(t, int64(cfg.PongTimeout), int64(cfg.HeartbeatInterval), "pong > heartbeat")

	// PongTimeout <= HeartbeatInterval is corrected to 2x.
	tight := normalizeHubConfig(HubConfig{HeartbeatInterval: 10 * time.Second, PongTimeout: 5 * time.Second})
	core.AssertEqual(t, 20*time.Second, tight.PongTimeout, "pong corrected to 2x heartbeat")
}

func TestStream_Auth_APIKey_Behaviour(t *core.T) {
	auth := NewAPIKeyAuth(map[string]string{"sk-live": "user-42"})

	good := httptest.NewRequest(http.MethodGet, "/stream/ws", nil)
	good.Header.Set("Authorization", "Bearer sk-live")
	result := auth.Authenticate(good)
	core.AssertTrue(t, result.Valid, "valid key authenticates")
	core.AssertEqual(t, "user-42", result.UserID, "user id resolved")
	core.AssertNotNil(t, result.Claims, "claims initialised")

	unknown := httptest.NewRequest(http.MethodGet, "/stream/ws", nil)
	unknown.Header.Set("Authorization", "Bearer nope")
	bad := auth.Authenticate(unknown)
	core.AssertFalse(t, bad.Valid, "unknown key rejected")
	core.AssertErrorIs(t, bad.Error, ErrInvalidAPIKey, "carries ErrInvalidAPIKey")

	missing := httptest.NewRequest(http.MethodGet, "/stream/ws", nil)
	noHeader := auth.Authenticate(missing)
	core.AssertFalse(t, noHeader.Valid, "missing header rejected")
	core.AssertErrorIs(t, noHeader.Error, ErrMissingAuthHeader, "carries ErrMissingAuthHeader")

	malformed := httptest.NewRequest(http.MethodGet, "/stream/ws", nil)
	malformed.Header.Set("Authorization", "Token sk-live")
	bad2 := auth.Authenticate(malformed)
	core.AssertFalse(t, bad2.Valid, "non-Bearer header rejected")
	core.AssertErrorIs(t, bad2.Error, ErrMalformedAuthHeader, "carries ErrMalformedAuthHeader")

	emptyBearer := httptest.NewRequest(http.MethodGet, "/stream/ws", nil)
	emptyBearer.Header.Set("Authorization", "Bearer ")
	core.AssertFalse(t, auth.Authenticate(emptyBearer).Valid, "empty bearer token rejected")

	core.AssertFalse(t, auth.Authenticate(nil).Valid, "nil request rejected")
}

func TestStream_Auth_BearerAndQuery_Behaviour(t *core.T) {
	validate := func(token string) AuthResult {
		if token == "sk-live" {
			return AuthResult{Valid: true, UserID: "user-1"}
		}
		return AuthResult{Valid: false}
	}

	bearer := &BearerTokenAuth{Validate: validate}
	req := httptest.NewRequest(http.MethodGet, "/stream/ws", nil)
	req.Header.Set("Authorization", "Bearer sk-live")
	core.AssertTrue(t, bearer.Authenticate(req).Valid, "bearer validates")
	core.AssertFalse(t, (&BearerTokenAuth{}).Authenticate(req).Valid, "nil Validate rejects")

	query := &QueryTokenAuth{Validate: validate}
	qReq := httptest.NewRequest(http.MethodGet, "/stream/ws?token=sk-live", nil)
	core.AssertTrue(t, query.Authenticate(qReq).Valid, "query token validates")
	noToken := httptest.NewRequest(http.MethodGet, "/stream/ws", nil)
	core.AssertFalse(t, query.Authenticate(noToken).Valid, "missing query token rejected")
}

func TestStream_Auth_FuncAdapters_Behaviour(t *core.T) {
	authFunc := AuthenticatorFunc(func(*http.Request) AuthResult {
		return AuthResult{Valid: true, UserID: "user-9"}
	})
	req := httptest.NewRequest(http.MethodGet, "/stream/ws", nil)
	core.AssertTrue(t, authFunc.Authenticate(req).Valid, "func adapter authenticates")
	core.AssertFalse(t, authFunc.Authenticate(nil).Valid, "nil request rejected")

	var nilFunc AuthenticatorFunc
	core.AssertFalse(t, nilFunc.Authenticate(req).Valid, "nil func rejects")

	connFunc := ConnAuthenticatorFunc(func(handshake []byte) AuthResult {
		if string(handshake) == "hello" {
			return AuthResult{Valid: true, UserID: "peer-1"}
		}
		return AuthResult{Valid: false}
	})
	core.AssertTrue(t, connFunc.AuthenticateConn([]byte("hello")).Valid, "conn auth accepts handshake")
	core.AssertFalse(t, connFunc.AuthenticateConn([]byte("bye")).Valid, "conn auth rejects bad handshake")
	var nilConn ConnAuthenticatorFunc
	core.AssertFalse(t, nilConn.AuthenticateConn([]byte("hello")).Valid, "nil conn func rejects")
}

func TestStream_Message_Type_Behaviour(t *core.T) {
	core.AssertEqual(t, "event", TypeEvent.String(), "event type string")
	core.AssertEqual(t, "process_output", TypeProcessOutput.String(), "process output type string")
	core.AssertEqual(t, "ping", TypePing.String(), "ping type string")

	msg := Message{Type: TypeEvent, Channel: "hashrate", Data: map[string]any{"h": 123}}
	frame := core.JSONMarshal(msg)
	core.AssertTrue(t, frame.OK, "message marshals")
	core.AssertContains(t, string(frame.Value.([]byte)), "hashrate", "channel in JSON")
	core.AssertContains(t, string(frame.Value.([]byte)), "event", "type in JSON")
}
