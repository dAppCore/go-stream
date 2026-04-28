// SPDX-License-Identifier: EUPL-1.2

package stream

import core "dappco.re/go"

type T = core.T
type CancelFunc = core.CancelFunc
type Duration = core.Duration
type Request = core.Request

const (
	Millisecond = core.Millisecond
	Second      = core.Second
)

var (
	Background         = core.Background
	NewHTTPTestRequest = core.NewHTTPTestRequest
	Sleep              = core.Sleep
	WithCancel         = core.WithCancel
	WithTimeout        = core.WithTimeout

	AssertContains  = core.AssertContains
	AssertEqual     = core.AssertEqual
	AssertError     = core.AssertError
	AssertFalse     = core.AssertFalse
	AssertNil       = core.AssertNil
	AssertNoError   = core.AssertNoError
	AssertNotEqual  = core.AssertNotEqual
	AssertNotNil    = core.AssertNotNil
	AssertNotPanics = core.AssertNotPanics
	AssertTrue      = core.AssertTrue
)

func ax7RunningHub(t *T) (*Hub, CancelFunc) {
	hub := NewHub()
	ctx, cancel := WithCancel(Background())
	go hub.Run(ctx)
	waitForRunningHub(t, hub)
	return hub, cancel
}

func ax7Timeout(duration Duration) <-chan struct{} {
	ctx, cancel := WithTimeout(Background(), duration)
	done := make(chan struct{})
	go func() {
		defer cancel()
		<-ctx.Done()
		close(done)
	}()
	return done
}

func TestAX7_APIKeyAuthenticator_Authenticate_Good(t *T) {
	authenticator := NewAPIKeyAuth(map[string]string{"sk-live": "user-42"})
	request := NewHTTPTestRequest("GET", "/stream/ws", nil)
	request.Header.Set("Authorization", "Bearer sk-live")

	result := authenticator.Authenticate(request)
	AssertTrue(t, result.Valid)
	AssertEqual(t, "user-42", result.UserID)
	AssertNotNil(t, result.Claims)
}

func TestAX7_APIKeyAuthenticator_Authenticate_Bad(t *T) {
	authenticator := NewAPIKeyAuth(map[string]string{"sk-live": "user-42"})
	request := NewHTTPTestRequest("GET", "/stream/ws", nil)

	result := authenticator.Authenticate(request)
	AssertFalse(t, result.Valid)
	AssertEqual(t, ErrMissingAuthHeader, result.Error)
}

func TestAX7_APIKeyAuthenticator_Authenticate_Ugly(t *T) {
	var authenticator *APIKeyAuthenticator
	request := NewHTTPTestRequest("GET", "/stream/ws", nil)
	request.Header.Set("Authorization", "Bearer sk-live")

	result := authenticator.Authenticate(request)
	AssertFalse(t, result.Valid)
	AssertEqual(t, "", result.UserID)
}

func TestAX7_AuthenticatorFunc_Authenticate_Good(t *T) {
	authenticator := AuthenticatorFunc(func(request *Request) AuthResult {
		return AuthResult{Valid: request.URL.Path == "/stream/ws", UserID: "agent"}
	})
	request := NewHTTPTestRequest("GET", "/stream/ws", nil)

	result := authenticator.Authenticate(request)
	AssertTrue(t, result.Valid)
	AssertEqual(t, "agent", result.UserID)
	AssertNotNil(t, result.Claims)
}

func TestAX7_AuthenticatorFunc_Authenticate_Bad(t *T) {
	authenticator := AuthenticatorFunc(func(request *Request) AuthResult {
		return AuthResult{Valid: false, Error: ErrAuthRejected}
	})
	request := NewHTTPTestRequest("GET", "/stream/ws", nil)

	result := authenticator.Authenticate(request)
	AssertFalse(t, result.Valid)
	AssertEqual(t, ErrAuthRejected, result.Error)
}

func TestAX7_AuthenticatorFunc_Authenticate_Ugly(t *T) {
	var authenticator AuthenticatorFunc
	request := NewHTTPTestRequest("GET", "/stream/ws", nil)

	result := authenticator.Authenticate(request)
	AssertFalse(t, result.Valid)
	AssertNil(t, result.Claims)
}

func TestAX7_BearerTokenAuth_Authenticate_Good(t *T) {
	authenticator := &BearerTokenAuth{Validate: func(token string) AuthResult {
		return AuthResult{Valid: token == "jwt-valid", UserID: "user-99"}
	}}
	request := NewHTTPTestRequest("GET", "/stream/ws", nil)
	request.Header.Set("Authorization", "Bearer jwt-valid")

	result := authenticator.Authenticate(request)
	AssertTrue(t, result.Valid)
	AssertEqual(t, "user-99", result.UserID)
}

func TestAX7_BearerTokenAuth_Authenticate_Bad(t *T) {
	authenticator := &BearerTokenAuth{Validate: func(token string) AuthResult {
		return AuthResult{Valid: false, Error: ErrAuthRejected}
	}}
	request := NewHTTPTestRequest("GET", "/stream/ws", nil)
	request.Header.Set("Authorization", "Bearer jwt-invalid")

	result := authenticator.Authenticate(request)
	AssertFalse(t, result.Valid)
	AssertEqual(t, ErrAuthRejected, result.Error)
}

func TestAX7_BearerTokenAuth_Authenticate_Ugly(t *T) {
	authenticator := &BearerTokenAuth{}
	request := NewHTTPTestRequest("GET", "/stream/ws", nil)
	request.Header.Set("Authorization", "Bearer jwt-valid")

	result := authenticator.Authenticate(request)
	AssertFalse(t, result.Valid)
	AssertEqual(t, "", result.UserID)
}

func TestAX7_ConnAuthenticatorFunc_AuthenticateConn_Good(t *T) {
	authenticator := ConnAuthenticatorFunc(func(handshake []byte) AuthResult {
		return AuthResult{Valid: string(handshake) == "hello", UserID: "peer-1"}
	})
	result := authenticator.AuthenticateConn([]byte("hello"))

	AssertTrue(t, result.Valid)
	AssertEqual(t, "peer-1", result.UserID)
	AssertNotNil(t, result.Claims)
}

func TestAX7_ConnAuthenticatorFunc_AuthenticateConn_Bad(t *T) {
	authenticator := ConnAuthenticatorFunc(func(handshake []byte) AuthResult {
		return AuthResult{Valid: false, Error: ErrAuthRejected}
	})
	result := authenticator.AuthenticateConn([]byte("bad"))

	AssertFalse(t, result.Valid)
	AssertEqual(t, ErrAuthRejected, result.Error)
	AssertNil(t, result.Claims)
}

func TestAX7_ConnAuthenticatorFunc_AuthenticateConn_Ugly(t *T) {
	var authenticator ConnAuthenticatorFunc
	result := authenticator.AuthenticateConn(nil)

	AssertFalse(t, result.Valid)
	AssertEqual(t, "", result.UserID)
	AssertNil(t, result.Claims)
}

func TestAX7_DefaultHubConfig_Good(t *T) {
	config := DefaultHubConfig()

	AssertEqual(t, 30*Second, config.HeartbeatInterval)
	AssertEqual(t, 60*Second, config.PongTimeout)
	AssertEqual(t, 10*Second, config.WriteTimeout)
}

func TestAX7_DefaultHubConfig_Bad(t *T) {
	config := DefaultHubConfig()

	AssertNil(t, config.OnConnect)
	AssertNil(t, config.OnDisconnect)
	AssertNil(t, config.ChannelAuthoriser)
}

func TestAX7_DefaultHubConfig_Ugly(t *T) {
	config := normalizeHubConfig(HubConfig{HeartbeatInterval: Second, PongTimeout: Millisecond})

	AssertEqual(t, Second, config.HeartbeatInterval)
	AssertEqual(t, 2*Second, config.PongTimeout)
	AssertEqual(t, 10*Second, config.WriteTimeout)
}

func TestAX7_Hub_AddPeer_Good(t *T) {
	hub := NewHub()
	peer := NewPeer("ws")

	AssertNoError(t, hub.AddPeer(peer))
	AssertEqual(t, 1, hub.PeerCount())
	AssertEqual(t, 0, len(peer.Subscriptions()))
}

func TestAX7_Hub_AddPeer_Bad(t *T) {
	hub := NewHub()

	err := hub.AddPeer(nil)
	AssertError(t, err)
	AssertContains(t, err.Error(), "nil peer")
}

func TestAX7_Hub_AddPeer_Ugly(t *T) {
	hub := NewHub()
	peer := &Peer{Transport: "ws"}

	AssertNoError(t, hub.AddPeer(peer))
	AssertNotNil(t, peer.SendQueue())
	AssertEqual(t, 1, hub.PeerCount())
}

func TestAX7_Hub_AllChannels_Good(t *T) {
	hub := NewHub()
	stopA := hub.Subscribe("block", func([]byte) {})
	defer stopA()
	stopB := hub.Subscribe("hashrate", func([]byte) {})
	defer stopB()

	var channels []string
	for channel := range hub.AllChannels() {
		channels = append(channels, channel)
	}
	AssertEqual(t, []string{"block", "hashrate"}, channels)
}

func TestAX7_Hub_AllChannels_Bad(t *T) {
	var hub *Hub
	count := 0

	for range hub.AllChannels() {
		count++
	}
	AssertEqual(t, 0, count)
}

func TestAX7_Hub_AllChannels_Ugly(t *T) {
	hub := NewHub()
	stop := hub.Subscribe("events", func([]byte) {})
	seq := hub.AllChannels()
	stop()

	var channels []string
	for channel := range seq {
		channels = append(channels, channel)
	}
	AssertEqual(t, []string{"events"}, channels)
}

func TestAX7_Hub_AllPeers_Good(t *T) {
	hub := NewHub()
	AssertNoError(t, hub.AddPeer(NewPeer("ws")))
	AssertNoError(t, hub.AddPeer(NewPeer("sse")))

	count := 0
	for range hub.AllPeers() {
		count++
	}
	AssertEqual(t, 2, count)
}

func TestAX7_Hub_AllPeers_Bad(t *T) {
	var hub *Hub
	count := 0

	for range hub.AllPeers() {
		count++
	}
	AssertEqual(t, 0, count)
}

func TestAX7_Hub_AllPeers_Ugly(t *T) {
	hub := NewHub()
	peer := NewPeer("ws")
	AssertNoError(t, hub.AddPeer(peer))
	seq := hub.AllPeers()
	hub.RemovePeer(peer)

	count := 0
	for range seq {
		count++
	}
	AssertEqual(t, 1, count)
}

func TestAX7_Hub_BroadcastFromBridge_Good(t *T) {
	hub, cancel := ax7RunningHub(t)
	defer cancel()
	peer := NewPeer("ws")
	AssertNoError(t, hub.AddPeer(peer))
	waitForPeerCount(t, hub, 1)

	AssertNoError(t, hub.BroadcastFromBridge([]byte("bridge")))
	frame := <-peer.SendQueue()
	AssertEqual(t, "bridge", string(frame))
}

func TestAX7_Hub_BroadcastFromBridge_Bad(t *T) {
	hub := NewHub()

	err := hub.BroadcastFromBridge([]byte("bridge"))
	AssertEqual(t, ErrHubNotRunning, err)
	AssertEqual(t, 0, hub.PeerCount())
}

func TestAX7_Hub_BroadcastFromBridge_Ugly(t *T) {
	hub, cancel := ax7RunningHub(t)
	defer cancel()
	seen := false
	stop := hub.SubscribeBroadcast(func([]byte) { seen = true })
	defer stop()

	AssertNoError(t, hub.BroadcastFromBridge([]byte("bridge")))
	Sleep(20 * Millisecond)
	AssertFalse(t, seen)
}

func TestAX7_Hub_BroadcastFromPeer_Bad(t *T) {
	hub := NewHub()
	peer := NewPeer("ws")

	err := hub.BroadcastFromPeer(peer, []byte("frame"))
	AssertEqual(t, ErrHubNotRunning, err)
	AssertEqual(t, 0, hub.PeerCount())
}

func TestAX7_Hub_BroadcastFromPeer_Ugly(t *T) {
	hub, cancel := ax7RunningHub(t)
	defer cancel()
	source := NewPeer("ws")
	receiver := NewPeer("ws")
	AssertNoError(t, hub.AddPeer(source))
	AssertNoError(t, hub.AddPeer(receiver))
	waitForPeerCount(t, hub, 2)

	AssertNoError(t, hub.BroadcastFromPeer(source, []byte("fanout")))
	AssertEqual(t, "fanout", string(<-receiver.SendQueue()))
}

func TestAX7_Hub_CanSubscribePeer_Good(t *T) {
	hub := NewHubWithConfig(HubConfig{ChannelAuthoriser: func(peer *Peer, channel string) bool {
		return peer.UserID == "agent" && channel == "private"
	}})
	peer := NewPeer("ws")
	peer.UserID = "agent"

	AssertNoError(t, hub.CanSubscribePeer(peer, "private"))
	AssertNoError(t, hub.CanSubscribePeer(peer, "*"))
}

func TestAX7_Hub_CanSubscribePeer_Ugly(t *T) {
	hub := NewHub()

	err := hub.CanSubscribePeer(NewPeer("ws"), "")
	AssertEqual(t, ErrEmptyChannel, err)
	AssertError(t, err)
}

func TestAX7_Hub_ChannelCount_Good(t *T) {
	hub := NewHub()
	stop := hub.Subscribe("events", func([]byte) {})
	defer stop()

	AssertEqual(t, 1, hub.ChannelCount())
	AssertEqual(t, 1, hub.ChannelSubscriberCount("events"))
}

func TestAX7_Hub_ChannelCount_Bad(t *T) {
	var hub *Hub

	AssertEqual(t, 0, hub.ChannelCount())
	AssertEqual(t, 0, hub.ChannelSubscriberCount("missing"))
}

func TestAX7_Hub_ChannelCount_Ugly(t *T) {
	hub := NewHub()
	stop := hub.Subscribe("*", func([]byte) {})
	defer stop()

	AssertEqual(t, 0, hub.ChannelCount())
	AssertEqual(t, 1, hub.ChannelSubscriberCount("*"))
}

func TestAX7_Hub_ChannelSubscriberCount_Good(t *T) {
	hub := NewHub()
	peer := NewPeer("ws")
	AssertNoError(t, hub.AddPeer(peer))
	AssertNoError(t, hub.SubscribePeer(peer, "hashrate"))
	stop := hub.Subscribe("hashrate", func([]byte) {})
	defer stop()

	AssertEqual(t, 2, hub.ChannelSubscriberCount("hashrate"))
	AssertEqual(t, 1, hub.PeerCount())
}

func TestAX7_Hub_ChannelSubscriberCount_Bad(t *T) {
	hub := NewHub()

	AssertEqual(t, 0, hub.ChannelSubscriberCount("missing"))
	AssertEqual(t, 0, hub.ChannelCount())
}

func TestAX7_Hub_ChannelSubscriberCount_Ugly(t *T) {
	hub := NewHub()
	peer := NewPeer("ws")
	AssertNoError(t, hub.AddPeer(peer))
	AssertNoError(t, hub.SubscribePeer(peer, "*"))

	AssertEqual(t, 1, hub.ChannelSubscriberCount("*"))
	AssertEqual(t, 0, hub.ChannelCount())
}

func TestAX7_Hub_Config_Good(t *T) {
	hub := NewHubWithConfig(HubConfig{HeartbeatInterval: Second, PongTimeout: 3 * Second})

	config := hub.Config()
	AssertEqual(t, Second, config.HeartbeatInterval)
	AssertEqual(t, 3*Second, config.PongTimeout)
}

func TestAX7_Hub_Config_Bad(t *T) {
	var hub *Hub

	config := hub.Config()
	AssertEqual(t, 30*Second, config.HeartbeatInterval)
	AssertEqual(t, 60*Second, config.PongTimeout)
}

func TestAX7_Hub_Config_Ugly(t *T) {
	hub := NewHubWithConfig(HubConfig{HeartbeatInterval: Second, PongTimeout: Second})

	config := hub.Config()
	AssertEqual(t, Second, config.HeartbeatInterval)
	AssertEqual(t, 2*Second, config.PongTimeout)
}

func TestAX7_Hub_PeerCount_Good(t *T) {
	hub := NewHub()
	AssertNoError(t, hub.AddPeer(NewPeer("ws")))

	AssertEqual(t, 1, hub.PeerCount())
	AssertEqual(t, 0, hub.ChannelCount())
}

func TestAX7_Hub_PeerCount_Bad(t *T) {
	var hub *Hub

	AssertEqual(t, 0, hub.PeerCount())
	AssertEqual(t, HubStats{}, hub.Stats())
}

func TestAX7_Hub_PeerCount_Ugly(t *T) {
	hub := NewHub()
	peer := NewPeer("ws")
	AssertNoError(t, hub.AddPeer(peer))
	hub.RemovePeer(peer)

	AssertEqual(t, 0, hub.PeerCount())
	AssertEqual(t, []string{}, peer.Subscriptions())
}

func TestAX7_Hub_PublishFromBridge_Good(t *T) {
	hub, cancel := ax7RunningHub(t)
	defer cancel()
	received := make(chan []byte, 1)
	stop := hub.Subscribe("block", func(frame []byte) { received <- append([]byte(nil), frame...) })
	defer stop()

	AssertNoError(t, hub.PublishFromBridge("block", []byte("template")))
	AssertEqual(t, "template", string(<-received))
}

func TestAX7_Hub_PublishFromBridge_Bad(t *T) {
	hub := NewHub()

	err := hub.PublishFromBridge("block", []byte("template"))
	AssertEqual(t, ErrHubNotRunning, err)
	AssertEqual(t, 0, hub.ChannelCount())
}

func TestAX7_Hub_PublishFromBridge_Ugly(t *T) {
	hub, cancel := ax7RunningHub(t)
	defer cancel()
	seen := false
	stop := hub.SubscribePublished(func(string, []byte) { seen = true })
	defer stop()

	AssertNoError(t, hub.PublishFromBridge("block", []byte("template")))
	Sleep(20 * Millisecond)
	AssertFalse(t, seen)
}

func TestAX7_Hub_PublishFromPeer_Bad(t *T) {
	hub := NewHub()
	peer := NewPeer("ws")

	err := hub.PublishFromPeer(peer, "block", []byte("template"))
	AssertEqual(t, ErrHubNotRunning, err)
	AssertEqual(t, 0, hub.ChannelCount())
}

func TestAX7_Hub_PublishFromPeer_Ugly(t *T) {
	hub, cancel := ax7RunningHub(t)
	defer cancel()
	source := NewPeer("ws")
	receiver := NewPeer("ws")
	AssertNoError(t, hub.AddPeer(source))
	AssertNoError(t, hub.AddPeer(receiver))
	AssertNoError(t, hub.SubscribePeer(receiver, "block"))

	AssertNoError(t, hub.PublishFromPeer(source, "block", []byte("template")))
	AssertEqual(t, "template", string(<-receiver.SendQueue()))
}

func TestAX7_Hub_RemovePeer_Good(t *T) {
	hub := NewHub()
	peer := NewPeer("ws")
	AssertNoError(t, hub.AddPeer(peer))

	hub.RemovePeer(peer)
	AssertEqual(t, 0, hub.PeerCount())
	AssertEqual(t, []string{}, peer.Subscriptions())
}

func TestAX7_Hub_RemovePeer_Bad(t *T) {
	var hub *Hub
	peer := NewPeer("ws")

	AssertNotPanics(t, func() { hub.RemovePeer(peer) })
	AssertEqual(t, "ws", peer.Transport)
}

func TestAX7_Hub_RemovePeer_Ugly(t *T) {
	hub := NewHub()
	peer := NewPeer("ws")

	AssertNotPanics(t, func() { hub.RemovePeer(peer) })
	AssertEqual(t, 0, hub.PeerCount())
}

func TestAX7_Hub_SendToChannel_Good(t *T) {
	hub, cancel := ax7RunningHub(t)
	defer cancel()
	received := make(chan []byte, 1)
	stop := hub.Subscribe("hashrate", func(frame []byte) { received <- append([]byte(nil), frame...) })
	defer stop()

	AssertNoError(t, hub.SendToChannel("hashrate", []byte("123")))
	AssertEqual(t, "123", string(<-received))
}

func TestAX7_Hub_SendToChannel_Bad(t *T) {
	var hub *Hub

	err := hub.SendToChannel("hashrate", []byte("123"))
	AssertError(t, err)
	AssertContains(t, err.Error(), "nil hub")
}

func TestAX7_Hub_SendToChannel_Ugly(t *T) {
	hub := NewHub()

	err := hub.SendToChannel("hashrate", []byte("123"))
	AssertEqual(t, ErrHubNotRunning, err)
	AssertEqual(t, 0, hub.PeerCount())
}

func TestAX7_Hub_Stats_Good(t *T) {
	hub := NewHub()
	peer := NewPeer("ws")
	AssertNoError(t, hub.AddPeer(peer))
	AssertNoError(t, hub.SubscribePeer(peer, "hashrate"))

	stats := hub.Stats()
	AssertEqual(t, 1, stats.Peers)
	AssertEqual(t, 1, stats.SubscriberCount["hashrate"])
}

func TestAX7_Hub_Stats_Bad(t *T) {
	var hub *Hub

	stats := hub.Stats()
	AssertEqual(t, 0, stats.Peers)
	AssertEqual(t, 0, stats.Channels)
}

func TestAX7_Hub_Stats_Ugly(t *T) {
	hub := NewHub()
	stop := hub.Subscribe("events", func([]byte) {})
	defer stop()

	stats := hub.Stats()
	AssertEqual(t, 1, stats.Channels)
	AssertEqual(t, 1, stats.SubscriberCount["events"])
}

func TestAX7_Hub_SubscribeBroadcast_Good(t *T) {
	hub, cancel := ax7RunningHub(t)
	defer cancel()
	received := make(chan []byte, 1)
	stop := hub.SubscribeBroadcast(func(frame []byte) { received <- append([]byte(nil), frame...) })
	defer stop()

	AssertNoError(t, hub.Broadcast([]byte("shutdown")))
	AssertEqual(t, "shutdown", string(<-received))
}

func TestAX7_Hub_SubscribeBroadcast_Bad(t *T) {
	var hub *Hub

	stop := hub.SubscribeBroadcast(func([]byte) {})
	AssertNotNil(t, stop)
	AssertNotPanics(t, stop)
}

func TestAX7_Hub_SubscribeBroadcast_Ugly(t *T) {
	hub, cancel := ax7RunningHub(t)
	defer cancel()
	received := make(chan []byte, 1)
	stop := hub.SubscribeBroadcast(func(frame []byte) { received <- frame })
	stop()

	AssertNoError(t, hub.Broadcast([]byte("shutdown")))
	select {
	case frame := <-received:
		t.Fatalf("received after unsubscribe: %q", string(frame))
	case <-ax7Timeout(20 * Millisecond):
	}
}

func TestAX7_Hub_SubscribePeer_Good(t *T) {
	hub := NewHub()
	peer := NewPeer("ws")
	AssertNoError(t, hub.AddPeer(peer))

	AssertNoError(t, hub.SubscribePeer(peer, "hashrate"))
	AssertEqual(t, []string{"hashrate"}, peer.Subscriptions())
}

func TestAX7_Hub_SubscribePeer_Bad(t *T) {
	hub := NewHub()

	err := hub.SubscribePeer(nil, "hashrate")
	AssertError(t, err)
	AssertContains(t, err.Error(), "nil peer")
}

func TestAX7_Hub_SubscribePeer_Ugly(t *T) {
	hub := NewHubWithConfig(HubConfig{ChannelAuthoriser: func(*Peer, string) bool { return false }})
	peer := NewPeer("ws")

	err := hub.SubscribePeer(peer, "private")
	AssertEqual(t, ErrAuthRejected, err)
	AssertEqual(t, []string{}, peer.Subscriptions())
}

func TestAX7_Hub_SubscribePublished_Good(t *T) {
	hub, cancel := ax7RunningHub(t)
	defer cancel()
	received := make(chan string, 1)
	stop := hub.SubscribePublished(func(channel string, frame []byte) { received <- channel + ":" + string(frame) })
	defer stop()

	AssertNoError(t, hub.Publish("block", []byte("template")))
	AssertEqual(t, "block:template", <-received)
}

func TestAX7_Hub_SubscribePublished_Bad(t *T) {
	var hub *Hub

	stop := hub.SubscribePublished(func(string, []byte) {})
	AssertNotNil(t, stop)
	AssertNotPanics(t, stop)
}

func TestAX7_Hub_SubscribePublished_Ugly(t *T) {
	hub, cancel := ax7RunningHub(t)
	defer cancel()
	received := make(chan string, 1)
	stop := hub.SubscribePublished(func(channel string, frame []byte) { received <- channel })
	stop()

	AssertNoError(t, hub.Publish("block", []byte("template")))
	select {
	case channel := <-received:
		t.Fatalf("received after unsubscribe: %q", channel)
	case <-ax7Timeout(20 * Millisecond):
	}
}

func TestAX7_Hub_SubscribeWithError_Bad(t *T) {
	hub := NewHub()

	stop, err := hub.SubscribeWithError("", func([]byte) {})
	AssertEqual(t, ErrEmptyChannel, err)
	AssertNotNil(t, stop)
}

func TestAX7_Hub_SubscribeWithError_Ugly(t *T) {
	hub := NewHub()

	stop, err := hub.SubscribeWithError("events", nil)
	AssertError(t, err)
	AssertNotNil(t, stop)
}

func TestAX7_Hub_UnsubscribePeer_Good(t *T) {
	hub := NewHub()
	peer := NewPeer("ws")
	AssertNoError(t, hub.AddPeer(peer))
	AssertNoError(t, hub.SubscribePeer(peer, "block"))

	hub.UnsubscribePeer(peer, "block")
	AssertEqual(t, []string{}, peer.Subscriptions())
	AssertEqual(t, 0, hub.ChannelSubscriberCount("block"))
}

func TestAX7_Hub_UnsubscribePeer_Bad(t *T) {
	hub := NewHub()

	AssertNotPanics(t, func() { hub.UnsubscribePeer(nil, "block") })
	AssertEqual(t, 0, hub.ChannelCount())
}

func TestAX7_Hub_UnsubscribePeer_Ugly(t *T) {
	hub := NewHub()
	peer := NewPeer("ws")
	AssertNoError(t, hub.AddPeer(peer))

	AssertNotPanics(t, func() { hub.UnsubscribePeer(peer, "") })
	AssertEqual(t, 0, len(peer.Subscriptions()))
}

func TestAX7_NewAPIKeyAuth_Good(t *T) {
	authenticator := NewAPIKeyAuth(map[string]string{"sk-live": "user-42"})

	AssertNotNil(t, authenticator)
	AssertEqual(t, "user-42", authenticator.Keys["sk-live"])
	AssertEqual(t, 1, len(authenticator.Keys))
}

func TestAX7_NewAPIKeyAuth_Bad(t *T) {
	authenticator := NewAPIKeyAuth(nil)

	AssertNotNil(t, authenticator)
	AssertNotNil(t, authenticator.Keys)
	AssertEqual(t, 0, len(authenticator.Keys))
}

func TestAX7_NewAPIKeyAuth_Ugly(t *T) {
	keys := map[string]string{"sk-live": "user-42"}
	authenticator := NewAPIKeyAuth(keys)
	keys["sk-live"] = "mutated"

	AssertEqual(t, "user-42", authenticator.Keys["sk-live"])
	AssertEqual(t, "mutated", keys["sk-live"])
}

func TestAX7_NewHub_Good(t *T) {
	hub := NewHub()

	AssertNotNil(t, hub)
	AssertFalse(t, hub.Running())
	AssertEqual(t, 0, hub.PeerCount())
}

func TestAX7_NewHub_Bad(t *T) {
	hub := NewHub()

	AssertNotNil(t, hub.Config())
	AssertEqual(t, 30*Second, hub.Config().HeartbeatInterval)
	AssertEqual(t, 0, hub.ChannelCount())
}

func TestAX7_NewHub_Ugly(t *T) {
	left := NewHub()
	right := NewHub()

	AssertNotEqual(t, left, right)
	AssertNotNil(t, left.done)
	AssertNotNil(t, right.done)
}

func TestAX7_NewHubWithConfig_Good(t *T) {
	hub := NewHubWithConfig(HubConfig{HeartbeatInterval: Second, PongTimeout: 3 * Second})

	AssertNotNil(t, hub)
	AssertEqual(t, Second, hub.Config().HeartbeatInterval)
	AssertEqual(t, 3*Second, hub.Config().PongTimeout)
}

func TestAX7_NewHubWithConfig_Bad(t *T) {
	hub := NewHubWithConfig(HubConfig{})

	AssertEqual(t, 30*Second, hub.Config().HeartbeatInterval)
	AssertEqual(t, 60*Second, hub.Config().PongTimeout)
	AssertEqual(t, 10*Second, hub.Config().WriteTimeout)
}

func TestAX7_NewHubWithConfig_Ugly(t *T) {
	called := false
	hub := NewHubWithConfig(HubConfig{OnConnect: func(*Peer) { called = true }})

	AssertNoError(t, hub.AddPeer(NewPeer("ws")))
	AssertTrue(t, called)
}

func TestAX7_Peer_Close_Bad(t *T) {
	var peer *Peer

	AssertNotPanics(t, func() { peer.Close() })
	AssertNil(t, peer)
}

func TestAX7_Peer_SendQueue_Good(t *T) {
	peer := NewPeer("ws")
	queue := peer.SendQueue()

	AssertNotNil(t, queue)
	AssertTrue(t, peer.Send([]byte("frame")))
	AssertEqual(t, "frame", string(<-queue))
}

func TestAX7_Peer_SendQueue_Ugly(t *T) {
	peer := NewPeer("ws")
	queue := peer.SendQueue()
	peer.Close()

	_, ok := <-queue
	AssertFalse(t, ok)
	AssertFalse(t, peer.Send([]byte("late")))
}

func TestAX7_Peer_SetCloseHook_Ugly(t *T) {
	peer := NewPeer("ws")
	count := 0
	peer.SetCloseHook(func() { count++ })
	peer.SetCloseHook(func() { count += 10 })

	peer.Close()
	AssertEqual(t, 10, count)
	AssertEqual(t, []string{}, peer.Subscriptions())
}

func TestAX7_QueryTokenAuth_Authenticate_Good(t *T) {
	authenticator := &QueryTokenAuth{Validate: func(token string) AuthResult {
		return AuthResult{Valid: token == "query-token", UserID: "browser"}
	}}
	request := NewHTTPTestRequest("GET", "/stream/ws?token=query-token", nil)

	result := authenticator.Authenticate(request)
	AssertTrue(t, result.Valid)
	AssertEqual(t, "browser", result.UserID)
}

func TestAX7_QueryTokenAuth_Authenticate_Bad(t *T) {
	authenticator := &QueryTokenAuth{Validate: func(token string) AuthResult {
		return AuthResult{Valid: token == "query-token"}
	}}
	request := NewHTTPTestRequest("GET", "/stream/ws", nil)

	result := authenticator.Authenticate(request)
	AssertFalse(t, result.Valid)
	AssertEqual(t, "", result.UserID)
}

func TestAX7_QueryTokenAuth_Authenticate_Ugly(t *T) {
	var authenticator *QueryTokenAuth
	request := NewHTTPTestRequest("GET", "/stream/ws?token=query-token", nil)

	result := authenticator.Authenticate(request)
	AssertFalse(t, result.Valid)
	AssertNil(t, result.Claims)
}
