// SPDX-License-Identifier: EUPL-1.2

package tcp

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dappco.re/go/stream"
)

func TestTCP_Listen_Good(t *testing.T) {
	hub := stream.NewHubWithConfig(stream.HubConfig{
		OnConnect: func(peer *stream.Peer) {
			if peer.UserID != "user-42" {
				t.Errorf("peer.UserID = %q, want %q", peer.UserID, "user-42")
			}
			if peer.Claims["role"] != "admin" {
				t.Errorf("peer.Claims[role] = %v, want %q", peer.Claims["role"], "admin")
			}
		},
	})

	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{
		Addr: "127.0.0.1:0",
		ConnAuthenticator: stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
			if string(handshake) != "hello" {
				return stream.AuthResult{Valid: false}
			}
			return stream.AuthResult{
				Valid:  true,
				UserID: "user-42",
				Claims: map[string]any{"role": "admin"},
			}
		}),
	})
	adapter.Mount(hub)

	listenContext, listenCancel := context.WithCancel(context.Background())
	defer listenCancel()
	go func() {
		_ = adapter.Listen(listenContext)
	}()

	address := waitForListenerAddress(t, adapter)
	connection, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer connection.Close()

	if _, err := connection.Write(encodeFrame("", []byte("hello"))); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	waitForPeerCount(t, hub, 1)
}

func TestTCP_Listen_NoAuthenticator_Good(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{
		Addr: "127.0.0.1:0",
	})
	adapter.Mount(hub)

	listenContext, listenCancel := context.WithCancel(context.Background())
	defer listenCancel()
	go func() {
		_ = adapter.Listen(listenContext)
	}()

	address := waitForListenerAddress(t, adapter)
	connection, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer connection.Close()

	received := make(chan []byte, 1)
	unsubscribe := hub.Subscribe("block", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	if _, err := connection.Write(encodeFrame("block", []byte("template"))); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	select {
	case frame := <-received:
		if string(frame) != "template" {
			t.Fatalf("received frame = %q, want %q", string(frame), "template")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for unauthenticated frame")
	}
}

func TestTCP_Listen_SelfDelivery_Good(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{
		Addr: "127.0.0.1:0",
	})
	adapter.Mount(hub)

	listenContext, listenCancel := context.WithCancel(context.Background())
	defer listenCancel()
	go func() {
		_ = adapter.Listen(listenContext)
	}()

	address := waitForListenerAddress(t, adapter)
	connection, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer connection.Close()

	received := make(chan []byte, 1)
	unsubscribe := hub.Subscribe("block", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	if _, err := connection.Write(encodeFrame("block", []byte("template"))); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	select {
	case frame := <-received:
		if string(frame) != "template" {
			t.Fatalf("received frame = %q, want %q", string(frame), "template")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for published TCP frame")
	}

	channel, frame, err := readFrame(connection, 2*time.Second, MaxFrameSize)
	if err != nil {
		t.Fatalf("readFrame() error = %v", err)
	}
	if channel != "block" {
		t.Fatalf("readFrame() channel = %q, want %q", channel, "block")
	}
	if string(frame) != "template" {
		t.Fatalf("readFrame() frame = %q, want %q", string(frame), "template")
	}
}

func TestTCP_Listen_ContextCancel_ClosesPeer_Good(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{
		Addr: "127.0.0.1:0",
	})
	adapter.Mount(hub)

	listenContext, listenCancel := context.WithCancel(context.Background())
	go func() {
		_ = adapter.Listen(listenContext)
	}()

	address := waitForListenerAddress(t, adapter)
	connection, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer connection.Close()

	if _, err := connection.Write(encodeFrame("", []byte("hello"))); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	waitForPeerCount(t, hub, 1)

	channel, frame, err := readFrame(connection, 2*time.Second, MaxFrameSize)
	if err != nil {
		t.Fatalf("readFrame() initial echo error = %v", err)
	}
	if channel != "" {
		t.Fatalf("readFrame() initial echo channel = %q, want %q", channel, "")
	}
	if string(frame) != "hello" {
		t.Fatalf("readFrame() initial echo frame = %q, want %q", string(frame), "hello")
	}

	listenCancel()

	channel, frame, err = readFrame(connection, 2*time.Second, MaxFrameSize)
	if err == nil {
		t.Fatalf("readFrame() = (%q, %q, nil), want connection close", channel, string(frame))
	}
	if err == stream.ErrHandshakeTimeout {
		t.Fatalf("readFrame() error = %v, want connection close", err)
	}

	waitForPeerCount(t, hub, 0)
}

func TestTCP_Listen_Bad(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{
		Addr: "127.0.0.1:0",
		ConnAuthenticator: stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
			return stream.AuthResult{Valid: false}
		}),
	})
	adapter.Mount(hub)

	listenContext, listenCancel := context.WithCancel(context.Background())
	defer listenCancel()
	go func() {
		_ = adapter.Listen(listenContext)
	}()

	address := waitForListenerAddress(t, adapter)
	connection, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer connection.Close()

	if _, err := connection.Write(encodeFrame("", []byte("nope"))); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	waitForPeerCount(t, hub, 0)
}

func TestTCP_Listen_Ugly(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{
		Addr: "127.0.0.1:0",
		ConnAuthenticator: stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
			return stream.AuthResult{Valid: true}
		}),
		HandshakeTimeout: 50 * time.Millisecond,
	})
	adapter.Mount(hub)

	listenContext, listenCancel := context.WithCancel(context.Background())
	defer listenCancel()
	go func() {
		_ = adapter.Listen(listenContext)
	}()

	address := waitForListenerAddress(t, adapter)
	connection, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer connection.Close()

	time.Sleep(120 * time.Millisecond)
	waitForPeerCount(t, hub, 0)
}

func TestTCP_Listen_NoAuthenticator_LargeInitialFrame_Good(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{
		Addr: "127.0.0.1:0",
	})
	adapter.Mount(hub)

	listenContext, listenCancel := context.WithCancel(context.Background())
	defer listenCancel()
	go func() {
		_ = adapter.Listen(listenContext)
	}()

	address := waitForListenerAddress(t, adapter)
	connection, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer connection.Close()

	received := make(chan []byte, 1)
	unsubscribe := hub.Subscribe("block", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	largeFrame := bytes.Repeat([]byte("a"), maxHandshakeFrameSize+1)
	if _, err := connection.Write(encodeFrame("block", largeFrame)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	select {
	case frame := <-received:
		if string(frame) != string(largeFrame) {
			t.Fatalf("received frame size = %d, want %d", len(frame), len(largeFrame))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for large initial frame")
	}
}

func TestReconnectingTCP_Send_Concurrent_Good(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()

	serverAccepted := make(chan net.Conn, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		serverAccepted <- connection
	}()

	client := NewReconnectingTCP(ReconnectConfig{Addr: listener.Addr().String()})

	connectContext, connectCancel := context.WithCancel(context.Background())
	connectDone := make(chan error, 1)
	go func() {
		connectDone <- client.Connect(connectContext)
	}()
	defer func() {
		connectCancel()
		_ = client.Close()
		<-connectDone
	}()

	serverConnection := <-serverAccepted
	defer serverConnection.Close()

	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if client.State() == stream.StateConnected {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if client.State() != stream.StateConnected {
		t.Fatal("client did not reach connected state")
	}

	senderCount := 32
	var sendGroup sync.WaitGroup
	for index := range senderCount {
		sendGroup.Add(1)
		go func(index int) {
			defer sendGroup.Done()
			if sendErr := client.Send("hashrate", []byte{byte(index)}); sendErr != nil {
				t.Errorf("Send() error = %v", sendErr)
			}
		}(index)
	}
	sendGroup.Wait()

	receivedValues := map[byte]bool{}
	for len(receivedValues) < senderCount {
		channel, frame, readErr := readFrame(serverConnection, 2*time.Second, MaxFrameSize)
		if readErr != nil {
			t.Fatalf("readFrame() error = %v", readErr)
		}
		if channel != "hashrate" {
			t.Fatalf("readFrame() channel = %q, want %q", channel, "hashrate")
		}
		if len(frame) != 1 {
			t.Fatalf("len(frame) = %d, want %d", len(frame), 1)
		}
		receivedValues[frame[0]] = true
	}
}

func TestTCP_Listen_AuthHandshakeTooLarge_Good(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	adapter := New(Config{
		Addr: "127.0.0.1:0",
		ConnAuthenticator: stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
			return stream.AuthResult{Valid: true}
		}),
	})
	adapter.Mount(hub)

	listenContext, listenCancel := context.WithCancel(context.Background())
	defer listenCancel()
	go func() {
		_ = adapter.Listen(listenContext)
	}()

	address := waitForListenerAddress(t, adapter)
	connection, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer connection.Close()

	tooLargeHandshake := make([]byte, maxHandshakeFrameSize+1)
	if _, err := connection.Write(encodeFrame("", tooLargeHandshake)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	waitForPeerCount(t, hub, 0)
}

func TestTCP_Dial_NilContext_Good(t *testing.T) {
	hub := stream.NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubContext)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer connection.Close()
		_, _ = connection.Write(encodeFrame("block", []byte("template")))
		time.Sleep(50 * time.Millisecond)
	}()

	adapter := New(Config{Addr: listener.Addr().String()})
	peer, err := adapter.Dial(nil, hub)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	if peer == nil {
		t.Fatal("Dial() peer = nil")
	}
	defer peer.Close()

	received := make(chan []byte, 1)
	unsubscribe := hub.Subscribe("block", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	select {
	case frame := <-received:
		if string(frame) != "template" {
			t.Fatalf("received frame = %q, want %q", string(frame), "template")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for dialed frame")
	}

	<-serverDone
}

func TestTCP_Dial_HubNotRunning_Bad(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer connection.Close()
		_, _, _ = readFrame(connection, 2*time.Second, MaxFrameSize)
	}()

	adapter := New(Config{Addr: listener.Addr().String()})
	peer, err := adapter.Dial(context.Background(), stream.NewHub())
	if err == nil {
		if peer != nil {
			peer.Close()
		}
		t.Fatal("Dial() error = nil, want hub lifecycle failure")
	}
	if peer != nil {
		t.Fatalf("Dial() peer = %#v, want nil", peer)
	}

	<-serverDone
}

func TestTCP_Dial_Handshake_Good(t *testing.T) {
	serverHub := stream.NewHub()
	serverHubContext, serverHubCancel := context.WithCancel(context.Background())
	defer serverHubCancel()
	go serverHub.Run(serverHubContext)

	serverAdapter := New(Config{
		Addr: "127.0.0.1:0",
		ConnAuthenticator: stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
			if string(handshake) != "trusted" {
				return stream.AuthResult{Valid: false}
			}
			return stream.AuthResult{Valid: true, UserID: "peer-1"}
		}),
	})
	serverAdapter.Mount(serverHub)

	listenContext, listenCancel := context.WithCancel(context.Background())
	defer listenCancel()
	go func() {
		_ = serverAdapter.Listen(listenContext)
	}()

	clientHub := stream.NewHub()
	clientHubContext, clientHubCancel := context.WithCancel(context.Background())
	defer clientHubCancel()
	go clientHub.Run(clientHubContext)

	clientAdapter := New(Config{
		Addr:           waitForListenerAddress(t, serverAdapter),
		HandshakeFrame: []byte("trusted"),
	})

	received := make(chan []byte, 1)
	unsubscribe := clientHub.Subscribe("block", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	peer, err := clientAdapter.Dial(context.Background(), clientHub)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer peer.Close()

	waitForPeerCount(t, serverHub, 1)

	if err := serverHub.Publish("block", []byte("template")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case frame := <-received:
		if string(frame) != "template" {
			t.Fatalf("received frame = %q, want %q", string(frame), "template")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for dialed handshake frame")
	}
}

func TestReconnectingTCP_State_Good(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer connection.Close()
		time.Sleep(100 * time.Millisecond)
	}()

	client := NewReconnectingTCP(ReconnectConfig{
		Addr:           listener.Addr().String(),
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
	})
	if client.State() != stream.StateDisconnected {
		t.Fatalf("State() = %v, want %v", client.State(), stream.StateDisconnected)
	}

	connectContext, connectCancel := context.WithCancel(context.Background())
	defer connectCancel()
	connectDone := make(chan error, 1)
	go func() {
		connectDone <- client.Connect(connectContext)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if client.State() == stream.StateConnected {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if client.State() != stream.StateConnected {
		t.Fatalf("State() = %v, want %v", client.State(), stream.StateConnected)
	}

	connectCancel()
	select {
	case err := <-connectDone:
		if err != nil {
			t.Fatalf("Connect() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Connect() to return")
	}

	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if client.State() != stream.StateDisconnected {
		t.Fatalf("State() = %v, want %v", client.State(), stream.StateDisconnected)
	}

	<-serverDone
}

func TestReconnectingTCP_OnReconnect_Good(t *testing.T) {
	var reconnectCount atomic.Int32
	client := NewReconnectingTCP(ReconnectConfig{
		Addr:           "127.0.0.1:1",
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		MaxRetries:     1,
		OnReconnect: func(attempt int) {
			reconnectCount.Store(int32(attempt))
		},
	})

	err := client.Connect(context.Background())
	if err == nil {
		t.Fatal("Connect() error = nil, want dial error")
	}
	if reconnectCount.Load() != 1 {
		t.Fatalf("OnReconnect attempt = %d, want %d", reconnectCount.Load(), 1)
	}
	if client.State() != stream.StateDisconnected {
		t.Fatalf("State() = %v, want %v", client.State(), stream.StateDisconnected)
	}
}

func TestReconnectingTCP_Connect_Handshake_Good(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()

	received := make(chan []byte, 1)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer connection.Close()

		channel, frame, readErr := readFrame(connection, time.Second, MaxFrameSize)
		if readErr != nil {
			return
		}
		if channel != "auth" {
			return
		}
		received <- append([]byte(nil), frame...)
		_ = writeFull(connection, encodeFrame("block", []byte("template")))
	}()

	clientMessages := make(chan []byte, 1)
	client := NewReconnectingTCP(ReconnectConfig{
		Addr:             listener.Addr().String(),
		HandshakeChannel: "auth",
		HandshakeFrame:   []byte("trusted"),
		InitialBackoff:   10 * time.Millisecond,
		MaxBackoff:       10 * time.Millisecond,
		OnMessage: func(channel string, frame []byte) {
			if channel == "block" {
				clientMessages <- append([]byte(nil), frame...)
			}
		},
	})

	connectContext, connectCancel := context.WithCancel(context.Background())
	connectDone := make(chan error, 1)
	go func() {
		connectDone <- client.Connect(connectContext)
	}()

	select {
	case frame := <-received:
		if string(frame) != "trusted" {
			t.Fatalf("handshake frame = %q, want %q", string(frame), "trusted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for handshake frame")
	}

	select {
	case frame := <-clientMessages:
		if string(frame) != "template" {
			t.Fatalf("received frame = %q, want %q", string(frame), "template")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for reconnecting client frame")
	}

	connectCancel()
	select {
	case err := <-connectDone:
		if err != nil && err != context.Canceled {
			t.Fatalf("Connect() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Connect() to return")
	}

	<-serverDone
}

func waitForListenerAddress(t *testing.T, adapter *Adapter) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		adapter.mutex.Lock()
		listener := adapter.listener
		adapter.mutex.Unlock()
		if listener != nil {
			return listener.Addr().String()
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for listener")
	return ""
}

func waitForPeerCount(t *testing.T, hub *stream.Hub, expected int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hub.PeerCount() == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("PeerCount() = %d, want %d", hub.PeerCount(), expected)
}

func TestWriteFull_Good(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	wrapped := &partialWriteConn{Conn: left, chunkSize: 2}
	payload := []byte("hello")
	received := make(chan []byte, 1)
	go func() {
		buffer := make([]byte, len(payload))
		_, err := io.ReadFull(right, buffer)
		if err != nil {
			received <- nil
			return
		}
		received <- buffer
	}()

	if err := writeFull(wrapped, payload); err != nil {
		t.Fatalf("writeFull() error = %v", err)
	}

	select {
	case frame := <-received:
		if string(frame) != "hello" {
			t.Fatalf("received frame = %q, want %q", string(frame), "hello")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for payload")
	}
}

type partialWriteConn struct {
	net.Conn
	chunkSize int
}

func (conn *partialWriteConn) Write(payload []byte) (int, error) {
	if conn.chunkSize > 0 && len(payload) > conn.chunkSize {
		payload = payload[:conn.chunkSize]
	}
	return conn.Conn.Write(payload)
}
