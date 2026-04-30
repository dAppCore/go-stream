// SPDX-License-Identifier: EUPL-1.2

package stream

import (
	"sync"
	"testing"
)

func TestAX7_ConnectionState_String_Good(t *testing.T) {
	cases := []struct {
		state    ConnectionState
		expected string
	}{
		{StateDisconnected, "disconnected"},
		{StateConnecting, "connecting"},
		{StateConnected, "connected"},
	}
	for _, testCase := range cases {
		if testCase.state.String() != testCase.expected {
			t.Fatalf("ConnectionState(%d).String() = %q, want %q", testCase.state, testCase.state.String(), testCase.expected)
		}
	}
}

func TestAX7_ConnectionState_String_Bad(t *testing.T) {
	// Unknown ConnectionState value falls through to default ("disconnected").
	unknown := ConnectionState(99)
	if unknown.String() != "disconnected" {
		t.Fatalf("ConnectionState(99).String() = %q, want %q", unknown.String(), "disconnected")
	}
}

func TestAX7_ConnectionState_String_Ugly(t *testing.T) {
	// Negative ConnectionState value still returns "disconnected".
	negative := ConnectionState(-1)
	if negative.String() != "disconnected" {
		t.Fatalf("ConnectionState(-1).String() = %q, want %q", negative.String(), "disconnected")
	}
}

func TestEnvelope_Fields_Good(t *testing.T) {
	envelope := Envelope{
		SourceID: "node-a",
		Channel:  "block",
		Frame:    []byte("template"),
	}
	if envelope.SourceID != "node-a" {
		t.Fatalf("Envelope.SourceID = %q, want %q", envelope.SourceID, "node-a")
	}
	if envelope.Channel != "block" {
		t.Fatalf("Envelope.Channel = %q, want %q", envelope.Channel, "block")
	}
	if string(envelope.Frame) != "template" {
		t.Fatalf("Envelope.Frame = %q, want %q", string(envelope.Frame), "template")
	}
}

func TestEnvelope_Fields_Bad(t *testing.T) {
	// Zero-value Envelope has empty fields — no panic.
	envelope := Envelope{}
	if envelope.SourceID != "" {
		t.Fatalf("zero Envelope.SourceID = %q, want empty", envelope.SourceID)
	}
	if envelope.Channel != "" {
		t.Fatalf("zero Envelope.Channel = %q, want empty", envelope.Channel)
	}
	if envelope.Frame != nil {
		t.Fatalf("zero Envelope.Frame = %v, want nil", envelope.Frame)
	}
}

func TestEnvelope_Fields_Ugly(t *testing.T) {
	// Envelope with nil frame does not panic on len().
	envelope := Envelope{SourceID: "test", Frame: nil}
	if len(envelope.Frame) != 0 {
		t.Fatalf("len(nil Envelope.Frame) = %d, want 0", len(envelope.Frame))
	}
}

func TestAX7_NewPeer_Good(t *testing.T) {
	peer := NewPeer("ws")
	if peer == nil {
		t.Fatal("NewPeer() = nil")
	}
	if peer.ID == "" {
		t.Fatal("NewPeer().ID is empty")
	}
	if peer.Transport != "ws" {
		t.Fatalf("NewPeer().Transport = %q, want %q", peer.Transport, "ws")
	}
	if peer.Claims == nil {
		t.Fatal("NewPeer().Claims = nil, want empty map")
	}
	if peer.SendQueue() == nil {
		t.Fatal("NewPeer().SendQueue() = nil, want channel")
	}
}

func TestAX7_NewPeer_Bad(t *testing.T) {
	// NewPeer with empty transport creates a valid peer.
	peer := NewPeer("")
	if peer == nil {
		t.Fatal("NewPeer('') = nil")
	}
	if peer.Transport != "" {
		t.Fatalf("NewPeer('').Transport = %q, want empty", peer.Transport)
	}
}

func TestAX7_NewPeer_Ugly(t *testing.T) {
	// Two peers created simultaneously have different IDs.
	peer1 := NewPeer("ws")
	peer2 := NewPeer("ws")
	if peer1.ID == peer2.ID {
		t.Fatalf("two NewPeer() calls produced the same ID: %q", peer1.ID)
	}
}

func TestAX7_Peer_Send_Good(t *testing.T) {
	peer := NewPeer("ws")
	ok := peer.Send([]byte("hello"))
	if !ok {
		t.Fatal("Send() returned false, want true")
	}
	select {
	case frame := <-peer.SendQueue():
		if string(frame) != "hello" {
			t.Fatalf("received frame = %q, want %q", string(frame), "hello")
		}
	default:
		t.Fatal("no frame received from SendQueue()")
	}
}

func TestAX7_Peer_Send_Bad(t *testing.T) {
	// Send to nil peer returns false without panic.
	var peer *Peer
	ok := peer.Send([]byte("hello"))
	if ok {
		t.Fatal("nil peer Send() = true, want false")
	}
}

func TestAX7_Peer_Send_Ugly(t *testing.T) {
	// Send after Close returns false without panic.
	peer := NewPeer("ws")
	peer.Close()
	ok := peer.Send([]byte("hello"))
	if ok {
		t.Fatal("Send() after Close() = true, want false")
	}
}

func TestAX7_Peer_Close_Ugly(t *testing.T) {
	// Double Close does not panic.
	peer := NewPeer("ws")
	peer.Close()
	peer.Close()
}

func TestAX7_Peer_SetCloseHook_Good(t *testing.T) {
	peer := NewPeer("ws")
	invoked := false
	peer.SetCloseHook(func() { invoked = true })
	peer.Close()
	if !invoked {
		t.Fatal("close hook was not invoked")
	}
}

func TestAX7_Peer_SetCloseHook_Bad(t *testing.T) {
	// SetCloseHook on nil peer does not panic.
	var peer *Peer
	peer.SetCloseHook(func() {})
	if peer != nil {
		t.Fatal("nil peer changed after SetCloseHook")
	}
}

func TestAX7_Peer_SendQueue_Bad(t *testing.T) {
	// SendQueue on nil peer returns nil.
	var peer *Peer
	if peer.SendQueue() != nil {
		t.Fatal("nil peer SendQueue() != nil")
	}
}

func TestPeer_Subscriptions_SortedCopy_Good(t *testing.T) {
	// Subscriptions returns a sorted copy.
	peer := NewPeer("ws")
	peer.mutex.Lock()
	peer.subscriptions["block"] = true
	peer.subscriptions["hashrate"] = true
	peer.subscriptions["agent"] = true
	peer.mutex.Unlock()

	subs := peer.Subscriptions()
	expected := []string{"agent", "block", "hashrate"}
	if len(subs) != len(expected) {
		t.Fatalf("Subscriptions() length = %d, want %d", len(subs), len(expected))
	}
	for index, channel := range expected {
		if subs[index] != channel {
			t.Fatalf("Subscriptions()[%d] = %q, want %q", index, subs[index], channel)
		}
	}
}

func TestPipe_NilStreams_Good(t *testing.T) {
	// Pipe with nil src returns a no-op stop function without panic.
	stop := Pipe(nil, NewHub())
	stop()

	// Pipe with nil dst returns a no-op stop function without panic.
	stop = Pipe(NewHub(), nil)
	stop()
}

func TestPipe_SameStream_Bad(t *testing.T) {
	// Pipe with src == dst returns a no-op stop function (no infinite loop).
	hub := NewHub()
	stop := Pipe(hub, hub)
	stop()
}

func TestPipe_StopConcurrency_Ugly(t *testing.T) {
	// Calling stop multiple times concurrently does not panic.
	hub1 := NewHub()
	hub2 := NewHub()
	stop := Pipe(hub1, hub2)
	var waitGroup sync.WaitGroup
	for index := 0; index < 10; index++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			stop()
		}()
	}
	waitGroup.Wait()
}

func TestEncodeTCPFrame_Good(t *testing.T) {
	frame := encodeTCPFrame("block", []byte("template"))
	if len(frame) == 0 {
		t.Fatal("encodeTCPFrame() produced empty output")
	}
	// The frame should contain the payload length prefix, channel length, channel, and data.
	// Total: 4 (payload len) + 4 (channel len) + 5 ("block") + 8 ("template") = 21
	if len(frame) != 21 {
		t.Fatalf("encodeTCPFrame() len = %d, want %d", len(frame), 21)
	}
}

func TestEncodeTCPFrame_Bad(t *testing.T) {
	// Empty channel and empty frame produces a minimal valid frame.
	frame := encodeTCPFrame("", []byte{})
	// 4 (payload len) + 4 (channel len=0) + 0 (channel) + 0 (frame) = 8
	if len(frame) != 8 {
		t.Fatalf("encodeTCPFrame('', []) len = %d, want %d", len(frame), 8)
	}
}

func TestCloneFrame_Good(t *testing.T) {
	original := []byte("hello")
	cloned := cloneFrame(original)
	if string(cloned) != "hello" {
		t.Fatalf("cloneFrame() = %q, want %q", string(cloned), "hello")
	}
	// Modifying the clone should not affect the original.
	cloned[0] = 'H'
	if string(original) != "hello" {
		t.Fatalf("modifying clone affected original: %q", string(original))
	}
}

func TestCloneFrame_Bad(t *testing.T) {
	// cloneFrame of nil returns nil.
	cloned := cloneFrame(nil)
	if cloned != nil {
		t.Fatalf("cloneFrame(nil) = %v, want nil", cloned)
	}
}

func TestCloneFrame_Ugly(t *testing.T) {
	// cloneFrame of empty slice returns nil.
	cloned := cloneFrame([]byte{})
	if cloned != nil {
		t.Fatalf("cloneFrame([]byte{}) = %v, want nil", cloned)
	}
}

func TestOnceFunction_Good(t *testing.T) {
	count := 0
	handler := onceFunction(func() { count++ })
	handler()
	handler()
	handler()
	if count != 1 {
		t.Fatalf("onceFunction handler invoked %d times, want 1", count)
	}
}

func TestOnceFunction_Bad(t *testing.T) {
	// onceFunction with nil handler returns a no-op function.
	handler := onceFunction(nil)
	if handler == nil {
		t.Fatal("onceFunction(nil) returned nil")
	}
	handler() // should not panic
}

func TestOnceFunction_Ugly(t *testing.T) {
	// Concurrent calls to onceFunction result execute the handler exactly once.
	count := 0
	var counterMutex sync.Mutex
	handler := onceFunction(func() {
		counterMutex.Lock()
		count++
		counterMutex.Unlock()
	})
	var waitGroup sync.WaitGroup
	for index := 0; index < 50; index++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			handler()
		}()
	}
	waitGroup.Wait()
	if count != 1 {
		t.Fatalf("concurrent onceFunction handler invoked %d times, want 1", count)
	}
}

func TestRandomUUID_Good(t *testing.T) {
	id := randomUUID()
	if len(id) != 36 {
		t.Fatalf("randomUUID() length = %d, want 36", len(id))
	}
	// Verify UUID v4 format: 8-4-4-4-12
	if id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		t.Fatalf("randomUUID() = %q, not in UUID format", id)
	}
}

func TestRandomUUID_Bad(t *testing.T) {
	// Two calls produce different UUIDs.
	id1 := randomUUID()
	id2 := randomUUID()
	if id1 == id2 {
		t.Fatalf("randomUUID() produced duplicate: %q", id1)
	}
}
