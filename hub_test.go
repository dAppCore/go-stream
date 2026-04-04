// SPDX-License-Identifier: EUPL-1.2

package stream

import (
	"context"
	"testing"
	"time"
)

func TestHub_Pipe_Good(t *testing.T) {
	sourceHub := NewHub()
	destinationHub := NewHub()

	sourceContext, sourceCancel := context.WithCancel(context.Background())
	defer sourceCancel()
	destinationContext, destinationCancel := context.WithCancel(context.Background())
	defer destinationCancel()

	go sourceHub.Run(sourceContext)
	go destinationHub.Run(destinationContext)
	waitForRunningHub(t, sourceHub)
	waitForRunningHub(t, destinationHub)

	stop := Pipe(sourceHub, destinationHub)
	defer stop()

	received := make(chan []byte, 1)
	unsubscribe := destinationHub.Subscribe("hashrate", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	if err := sourceHub.Publish("hashrate", []byte("123456")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case frame := <-received:
		if string(frame) != "123456" {
			t.Fatalf("received frame = %q, want %q", string(frame), "123456")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for forwarded frame")
	}
}

func TestHub_Pipe_Bad(t *testing.T) {
	sourceHub := NewHub()
	destinationHub := NewHub()

	sourceContext, sourceCancel := context.WithCancel(context.Background())
	defer sourceCancel()
	destinationContext, destinationCancel := context.WithCancel(context.Background())
	defer destinationCancel()

	go sourceHub.Run(sourceContext)
	go destinationHub.Run(destinationContext)
	waitForRunningHub(t, sourceHub)
	waitForRunningHub(t, destinationHub)

	stop := Pipe(sourceHub, destinationHub)
	received := make(chan []byte, 1)
	unsubscribe := destinationHub.Subscribe("block", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	stop()

	if err := sourceHub.Publish("block", []byte("template")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case frame := <-received:
		t.Fatalf("received unexpected frame after stop: %q", string(frame))
	case <-time.After(200 * time.Millisecond):
	}
}

func TestHub_Pipe_Ugly(t *testing.T) {
	hub := NewHub()
	hubContext, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()

	go hub.Run(hubContext)
	waitForRunningHub(t, hub)

	stop := Pipe(hub, hub)
	defer stop()

	received := make(chan []byte, 1)
	unsubscribe := hub.Subscribe("agent", func(frame []byte) {
		received <- append([]byte(nil), frame...)
	})
	defer unsubscribe()

	if err := hub.Publish("agent", []byte("event")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case frame := <-received:
		if string(frame) != "event" {
			t.Fatalf("received frame = %q, want %q", string(frame), "event")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for local frame")
	}
}

func waitForRunningHub(t *testing.T, hub *Hub) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hub.mu.RLock()
		running := hub.running
		hub.mu.RUnlock()
		if running {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for hub to start")
}
