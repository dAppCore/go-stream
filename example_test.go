// SPDX-License-Identifier: EUPL-1.2

package stream_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"time"

	"dappco.re/go/stream"
)

func ExampleNewHub() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := stream.NewHub()
	go hub.Run(ctx)

	received := make(chan string, 1)
	stop := hub.Subscribe("hashrate", func(frame []byte) {
		received <- string(frame)
	})
	defer stop()

	waitForHub(hub)
	_ = hub.Publish("hashrate", []byte(`{"h":123456}`))

	select {
	case frame := <-received:
		fmt.Println(frame)
	case <-time.After(time.Second):
		fmt.Println("timeout")
	}

	// Output:
	// {"h":123456}
}

func ExamplePipe() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceHub := stream.NewHub()
	destinationHub := stream.NewHub()
	go sourceHub.Run(ctx)
	go destinationHub.Run(ctx)

	received := make(chan string, 1)
	stopSubscribe := destinationHub.Subscribe("block", func(frame []byte) {
		received <- string(frame)
	})
	defer stopSubscribe()

	stopPipe := stream.Pipe(sourceHub, destinationHub)
	defer stopPipe()

	waitForHub(sourceHub)
	waitForHub(destinationHub)
	_ = sourceHub.Publish("block", []byte(`{"height":42}`))

	select {
	case frame := <-received:
		fmt.Println(frame)
	case <-time.After(time.Second):
		fmt.Println("timeout")
	}

	// Output:
	// {"height":42}
}

func waitForHub(hub *stream.Hub) {
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if hub.Broadcast(nil) == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func ExampleNewAPIKeyAuth() {
	authenticator := stream.NewAPIKeyAuth(map[string]string{
		"sk-live": "user-42",
	})

	request := httptest.NewRequest("GET", "http://example.com/stream/ws", nil)
	request.Header.Set("Authorization", "Bearer sk-live")

	result := authenticator.Authenticate(request)
	fmt.Println(result.Valid, result.UserID)

	// Output:
	// true user-42
}
