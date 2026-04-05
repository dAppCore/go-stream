// SPDX-License-Identifier: EUPL-1.2

package zmq_test

import (
	"context"

	"dappco.re/go/stream"
	"dappco.re/go/stream/adapter/zmq"
)

func ExampleAdapter_Start() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := stream.NewHub()
	go hub.Run(ctx)

	adapter := zmq.New(zmq.Config{
		Mode:     zmq.ModePubSub,
		Endpoint: "tcp://127.0.0.1:5555",
		Role:     zmq.RoleSubscriber,
		Topics:   []string{"block"},
	})
	adapter.Mount(hub)

	go func() {
		_ = adapter.Start(ctx)
	}()
}
