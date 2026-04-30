// SPDX-License-Identifier: EUPL-1.2

package sse_test

import (
	"context"
	"net/http"

	"dappco.re/go/stream"
	"dappco.re/go/stream/adapter/sse"
)

func ExampleAdapter_HandlerForChannel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := stream.NewHub()
	go hub.Run(ctx)

	adapter := sse.New(sse.Config{})
	adapter.Mount(hub)

	http.Handle("/stream/hashrate", adapter.HandlerForChannel("hashrate"))
}
