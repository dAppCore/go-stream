// SPDX-License-Identifier: EUPL-1.2

package ws_test

import (
	"context"
	"net/http"

	"dappco.re/go/stream"
	"dappco.re/go/stream/adapter/ws"
)

func ExampleAdapter_Handler() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := stream.NewHub()
	go hub.Run(ctx)

	adapter := ws.New(ws.Config{
		Authenticator: stream.NewAPIKeyAuth(map[string]string{
			"sk-live": "user-42",
		}),
	})
	adapter.Mount(hub)

	http.Handle("/stream/ws", adapter.Handler())
}
