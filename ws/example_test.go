// SPDX-License-Identifier: EUPL-1.2

package ws_test

import (
	"context"
	"net/http"

	"dappco.re/go/stream/ws"
)

func ExampleHub_Handler() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := ws.NewHub()
	go hub.Run(ctx)

	http.Handle("/stream/ws", hub.Handler())
}
