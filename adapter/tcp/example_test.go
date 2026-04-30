// SPDX-License-Identifier: EUPL-1.2

package tcp_test

import (
	"context"

	"dappco.re/go/stream"
	"dappco.re/go/stream/adapter/tcp"
)

func ExampleAdapter_Listen() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := stream.NewHub()
	go hub.Run(ctx)

	adapter := tcp.New(tcp.Config{
		Addr: ":9000",
		ConnAuthenticator: stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
			if string(handshake) != "trusted" {
				return stream.AuthResult{Valid: false}
			}
			return stream.AuthResult{Valid: true, UserID: "peer-1"}
		}),
	})
	adapter.Mount(hub)

	go func() {
		_ = adapter.Listen(ctx)
	}()
}
