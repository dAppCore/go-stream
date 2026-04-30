// SPDX-License-Identifier: EUPL-1.2

package redis_test

import (
	"context"

	"dappco.re/go/stream"
	"dappco.re/go/stream/adapter/redis"
)

func ExampleNewBridge() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := stream.NewHub()
	go hub.Run(ctx)

	bridge, err := redis.NewBridge(hub, redis.Config{
		Addr:   "127.0.0.1:6379",
		Prefix: "pool",
	})
	if err != nil {
		return
	}
	defer bridge.Stop()

	go func() {
		_ = bridge.Start(ctx)
	}()

	_ = bridge.PublishToChannel("block", []byte("template"))
}
