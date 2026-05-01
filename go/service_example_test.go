// SPDX-License-Identifier: EUPL-1.2

package stream_test

import (
	"context"

	core "dappco.re/go"
	"dappco.re/go/stream"
)

// ExampleNewService constructs the stream service factory through
// `NewService` for go-stream Core service registration. The factory
// produces a *stream.Service ready for c.Service() — OnStartup wires
// the stream.* action handlers and runs the Hub event loop in a
// background goroutine, OnShutdown cancels it.
//
// Usage example: `c.Service("stream", stream.NewService(stream.DefaultHubConfig()))`
func ExampleNewService() {
	factory := stream.NewService(stream.DefaultHubConfig())
	core.Println(factory != nil)
	// Output: true
}

// ExampleService_OnStartup registers the stream.* action handlers and
// spawns the Hub event loop through `Service.OnStartup` for go-stream
// Core service registration. Idempotent — multiple startups won't
// double-register.
//
// Usage example: `r := svc.OnStartup(ctx)`
func ExampleService_OnStartup() {
	c := core.New()
	r := stream.NewService(stream.DefaultHubConfig())(c)
	if !r.OK {
		core.Println("startup-init-failed")
		return
	}
	svc := r.Value.(*stream.Service)
	startup := svc.OnStartup(context.Background())
	defer svc.OnShutdown(context.Background())
	core.Println(startup.OK)
	// Output: true
}

// ExampleService_OnShutdown stops the Hub event loop through
// `Service.OnShutdown` for go-stream Core service registration.
//
// Usage example: `r := svc.OnShutdown(ctx)`
func ExampleService_OnShutdown() {
	c := core.New()
	r := stream.NewService(stream.DefaultHubConfig())(c)
	if !r.OK {
		core.Println("startup-init-failed")
		return
	}
	svc := r.Value.(*stream.Service)
	svc.OnStartup(context.Background())
	shutdown := svc.OnShutdown(context.Background())
	core.Println(shutdown.OK)
	// Output: true
}
