// SPDX-License-Identifier: EUPL-1.2

// Behaviour-level tests for the adapter/ws legacy compat wrappers: the
// thin constructor + Pipe + NewRedisBridge re-exports that preserve the
// old go-ws call surface.
package ws

import (
	"time"

	core "dappco.re/go"
	"dappco.re/go/stream"
	"dappco.re/go/stream/adapter/redis"
)

func TestWsAdapterCompat_Constructors_Behaviour(t *core.T) {
	core.AssertNotNil(t, NewHub(), "NewHub constructs")
	core.AssertEqual(t, 10*time.Second, NewHubWithConfig(HubConfig{HeartbeatInterval: 10 * time.Second}).Config().HeartbeatInterval, "NewHubWithConfig honours config")
	core.AssertEqual(t, 30*time.Second, DefaultHubConfig().HeartbeatInterval, "DefaultHubConfig default")
	core.AssertNotNil(t, NewPeer("ws"), "NewPeer constructs")
	core.AssertNotNil(t, NewAPIKeyAuth(map[string]string{"k": "u"}), "NewAPIKeyAuth constructs")
}

func TestWsAdapterCompat_Pipe_Behaviour(t *core.T) {
	stop := Pipe(stream.NewHub(), stream.NewHub())
	core.AssertNotNil(t, stop, "Pipe returns a stopper")
	stop()
}

func TestWsAdapterCompat_NewRedisBridge_Behaviour(t *core.T) {
	// Unreachable address fails the ping but still proves the wrapper
	// routes into the redis bridge constructor.
	core.AssertFalse(t, NewRedisBridge(stream.NewHub(), redis.Config{Addr: "127.0.0.1:1"}).OK, "NewRedisBridge routes (ping fails)")
	core.AssertFalse(t, NewRedisBridge(nil, redis.Config{Addr: "127.0.0.1:1"}).OK, "nil hub rejected")
}
