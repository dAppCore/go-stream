// SPDX-License-Identifier: EUPL-1.2

// Service registration for the stream package. Exposes the Hub surface
// as a Core service with action handlers so consumers can wire stream
// publish/broadcast/stats operations through the same plumbing as
// every other core service.
//
// Usage example: `c, _ := core.New(core.WithName("stream", stream.NewService(stream.HubConfig{HeartbeatInterval: 30*time.Second})))`

package stream

import (
	"context"

	core "dappco.re/go"
)

// Service is the registerable handle for the stream package — embeds
// *core.ServiceRuntime[HubConfig] for typed options access and holds
// a live *Hub ready for direct method calls or action use.
//
// Usage example: `svc := core.MustServiceFor[*stream.Service](c, "stream"); svc.Hub.Publish("alerts", []byte("msg"))`
type Service struct {
	*core.ServiceRuntime[HubConfig]
	// Hub is the live *Hub the service was constructed with.
	// Usage example: `svc.Hub.Publish("alerts", []byte("msg"))`
	Hub           *Hub
	registrations core.Once
	cancel        context.CancelFunc
}

// NewService returns a factory that constructs the hub and produces a
// *Service ready for c.Service() registration. Use through core.WithName
// so the framework wires lifecycle (OnStartup runs the hub event loop
// in the background, OnShutdown stops it).
//
// Usage example: `c, _ := core.New(core.WithName("stream", stream.NewService(stream.HubConfig{})))`
func NewService(config HubConfig) func(*core.Core) core.Result {
	return func(c *core.Core) core.Result {
		return core.Ok(&Service{
			ServiceRuntime: core.NewServiceRuntime(c, config),
			Hub:            NewHubWithConfig(config),
		})
	}
}

// OnStartup registers the stream action handlers on the attached Core
// and starts the hub event loop in a background goroutine. Implements
// core.Startable. Idempotent via core.Once.
//
// Usage example: `r := svc.OnStartup(ctx)`
func (s *Service) OnStartup(ctx context.Context) core.Result {
	if s == nil {
		return core.Ok(nil)
	}
	s.registrations.Do(func() {
		c := s.Core()
		if c == nil {
			return
		}
		c.Action("stream.publish", s.handlePublish)
		c.Action("stream.broadcast", s.handleBroadcast)
		c.Action("stream.send_channel", s.handleSendChannel)
		c.Action("stream.stats", s.handleStats)
		c.Action("stream.running", s.handleRunning)
		c.Action("stream.config", s.handleConfig)

		hubCtx, cancel := context.WithCancel(ctx)
		s.cancel = cancel
		go s.Hub.Run(hubCtx)
	})
	return core.Ok(nil)
}

// OnShutdown stops the hub event loop. Implements core.Stoppable.
//
// Usage example: `r := svc.OnShutdown(ctx)`
func (s *Service) OnShutdown(context.Context) core.Result {
	if s == nil {
		return core.Ok(nil)
	}
	if s.cancel != nil {
		s.cancel()
	}
	return core.Ok(nil)
}

// handlePublish — `stream.publish` action handler. Reads opts.channel +
// opts.frame ([]byte) and publishes the frame on the channel.
//
//	r := c.Action("stream.publish").Run(ctx, core.NewOptions(
//	    core.Option{Key: "channel", Value: "alerts"},
//	    core.Option{Key: "frame", Value: []byte("msg")},
//	))
func (s *Service) handlePublish(_ core.Context, opts core.Options) core.Result {
	if s == nil || s.Hub == nil {
		return core.Fail(core.E("stream.publish", "service not initialised", nil))
	}
	channel := opts.String("channel")
	if channel == "" {
		return core.Fail(core.E("stream.publish", "channel is required", nil))
	}
	frameR := frameBytes(opts.Get("frame"))
	if !frameR.OK {
		return frameR
	}
	return s.Hub.Publish(channel, frameR.Value.([]byte))
}

// handleBroadcast — `stream.broadcast` action handler. Reads opts.frame
// ([]byte) and broadcasts the frame on every channel.
//
//	r := c.Action("stream.broadcast").Run(ctx, core.NewOptions(
//	    core.Option{Key: "frame", Value: []byte("ping")},
//	))
func (s *Service) handleBroadcast(_ core.Context, opts core.Options) core.Result {
	if s == nil || s.Hub == nil {
		return core.Fail(core.E("stream.broadcast", "service not initialised", nil))
	}
	frameR := frameBytes(opts.Get("frame"))
	if !frameR.OK {
		return frameR
	}
	return s.Hub.Broadcast(frameR.Value.([]byte))
}

// handleSendChannel — `stream.send_channel` action handler. Reads
// opts.channel + opts.frame and sends the frame to the named channel.
//
//	r := c.Action("stream.send_channel").Run(ctx, core.NewOptions(
//	    core.Option{Key: "channel", Value: "alerts"},
//	    core.Option{Key: "frame", Value: []byte("hello")},
//	))
func (s *Service) handleSendChannel(_ core.Context, opts core.Options) core.Result {
	if s == nil || s.Hub == nil {
		return core.Fail(core.E("stream.send_channel", "service not initialised", nil))
	}
	channel := opts.String("channel")
	if channel == "" {
		return core.Fail(core.E("stream.send_channel", "channel is required", nil))
	}
	frameR := frameBytes(opts.Get("frame"))
	if !frameR.OK {
		return frameR
	}
	return s.Hub.SendToChannel(channel, frameR.Value.([]byte))
}

// handleStats — `stream.stats` action handler. Returns the current
// HubStats snapshot in r.Value.
//
//	r := c.Action("stream.stats").Run(ctx, core.NewOptions())
//	stats, _ := r.Value.(HubStats)
func (s *Service) handleStats(_ core.Context, _ core.Options) core.Result {
	if s == nil || s.Hub == nil {
		return core.Fail(core.E("stream.stats", "service not initialised", nil))
	}
	return core.Ok(s.Hub.Stats())
}

// handleRunning — `stream.running` action handler. Returns the bool
// running state of the hub event loop in r.Value.
//
//	r := c.Action("stream.running").Run(ctx, core.NewOptions())
//	running, _ := r.Value.(bool)
func (s *Service) handleRunning(_ core.Context, _ core.Options) core.Result {
	if s == nil || s.Hub == nil {
		return core.Fail(core.E("stream.running", "service not initialised", nil))
	}
	return core.Ok(s.Hub.Running())
}

// handleConfig — `stream.config` action handler. Returns the current
// HubConfig in r.Value.
//
//	r := c.Action("stream.config").Run(ctx, core.NewOptions())
//	cfg, _ := r.Value.(HubConfig)
func (s *Service) handleConfig(_ core.Context, _ core.Options) core.Result {
	if s == nil || s.Hub == nil {
		return core.Fail(core.E("stream.config", "service not initialised", nil))
	}
	return core.Ok(s.Hub.Config())
}

// frameBytes accepts an []byte or a string from a core.Options value
// and returns the bytes form via core.Result, since callers may pass
// either shape through the IPC layer.
//
// Usage example: `bytesR := frameBytes(opts.Get("frame"))`
func frameBytes(r core.Result) core.Result {
	if !r.OK {
		return core.Fail(core.E("frameBytes", "frame is required", nil))
	}
	switch v := r.Value.(type) {
	case []byte:
		return core.Ok(v)
	case string:
		return core.Ok([]byte(v))
	default:
		return core.Fail(core.E("frameBytes", "frame must be []byte or string", nil))
	}
}
