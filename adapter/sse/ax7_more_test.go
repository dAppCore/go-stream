// SPDX-License-Identifier: EUPL-1.2

package sse

import (
	core "dappco.re/go"
	"dappco.re/go/stream"
)

func TestAX7_New_Good(t *core.T) {
	adapter := New(Config{HeartbeatInterval: core.Second, RetryMs: 99})

	core.AssertNotNil(t, adapter)
	core.AssertEqual(t, core.Second, adapter.config.HeartbeatInterval)
	core.AssertEqual(t, 99, adapter.config.RetryMs)
}

func TestAX7_New_Bad(t *core.T) {
	adapter := New(Config{})

	core.AssertNotNil(t, adapter)
	core.AssertEqual(t, 15*core.Second, adapter.config.HeartbeatInterval)
	core.AssertEqual(t, 3000, adapter.config.RetryMs)
}

func TestAX7_New_Ugly(t *core.T) {
	authenticator := stream.NewAPIKeyAuth(map[string]string{"sk": "user"})
	adapter := New(Config{Authenticator: authenticator})

	core.AssertEqual(t, authenticator, adapter.config.Authenticator)
	core.AssertEqual(t, 15*core.Second, adapter.config.HeartbeatInterval)
	core.AssertEqual(t, 3000, adapter.config.RetryMs)
}

func TestAX7_Adapter_Mount_Good(t *core.T) {
	adapter := New(Config{})
	hub := stream.NewHub()

	adapter.Mount(hub)
	core.AssertEqual(t, hub, adapter.hub)
	core.AssertFalse(t, adapter.hub.Running())
}

func TestAX7_Adapter_Mount_Bad(t *core.T) {
	adapter := New(Config{})

	adapter.Mount(nil)
	core.AssertNil(t, adapter.hub)
	core.AssertNotNil(t, adapter.Handler())
}

func TestAX7_Adapter_Mount_Ugly(t *core.T) {
	adapter := New(Config{})
	first := stream.NewHub()
	second := stream.NewHub()

	adapter.Mount(first)
	adapter.Mount(second)
	core.AssertEqual(t, second, adapter.hub)
}

func TestAX7_Adapter_ServeHTTP_Bad(t *core.T) {
	adapter := New(Config{})
	recorder := core.NewHTTPTestRecorder()
	request := core.NewHTTPTestRequest("GET", "/stream/events", nil)

	adapter.ServeHTTP(recorder, request)
	core.AssertEqual(t, 500, recorder.Code)
	core.AssertContains(t, recorder.Body.String(), "not mounted")
}

func TestAX7_Adapter_ServeHTTP_Ugly(t *core.T) {
	adapter := New(Config{})
	adapter.Mount(stream.NewHub())
	recorder := core.NewHTTPTestRecorder()
	request := core.NewHTTPTestRequest("GET", "/stream/events?channel=events", nil)

	adapter.ServeHTTP(recorder, request)
	core.AssertEqual(t, 500, recorder.Code)
	core.AssertContains(t, recorder.Body.String(), "not running")
}

func TestAX7_Adapter_HandlerForChannel_Bad(t *core.T) {
	adapter := New(Config{})
	handler := adapter.HandlerForChannel("")

	recorder := core.NewHTTPTestRecorder()
	handler.ServeHTTP(recorder, core.NewHTTPTestRequest("GET", "/stream/events", nil))
	core.AssertEqual(t, 500, recorder.Code)
}

func TestAX7_Adapter_HandlerForChannel_Ugly(t *core.T) {
	adapter := New(Config{})
	adapter.Mount(stream.NewHub())
	handler := adapter.HandlerForChannel("private")

	recorder := core.NewHTTPTestRecorder()
	handler.ServeHTTP(recorder, core.NewHTTPTestRequest("GET", "/stream/events", nil))
	core.AssertContains(t, recorder.Body.String(), "not running")
}
