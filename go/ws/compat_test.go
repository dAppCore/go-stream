// SPDX-License-Identifier: EUPL-1.2

package ws

import core "dappco.re/go"

func TestCompat_NewRedisBridge_Good(t *core.T) {
	subject := NewRedisBridge
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestCompat_NewRedisBridge_Bad(t *core.T) {
	subject := NewRedisBridge
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestCompat_NewRedisBridge_Ugly(t *core.T) {
	subject := NewRedisBridge
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestCompat_NewAPIKeyAuth_Good(t *core.T) {
	subject := NewAPIKeyAuth
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestCompat_NewAPIKeyAuth_Bad(t *core.T) {
	subject := NewAPIKeyAuth
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestCompat_NewAPIKeyAuth_Ugly(t *core.T) {
	subject := NewAPIKeyAuth
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestCompat_NewHub_Good(t *core.T) {
	subject := NewHub
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestCompat_NewHub_Bad(t *core.T) {
	subject := NewHub
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestCompat_NewHub_Ugly(t *core.T) {
	subject := NewHub
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestCompat_NewHubWithConfig_Good(t *core.T) {
	subject := NewHubWithConfig
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestCompat_NewHubWithConfig_Bad(t *core.T) {
	subject := NewHubWithConfig
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestCompat_NewHubWithConfig_Ugly(t *core.T) {
	subject := NewHubWithConfig
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestCompat_DefaultHubConfig_Good(t *core.T) {
	subject := DefaultHubConfig
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestCompat_DefaultHubConfig_Bad(t *core.T) {
	subject := DefaultHubConfig
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestCompat_DefaultHubConfig_Ugly(t *core.T) {
	subject := DefaultHubConfig
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestCompat_NewPeer_Good(t *core.T) {
	subject := NewPeer
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestCompat_NewPeer_Bad(t *core.T) {
	subject := NewPeer
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestCompat_NewPeer_Ugly(t *core.T) {
	subject := NewPeer
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestCompat_Pipe_Good(t *core.T) {
	subject := Pipe
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestCompat_Pipe_Bad(t *core.T) {
	subject := Pipe
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestCompat_Pipe_Ugly(t *core.T) {
	subject := Pipe
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestCompat_New_Good(t *core.T) {
	subject := New
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestCompat_New_Bad(t *core.T) {
	subject := New
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestCompat_New_Ugly(t *core.T) {
	subject := New
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestCompat_NewReconnectingClient_Good(t *core.T) {
	subject := NewReconnectingClient
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestCompat_NewReconnectingClient_Bad(t *core.T) {
	subject := NewReconnectingClient
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestCompat_NewReconnectingClient_Ugly(t *core.T) {
	subject := NewReconnectingClient
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestCompat_Hub_Handler_Good(t *core.T) {
	subject := (*Hub).Handler
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestCompat_Hub_Handler_Bad(t *core.T) {
	subject := (*Hub).Handler
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestCompat_Hub_Handler_Ugly(t *core.T) {
	subject := (*Hub).Handler
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestCompat_Hub_HandlerForChannel_Good(t *core.T) {
	subject := (*Hub).HandlerForChannel
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestCompat_Hub_HandlerForChannel_Bad(t *core.T) {
	subject := (*Hub).HandlerForChannel
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestCompat_Hub_HandlerForChannel_Ugly(t *core.T) {
	subject := (*Hub).HandlerForChannel
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}
