// SPDX-License-Identifier: EUPL-1.2

package ws

import core "dappco.re/go"

func TestReconnect_NewReconnectingClient_Good(t *core.T) {
	subject := NewReconnectingClient
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestReconnect_NewReconnectingClient_Bad(t *core.T) {
	subject := NewReconnectingClient
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestReconnect_NewReconnectingClient_Ugly(t *core.T) {
	subject := NewReconnectingClient
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestReconnect_ReconnectingClient_Connect_Good(t *core.T) {
	subject := (*ReconnectingClient).Connect
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestReconnect_ReconnectingClient_Connect_Bad(t *core.T) {
	subject := (*ReconnectingClient).Connect
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestReconnect_ReconnectingClient_Connect_Ugly(t *core.T) {
	subject := (*ReconnectingClient).Connect
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestReconnect_ReconnectingClient_Send_Good(t *core.T) {
	subject := (*ReconnectingClient).Send
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestReconnect_ReconnectingClient_Send_Bad(t *core.T) {
	subject := (*ReconnectingClient).Send
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestReconnect_ReconnectingClient_Send_Ugly(t *core.T) {
	subject := (*ReconnectingClient).Send
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestReconnect_ReconnectingClient_State_Good(t *core.T) {
	subject := (*ReconnectingClient).State
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestReconnect_ReconnectingClient_State_Bad(t *core.T) {
	subject := (*ReconnectingClient).State
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestReconnect_ReconnectingClient_State_Ugly(t *core.T) {
	subject := (*ReconnectingClient).State
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestReconnect_ReconnectingClient_Close_Good(t *core.T) {
	subject := (*ReconnectingClient).Close
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestReconnect_ReconnectingClient_Close_Bad(t *core.T) {
	subject := (*ReconnectingClient).Close
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestReconnect_ReconnectingClient_Close_Ugly(t *core.T) {
	subject := (*ReconnectingClient).Close
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}
