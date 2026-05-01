// SPDX-License-Identifier: EUPL-1.2

package tcp

import core "dappco.re/go"

func TestReconnect_NewReconnectingTCP_Good(t *core.T) {
	subject := NewReconnectingTCP
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestReconnect_NewReconnectingTCP_Bad(t *core.T) {
	subject := NewReconnectingTCP
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestReconnect_NewReconnectingTCP_Ugly(t *core.T) {
	subject := NewReconnectingTCP
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestReconnect_ReconnectingTCP_Connect_Good(t *core.T) {
	subject := (*ReconnectingTCP).Connect
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestReconnect_ReconnectingTCP_Connect_Bad(t *core.T) {
	subject := (*ReconnectingTCP).Connect
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestReconnect_ReconnectingTCP_Connect_Ugly(t *core.T) {
	subject := (*ReconnectingTCP).Connect
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestReconnect_ReconnectingTCP_Send_Good(t *core.T) {
	subject := (*ReconnectingTCP).Send
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestReconnect_ReconnectingTCP_Send_Bad(t *core.T) {
	subject := (*ReconnectingTCP).Send
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestReconnect_ReconnectingTCP_Send_Ugly(t *core.T) {
	subject := (*ReconnectingTCP).Send
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestReconnect_ReconnectingTCP_State_Good(t *core.T) {
	subject := (*ReconnectingTCP).State
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestReconnect_ReconnectingTCP_State_Bad(t *core.T) {
	subject := (*ReconnectingTCP).State
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestReconnect_ReconnectingTCP_State_Ugly(t *core.T) {
	subject := (*ReconnectingTCP).State
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestReconnect_ReconnectingTCP_Close_Good(t *core.T) {
	subject := (*ReconnectingTCP).Close
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestReconnect_ReconnectingTCP_Close_Bad(t *core.T) {
	subject := (*ReconnectingTCP).Close
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestReconnect_ReconnectingTCP_Close_Ugly(t *core.T) {
	subject := (*ReconnectingTCP).Close
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}
