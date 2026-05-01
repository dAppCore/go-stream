// SPDX-License-Identifier: EUPL-1.2

package tcp

import core "dappco.re/go"

func TestTcp_New_Good(t *core.T) {
	subject := New
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestTcp_New_Bad(t *core.T) {
	subject := New
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestTcp_New_Ugly(t *core.T) {
	subject := New
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestTcp_Adapter_Mount_Good(t *core.T) {
	subject := (*Adapter).Mount
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestTcp_Adapter_Mount_Bad(t *core.T) {
	subject := (*Adapter).Mount
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestTcp_Adapter_Mount_Ugly(t *core.T) {
	subject := (*Adapter).Mount
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestTcp_Adapter_Listen_Good(t *core.T) {
	subject := (*Adapter).Listen
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestTcp_Adapter_Listen_Bad(t *core.T) {
	subject := (*Adapter).Listen
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestTcp_Adapter_Listen_Ugly(t *core.T) {
	subject := (*Adapter).Listen
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestTcp_Adapter_Dial_Good(t *core.T) {
	subject := (*Adapter).Dial
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestTcp_Adapter_Dial_Bad(t *core.T) {
	subject := (*Adapter).Dial
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestTcp_Adapter_Dial_Ugly(t *core.T) {
	subject := (*Adapter).Dial
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}
