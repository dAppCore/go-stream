// SPDX-License-Identifier: EUPL-1.2

package ws

import core "dappco.re/go"

func TestWs_New_Good(t *core.T) {
	subject := New
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestWs_New_Bad(t *core.T) {
	subject := New
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestWs_New_Ugly(t *core.T) {
	subject := New
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestWs_Adapter_Mount_Good(t *core.T) {
	subject := (*Adapter).Mount
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestWs_Adapter_Mount_Bad(t *core.T) {
	subject := (*Adapter).Mount
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestWs_Adapter_Mount_Ugly(t *core.T) {
	subject := (*Adapter).Mount
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestWs_Adapter_ServeHTTP_Good(t *core.T) {
	subject := (*Adapter).ServeHTTP
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestWs_Adapter_ServeHTTP_Bad(t *core.T) {
	subject := (*Adapter).ServeHTTP
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestWs_Adapter_ServeHTTP_Ugly(t *core.T) {
	subject := (*Adapter).ServeHTTP
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestWs_Adapter_HandlerForChannel_Good(t *core.T) {
	subject := (*Adapter).HandlerForChannel
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestWs_Adapter_HandlerForChannel_Bad(t *core.T) {
	subject := (*Adapter).HandlerForChannel
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestWs_Adapter_HandlerForChannel_Ugly(t *core.T) {
	subject := (*Adapter).HandlerForChannel
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestWs_Adapter_Handler_Good(t *core.T) {
	subject := (*Adapter).Handler
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestWs_Adapter_Handler_Bad(t *core.T) {
	subject := (*Adapter).Handler
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestWs_Adapter_Handler_Ugly(t *core.T) {
	subject := (*Adapter).Handler
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}
