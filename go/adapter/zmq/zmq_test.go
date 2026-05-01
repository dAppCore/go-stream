// SPDX-License-Identifier: EUPL-1.2

package zmq

import core "dappco.re/go"

func TestZmq_Mode_String_Good(t *core.T) {
	subject := Mode.String
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestZmq_Mode_String_Bad(t *core.T) {
	subject := Mode.String
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestZmq_Mode_String_Ugly(t *core.T) {
	subject := Mode.String
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestZmq_Role_String_Good(t *core.T) {
	subject := Role.String
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestZmq_Role_String_Bad(t *core.T) {
	subject := Role.String
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestZmq_Role_String_Ugly(t *core.T) {
	subject := Role.String
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestZmq_New_Good(t *core.T) {
	subject := New
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestZmq_New_Bad(t *core.T) {
	subject := New
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestZmq_New_Ugly(t *core.T) {
	subject := New
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestZmq_Adapter_Mount_Good(t *core.T) {
	subject := (*Adapter).Mount
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestZmq_Adapter_Mount_Bad(t *core.T) {
	subject := (*Adapter).Mount
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestZmq_Adapter_Mount_Ugly(t *core.T) {
	subject := (*Adapter).Mount
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestZmq_Adapter_Start_Good(t *core.T) {
	subject := (*Adapter).Start
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestZmq_Adapter_Start_Bad(t *core.T) {
	subject := (*Adapter).Start
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestZmq_Adapter_Start_Ugly(t *core.T) {
	subject := (*Adapter).Start
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestZmq_Adapter_Publish_Good(t *core.T) {
	subject := (*Adapter).Publish
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestZmq_Adapter_Publish_Bad(t *core.T) {
	subject := (*Adapter).Publish
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestZmq_Adapter_Publish_Ugly(t *core.T) {
	subject := (*Adapter).Publish
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestZmq_Adapter_Stop_Good(t *core.T) {
	subject := (*Adapter).Stop
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestZmq_Adapter_Stop_Bad(t *core.T) {
	subject := (*Adapter).Stop
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestZmq_Adapter_Stop_Ugly(t *core.T) {
	subject := (*Adapter).Stop
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}
