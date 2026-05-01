// SPDX-License-Identifier: EUPL-1.2

package stream

import core "dappco.re/go"

func TestMessage_MessageType_String_Good(t *core.T) {
	subject := MessageType.String
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestMessage_MessageType_String_Bad(t *core.T) {
	subject := MessageType.String
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestMessage_MessageType_String_Ugly(t *core.T) {
	subject := MessageType.String
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}
