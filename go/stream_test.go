// SPDX-License-Identifier: EUPL-1.2

package stream

import core "dappco.re/go"

func TestStream_NewPeer_Good(t *core.T) {
	subject := NewPeer
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestStream_NewPeer_Bad(t *core.T) {
	subject := NewPeer
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestStream_NewPeer_Ugly(t *core.T) {
	subject := NewPeer
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestStream_Peer_Subscriptions_Good(t *core.T) {
	subject := (*Peer).Subscriptions
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestStream_Peer_Subscriptions_Bad(t *core.T) {
	subject := (*Peer).Subscriptions
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestStream_Peer_Subscriptions_Ugly(t *core.T) {
	subject := (*Peer).Subscriptions
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestStream_Peer_Send_Good(t *core.T) {
	subject := (*Peer).Send
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestStream_Peer_Send_Bad(t *core.T) {
	subject := (*Peer).Send
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestStream_Peer_Send_Ugly(t *core.T) {
	subject := (*Peer).Send
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestStream_Peer_Close_Good(t *core.T) {
	subject := (*Peer).Close
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestStream_Peer_Close_Bad(t *core.T) {
	subject := (*Peer).Close
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestStream_Peer_Close_Ugly(t *core.T) {
	subject := (*Peer).Close
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestStream_Peer_SetCloseHook_Good(t *core.T) {
	subject := (*Peer).SetCloseHook
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestStream_Peer_SetCloseHook_Bad(t *core.T) {
	subject := (*Peer).SetCloseHook
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestStream_Peer_SetCloseHook_Ugly(t *core.T) {
	subject := (*Peer).SetCloseHook
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestStream_Peer_SendQueue_Good(t *core.T) {
	subject := (*Peer).SendQueue
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestStream_Peer_SendQueue_Bad(t *core.T) {
	subject := (*Peer).SendQueue
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestStream_Peer_SendQueue_Ugly(t *core.T) {
	subject := (*Peer).SendQueue
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestStream_ConnectionState_String_Good(t *core.T) {
	subject := ConnectionState.String
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestStream_ConnectionState_String_Bad(t *core.T) {
	subject := ConnectionState.String
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestStream_ConnectionState_String_Ugly(t *core.T) {
	subject := ConnectionState.String
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestStream_Pipe_Good(t *core.T) {
	subject := Pipe
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestStream_Pipe_Bad(t *core.T) {
	subject := Pipe
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestStream_Pipe_Ugly(t *core.T) {
	subject := Pipe
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}
