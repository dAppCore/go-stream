// SPDX-License-Identifier: EUPL-1.2

package stream

func ExampleNewPeer() {
	_ = NewPeer
}

func ExamplePeer_Subscriptions() {
	_ = (*Peer).Subscriptions
}

func ExamplePeer_Send() {
	_ = (*Peer).Send
}

func ExamplePeer_Close() {
	_ = (*Peer).Close
}

func ExamplePeer_SetCloseHook() {
	_ = (*Peer).SetCloseHook
}

func ExamplePeer_SendQueue() {
	_ = (*Peer).SendQueue
}

func ExampleConnectionState_String() {
	_ = ConnectionState.String
}

func ExamplePipe() {
	_ = Pipe
}
