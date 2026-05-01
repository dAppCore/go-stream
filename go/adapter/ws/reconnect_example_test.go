// SPDX-License-Identifier: EUPL-1.2

package ws

func ExampleNewReconnectingClient() {
	_ = NewReconnectingClient
}

func ExampleReconnectingClient_Connect() {
	_ = (*ReconnectingClient).Connect
}

func ExampleReconnectingClient_Send() {
	_ = (*ReconnectingClient).Send
}

func ExampleReconnectingClient_State() {
	_ = (*ReconnectingClient).State
}

func ExampleReconnectingClient_Close() {
	_ = (*ReconnectingClient).Close
}
