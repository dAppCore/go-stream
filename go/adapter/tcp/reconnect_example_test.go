// SPDX-License-Identifier: EUPL-1.2

package tcp

func ExampleNewReconnectingTCP() {
	_ = NewReconnectingTCP
}

func ExampleReconnectingTCP_Connect() {
	_ = (*ReconnectingTCP).Connect
}

func ExampleReconnectingTCP_Send() {
	_ = (*ReconnectingTCP).Send
}

func ExampleReconnectingTCP_State() {
	_ = (*ReconnectingTCP).State
}

func ExampleReconnectingTCP_Close() {
	_ = (*ReconnectingTCP).Close
}
