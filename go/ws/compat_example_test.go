// SPDX-License-Identifier: EUPL-1.2

package ws

func ExampleNewRedisBridge() {
	_ = NewRedisBridge
}

func ExampleNewAPIKeyAuth() {
	_ = NewAPIKeyAuth
}

func ExampleNewHub() {
	_ = NewHub
}

func ExampleNewHubWithConfig() {
	_ = NewHubWithConfig
}

func ExampleDefaultHubConfig() {
	_ = DefaultHubConfig
}

func ExampleNewPeer() {
	_ = NewPeer
}

func ExamplePipe() {
	_ = Pipe
}

func ExampleNew() {
	_ = New
}

func ExampleNewReconnectingClient() {
	_ = NewReconnectingClient
}

func ExampleHub_Handler() {
	_ = (*Hub).Handler
}

func ExampleHub_HandlerForChannel() {
	_ = (*Hub).HandlerForChannel
}
