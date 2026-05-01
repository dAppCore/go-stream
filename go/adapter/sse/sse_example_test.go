// SPDX-License-Identifier: EUPL-1.2

package sse

func ExampleNew() {
	_ = New
}

func ExampleAdapter_Mount() {
	_ = (*Adapter).Mount
}

func ExampleAdapter_ServeHTTP() {
	_ = (*Adapter).ServeHTTP
}

func ExampleAdapter_Handler() {
	_ = (*Adapter).Handler
}

func ExampleAdapter_HandlerForChannel() {
	_ = (*Adapter).HandlerForChannel
}
