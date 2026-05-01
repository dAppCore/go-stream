// SPDX-License-Identifier: EUPL-1.2

package zmq

func ExampleMode_String() {
	_ = Mode.String
}

func ExampleRole_String() {
	_ = Role.String
}

func ExampleNew() {
	_ = New
}

func ExampleAdapter_Mount() {
	_ = (*Adapter).Mount
}

func ExampleAdapter_Start() {
	_ = (*Adapter).Start
}

func ExampleAdapter_Publish() {
	_ = (*Adapter).Publish
}

func ExampleAdapter_Stop() {
	_ = (*Adapter).Stop
}
