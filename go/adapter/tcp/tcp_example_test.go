// SPDX-License-Identifier: EUPL-1.2

package tcp

func ExampleNew() {
	_ = New
}

func ExampleAdapter_Mount() {
	_ = (*Adapter).Mount
}

func ExampleAdapter_Listen() {
	_ = (*Adapter).Listen
}

func ExampleAdapter_Dial() {
	_ = (*Adapter).Dial
}
