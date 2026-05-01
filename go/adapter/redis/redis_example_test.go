// SPDX-License-Identifier: EUPL-1.2

package redis

func ExampleNewBridge() {
	_ = NewBridge
}

func ExampleBridge_Start() {
	_ = (*Bridge).Start
}

func ExampleBridge_Stop() {
	_ = (*Bridge).Stop
}

func ExampleBridge_PublishToChannel() {
	_ = (*Bridge).PublishToChannel
}

func ExampleBridge_PublishBroadcast() {
	_ = (*Bridge).PublishBroadcast
}

func ExampleBridge_SourceID() {
	_ = (*Bridge).SourceID
}
