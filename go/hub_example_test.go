// SPDX-License-Identifier: EUPL-1.2

package stream

func ExampleNewHub() {
	_ = NewHub
}

func ExampleNewHubWithConfig() {
	_ = NewHubWithConfig
}

func ExampleHub_Config() {
	_ = (*Hub).Config
}

func ExampleHub_Running() {
	_ = (*Hub).Running
}

func ExampleHub_Run() {
	_ = (*Hub).Run
}

func ExampleHub_SendToChannel() {
	_ = (*Hub).SendToChannel
}

func ExampleHub_PublishFromPeer() {
	_ = (*Hub).PublishFromPeer
}

func ExampleHub_PublishFromBridge() {
	_ = (*Hub).PublishFromBridge
}

func ExampleHub_SubscribeWithError() {
	_ = (*Hub).SubscribeWithError
}

func ExampleHub_SubscribeE() {
	_ = (*Hub).SubscribeE
}

func ExampleHub_Subscribe() {
	_ = (*Hub).Subscribe
}

func ExampleHub_SubscribePeer() {
	_ = (*Hub).SubscribePeer
}

func ExampleHub_CanSubscribePeer() {
	_ = (*Hub).CanSubscribePeer
}

func ExampleHub_UnsubscribePeer() {
	_ = (*Hub).UnsubscribePeer
}

func ExampleHub_Publish() {
	_ = (*Hub).Publish
}

func ExampleHub_Broadcast() {
	_ = (*Hub).Broadcast
}

func ExampleHub_BroadcastFromPeer() {
	_ = (*Hub).BroadcastFromPeer
}

func ExampleHub_BroadcastFromBridge() {
	_ = (*Hub).BroadcastFromBridge
}

func ExampleHub_Pipe() {
	_ = (*Hub).Pipe
}

func ExampleHub_Stats() {
	_ = (*Hub).Stats
}

func ExampleHub_SubscribePublished() {
	_ = (*Hub).SubscribePublished
}

func ExampleHub_SubscribeBroadcast() {
	_ = (*Hub).SubscribeBroadcast
}

func ExampleHub_PeerCount() {
	_ = (*Hub).PeerCount
}

func ExampleHub_ChannelCount() {
	_ = (*Hub).ChannelCount
}

func ExampleHub_ChannelSubscriberCount() {
	_ = (*Hub).ChannelSubscriberCount
}

func ExampleHub_AllPeers() {
	_ = (*Hub).AllPeers
}

func ExampleHub_AllChannels() {
	_ = (*Hub).AllChannels
}

func ExampleHub_AddPeer() {
	_ = (*Hub).AddPeer
}

func ExampleHub_RemovePeer() {
	_ = (*Hub).RemovePeer
}
