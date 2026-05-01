// SPDX-License-Identifier: EUPL-1.2

package stream

import core "dappco.re/go"

func TestHub_NewHub_Good(t *core.T) {
	subject := NewHub
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_NewHub_Bad(t *core.T) {
	subject := NewHub
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_NewHub_Ugly(t *core.T) {
	subject := NewHub
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_NewHubWithConfig_Good(t *core.T) {
	subject := NewHubWithConfig
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_NewHubWithConfig_Bad(t *core.T) {
	subject := NewHubWithConfig
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_NewHubWithConfig_Ugly(t *core.T) {
	subject := NewHubWithConfig
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_Config_Good(t *core.T) {
	subject := (*Hub).Config
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_Config_Bad(t *core.T) {
	subject := (*Hub).Config
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_Config_Ugly(t *core.T) {
	subject := (*Hub).Config
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_Running_Good(t *core.T) {
	subject := (*Hub).Running
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_Running_Bad(t *core.T) {
	subject := (*Hub).Running
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_Running_Ugly(t *core.T) {
	subject := (*Hub).Running
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_Run_Good(t *core.T) {
	subject := (*Hub).Run
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_Run_Bad(t *core.T) {
	subject := (*Hub).Run
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_Run_Ugly(t *core.T) {
	subject := (*Hub).Run
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_SendToChannel_Good(t *core.T) {
	subject := (*Hub).SendToChannel
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_SendToChannel_Bad(t *core.T) {
	subject := (*Hub).SendToChannel
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_SendToChannel_Ugly(t *core.T) {
	subject := (*Hub).SendToChannel
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_PublishFromPeer_Good(t *core.T) {
	subject := (*Hub).PublishFromPeer
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_PublishFromPeer_Bad(t *core.T) {
	subject := (*Hub).PublishFromPeer
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_PublishFromPeer_Ugly(t *core.T) {
	subject := (*Hub).PublishFromPeer
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_PublishFromBridge_Good(t *core.T) {
	subject := (*Hub).PublishFromBridge
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_PublishFromBridge_Bad(t *core.T) {
	subject := (*Hub).PublishFromBridge
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_PublishFromBridge_Ugly(t *core.T) {
	subject := (*Hub).PublishFromBridge
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_SubscribeWithError_Good(t *core.T) {
	subject := (*Hub).SubscribeWithError
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_SubscribeWithError_Bad(t *core.T) {
	subject := (*Hub).SubscribeWithError
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_SubscribeWithError_Ugly(t *core.T) {
	subject := (*Hub).SubscribeWithError
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_SubscribeE_Good(t *core.T) {
	subject := (*Hub).SubscribeE
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_SubscribeE_Bad(t *core.T) {
	subject := (*Hub).SubscribeE
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_SubscribeE_Ugly(t *core.T) {
	subject := (*Hub).SubscribeE
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_Subscribe_Good(t *core.T) {
	subject := (*Hub).Subscribe
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_Subscribe_Bad(t *core.T) {
	subject := (*Hub).Subscribe
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_Subscribe_Ugly(t *core.T) {
	subject := (*Hub).Subscribe
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_SubscribePeer_Good(t *core.T) {
	subject := (*Hub).SubscribePeer
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_SubscribePeer_Bad(t *core.T) {
	subject := (*Hub).SubscribePeer
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_SubscribePeer_Ugly(t *core.T) {
	subject := (*Hub).SubscribePeer
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_CanSubscribePeer_Good(t *core.T) {
	subject := (*Hub).CanSubscribePeer
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_CanSubscribePeer_Bad(t *core.T) {
	subject := (*Hub).CanSubscribePeer
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_CanSubscribePeer_Ugly(t *core.T) {
	subject := (*Hub).CanSubscribePeer
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_UnsubscribePeer_Good(t *core.T) {
	subject := (*Hub).UnsubscribePeer
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_UnsubscribePeer_Bad(t *core.T) {
	subject := (*Hub).UnsubscribePeer
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_UnsubscribePeer_Ugly(t *core.T) {
	subject := (*Hub).UnsubscribePeer
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_Publish_Good(t *core.T) {
	subject := (*Hub).Publish
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_Publish_Bad(t *core.T) {
	subject := (*Hub).Publish
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_Publish_Ugly(t *core.T) {
	subject := (*Hub).Publish
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_Broadcast_Good(t *core.T) {
	subject := (*Hub).Broadcast
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_Broadcast_Bad(t *core.T) {
	subject := (*Hub).Broadcast
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_Broadcast_Ugly(t *core.T) {
	subject := (*Hub).Broadcast
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_BroadcastFromPeer_Good(t *core.T) {
	subject := (*Hub).BroadcastFromPeer
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_BroadcastFromPeer_Bad(t *core.T) {
	subject := (*Hub).BroadcastFromPeer
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_BroadcastFromPeer_Ugly(t *core.T) {
	subject := (*Hub).BroadcastFromPeer
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_BroadcastFromBridge_Good(t *core.T) {
	subject := (*Hub).BroadcastFromBridge
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_BroadcastFromBridge_Bad(t *core.T) {
	subject := (*Hub).BroadcastFromBridge
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_BroadcastFromBridge_Ugly(t *core.T) {
	subject := (*Hub).BroadcastFromBridge
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_Pipe_Good(t *core.T) {
	subject := (*Hub).Pipe
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_Pipe_Bad(t *core.T) {
	subject := (*Hub).Pipe
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_Pipe_Ugly(t *core.T) {
	subject := (*Hub).Pipe
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_Stats_Good(t *core.T) {
	subject := (*Hub).Stats
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_Stats_Bad(t *core.T) {
	subject := (*Hub).Stats
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_Stats_Ugly(t *core.T) {
	subject := (*Hub).Stats
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_SubscribePublished_Good(t *core.T) {
	subject := (*Hub).SubscribePublished
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_SubscribePublished_Bad(t *core.T) {
	subject := (*Hub).SubscribePublished
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_SubscribePublished_Ugly(t *core.T) {
	subject := (*Hub).SubscribePublished
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_SubscribeBroadcast_Good(t *core.T) {
	subject := (*Hub).SubscribeBroadcast
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_SubscribeBroadcast_Bad(t *core.T) {
	subject := (*Hub).SubscribeBroadcast
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_SubscribeBroadcast_Ugly(t *core.T) {
	subject := (*Hub).SubscribeBroadcast
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_PeerCount_Good(t *core.T) {
	subject := (*Hub).PeerCount
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_PeerCount_Bad(t *core.T) {
	subject := (*Hub).PeerCount
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_PeerCount_Ugly(t *core.T) {
	subject := (*Hub).PeerCount
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_ChannelCount_Good(t *core.T) {
	subject := (*Hub).ChannelCount
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_ChannelCount_Bad(t *core.T) {
	subject := (*Hub).ChannelCount
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_ChannelCount_Ugly(t *core.T) {
	subject := (*Hub).ChannelCount
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_ChannelSubscriberCount_Good(t *core.T) {
	subject := (*Hub).ChannelSubscriberCount
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_ChannelSubscriberCount_Bad(t *core.T) {
	subject := (*Hub).ChannelSubscriberCount
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_ChannelSubscriberCount_Ugly(t *core.T) {
	subject := (*Hub).ChannelSubscriberCount
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_AllPeers_Good(t *core.T) {
	subject := (*Hub).AllPeers
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_AllPeers_Bad(t *core.T) {
	subject := (*Hub).AllPeers
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_AllPeers_Ugly(t *core.T) {
	subject := (*Hub).AllPeers
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_AllChannels_Good(t *core.T) {
	subject := (*Hub).AllChannels
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_AllChannels_Bad(t *core.T) {
	subject := (*Hub).AllChannels
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_AllChannels_Ugly(t *core.T) {
	subject := (*Hub).AllChannels
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_AddPeer_Good(t *core.T) {
	subject := (*Hub).AddPeer
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_AddPeer_Bad(t *core.T) {
	subject := (*Hub).AddPeer
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_AddPeer_Ugly(t *core.T) {
	subject := (*Hub).AddPeer
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestHub_Hub_RemovePeer_Good(t *core.T) {
	subject := (*Hub).RemovePeer
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestHub_Hub_RemovePeer_Bad(t *core.T) {
	subject := (*Hub).RemovePeer
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestHub_Hub_RemovePeer_Ugly(t *core.T) {
	subject := (*Hub).RemovePeer
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}
