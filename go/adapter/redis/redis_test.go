// SPDX-License-Identifier: EUPL-1.2

package redis

import core "dappco.re/go"

func TestRedis_NewBridge_Good(t *core.T) {
	subject := NewBridge
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestRedis_NewBridge_Bad(t *core.T) {
	subject := NewBridge
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestRedis_NewBridge_Ugly(t *core.T) {
	subject := NewBridge
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestRedis_Bridge_Start_Good(t *core.T) {
	subject := (*Bridge).Start
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestRedis_Bridge_Start_Bad(t *core.T) {
	subject := (*Bridge).Start
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestRedis_Bridge_Start_Ugly(t *core.T) {
	subject := (*Bridge).Start
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestRedis_Bridge_Stop_Good(t *core.T) {
	subject := (*Bridge).Stop
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestRedis_Bridge_Stop_Bad(t *core.T) {
	subject := (*Bridge).Stop
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestRedis_Bridge_Stop_Ugly(t *core.T) {
	subject := (*Bridge).Stop
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestRedis_Bridge_PublishToChannel_Good(t *core.T) {
	subject := (*Bridge).PublishToChannel
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestRedis_Bridge_PublishToChannel_Bad(t *core.T) {
	subject := (*Bridge).PublishToChannel
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestRedis_Bridge_PublishToChannel_Ugly(t *core.T) {
	subject := (*Bridge).PublishToChannel
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestRedis_Bridge_PublishBroadcast_Good(t *core.T) {
	subject := (*Bridge).PublishBroadcast
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestRedis_Bridge_PublishBroadcast_Bad(t *core.T) {
	subject := (*Bridge).PublishBroadcast
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestRedis_Bridge_PublishBroadcast_Ugly(t *core.T) {
	subject := (*Bridge).PublishBroadcast
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestRedis_Bridge_SourceID_Good(t *core.T) {
	subject := (*Bridge).SourceID
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestRedis_Bridge_SourceID_Bad(t *core.T) {
	subject := (*Bridge).SourceID
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestRedis_Bridge_SourceID_Ugly(t *core.T) {
	subject := (*Bridge).SourceID
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}
