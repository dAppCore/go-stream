// SPDX-License-Identifier: EUPL-1.2

package zmq

import (
	core "dappco.re/go"
	"dappco.re/go/stream"
)

func TestAX7_Mode_String_Good(t *core.T) {
	core.AssertEqual(t, "pubsub", ModePubSub.String())
	core.AssertEqual(t, "pushpull", ModePushPull.String())
	core.AssertNotEqual(t, ModePubSub.String(), ModePushPull.String())
}

func TestAX7_Mode_String_Bad(t *core.T) {
	mode := Mode(99)

	core.AssertEqual(t, "unknown", mode.String())
	core.AssertNotEqual(t, "pubsub", mode.String())
}

func TestAX7_Mode_String_Ugly(t *core.T) {
	mode := Mode(-1)

	core.AssertEqual(t, "unknown", mode.String())
	core.AssertNotPanics(t, func() { _ = mode.String() })
}

func TestAX7_Role_String_Good(t *core.T) {
	core.AssertEqual(t, "publisher", RolePublisher.String())
	core.AssertEqual(t, "subscriber", RoleSubscriber.String())
	core.AssertEqual(t, "pusher", RolePusher.String())
	core.AssertEqual(t, "puller", RolePuller.String())
}

func TestAX7_Role_String_Bad(t *core.T) {
	role := Role(99)

	core.AssertEqual(t, "unknown", role.String())
	core.AssertNotEqual(t, "publisher", role.String())
}

func TestAX7_Role_String_Ugly(t *core.T) {
	role := Role(-1)

	core.AssertEqual(t, "unknown", role.String())
	core.AssertNotPanics(t, func() { _ = role.String() })
}

func TestAX7_New_Good(t *core.T) {
	adapter := New(Config{Mode: ModePubSub, Endpoint: "tcp://127.0.0.1:1", Role: RolePublisher})

	core.AssertNotNil(t, adapter)
	core.AssertEqual(t, ModePubSub, adapter.config.Mode)
	core.AssertEqual(t, 5*core.Second, adapter.config.HandshakeTimeout)
}

func TestAX7_New_Bad(t *core.T) {
	adapter := New(Config{})

	core.AssertNotNil(t, adapter)
	core.AssertEqual(t, ModePubSub, adapter.config.Mode)
	core.AssertEqual(t, "", adapter.config.Endpoint)
}

func TestAX7_New_Ugly(t *core.T) {
	adapter := New(Config{HandshakeTimeout: core.Millisecond, Topics: []string{"block"}})

	core.AssertEqual(t, core.Millisecond, adapter.config.HandshakeTimeout)
	core.AssertEqual(t, []string{"block"}, adapter.config.Topics)
}

func TestAX7_Adapter_Mount_Good(t *core.T) {
	adapter := New(Config{})
	hub := stream.NewHub()

	adapter.Mount(hub)
	core.AssertEqual(t, hub, adapter.hub)
	core.AssertFalse(t, adapter.running)
}

func TestAX7_Adapter_Mount_Bad(t *core.T) {
	adapter := New(Config{})

	adapter.Mount(nil)
	core.AssertNil(t, adapter.hub)
	core.AssertNotNil(t, adapter)
}

func TestAX7_Adapter_Mount_Ugly(t *core.T) {
	adapter := New(Config{})
	first := stream.NewHub()
	second := stream.NewHub()

	adapter.Mount(first)
	adapter.Mount(second)
	core.AssertEqual(t, second, adapter.hub)
}

func TestAX7_Adapter_Start_Good(t *core.T) {
	hub := stream.NewHub()
	ctx, cancel := core.WithCancel(core.Background())
	defer cancel()
	go hub.Run(ctx)
	adapter := New(Config{Mode: ModePubSub, Endpoint: randomTCPEndpoint(t), Role: RolePublisher})
	adapter.Mount(hub)

	go func() {
		if err := adapter.Start(ctx); err != nil {
			t.Errorf("Start() error = %v", err)
		}
	}()
	waitForAdapterRunning(t, adapter)
	core.AssertTrue(t, adapter.running)
}

func TestAX7_Adapter_Start_Bad(t *core.T) {
	adapter := New(Config{Mode: Mode(99), Endpoint: randomTCPEndpoint(t), Role: RolePublisher})
	adapter.Mount(stream.NewHub())

	err := adapter.Start(core.Background())
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "invalid mode")
}

func TestAX7_Adapter_Stop_Good(t *core.T) {
	hub := stream.NewHub()
	ctx, cancel := core.WithCancel(core.Background())
	defer cancel()
	go hub.Run(ctx)
	adapter := New(Config{Mode: ModePubSub, Endpoint: randomTCPEndpoint(t), Role: RolePublisher})
	adapter.Mount(hub)
	go func() { _ = adapter.Start(ctx) }()
	waitForAdapterRunning(t, adapter)

	core.AssertNoError(t, adapter.Stop())
	core.Sleep(50 * core.Millisecond)
	core.AssertFalse(t, adapter.running)
}

func TestAX7_Adapter_Stop_Bad(t *core.T) {
	var adapter *Adapter

	core.AssertNoError(t, adapter.Stop())
	core.AssertNil(t, adapter)
}

func TestAX7_Adapter_Stop_Ugly(t *core.T) {
	adapter := New(Config{Mode: ModePubSub, Endpoint: randomTCPEndpoint(t), Role: RolePublisher})

	core.AssertNoError(t, adapter.Stop())
	core.AssertFalse(t, adapter.running)
}

func TestAX7_Adapter_Publish_Ugly(t *core.T) {
	adapter := New(Config{Mode: ModePubSub, Endpoint: randomTCPEndpoint(t), Role: RolePublisher})
	adapter.Mount(stream.NewHub())

	err := adapter.Publish("block", []byte("template"))
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "not started")
}
