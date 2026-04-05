// SPDX-License-Identifier: EUPL-1.2

// Package zmq wires a hub to ZeroMQ sockets.
//
//	adapter := zmq.New(zmq.Config{
//	    Mode:     zmq.ModePubSub,
//	    Endpoint: "tcp://127.0.0.1:5555",
//	    Role:     zmq.RoleSubscriber,
//	})
//	adapter.Mount(hub)
//	go adapter.Start(ctx)
//	defer adapter.Stop()
package zmq

import (
	"context"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"

	"dappco.re/go/core"
	"dappco.re/go/stream"
)

const maxHandshakeFrameSize = 4 << 10

// Mode selects the ZMQ socket pattern.
type Mode int

const (
	ModePubSub Mode = iota
	ModePushPull
)

// Role is the ZMQ socket role.
type Role int

const (
	RolePublisher Role = iota
	RoleSubscriber
	RolePusher
	RolePuller
)

//	cfg := zmq.Config{
//	    Mode:     zmq.ModePubSub,
//	    Endpoint: "tcp://127.0.0.1:5555",
//	    Role:     zmq.RoleSubscriber,
//	}
type Config struct {
	Mode     Mode
	Endpoint string
	Role     Role
	Topics   []string

	// ConnAuthenticator validates the first received frame before normal dispatch.
	// When nil, the adapter accepts the connection without handshake validation.
	ConnAuthenticator stream.ConnAuthenticator

	// HandshakeTimeout limits how long the adapter waits for the first frame when
	// ConnAuthenticator is configured. Defaults to 5 seconds.
	HandshakeTimeout time.Duration
}

// adapter := zmq.New(zmq.Config{Mode: zmq.ModePubSub, Endpoint: "tcp://127.0.0.1:5555", Role: zmq.RoleSubscriber})
type Adapter struct {
	hub    *stream.Hub
	config Config

	mu      sync.RWMutex
	running bool
	socket  zmq4.Socket
	cancel  context.CancelFunc
}

// adapter := zmq.New(zmq.Config{Mode: zmq.ModePubSub, Endpoint: "tcp://127.0.0.1:5555", Role: zmq.RoleSubscriber})
func New(config Config) *Adapter {
	if config.HandshakeTimeout == 0 {
		config.HandshakeTimeout = 5 * time.Second
	}
	return &Adapter{config: config}
}

// adapter.Mount(hub)
func (adapter *Adapter) Mount(hub *stream.Hub) {
	adapter.hub = hub
}

// go adapter.Start(ctx)
//
// Start connects the socket, validates the optional handshake, and forwards
// received frames into the mounted hub until the context is cancelled.
func (adapter *Adapter) Start(ctx context.Context) error {
	if adapter == nil {
		return core.E("stream.zmq", "nil adapter", nil)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if adapter.config.Endpoint == "" {
		return core.E("stream.zmq", "empty endpoint", nil)
	}
	if adapter.hub == nil {
		return core.E("stream.zmq", "stream hub not mounted", nil)
	}
	if err := adapter.validateRole(); err != nil {
		return err
	}

	runContext, runCancel := context.WithCancel(ctx)
	socket, err := adapter.newSocket(runContext)
	if err != nil {
		runCancel()
		return err
	}
	if err := adapter.connectSocket(socket); err != nil {
		_ = socket.Close()
		runCancel()
		return err
	}

	adapter.mu.Lock()
	if adapter.running {
		adapter.mu.Unlock()
		_ = socket.Close()
		runCancel()
		return nil
	}
	adapter.running = true
	adapter.socket = socket
	adapter.cancel = runCancel
	adapter.mu.Unlock()

	defer func() {
		adapter.mu.Lock()
		adapter.running = false
		adapter.socket = nil
		adapter.cancel = nil
		adapter.mu.Unlock()
		runCancel()
		_ = socket.Close()
	}()

	if !adapter.isReceiver() {
		<-runContext.Done()
		return nil
	}

	if adapter.config.ConnAuthenticator != nil {
		handshake, err := adapter.recvWithTimeout(runContext, socket, adapter.config.HandshakeTimeout)
		if err != nil {
			if err == context.Canceled {
				return nil
			}
			return err
		}
		if len(handshake.Bytes()) > maxHandshakeFrameSize {
			return stream.ErrAuthRejected
		}
		authResult := adapter.config.ConnAuthenticator.AuthenticateConn(handshake.Bytes())
		if !authResult.Valid {
			return stream.ErrAuthRejected
		}
	}

	for {
		message, err := socket.Recv()
		if err != nil {
			if runContext.Err() != nil {
				return nil
			}
			return err
		}

		channel, frame, ok := decodeMessage(message)
		if !ok {
			continue
		}
		if channel == "" {
			_ = adapter.hub.Broadcast(frame)
			continue
		}
		_ = adapter.hub.Publish(channel, frame)
	}
}

// _ = adapter.Publish("block", templateBytes)
func (adapter *Adapter) Publish(channel string, frame []byte) error {
	if adapter == nil {
		return core.E("stream.zmq", "nil adapter", nil)
	}
	if !adapter.isSender() {
		return core.E("stream.zmq", "publish not supported for this role", nil)
	}

	adapter.mu.RLock()
	socket := adapter.socket
	running := adapter.running
	adapter.mu.RUnlock()
	if !running || socket == nil {
		return core.E("stream.zmq", "adapter not started", nil)
	}

	return socket.Send(zmq4.NewMsg(encodeMessage(channel, frame)))
}

// Stop shuts down the adapter.
//
//	defer adapter.Stop()
func (adapter *Adapter) Stop() error {
	if adapter == nil {
		return nil
	}

	adapter.mu.RLock()
	cancel := adapter.cancel
	socket := adapter.socket
	adapter.mu.RUnlock()

	if cancel != nil {
		cancel()
	}
	if socket != nil {
		return socket.Close()
	}
	return nil
}

func (adapter *Adapter) validateRole() error {
	switch adapter.config.Mode {
	case ModePubSub:
		if adapter.config.Role != RolePublisher && adapter.config.Role != RoleSubscriber {
			return core.E("stream.zmq", "invalid pubsub role", nil)
		}
	case ModePushPull:
		if adapter.config.Role != RolePusher && adapter.config.Role != RolePuller {
			return core.E("stream.zmq", "invalid pushpull role", nil)
		}
	default:
		return core.E("stream.zmq", "invalid mode", nil)
	}
	return nil
}

func (adapter *Adapter) newSocket(ctx context.Context) (zmq4.Socket, error) {
	switch adapter.config.Role {
	case RolePublisher:
		return zmq4.NewPub(ctx), nil
	case RoleSubscriber:
		socket := zmq4.NewSub(ctx)
		topics := adapter.config.Topics
		if len(topics) == 0 {
			topics = []string{""}
		}
		for _, topic := range topics {
			if err := socket.SetOption(zmq4.OptionSubscribe, topic); err != nil {
				return nil, err
			}
		}
		return socket, nil
	case RolePusher:
		return zmq4.NewPush(ctx), nil
	case RolePuller:
		return zmq4.NewPull(ctx), nil
	default:
		return nil, core.E("stream.zmq", "invalid role", nil)
	}
}

func (adapter *Adapter) connectSocket(socket zmq4.Socket) error {
	if adapter.shouldListen() {
		return socket.Listen(listenEndpoint(adapter.config.Endpoint))
	}
	return socket.Dial(adapter.config.Endpoint)
}

func (adapter *Adapter) shouldListen() bool {
	if adapter.config.Mode == ModePushPull {
		return adapter.config.Role == RolePusher
	}
	return adapter.config.Role == RolePublisher
}

func (adapter *Adapter) isSender() bool {
	return adapter.config.Role == RolePublisher || adapter.config.Role == RolePusher
}

func (adapter *Adapter) isReceiver() bool {
	return adapter.config.Role == RoleSubscriber || adapter.config.Role == RolePuller
}

func decodeMessage(message zmq4.Msg) (string, []byte, bool) {
	payload := message.Bytes()
	for index, value := range payload {
		if value != 0 {
			continue
		}
		channel := string(payload[:index])
		frame := append([]byte(nil), payload[index+1:]...)
		return channel, frame, true
	}
	return "", nil, false
}

func (adapter *Adapter) recvWithTimeout(ctx context.Context, socket zmq4.Socket, timeout time.Duration) (zmq4.Msg, error) {
	if timeout <= 0 {
		msg, err := socket.Recv()
		return msg, err
	}

	type result struct {
		message zmq4.Msg
		err     error
	}

	receive := make(chan result, 1)
	go func() {
		msg, err := socket.Recv()
		receive <- result{message: msg, err: err}
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		_ = socket.Close()
		return zmq4.Msg{}, ctx.Err()
	case outcome := <-receive:
		return outcome.message, outcome.err
	case <-timer.C:
		_ = socket.Close()
		return zmq4.Msg{}, stream.ErrHandshakeTimeout
	}
}

func encodeMessage(channel string, frame []byte) []byte {
	output := make([]byte, 0, len(channel)+1+len(frame))
	output = append(output, []byte(channel)...)
	output = append(output, 0)
	output = append(output, frame...)
	return output
}

func listenEndpoint(endpoint string) string {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "tcp" {
		return endpoint
	}

	host, port, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		return endpoint
	}
	if host == "" || host == "*" {
		return endpoint
	}

	parsed.Host = net.JoinHostPort("*", port)
	return parsed.String()
}
