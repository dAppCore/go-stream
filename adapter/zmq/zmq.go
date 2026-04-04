// SPDX-License-Identifier: EUPL-1.2

// Package zmq is the ZeroMQ transport adapter for stream.Hub.
// High-throughput IPC for daemon block notifications and inter-process job broadcasts.
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

// Config configures the ZMQ adapter.
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

// Adapter is the ZMQ transport adapter.
type Adapter struct {
	hub    *stream.Hub
	config Config

	mu      sync.RWMutex
	running bool
	socket  zmq4.Socket
	cancel  context.CancelFunc
}

// New creates a ZMQ adapter. Call Mount and Start before use.
func New(config Config) *Adapter {
	if config.HandshakeTimeout == 0 {
		config.HandshakeTimeout = 5 * time.Second
	}
	return &Adapter{config: config}
}

// Mount wires the adapter to a hub.
func (a *Adapter) Mount(hub *stream.Hub) {
	a.hub = hub
}

// Start opens the ZMQ socket and begins receive/dispatch. Blocks until ctx cancelled.
func (a *Adapter) Start(ctx context.Context) error {
	if a == nil {
		return core.E("stream.zmq", "nil adapter", nil)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if a.config.Endpoint == "" {
		return core.E("stream.zmq", "empty endpoint", nil)
	}
	if a.hub == nil {
		return core.E("stream.zmq", "stream hub not mounted", nil)
	}
	if err := a.validateRole(); err != nil {
		return err
	}

	runContext, runCancel := context.WithCancel(ctx)
	socket, err := a.newSocket(runContext)
	if err != nil {
		runCancel()
		return err
	}
	if err := a.connectSocket(socket); err != nil {
		_ = socket.Close()
		runCancel()
		return err
	}

	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		_ = socket.Close()
		runCancel()
		return nil
	}
	a.running = true
	a.socket = socket
	a.cancel = runCancel
	a.mu.Unlock()

	defer func() {
		a.mu.Lock()
		a.running = false
		a.socket = nil
		a.cancel = nil
		a.mu.Unlock()
		runCancel()
		_ = socket.Close()
	}()

	if !a.isReceiver() {
		<-runContext.Done()
		return nil
	}

	if a.config.ConnAuthenticator != nil {
		handshake, err := a.recvWithTimeout(runContext, socket, a.config.HandshakeTimeout)
		if err != nil {
			if err == context.Canceled {
				return nil
			}
			return err
		}
		result := a.config.ConnAuthenticator.AuthenticateConn(handshake.Bytes())
		if !result.Valid {
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
			_ = a.hub.Broadcast(frame)
			continue
		}
		_ = a.hub.Publish(channel, frame)
	}
}

// Publish sends frame with topic (channel name) via the ZMQ socket.
func (a *Adapter) Publish(channel string, frame []byte) error {
	if a == nil {
		return core.E("stream.zmq", "nil adapter", nil)
	}
	if !a.isSender() {
		return core.E("stream.zmq", "publish not supported for this role", nil)
	}

	a.mu.RLock()
	socket := a.socket
	running := a.running
	a.mu.RUnlock()
	if !running || socket == nil {
		return core.E("stream.zmq", "adapter not started", nil)
	}

	return socket.Send(zmq4.NewMsg(encodeMessage(channel, frame)))
}

// Stop shuts down the adapter.
func (a *Adapter) Stop() error {
	if a == nil {
		return nil
	}

	a.mu.RLock()
	cancel := a.cancel
	socket := a.socket
	a.mu.RUnlock()

	if cancel != nil {
		cancel()
	}
	if socket != nil {
		return socket.Close()
	}
	return nil
}

func (a *Adapter) validateRole() error {
	switch a.config.Mode {
	case ModePubSub:
		if a.config.Role != RolePublisher && a.config.Role != RoleSubscriber {
			return core.E("stream.zmq", "invalid pubsub role", nil)
		}
	case ModePushPull:
		if a.config.Role != RolePusher && a.config.Role != RolePuller {
			return core.E("stream.zmq", "invalid pushpull role", nil)
		}
	default:
		return core.E("stream.zmq", "invalid mode", nil)
	}
	return nil
}

func (a *Adapter) newSocket(ctx context.Context) (zmq4.Socket, error) {
	switch a.config.Role {
	case RolePublisher:
		return zmq4.NewPub(ctx), nil
	case RoleSubscriber:
		socket := zmq4.NewSub(ctx)
		topics := a.config.Topics
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

func (a *Adapter) connectSocket(socket zmq4.Socket) error {
	if a.shouldListen() {
		return socket.Listen(listenEndpoint(a.config.Endpoint))
	}
	return socket.Dial(a.config.Endpoint)
}

func (a *Adapter) shouldListen() bool {
	if a.config.Mode == ModePushPull {
		return a.config.Role == RolePusher
	}
	return a.config.Role == RolePublisher
}

func (a *Adapter) isSender() bool {
	return a.config.Role == RolePublisher || a.config.Role == RolePusher
}

func (a *Adapter) isReceiver() bool {
	return a.config.Role == RoleSubscriber || a.config.Role == RolePuller
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

func (a *Adapter) recvWithTimeout(ctx context.Context, socket zmq4.Socket, timeout time.Duration) (zmq4.Msg, error) {
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
