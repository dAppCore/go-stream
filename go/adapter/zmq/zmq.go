// SPDX-License-Identifier: EUPL-1.2

//	adapter := zmq.New(zmq.Config{
//		Mode:     zmq.ModePubSub,
//		Endpoint: "tcp://127.0.0.1:5555",
//		Role:     zmq.RoleSubscriber,
//	})
//
// adapter.Mount(hub)
// go adapter.Start(ctx)
// defer adapter.Stop()
package zmq

import (
	"context"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"

	"dappco.re/go"
	"dappco.re/go/stream"
)

const maxHandshakeFrameSize = 4 << 10

// mode := zmq.ModePubSub
type Mode int

const (
	ModePubSub Mode = iota
	ModePushPull
)

// core.Print(nil, "mode=%s", zmq.ModePubSub.String())
func (mode Mode) String() string {
	switch mode {
	case ModePubSub:
		return "pubsub"
	case ModePushPull:
		return "pushpull"
	default:
		return "unknown"
	}
}

// role := zmq.RoleSubscriber
type Role int

const (
	RolePublisher Role = iota
	RoleSubscriber
	RolePusher
	RolePuller
)

// core.Print(nil, "role=%s", zmq.RoleSubscriber.String())
func (role Role) String() string {
	switch role {
	case RolePublisher:
		return "publisher"
	case RoleSubscriber:
		return "subscriber"
	case RolePusher:
		return "pusher"
	case RolePuller:
		return "puller"
	default:
		return "unknown"
	}
}

//	config := zmq.Config{
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

	mutex   sync.RWMutex
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
func (adapter *Adapter) Start(ctx context.Context) core.Result {
	if adapter == nil {
		return core.Fail(core.E("stream.zmq", "nil adapter", nil))
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if adapter.config.Endpoint == "" {
		return core.Fail(core.E("stream.zmq", "empty endpoint", nil))
	}
	if adapter.hub == nil {
		return core.Fail(core.E("stream.zmq", "stream hub not mounted", nil))
	}
	if r := adapter.validateRole(); !r.OK {
		return r
	}

	runContext, runCancel := context.WithCancel(ctx)
	socketResult := adapter.newSocket(runContext)
	if !socketResult.OK {
		runCancel()
		return socketResult
	}
	socket := socketResult.Value.(zmq4.Socket)
	if r := adapter.connectSocket(socket); !r.OK {
		err := r.Value.(error)
		if closeErr := socket.Close(); closeErr != nil {
			runCancel()
			return core.Fail(core.ErrorJoin(err, closeErr))
		}
		runCancel()
		return r
	}

	adapter.mutex.Lock()
	if adapter.running {
		adapter.mutex.Unlock()
		if err := socket.Close(); err != nil {
			runCancel()
			return core.Fail(err)
		}
		runCancel()
		return core.Ok(nil)
	}
	adapter.running = true
	adapter.socket = socket
	adapter.cancel = runCancel
	adapter.mutex.Unlock()

	defer func() {
		adapter.mutex.Lock()
		adapter.running = false
		adapter.socket = nil
		adapter.cancel = nil
		adapter.mutex.Unlock()
		runCancel()
		if err := socket.Close(); err != nil {
			return
		}
	}()

	if !adapter.isReceiver() {
		peer := adapter.registerPeer(socket, stream.AuthResult{})
		if peer != nil {
			defer adapter.hub.RemovePeer(peer)
		}
		<-runContext.Done()
		return core.Ok(nil)
	}

	authResult := stream.AuthResult{Valid: true}
	if adapter.config.ConnAuthenticator != nil {
		handshakeResult := adapter.recvWithTimeout(runContext, socket, adapter.config.HandshakeTimeout)
		if !handshakeResult.OK {
			if handshakeResult.Value == context.Canceled {
				return core.Ok(nil)
			}
			return handshakeResult
		}
		handshake := handshakeResult.Value.(zmq4.Msg)
		if len(handshake.Bytes()) > maxHandshakeFrameSize {
			return core.Fail(stream.ErrAuthRejected)
		}
		authResult = adapter.config.ConnAuthenticator.AuthenticateConn(handshake.Bytes())
		if !authResult.Valid {
			return core.Fail(stream.ErrAuthRejected)
		}
	}
	peer := adapter.registerPeer(socket, authResult)
	if peer != nil {
		defer adapter.hub.RemovePeer(peer)
	}

	for {
		message, err := socket.Recv()
		if err != nil {
			if runContext.Err() != nil {
				return core.Ok(nil)
			}
			return core.Fail(err)
		}

		channel, frame, ok := decodeMessage(message)
		if !ok {
			continue
		}
		if channel == "" {
			if r := adapter.hub.Broadcast(frame); !r.OK {
				return r
			}
			continue
		}
		if r := adapter.hub.Publish(channel, frame); !r.OK {
			return r
		}
	}
}

func (adapter *Adapter) registerPeer(socket zmq4.Socket, authResult stream.AuthResult) *stream.Peer {
	if adapter == nil || adapter.hub == nil {
		return nil
	}
	peer := stream.NewPeer("zmq")
	peer.UserID = authResult.UserID
	if authResult.Claims != nil {
		peer.Claims = authResult.Claims
	}
	if socket != nil {
		peer.SetCloseHook(func() {
			if err := socket.Close(); err != nil {
				return
			}
		})
	}
	if r := adapter.hub.AddPeer(peer); !r.OK {
		return nil
	}
	return peer
}

// _ = adapter.Publish("block", templateBytes)
func (adapter *Adapter) Publish(channel string, frame []byte) core.Result {
	if adapter == nil {
		return core.Fail(core.E("stream.zmq", "nil adapter", nil))
	}
	if !adapter.isSender() {
		return core.Fail(core.E("stream.zmq", "publish not supported for this role", nil))
	}

	adapter.mutex.RLock()
	defer adapter.mutex.RUnlock()
	if !adapter.running || adapter.socket == nil {
		return core.Fail(core.E("stream.zmq", "adapter not started", nil))
	}

	return core.ResultOf(nil, adapter.socket.Send(zmq4.NewMsg(encodeMessage(channel, frame))))
}

// defer adapter.Stop()
func (adapter *Adapter) Stop() core.Result {
	if adapter == nil {
		return core.Ok(nil)
	}

	adapter.mutex.Lock()
	cancel := adapter.cancel
	socket := adapter.socket
	adapter.running = false
	adapter.cancel = nil
	adapter.socket = nil
	adapter.mutex.Unlock()

	if cancel != nil {
		cancel()
	}
	if socket != nil {
		return core.ResultOf(nil, socket.Close())
	}
	return core.Ok(nil)
}

func (adapter *Adapter) validateRole() core.Result {
	switch adapter.config.Mode {
	case ModePubSub:
		if adapter.config.Role != RolePublisher && adapter.config.Role != RoleSubscriber {
			return core.Fail(core.E("stream.zmq", "invalid pubsub role", nil))
		}
	case ModePushPull:
		if adapter.config.Role != RolePusher && adapter.config.Role != RolePuller {
			return core.Fail(core.E("stream.zmq", "invalid pushpull role", nil))
		}
	default:
		return core.Fail(core.E("stream.zmq", "invalid mode", nil))
	}
	return core.Ok(nil)
}

func (adapter *Adapter) newSocket(ctx context.Context) core.Result {
	switch adapter.config.Role {
	case RolePublisher:
		return core.Ok(zmq4.NewPub(ctx))
	case RoleSubscriber:
		socket := zmq4.NewSub(ctx)
		topics := adapter.config.Topics
		if len(topics) == 0 {
			topics = []string{""}
		}
		for _, topic := range topics {
			if err := socket.SetOption(zmq4.OptionSubscribe, topic); err != nil {
				return core.Fail(err)
			}
		}
		return core.Ok(socket)
	case RolePusher:
		return core.Ok(zmq4.NewPush(ctx))
	case RolePuller:
		return core.Ok(zmq4.NewPull(ctx))
	default:
		return core.Fail(core.E("stream.zmq", "invalid role", nil))
	}
}

func (adapter *Adapter) connectSocket(socket zmq4.Socket) core.Result {
	if adapter.shouldListen() {
		return core.ResultOf(nil, socket.Listen(listenEndpoint(adapter.config.Endpoint)))
	}
	return core.ResultOf(nil, socket.Dial(adapter.config.Endpoint))
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

func (adapter *Adapter) recvWithTimeout(ctx context.Context, socket zmq4.Socket, timeout time.Duration) core.Result {
	if timeout <= 0 {
		msg, err := socket.Recv()
		return core.ResultOf(msg, err)
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
		if err := socket.Close(); err != nil {
			return core.Fail(err)
		}
		return core.Fail(ctx.Err())
	case outcome := <-receive:
		return core.ResultOf(outcome.message, outcome.err)
	case <-timer.C:
		if err := socket.Close(); err != nil {
			return core.Fail(err)
		}
		return core.Fail(stream.ErrHandshakeTimeout)
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
