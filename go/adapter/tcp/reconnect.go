// SPDX-License-Identifier: EUPL-1.2

package tcp

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"

	"dappco.re/go"
	"dappco.re/go/stream"
)

//	config := tcp.ReconnectConfig{
//	    Addr: "127.0.0.1:9000",
//	    OnReconnect: func(attempt int) {
//	        core.Print(nil, "tcp reconnect attempt=%d", attempt)
//	    },
//	    OnMessage: func(channel string, frame []byte) {
//	        _ = channel
//	        _ = frame
//	    },
//	}
//
// client := tcp.NewReconnectingTCP(config)
type ReconnectConfig struct {
	Addr              string
	HandshakeFrame    []byte
	HandshakeChannel  string
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64
	MaxRetries        int
	TLS               *tls.Config
	OnConnect         func()
	OnDisconnect      func()
	OnReconnect       func(attempt int)
	OnMessage         func(channel string, frame []byte)
}

// client := tcp.NewReconnectingTCP(tcp.ReconnectConfig{Addr: "10.69.69.165:9000"})
type ReconnectingTCP struct {
	config ReconnectConfig

	mutex      sync.RWMutex
	writeMutex sync.Mutex
	conn       net.Conn
	state      stream.ConnectionState
	closed     bool
}

// client := tcp.NewReconnectingTCP(tcp.ReconnectConfig{Addr: "10.69.69.165:9000"})
func NewReconnectingTCP(config ReconnectConfig) *ReconnectingTCP {
	if config.InitialBackoff == 0 {
		config.InitialBackoff = time.Second
	}
	if config.MaxBackoff == 0 {
		config.MaxBackoff = 30 * time.Second
	}
	if config.BackoffMultiplier <= 0 {
		config.BackoffMultiplier = 2
	}
	return &ReconnectingTCP{
		config: config,
		state:  stream.StateDisconnected,
	}
}

// err := client.Connect(ctx)
func (client *ReconnectingTCP) Connect(ctx context.Context) core.Result {
	if client == nil {
		return core.Fail(core.E("stream.tcp", "nil reconnecting tcp", nil))
	}
	if ctx == nil {
		ctx = context.Background()
	}

	backoff := client.config.InitialBackoff
	attempt := 0
	for {
		if client.isClosed() || ctx.Err() != nil {
			return core.Ok(nil)
		}

		client.setState(stream.StateConnecting)
		dialResult := client.dial(ctx)
		if !dialResult.OK {
			attempt++
			client.setState(stream.StateDisconnected)
			if client.config.MaxRetries > 0 && attempt > client.config.MaxRetries {
				return dialResult
			}
			if client.config.OnReconnect != nil {
				client.config.OnReconnect(attempt)
			}
			if r := sleepContext(ctx, backoff); !r.OK {
				return r
			}
			backoff = nextTCPBackoff(backoff, client.config.BackoffMultiplier, client.config.MaxBackoff)
			continue
		}
		conn := dialResult.Value.(net.Conn)
		if r := client.writeHandshake(conn); !r.OK {
			err := r.Value.(error)
			if closeErr := conn.Close(); closeErr != nil {
				err = core.ErrorJoin(err, closeErr)
			}
			attempt++
			client.setState(stream.StateDisconnected)
			if client.config.MaxRetries > 0 && attempt > client.config.MaxRetries {
				return core.Fail(err)
			}
			if client.config.OnReconnect != nil {
				client.config.OnReconnect(attempt)
			}
			if r := sleepContext(ctx, backoff); !r.OK {
				return r
			}
			backoff = nextTCPBackoff(backoff, client.config.BackoffMultiplier, client.config.MaxBackoff)
			continue
		}

		client.setConn(conn)
		stopClose := context.AfterFunc(ctx, func() {
			if err := conn.Close(); err != nil {
				return
			}
		})
		backoff = client.config.InitialBackoff
		attempt = 0
		if client.config.OnConnect != nil {
			client.config.OnConnect()
		}

		readResult := client.readLoop(ctx, conn)
		stopClose()

		client.clearConn(conn)
		client.setState(stream.StateDisconnected)
		if err := conn.Close(); err != nil && readResult.OK {
			readResult = core.Fail(err)
		}
		if client.config.OnDisconnect != nil {
			client.config.OnDisconnect()
		}

		if client.isClosed() || ctx.Err() != nil {
			return core.Ok(nil)
		}
		if readResult.OK {
			attempt = 0
		} else {
			attempt++
		}
		if client.config.MaxRetries > 0 && attempt > client.config.MaxRetries {
			return readResult
		}
		if client.config.OnReconnect != nil {
			client.config.OnReconnect(attempt)
		}
		if r := sleepContext(ctx, backoff); !r.OK {
			return r
		}
		backoff = nextTCPBackoff(backoff, client.config.BackoffMultiplier, client.config.MaxBackoff)
	}
}

// _ = client.Send("vpn:peer-abc123", encryptedPacket)
func (client *ReconnectingTCP) Send(channel string, frame []byte) core.Result {
	if client == nil {
		return core.Fail(core.E("stream.tcp", "nil reconnecting tcp", nil))
	}
	client.writeMutex.Lock()
	defer client.writeMutex.Unlock()

	client.mutex.RLock()
	connection := client.conn
	client.mutex.RUnlock()
	if connection == nil {
		return core.Fail(core.E("stream.tcp", "not connected", nil))
	}
	return writeAll(connection, encodeTCPFrame(channel, frame))
}

//	if client.State() == stream.StateConnected {
//	    _ = client.Send("vpn:peer-abc123", encryptedPacket)
//	}
func (client *ReconnectingTCP) State() stream.ConnectionState {
	if client == nil {
		return stream.StateDisconnected
	}
	client.mutex.RLock()
	defer client.mutex.RUnlock()
	return client.state
}

// _ = client.Close()
func (client *ReconnectingTCP) Close() core.Result {
	if client == nil {
		return core.Ok(nil)
	}
	client.mutex.Lock()
	client.closed = true
	conn := client.conn
	client.conn = nil
	client.state = stream.StateDisconnected
	client.mutex.Unlock()
	if conn != nil {
		return core.ResultOf(nil, conn.Close())
	}
	return core.Ok(nil)
}

func (client *ReconnectingTCP) dial(ctx context.Context) core.Result {
	dialer := &net.Dialer{}
	if client.config.TLS != nil {
		conn, err := dialer.DialContext(ctx, "tcp", client.config.Addr)
		if err != nil {
			return core.Fail(err)
		}
		tlsConn := tls.Client(conn, client.config.TLS)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			if closeErr := conn.Close(); closeErr != nil {
				return core.Fail(core.ErrorJoin(err, closeErr))
			}
			return core.Fail(err)
		}
		return core.Ok(net.Conn(tlsConn))
	}
	conn, err := dialer.DialContext(ctx, "tcp", client.config.Addr)
	return core.ResultOf(conn, err)
}

func (client *ReconnectingTCP) readLoop(ctx context.Context, conn net.Conn) core.Result {
	for {
		select {
		case <-ctx.Done():
			return core.Fail(ctx.Err())
		default:
		}
		frameResult := readTCPFrame(conn, 0, MaxFrameSize)
		if !frameResult.OK {
			return frameResult
		}
		incoming := frameResult.Value.(tcpFrame)
		if client.config.OnMessage != nil {
			client.config.OnMessage(incoming.channel, incoming.frame)
		}
	}
}

func (client *ReconnectingTCP) setConn(conn net.Conn) {
	client.mutex.Lock()
	client.conn = conn
	client.state = stream.StateConnected
	client.mutex.Unlock()
}

func (client *ReconnectingTCP) clearConn(conn net.Conn) {
	client.mutex.Lock()
	if client.conn == conn {
		client.conn = nil
		client.state = stream.StateDisconnected
	}
	client.mutex.Unlock()
}

func (client *ReconnectingTCP) setState(state stream.ConnectionState) {
	client.mutex.Lock()
	client.state = state
	client.mutex.Unlock()
}

func (client *ReconnectingTCP) isClosed() bool {
	client.mutex.RLock()
	defer client.mutex.RUnlock()
	return client.closed
}

func nextTCPBackoff(current time.Duration, multiplier float64, maximum time.Duration) time.Duration {
	next := time.Duration(float64(current) * multiplier)
	if next <= 0 {
		next = current
	}
	if next > maximum {
		return maximum
	}
	return next
}

func sleepContext(ctx context.Context, duration time.Duration) core.Result {
	if duration <= 0 {
		return core.Ok(nil)
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return core.Fail(ctx.Err())
	case <-timer.C:
		return core.Ok(nil)
	}
}

func (client *ReconnectingTCP) writeHandshake(conn net.Conn) core.Result {
	if conn == nil {
		return core.Fail(core.E("stream.tcp", "nil connection", nil))
	}
	if len(client.config.HandshakeFrame) == 0 && client.config.HandshakeChannel == "" {
		return core.Ok(nil)
	}
	return writeAll(conn, encodeTCPFrame(client.config.HandshakeChannel, client.config.HandshakeFrame))
}
