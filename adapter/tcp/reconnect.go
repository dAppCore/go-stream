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
func (client *ReconnectingTCP) Connect(ctx context.Context) error {
	if client == nil {
		return core.E("stream.tcp", "nil reconnecting tcp", nil)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	backoff := client.config.InitialBackoff
	attempt := 0
	for {
		if client.isClosed() || ctx.Err() != nil {
			return nil
		}

		client.setState(stream.StateConnecting)
		conn, err := client.dial(ctx)
		if err != nil {
			attempt++
			client.setState(stream.StateDisconnected)
			if client.config.MaxRetries > 0 && attempt > client.config.MaxRetries {
				return err
			}
			if client.config.OnReconnect != nil {
				client.config.OnReconnect(attempt)
			}
			if err := sleepContext(ctx, backoff); err != nil {
				return err
			}
			backoff = nextTCPBackoff(backoff, client.config.BackoffMultiplier, client.config.MaxBackoff)
			continue
		}
		if err := client.writeHandshake(conn); err != nil {
			if closeErr := conn.Close(); closeErr != nil {
				err = core.ErrorJoin(err, closeErr)
			}
			attempt++
			client.setState(stream.StateDisconnected)
			if client.config.MaxRetries > 0 && attempt > client.config.MaxRetries {
				return err
			}
			if client.config.OnReconnect != nil {
				client.config.OnReconnect(attempt)
			}
			if err := sleepContext(ctx, backoff); err != nil {
				return err
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

		readErr := client.readLoop(ctx, conn)
		stopClose()

		client.clearConn(conn)
		client.setState(stream.StateDisconnected)
		if err := conn.Close(); err != nil && readErr == nil {
			readErr = err
		}
		if client.config.OnDisconnect != nil {
			client.config.OnDisconnect()
		}

		if client.isClosed() || ctx.Err() != nil {
			return nil
		}
		if readErr == nil {
			attempt = 0
		} else {
			attempt++
		}
		if client.config.MaxRetries > 0 && attempt > client.config.MaxRetries {
			return readErr
		}
		if client.config.OnReconnect != nil {
			client.config.OnReconnect(attempt)
		}
		if err := sleepContext(ctx, backoff); err != nil {
			return err
		}
		backoff = nextTCPBackoff(backoff, client.config.BackoffMultiplier, client.config.MaxBackoff)
	}
}

// _ = client.Send("vpn:peer-abc123", encryptedPacket)
func (client *ReconnectingTCP) Send(channel string, frame []byte) error {
	if client == nil {
		return core.E("stream.tcp", "nil reconnecting tcp", nil)
	}
	client.writeMutex.Lock()
	defer client.writeMutex.Unlock()

	client.mutex.RLock()
	connection := client.conn
	client.mutex.RUnlock()
	if connection == nil {
		return core.E("stream.tcp", "not connected", nil)
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
func (client *ReconnectingTCP) Close() error {
	if client == nil {
		return nil
	}
	client.mutex.Lock()
	client.closed = true
	conn := client.conn
	client.conn = nil
	client.state = stream.StateDisconnected
	client.mutex.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (client *ReconnectingTCP) dial(ctx context.Context) (net.Conn, error) {
	dialer := &net.Dialer{}
	if client.config.TLS != nil {
		conn, err := dialer.DialContext(ctx, "tcp", client.config.Addr)
		if err != nil {
			return nil, err
		}
		tlsConn := tls.Client(conn, client.config.TLS)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			if closeErr := conn.Close(); closeErr != nil {
				return nil, core.ErrorJoin(err, closeErr)
			}
			return nil, err
		}
		return tlsConn, nil
	}
	return dialer.DialContext(ctx, "tcp", client.config.Addr)
}

func (client *ReconnectingTCP) readLoop(ctx context.Context, conn net.Conn) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		channel, frame, err := readTCPFrame(conn, 0, MaxFrameSize)
		if err != nil {
			return err
		}
		if client.config.OnMessage != nil {
			client.config.OnMessage(channel, frame)
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

func sleepContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (client *ReconnectingTCP) writeHandshake(conn net.Conn) error {
	if conn == nil {
		return core.E("stream.tcp", "nil connection", nil)
	}
	if len(client.config.HandshakeFrame) == 0 && client.config.HandshakeChannel == "" {
		return nil
	}
	return writeAll(conn, encodeTCPFrame(client.config.HandshakeChannel, client.config.HandshakeFrame))
}
