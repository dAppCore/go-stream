// SPDX-License-Identifier: EUPL-1.2

package tcp

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"

	"dappco.re/go/core"
)

// ReconnectConfig configures the client-side reconnecting TCP connection.
type ReconnectConfig struct {
	Addr              string
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64
	MaxRetries        int
	TLS               *tls.Config
	OnConnect         func()
	OnDisconnect      func()
	OnMessage         func(channel string, frame []byte)
}

// ReconnectingTCP connects to a TCP stream endpoint with automatic reconnection.
type ReconnectingTCP struct {
	config ReconnectConfig

	mu     sync.RWMutex
	conn   net.Conn
	closed bool
}

// NewReconnectingTCP creates a reconnecting TCP client.
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
	return &ReconnectingTCP{config: config}
}

// Connect starts the connection loop. Blocks until ctx is cancelled.
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

		conn, err := client.dial(ctx)
		if err != nil {
			attempt++
			if client.config.MaxRetries > 0 && attempt > client.config.MaxRetries {
				return err
			}
			if err := sleepContext(ctx, backoff); err != nil {
				return err
			}
			backoff = nextTCPBackoff(backoff, client.config.BackoffMultiplier, client.config.MaxBackoff)
			continue
		}

		client.setConn(conn)
		backoff = client.config.InitialBackoff
		attempt = 0
		if client.config.OnConnect != nil {
			client.config.OnConnect()
		}

		readErr := client.readLoop(ctx, conn)

		client.clearConn(conn)
		_ = conn.Close()
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
		if err := sleepContext(ctx, backoff); err != nil {
			return err
		}
		backoff = nextTCPBackoff(backoff, client.config.BackoffMultiplier, client.config.MaxBackoff)
	}
}

// Send transmits frame on channel through the TCP connection.
func (client *ReconnectingTCP) Send(channel string, frame []byte) error {
	if client == nil {
		return core.E("stream.tcp", "nil reconnecting tcp", nil)
	}
	client.mu.RLock()
	conn := client.conn
	client.mu.RUnlock()
	if conn == nil {
		return core.E("stream.tcp", "not connected", nil)
	}
	_, err := conn.Write(encodeFrame(channel, frame))
	return err
}

// Close shuts down the reconnecting client.
func (client *ReconnectingTCP) Close() error {
	if client == nil {
		return nil
	}
	client.mu.Lock()
	client.closed = true
	conn := client.conn
	client.conn = nil
	client.mu.Unlock()
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
			_ = conn.Close()
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
		channel, frame, err := readFrame(conn, 0, MaxFrameSize)
		if err != nil {
			return err
		}
		if client.config.OnMessage != nil {
			client.config.OnMessage(channel, frame)
		}
	}
}

func (client *ReconnectingTCP) setConn(conn net.Conn) {
	client.mu.Lock()
	client.conn = conn
	client.mu.Unlock()
}

func (client *ReconnectingTCP) clearConn(conn net.Conn) {
	client.mu.Lock()
	if client.conn == conn {
		client.conn = nil
	}
	client.mu.Unlock()
}

func (client *ReconnectingTCP) isClosed() bool {
	client.mu.RLock()
	defer client.mu.RUnlock()
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
