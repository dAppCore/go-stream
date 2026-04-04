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
func (rc *ReconnectingTCP) Connect(ctx context.Context) error {
	if rc == nil {
		return core.E("stream.tcp", "nil reconnecting tcp", nil)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	backoff := rc.config.InitialBackoff
	attempt := 0
	for {
		if rc.isClosed() || ctx.Err() != nil {
			return nil
		}

		conn, err := rc.dial(ctx)
		if err != nil {
			attempt++
			if rc.config.MaxRetries > 0 && attempt > rc.config.MaxRetries {
				return err
			}
			if err := sleepContext(ctx, backoff); err != nil {
				return err
			}
			backoff = nextTCPBackoff(backoff, rc.config.BackoffMultiplier, rc.config.MaxBackoff)
			continue
		}

		rc.setConn(conn)
		if rc.config.OnConnect != nil {
			rc.config.OnConnect()
		}

		readErr := rc.readLoop(ctx, conn)

		rc.clearConn(conn)
		_ = conn.Close()
		if rc.config.OnDisconnect != nil {
			rc.config.OnDisconnect()
		}

		if rc.isClosed() || ctx.Err() != nil {
			return nil
		}
		if readErr == nil {
			attempt = 0
		} else {
			attempt++
		}
		if rc.config.MaxRetries > 0 && attempt > rc.config.MaxRetries {
			return readErr
		}
		if err := sleepContext(ctx, backoff); err != nil {
			return err
		}
		backoff = nextTCPBackoff(backoff, rc.config.BackoffMultiplier, rc.config.MaxBackoff)
	}
}

// Send transmits frame on channel through the TCP connection.
func (rc *ReconnectingTCP) Send(channel string, frame []byte) error {
	if rc == nil {
		return core.E("stream.tcp", "nil reconnecting tcp", nil)
	}
	rc.mu.RLock()
	conn := rc.conn
	rc.mu.RUnlock()
	if conn == nil {
		return core.E("stream.tcp", "not connected", nil)
	}
	_, err := conn.Write(encodeFrame(channel, frame))
	return err
}

// Close shuts down the reconnecting client.
func (rc *ReconnectingTCP) Close() error {
	if rc == nil {
		return nil
	}
	rc.mu.Lock()
	rc.closed = true
	conn := rc.conn
	rc.conn = nil
	rc.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (rc *ReconnectingTCP) dial(ctx context.Context) (net.Conn, error) {
	dialer := &net.Dialer{}
	if rc.config.TLS != nil {
		conn, err := dialer.DialContext(ctx, "tcp", rc.config.Addr)
		if err != nil {
			return nil, err
		}
		tlsConn := tls.Client(conn, rc.config.TLS)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, err
		}
		return tlsConn, nil
	}
	return dialer.DialContext(ctx, "tcp", rc.config.Addr)
}

func (rc *ReconnectingTCP) readLoop(ctx context.Context, conn net.Conn) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		channel, frame, err := readFrame(conn, 0)
		if err != nil {
			return err
		}
		if rc.config.OnMessage != nil {
			rc.config.OnMessage(channel, frame)
		}
	}
}

func (rc *ReconnectingTCP) setConn(conn net.Conn) {
	rc.mu.Lock()
	rc.conn = conn
	rc.mu.Unlock()
}

func (rc *ReconnectingTCP) clearConn(conn net.Conn) {
	rc.mu.Lock()
	if rc.conn == conn {
		rc.conn = nil
	}
	rc.mu.Unlock()
}

func (rc *ReconnectingTCP) isClosed() bool {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.closed
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
