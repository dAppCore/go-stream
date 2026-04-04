// SPDX-License-Identifier: EUPL-1.2

package ws

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"dappco.re/go/core"
	"dappco.re/go/stream"
)

// ReconnectConfig configures the client-side reconnecting WebSocket.
type ReconnectConfig struct {
	URL               string
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64
	MaxRetries        int // 0 = unlimited
	OnConnect         func()
	OnDisconnect      func()
	OnReconnect       func(attempt int)
	OnMessage         func(msg stream.Message)
	Dialer            *websocket.Dialer
	Headers           http.Header
}

// ReconnectingClient is a WebSocket client with automatic reconnection.
type ReconnectingClient struct {
	config ReconnectConfig
	state  stream.ConnectionState

	mu     sync.RWMutex
	conn   *websocket.Conn
	closed bool
}

// NewReconnectingClient creates a reconnecting WebSocket client.
func NewReconnectingClient(config ReconnectConfig) *ReconnectingClient {
	if config.InitialBackoff == 0 {
		config.InitialBackoff = 500 * time.Millisecond
	}
	if config.MaxBackoff == 0 {
		config.MaxBackoff = 30 * time.Second
	}
	if config.BackoffMultiplier <= 0 {
		config.BackoffMultiplier = 2
	}
	return &ReconnectingClient{config: config, state: stream.StateDisconnected}
}

// Connect starts the connection loop. Blocks until ctx is cancelled.
func (rc *ReconnectingClient) Connect(ctx context.Context) error {
	if rc == nil {
		return errors.New("nil reconnecting client")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	dialer := rc.config.Dialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}

	backoff := rc.config.InitialBackoff
	attempt := 0

	for {
		if rc.isClosed() || ctx.Err() != nil {
			return nil
		}

		rc.setState(stream.StateConnecting)

		conn, _, err := dialer.DialContext(ctx, rc.config.URL, rc.config.Headers)
		if err != nil {
			attempt++
			rc.setState(stream.StateDisconnected)
			if rc.config.MaxRetries > 0 && attempt > rc.config.MaxRetries {
				return err
			}
			if rc.config.OnReconnect != nil {
				rc.config.OnReconnect(attempt)
			}
			if err := sleepContext(ctx, backoff); err != nil {
				return err
			}
			backoff = nextBackoff(backoff, rc.config.BackoffMultiplier, rc.config.MaxBackoff)
			continue
		}

		rc.mu.Lock()
		rc.conn = conn
		rc.state = stream.StateConnected
		rc.mu.Unlock()
		if rc.config.OnConnect != nil {
			rc.config.OnConnect()
		}

		readErr := rc.readLoop(ctx, conn)

		rc.mu.Lock()
		if rc.conn == conn {
			rc.conn = nil
		}
		rc.state = stream.StateDisconnected
		rc.mu.Unlock()
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
		if rc.config.OnReconnect != nil {
			rc.config.OnReconnect(attempt)
		}
		if err := sleepContext(ctx, backoff); err != nil {
			return err
		}
		backoff = nextBackoff(backoff, rc.config.BackoffMultiplier, rc.config.MaxBackoff)
	}
}

// Send marshals and sends a message through the WebSocket connection.
func (rc *ReconnectingClient) Send(msg stream.Message) error {
	if rc == nil {
		return errors.New("nil reconnecting client")
	}
	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now().UTC()
	}
	payload := core.JSONMarshal(msg)
	if !payload.OK {
		if err, ok := payload.Value.(error); ok {
			return err
		}
		return core.E("stream.ws", "failed to marshal message", nil)
	}

	rc.mu.RLock()
	conn := rc.conn
	rc.mu.RUnlock()
	if conn == nil {
		return core.E("stream.ws", "not connected", nil)
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if rc.conn == nil {
		return core.E("stream.ws", "not connected", nil)
	}
	return rc.conn.WriteMessage(websocket.TextMessage, payload.Value.([]byte))
}

// State returns the current connection state.
func (rc *ReconnectingClient) State() stream.ConnectionState {
	if rc == nil {
		return stream.StateDisconnected
	}
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.state
}

// Close shuts down the reconnecting client.
func (rc *ReconnectingClient) Close() error {
	if rc == nil {
		return nil
	}
	rc.mu.Lock()
	rc.closed = true
	conn := rc.conn
	rc.conn = nil
	rc.state = stream.StateDisconnected
	rc.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (rc *ReconnectingClient) readLoop(ctx context.Context, conn *websocket.Conn) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
			continue
		}
		var message stream.Message
		if !core.JSONUnmarshal(payload, &message).OK {
			continue
		}
		if rc.config.OnMessage != nil {
			rc.config.OnMessage(message)
		}
	}
}

func (rc *ReconnectingClient) isClosed() bool {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.closed
}

func (rc *ReconnectingClient) setState(state stream.ConnectionState) {
	rc.mu.Lock()
	rc.state = state
	rc.mu.Unlock()
}

func nextBackoff(current time.Duration, multiplier float64, maximum time.Duration) time.Duration {
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
