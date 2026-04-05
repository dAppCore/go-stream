// SPDX-License-Identifier: EUPL-1.2

package ws

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"dappco.re/go/core"
	"dappco.re/go/stream"
)

//	config := ws.ReconnectConfig{
//	    URL: "ws://127.0.0.1:8080/stream/ws",
//	    OnMessage: func(message stream.Message) {
//	        _ = message.Channel
//	    },
//	}
//
// client := ws.NewReconnectingClient(config)
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

// client := ws.NewReconnectingClient(ws.ReconnectConfig{URL: "ws://127.0.0.1:8080/stream/ws"})
// _ = client.Connect(context.Background())
type ReconnectingClient struct {
	config ReconnectConfig
	state  stream.ConnectionState

	mutex  sync.RWMutex
	conn   *websocket.Conn
	closed bool
}

// client := ws.NewReconnectingClient(ws.ReconnectConfig{URL: "ws://localhost:8080/stream/ws"})
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

// client := ws.NewReconnectingClient(ws.ReconnectConfig{URL: "ws://127.0.0.1:8080/stream/ws"})
// err := client.Connect(ctx)
func (client *ReconnectingClient) Connect(ctx context.Context) error {
	if client == nil {
		return core.E("stream.ws", "nil reconnecting client", nil)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	dialer := client.config.Dialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}

	backoff := client.config.InitialBackoff
	attempt := 0

	for {
		if client.isClosed() || ctx.Err() != nil {
			return nil
		}

		client.setState(stream.StateConnecting)

		conn, _, err := dialer.DialContext(ctx, client.config.URL, client.config.Headers)
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
			backoff = nextBackoff(backoff, client.config.BackoffMultiplier, client.config.MaxBackoff)
			continue
		}

		client.mutex.Lock()
		client.conn = conn
		client.state = stream.StateConnected
		client.mutex.Unlock()
		stopClose := context.AfterFunc(ctx, func() {
			_ = conn.Close()
		})
		backoff = client.config.InitialBackoff
		attempt = 0
		if client.config.OnConnect != nil {
			client.config.OnConnect()
		}

		readErr := client.readLoop(ctx, conn)
		stopClose()

		client.mutex.Lock()
		if client.conn == conn {
			client.conn = nil
		}
		client.state = stream.StateDisconnected
		client.mutex.Unlock()
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
		if client.config.OnReconnect != nil {
			client.config.OnReconnect(attempt)
		}
		if err := sleepContext(ctx, backoff); err != nil {
			return err
		}
		backoff = nextBackoff(backoff, client.config.BackoffMultiplier, client.config.MaxBackoff)
	}
}

// _ = client.Send(stream.Message{Type: stream.TypeEvent, Channel: "hashrate", Data: map[string]any{"h": 1234567}})
func (client *ReconnectingClient) Send(msg stream.Message) error {
	if client == nil {
		return core.E("stream.ws", "nil reconnecting client", nil)
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

	client.mutex.RLock()
	conn := client.conn
	client.mutex.RUnlock()
	if conn == nil {
		return core.E("stream.ws", "not connected", nil)
	}
	client.mutex.Lock()
	defer client.mutex.Unlock()
	if client.conn == nil {
		return core.E("stream.ws", "not connected", nil)
	}
	return client.conn.WriteMessage(websocket.TextMessage, payload.Value.([]byte))
}

// state := client.State()
func (client *ReconnectingClient) State() stream.ConnectionState {
	if client == nil {
		return stream.StateDisconnected
	}
	client.mutex.RLock()
	defer client.mutex.RUnlock()
	return client.state
}

// _ = client.Close()
func (client *ReconnectingClient) Close() error {
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

func (client *ReconnectingClient) readLoop(ctx context.Context, conn *websocket.Conn) error {
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
		if client.config.OnMessage != nil {
			client.config.OnMessage(message)
		}
	}
}

func (client *ReconnectingClient) isClosed() bool {
	client.mutex.RLock()
	defer client.mutex.RUnlock()
	return client.closed
}

func (client *ReconnectingClient) setState(state stream.ConnectionState) {
	client.mutex.Lock()
	client.state = state
	client.mutex.Unlock()
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
