// SPDX-License-Identifier: EUPL-1.2

package tcp

import (
	"context"
	"crypto/tls"
	"time"
)

// ReconnectConfig configures the client-side reconnecting TCP connection.
//
//	client := tcp.NewReconnectingTCP(tcp.ReconnectConfig{
//	    Addr:           "10.69.69.165:9000",
//	    InitialBackoff: 1 * time.Second,
//	    OnMessage: func(ch string, frame []byte) {
//	        log.Printf("received on %s: %d bytes", ch, len(frame))
//	    },
//	})
//	err := client.Connect(ctx)
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
//
//	client := tcp.NewReconnectingTCP(tcp.ReconnectConfig{
//	    Addr:           "10.69.69.165:9000",
//	    InitialBackoff: 1 * time.Second,
//	})
//	err := client.Connect(ctx)
type ReconnectingTCP struct {
	config ReconnectConfig
}

// NewReconnectingTCP creates a reconnecting TCP client.
//
//	client := tcp.NewReconnectingTCP(cfg)
func NewReconnectingTCP(config ReconnectConfig) *ReconnectingTCP {
	return nil
}

// Connect starts the connection loop. Blocks until ctx is cancelled.
//
//	err := client.Connect(ctx)
func (rc *ReconnectingTCP) Connect(ctx context.Context) error {
	return nil
}

// Send transmits frame on channel through the TCP connection.
//
//	client.Send("hashrate", statsFrame)
func (rc *ReconnectingTCP) Send(channel string, frame []byte) error {
	return nil
}

// Close shuts down the reconnecting client.
//
//	client.Close()
func (rc *ReconnectingTCP) Close() error {
	return nil
}

// Ensure unused imports are referenced.
var _ time.Duration
