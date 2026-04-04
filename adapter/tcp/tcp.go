// SPDX-License-Identifier: EUPL-1.2

// Package tcp is the raw TCP transport adapter for stream.Hub.
// Length-prefixed framing over plain or TLS TCP. Used by go-p2p VPN tunnels
// and go-proxy stratum sessions where WebSocket overhead is undesirable.
//
//	adapter := tcp.New(tcp.Config{Addr: ":9000", ConnAuthenticator: auth})
//	adapter.Mount(hub)
//	go adapter.Listen(ctx)
package tcp

import (
	"context"
	"crypto/tls"
	"time"

	"dappco.re/go/core/stream"
)

// MaxFrameSize is the maximum allowed frame size in bytes.
// Enforced at read time to prevent memory exhaustion.
const MaxFrameSize = 65535

// Config configures the TCP adapter.
//
//	// Listen mode (server):
//	cfg := tcp.Config{
//	    Addr:              ":9000",
//	    ConnAuthenticator: myAuth,
//	    TLS:               &tls.Config{Certificates: []tls.Certificate{cert}},
//	}
//
//	// Dial mode (client):
//	cfg := tcp.Config{
//	    Addr: "10.69.69.165:9000",
//	}
type Config struct {
	// Addr is the listen address (server) or dial address (client).
	Addr string

	// ConnAuthenticator validates the handshake frame. When nil, all connections accepted.
	ConnAuthenticator stream.ConnAuthenticator

	// HandshakeTimeout is how long to wait for the first frame from a new connection.
	// Defaults to 5 seconds.
	HandshakeTimeout time.Duration

	// TLS enables TLS when set. For server mode, must have Certificates. For client mode,
	// InsecureSkipVerify may be set for self-signed certs in trusted networks.
	TLS *tls.Config
}

// Adapter is the raw TCP transport adapter.
//
//	adapter := tcp.New(tcp.Config{Addr: ":9000", ConnAuthenticator: auth})
//	adapter.Mount(hub)
//	go adapter.Listen(ctx)
type Adapter struct {
	hub    *stream.Hub
	config Config
}

// New creates a TCP adapter. Call Mount before Listen or Dial.
//
//	adapter := tcp.New(tcp.Config{Addr: ":9000"})
func New(config Config) *Adapter {
	return nil
}

// Mount wires the adapter to a hub.
//
//	adapter.Mount(hub)
func (a *Adapter) Mount(hub *stream.Hub) {
}

// Listen starts the TCP accept loop. Blocks until ctx cancelled.
// Each accepted connection runs readPump and writePump goroutines.
//
//	go adapter.Listen(ctx)
func (a *Adapter) Listen(ctx context.Context) error {
	return nil
}

// Dial connects to a remote TCP stream endpoint. Returns a Peer that can send/receive.
// Reconnection is handled by ReconnectingTCP, not by Dial itself.
//
//	peer, err := adapter.Dial(ctx, hub)
func (a *Adapter) Dial(ctx context.Context, hub *stream.Hub) (*stream.Peer, error) {
	return nil, nil
}
