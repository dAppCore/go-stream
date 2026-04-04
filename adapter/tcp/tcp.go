// SPDX-License-Identifier: EUPL-1.2

// Package tcp is the raw TCP transport adapter for stream.Hub.
// Length-prefixed framing over plain or TLS TCP. Used by go-p2p VPN tunnels
// and go-proxy stratum sessions where WebSocket overhead is undesirable.
package tcp

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"

	"dappco.re/go/core"
	"dappco.re/go/stream"
)

// MaxFrameSize is the maximum allowed frame size in bytes.
const MaxFrameSize = 65535

const maxHandshakeFrameSize = 4 << 10

// Config configures the TCP adapter.
//
//	config := tcp.Config{Addr: ":9000", ConnAuthenticator: auth}
//	adapter := tcp.New(config)
type Config struct {
	Addr              string
	ConnAuthenticator stream.ConnAuthenticator
	HandshakeTimeout  time.Duration
	TLS               *tls.Config
}

// Adapter is the raw TCP transport adapter.
type Adapter struct {
	hub    *stream.Hub
	config Config

	mu       sync.Mutex
	listener net.Listener
}

// New creates a TCP adapter.
//
//	adapter := tcp.New(tcp.Config{Addr: ":9000", ConnAuthenticator: auth})
func New(config Config) *Adapter {
	if config.HandshakeTimeout == 0 {
		config.HandshakeTimeout = 5 * time.Second
	}
	return &Adapter{config: config}
}

// Mount wires the adapter to a hub.
//
//	adapter.Mount(hub)
func (adapter *Adapter) Mount(hub *stream.Hub) {
	adapter.hub = hub
}

// Listen starts the TCP accept loop. Blocks until ctx cancelled.
//
//	go adapter.Listen(ctx)
func (adapter *Adapter) Listen(ctx context.Context) error {
	if adapter == nil {
		return core.E("stream.tcp", "nil adapter", nil)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if adapter.hub == nil {
		return core.E("stream.tcp", "stream hub not mounted", nil)
	}
	if adapter.config.Addr == "" {
		return core.E("stream.tcp", "empty address", nil)
	}

	listener, err := adapter.listen()
	if err != nil {
		return err
	}
	defer func() {
		_ = listener.Close()
		adapter.mu.Lock()
		if adapter.listener == listener {
			adapter.listener = nil
		}
		adapter.mu.Unlock()
	}()

	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if isClosedNetworkError(err) {
				return nil
			}
			return err
		}
		go adapter.handleConn(ctx, conn, adapter.hub)
	}
}

// Dial connects to a remote TCP stream endpoint.
//
//	peer, err := adapter.Dial(ctx, hub)
func (adapter *Adapter) Dial(ctx context.Context, hub *stream.Hub) (*stream.Peer, error) {
	if adapter == nil {
		return nil, core.E("stream.tcp", "nil adapter", nil)
	}
	if hub == nil {
		hub = adapter.hub
	}
	if hub == nil {
		return nil, core.E("stream.tcp", "stream hub not mounted", nil)
	}
	conn, err := adapter.dial(ctx)
	if err != nil {
		return nil, err
	}
	peer := stream.NewPeer("tcp")
	peer.SetCloseHook(func() {
		_ = conn.Close()
	})
	_ = hub.AddPeer(peer)
	_ = hub.SubscribePeer(peer, "*")
	go adapter.pipePeer(ctx, conn, peer, hub)
	return peer, nil
}

func (adapter *Adapter) listen() (net.Listener, error) {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if adapter.listener != nil {
		return adapter.listener, nil
	}
	var (
		listener net.Listener
		err      error
	)
	if adapter.config.TLS != nil {
		listener, err = tls.Listen("tcp", adapter.config.Addr, adapter.config.TLS)
	} else {
		listener, err = net.Listen("tcp", adapter.config.Addr)
	}
	if err != nil {
		return nil, err
	}
	adapter.listener = listener
	return listener, nil
}

func (adapter *Adapter) dial(ctx context.Context) (net.Conn, error) {
	dialer := &net.Dialer{}
	if adapter.config.TLS != nil {
		conn, err := dialer.DialContext(ctx, "tcp", adapter.config.Addr)
		if err != nil {
			return nil, err
		}
		tlsConn := tls.Client(conn, adapter.config.TLS)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, err
		}
		return tlsConn, nil
	}
	return dialer.DialContext(ctx, "tcp", adapter.config.Addr)
}

func (adapter *Adapter) handleConn(ctx context.Context, conn net.Conn, hub *stream.Hub) {
	defer conn.Close()

	channel, frame, err := readFrame(conn, adapter.config.HandshakeTimeout, maxHandshakeFrameSize)
	if err != nil {
		return
	}

	authResult := stream.AuthResult{Valid: true}
	if auth := adapter.config.ConnAuthenticator; auth != nil {
		authResult = auth.AuthenticateConn(frame)
		if !authResult.Valid {
			return
		}
	}

	peer := stream.NewPeer("tcp")
	peer.UserID = authResult.UserID
	peer.Claims = authResult.Claims
	peer.SetCloseHook(func() {
		_ = conn.Close()
	})
	_ = hub.AddPeer(peer)
	_ = hub.SubscribePeer(peer, "*")
	defer hub.RemovePeer(peer)

	go adapter.writePump(ctx, conn, peer, hub.Config().WriteTimeout)

	if auth := adapter.config.ConnAuthenticator; auth == nil {
		dispatchFrame(hub, peer, channel, frame)
	}

	for {
		channel, frame, err := readFrame(conn, 0, MaxFrameSize)
		if err != nil {
			return
		}
		if channel == "" {
			_ = hub.BroadcastFromPeer(peer, frame)
			continue
		}
		_ = hub.PublishFromPeer(peer, channel, frame)
	}
}

func dispatchFrame(hub *stream.Hub, peer *stream.Peer, channel string, frame []byte) {
	if channel == "" {
		_ = hub.BroadcastFromPeer(peer, frame)
		return
	}
	_ = hub.PublishFromPeer(peer, channel, frame)
}

func (adapter *Adapter) pipePeer(ctx context.Context, conn net.Conn, peer *stream.Peer, hub *stream.Hub) {
	defer conn.Close()
	go adapter.writePump(ctx, conn, peer, hub.Config().WriteTimeout)
	for {
		channel, frame, err := readFrame(conn, 0, MaxFrameSize)
		if err != nil {
			hub.RemovePeer(peer)
			return
		}
		dispatchFrame(hub, peer, channel, frame)
	}
}

func (adapter *Adapter) writePump(ctx context.Context, conn net.Conn, peer *stream.Peer, writeTimeout time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case frame, ok := <-peer.SendQueue():
			if !ok {
				return
			}
			if writeTimeout > 0 {
				_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			}
			if err := writeFull(conn, frame); err != nil {
				return
			}
		}
	}
}

func readFrame(conn net.Conn, timeout time.Duration, maxFrameSize int) (string, []byte, error) {
	if timeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(timeout))
	} else {
		_ = conn.SetReadDeadline(time.Time{})
	}
	var length uint32
	if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return "", nil, stream.ErrHandshakeTimeout
		}
		return "", nil, err
	}
	if maxFrameSize > 0 && length > uint32(maxFrameSize) {
		return "", nil, core.E("stream.tcp", "frame too large", nil)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return "", nil, err
	}
	if len(payload) < 4 {
		return "", nil, core.E("stream.tcp", "invalid frame", nil)
	}
	channelLength := binary.BigEndian.Uint32(payload[:4])
	if int(channelLength) > len(payload)-4 {
		return "", nil, core.E("stream.tcp", "invalid frame", nil)
	}
	channel := string(payload[4 : 4+int(channelLength)])
	frame := append([]byte(nil), payload[4+int(channelLength):]...)
	return channel, frame, nil
}

func encodeFrame(channel string, frame []byte) []byte {
	channelBytes := []byte(channel)
	payloadLength := uint32(4 + len(channelBytes) + len(frame))
	buffer := make([]byte, 4+payloadLength)
	binary.BigEndian.PutUint32(buffer[:4], payloadLength)
	binary.BigEndian.PutUint32(buffer[4:8], uint32(len(channelBytes)))
	copy(buffer[8:], channelBytes)
	copy(buffer[8+len(channelBytes):], frame)
	return buffer
}

func writeFull(conn net.Conn, payload []byte) error {
	for len(payload) > 0 {
		written, err := conn.Write(payload)
		if err != nil {
			return err
		}
		if written <= 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}
func isClosedNetworkError(err error) bool {
	if err == nil {
		return false
	}
	return err == net.ErrClosed
}
