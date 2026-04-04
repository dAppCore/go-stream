// SPDX-License-Identifier: EUPL-1.2

// Package tcp is the raw TCP transport adapter for stream.Hub.
// Length-prefixed framing over plain or TLS TCP. Used by go-p2p VPN tunnels
// and go-proxy stratum sessions where WebSocket overhead is undesirable.
package tcp

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"dappco.re/go/core"
	"dappco.re/go/stream"
)

// MaxFrameSize is the maximum allowed frame size in bytes.
const MaxFrameSize = 65535

// Config configures the TCP adapter.
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

// New creates a TCP adapter. Call Mount before Listen or Dial.
func New(config Config) *Adapter {
	if config.HandshakeTimeout == 0 {
		config.HandshakeTimeout = 5 * time.Second
	}
	return &Adapter{config: config}
}

// Mount wires the adapter to a hub.
func (a *Adapter) Mount(hub *stream.Hub) {
	a.hub = hub
}

// Listen starts the TCP accept loop. Blocks until ctx cancelled.
func (a *Adapter) Listen(ctx context.Context) error {
	if a == nil {
		return errors.New("nil adapter")
	}
	if a.hub == nil {
		return core.E("stream.tcp", "stream hub not mounted", nil)
	}
	if a.config.Addr == "" {
		return core.E("stream.tcp", "empty address", nil)
	}

	listener, err := a.listen()
	if err != nil {
		return err
	}
	defer listener.Close()

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
		go a.handleConn(ctx, conn, a.hub)
	}
}

// Dial connects to a remote TCP stream endpoint. Returns a Peer that can send/receive.
func (a *Adapter) Dial(ctx context.Context, hub *stream.Hub) (*stream.Peer, error) {
	if a == nil {
		return nil, errors.New("nil adapter")
	}
	if hub == nil {
		hub = a.hub
	}
	if hub == nil {
		return nil, core.E("stream.tcp", "stream hub not mounted", nil)
	}
	conn, err := a.dial(ctx)
	if err != nil {
		return nil, err
	}
	_, _ = conn.Write(encodeFrame("", nil))
	peer := stream.NewPeer("tcp")
	_ = hub.AddPeer(peer)
	_ = hub.SubscribePeer(peer, "*")
	go a.pipePeer(ctx, conn, peer, hub)
	return peer, nil
}

func (a *Adapter) listen() (net.Listener, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.listener != nil {
		return a.listener, nil
	}
	var (
		listener net.Listener
		err      error
	)
	if a.config.TLS != nil {
		listener, err = tls.Listen("tcp", a.config.Addr, a.config.TLS)
	} else {
		listener, err = net.Listen("tcp", a.config.Addr)
	}
	if err != nil {
		return nil, err
	}
	a.listener = listener
	return listener, nil
}

func (a *Adapter) dial(ctx context.Context) (net.Conn, error) {
	dialer := &net.Dialer{}
	if a.config.TLS != nil {
		conn, err := dialer.DialContext(ctx, "tcp", a.config.Addr)
		if err != nil {
			return nil, err
		}
		tlsConn := tls.Client(conn, a.config.TLS)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, err
		}
		return tlsConn, nil
	}
	return dialer.DialContext(ctx, "tcp", a.config.Addr)
}

func (a *Adapter) handleConn(ctx context.Context, conn net.Conn, hub *stream.Hub) {
	defer conn.Close()

	_, handshake, err := readFrame(conn, a.config.HandshakeTimeout)
	if err != nil {
		return
	}

	if auth := a.config.ConnAuthenticator; auth != nil {
		result := auth.AuthenticateConn(handshake)
		if !result.Valid {
			return
		}
	}

	peer := stream.NewPeer("tcp")
	_ = hub.AddPeer(peer)
	_ = hub.SubscribePeer(peer, "*")
	defer hub.RemovePeer(peer)

	go a.writePump(ctx, conn, peer)

	for {
		channel, frame, err := readFrame(conn, 0)
		if err != nil {
			return
		}
		if channel == "" {
			_ = hub.Broadcast(frame)
			continue
		}
		_ = hub.Publish(channel, frame)
	}
}

func (a *Adapter) pipePeer(ctx context.Context, conn net.Conn, peer *stream.Peer, hub *stream.Hub) {
	defer conn.Close()
	go a.writePump(ctx, conn, peer)
	for {
		channel, frame, err := readFrame(conn, 0)
		if err != nil {
			hub.RemovePeer(peer)
			return
		}
		if channel == "" {
			_ = hub.Broadcast(frame)
			continue
		}
		_ = hub.Publish(channel, frame)
	}
}

func (a *Adapter) writePump(ctx context.Context, conn net.Conn, peer *stream.Peer) {
	for {
		select {
		case <-ctx.Done():
			return
		case frame, ok := <-peer.SendQueue():
			if !ok {
				return
			}
			if _, err := conn.Write(frame); err != nil {
				return
			}
		}
	}
}

func readFrame(conn net.Conn, timeout time.Duration) (string, []byte, error) {
	if timeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(timeout))
	}
	var length uint32
	if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return "", nil, stream.ErrHandshakeTimeout
		}
		return "", nil, err
	}
	if length > MaxFrameSize {
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
func isClosedNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	return false
}
