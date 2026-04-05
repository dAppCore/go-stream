// SPDX-License-Identifier: EUPL-1.2

// Package tcp carries hub frames over raw TCP.
//
//	adapter := tcp.New(tcp.Config{Addr: ":9000"})
//	adapter.Mount(hub)
//	go adapter.Listen(ctx)
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

//	config := tcp.Config{
//	    Addr:              ":9000",
//	    ConnAuthenticator: auth,
//	}
type Config struct {
	// tcp.New(tcp.Config{Addr: ":9000"})
	Addr string

	// tcp.New(tcp.Config{ConnAuthenticator: auth})
	ConnAuthenticator stream.ConnAuthenticator

	// tcp.New(tcp.Config{HandshakeFrame: []byte("trusted")})
	HandshakeFrame []byte

	// tcp.New(tcp.Config{HandshakeChannel: "auth"})
	HandshakeChannel string

	// tcp.New(tcp.Config{HandshakeTimeout: 5 * time.Second})
	HandshakeTimeout time.Duration

	// tcp.New(tcp.Config{TLS: &tls.Config{}})
	TLS *tls.Config
}

// adapter := tcp.New(tcp.Config{Addr: ":9000", ConnAuthenticator: auth})
type Adapter struct {
	hub    *stream.Hub
	config Config

	mutex    sync.Mutex
	listener net.Listener
}

// adapter := tcp.New(tcp.Config{Addr: ":9000", ConnAuthenticator: auth})
func New(config Config) *Adapter {
	if config.HandshakeTimeout == 0 {
		config.HandshakeTimeout = 5 * time.Second
	}
	return &Adapter{config: config}
}

// adapter.Mount(hub)
func (adapter *Adapter) Mount(hub *stream.Hub) {
	adapter.hub = hub
}

// go adapter.Listen(ctx)
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
		adapter.mutex.Lock()
		if adapter.listener == listener {
			adapter.listener = nil
		}
		adapter.mutex.Unlock()
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

// peer, err := adapter.Dial(ctx, hub)
func (adapter *Adapter) Dial(ctx context.Context, hub *stream.Hub) (*stream.Peer, error) {
	if adapter == nil {
		return nil, core.E("stream.tcp", "nil adapter", nil)
	}
	if ctx == nil {
		ctx = context.Background()
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
	if err := adapter.writeHandshake(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	peer := stream.NewPeer("tcp")
	peer.SetCloseHook(func() {
		_ = conn.Close()
	})
	if !hub.Running() {
		_ = conn.Close()
		return nil, stream.ErrHubNotRunning
	}
	if err := hub.AddPeer(peer); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := hub.SubscribePeer(peer, "*"); err != nil {
		hub.RemovePeer(peer)
		_ = conn.Close()
		return nil, err
	}
	go adapter.pipePeer(ctx, conn, peer, hub)
	return peer, nil
}

func (adapter *Adapter) listen() (net.Listener, error) {
	adapter.mutex.Lock()
	defer adapter.mutex.Unlock()
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
	if ctx == nil {
		ctx = context.Background()
	}
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
	stopClose := context.AfterFunc(ctx, func() {
		_ = conn.Close()
	})
	defer stopClose()

	handshakeMaxSize := MaxFrameSize
	if adapter.config.ConnAuthenticator != nil {
		handshakeMaxSize = maxHandshakeFrameSize
	}
	channel, frame, err := readFrame(conn, adapter.config.HandshakeTimeout, handshakeMaxSize)
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
	if !hub.Running() {
		return
	}
	if err := hub.AddPeer(peer); err != nil {
		return
	}
	if err := hub.SubscribePeer(peer, "*"); err != nil {
		hub.RemovePeer(peer)
		return
	}
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
	stopClose := context.AfterFunc(ctx, func() {
		_ = conn.Close()
	})
	defer stopClose()
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

func (adapter *Adapter) writeHandshake(conn net.Conn) error {
	if conn == nil {
		return core.E("stream.tcp", "nil connection", nil)
	}
	if len(adapter.config.HandshakeFrame) == 0 && adapter.config.HandshakeChannel == "" {
		return nil
	}
	return writeFull(conn, encodeFrame(adapter.config.HandshakeChannel, adapter.config.HandshakeFrame))
}

func isClosedNetworkError(err error) bool {
	if err == nil {
		return false
	}
	return err == net.ErrClosed
}
