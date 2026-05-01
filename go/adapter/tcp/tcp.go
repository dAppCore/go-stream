// SPDX-License-Identifier: EUPL-1.2

// adapter := tcp.New(tcp.Config{Addr: ":9000"})
// adapter.Mount(hub)
// go adapter.Listen(ctx)
package tcp

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"

	"dappco.re/go"
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

type tcpFrame struct {
	channel string
	frame   []byte
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
func (adapter *Adapter) Listen(ctx context.Context) core.Result {
	if adapter == nil {
		return core.Fail(core.E("stream.tcp", "nil adapter", nil))
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if adapter.hub == nil {
		return core.Fail(core.E("stream.tcp", "stream hub not mounted", nil))
	}
	if adapter.config.Addr == "" {
		return core.Fail(core.E("stream.tcp", "empty address", nil))
	}

	listenerResult := adapter.listen()
	if !listenerResult.OK {
		return listenerResult
	}
	listener := listenerResult.Value.(net.Listener)
	defer func() {
		if err := listener.Close(); err != nil {
			return
		}
		adapter.mutex.Lock()
		if adapter.listener == listener {
			adapter.listener = nil
		}
		adapter.mutex.Unlock()
	}()

	go func() {
		<-ctx.Done()
		if err := listener.Close(); err != nil {
			return
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return core.Ok(nil)
			}
			if isClosedNetworkError(err) {
				return core.Ok(nil)
			}
			return core.Fail(err)
		}
		go adapter.handleConn(ctx, conn, adapter.hub)
	}
}

// peer, err := adapter.Dial(ctx, hub)
func (adapter *Adapter) Dial(ctx context.Context, hub *stream.Hub) core.Result {
	if adapter == nil {
		return core.Fail(core.E("stream.tcp", "nil adapter", nil))
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if hub == nil {
		hub = adapter.hub
	}
	if hub == nil {
		return core.Fail(core.E("stream.tcp", "stream hub not mounted", nil))
	}
	dialResult := adapter.dial(ctx)
	if !dialResult.OK {
		return dialResult
	}
	conn := dialResult.Value.(net.Conn)
	if r := adapter.writeHandshake(conn); !r.OK {
		err := r.Value.(error)
		if closeErr := conn.Close(); closeErr != nil {
			return core.Fail(core.ErrorJoin(err, closeErr))
		}
		return r
	}
	peer := stream.NewPeer("tcp")
	peer.SetCloseHook(func() {
		if err := conn.Close(); err != nil {
			return
		}
	})
	if !hub.Running() {
		if err := conn.Close(); err != nil {
			return core.Fail(err)
		}
		return core.Fail(stream.ErrHubNotRunning)
	}
	if r := hub.AddPeer(peer); !r.OK {
		err := r.Value.(error)
		if closeErr := conn.Close(); closeErr != nil {
			return core.Fail(core.ErrorJoin(err, closeErr))
		}
		return r
	}
	if r := hub.SubscribePeer(peer, "*"); !r.OK {
		err := r.Value.(error)
		hub.RemovePeer(peer)
		if closeErr := conn.Close(); closeErr != nil {
			return core.Fail(core.ErrorJoin(err, closeErr))
		}
		return r
	}
	go adapter.pipePeer(ctx, conn, peer, hub)
	return core.Ok(peer)
}

func (adapter *Adapter) listen() core.Result {
	adapter.mutex.Lock()
	defer adapter.mutex.Unlock()
	if adapter.listener != nil {
		return core.Ok(adapter.listener)
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
		return core.Fail(err)
	}
	adapter.listener = listener
	return core.Ok(listener)
}

func (adapter *Adapter) dial(ctx context.Context) core.Result {
	if ctx == nil {
		ctx = context.Background()
	}
	dialer := &net.Dialer{}
	if adapter.config.TLS != nil {
		conn, err := dialer.DialContext(ctx, "tcp", adapter.config.Addr)
		if err != nil {
			return core.Fail(err)
		}
		tlsConn := tls.Client(conn, adapter.config.TLS)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			if closeErr := conn.Close(); closeErr != nil {
				return core.Fail(core.ErrorJoin(err, closeErr))
			}
			return core.Fail(err)
		}
		return core.Ok(net.Conn(tlsConn))
	}
	conn, err := dialer.DialContext(ctx, "tcp", adapter.config.Addr)
	return core.ResultOf(conn, err)
}

func (adapter *Adapter) handleConn(ctx context.Context, conn net.Conn, hub *stream.Hub) {
	defer conn.Close()
	stopClose := context.AfterFunc(ctx, func() {
		if err := conn.Close(); err != nil {
			return
		}
	})
	defer stopClose()

	handshakeMaxSize := MaxFrameSize
	if adapter.config.ConnAuthenticator != nil {
		handshakeMaxSize = maxHandshakeFrameSize
	}
	frameResult := readTCPFrame(conn, adapter.config.HandshakeTimeout, handshakeMaxSize)
	if !frameResult.OK {
		return
	}
	firstFrame := frameResult.Value.(tcpFrame)

	authResult := stream.AuthResult{Valid: true}
	if auth := adapter.config.ConnAuthenticator; auth != nil {
		authResult = auth.AuthenticateConn(firstFrame.frame)
		if !authResult.Valid {
			return
		}
	}

	peer := stream.NewPeer("tcp")
	peer.UserID = authResult.UserID
	if authResult.Claims != nil {
		peer.Claims = authResult.Claims
	}
	peer.SetCloseHook(func() {
		if err := conn.Close(); err != nil {
			return
		}
	})
	if !hub.Running() {
		return
	}
	if r := hub.AddPeer(peer); !r.OK {
		return
	}
	if r := hub.SubscribePeer(peer, "*"); !r.OK {
		hub.RemovePeer(peer)
		return
	}
	defer hub.RemovePeer(peer)

	go adapter.writePump(ctx, conn, peer, hub.Config().WriteTimeout)

	if auth := adapter.config.ConnAuthenticator; auth == nil {
		if r := dispatchTCPFrame(hub, peer, firstFrame.channel, firstFrame.frame); !r.OK {
			return
		}
	}

	for {
		frameResult := readTCPFrame(conn, 0, MaxFrameSize)
		if !frameResult.OK {
			return
		}
		incoming := frameResult.Value.(tcpFrame)
		if incoming.channel == "" {
			if r := hub.BroadcastFromPeer(peer, incoming.frame); !r.OK {
				return
			}
			continue
		}
		if r := hub.PublishFromPeer(peer, incoming.channel, incoming.frame); !r.OK {
			return
		}
	}
}

func dispatchTCPFrame(hub *stream.Hub, peer *stream.Peer, channel string, frame []byte) core.Result {
	if channel == "" {
		return hub.BroadcastFromPeer(peer, frame)
	}
	return hub.PublishFromPeer(peer, channel, frame)
}

func (adapter *Adapter) pipePeer(ctx context.Context, conn net.Conn, peer *stream.Peer, hub *stream.Hub) {
	defer conn.Close()
	stopClose := context.AfterFunc(ctx, func() {
		if err := conn.Close(); err != nil {
			return
		}
	})
	defer stopClose()
	go adapter.writePump(ctx, conn, peer, hub.Config().WriteTimeout)
	for {
		frameResult := readTCPFrame(conn, 0, MaxFrameSize)
		if !frameResult.OK {
			hub.RemovePeer(peer)
			return
		}
		incoming := frameResult.Value.(tcpFrame)
		if r := dispatchTCPFrame(hub, peer, incoming.channel, incoming.frame); !r.OK {
			hub.RemovePeer(peer)
			return
		}
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
				if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
					return
				}
			}
			if r := writeAll(conn, frame); !r.OK {
				return
			}
		}
	}
}

func readTCPFrame(conn net.Conn, timeout time.Duration, maxFrameSize int) core.Result {
	if timeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return core.Fail(err)
		}
	} else {
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			return core.Fail(err)
		}
	}
	var length uint32
	if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return core.Fail(stream.ErrHandshakeTimeout)
		}
		return core.Fail(err)
	}
	if maxFrameSize > 0 && length > uint32(maxFrameSize) {
		return core.Fail(core.E("stream.tcp", "frame too large", nil))
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return core.Fail(err)
	}
	if len(payload) < 4 {
		return core.Fail(core.E("stream.tcp", "invalid frame", nil))
	}
	channelLength := binary.BigEndian.Uint32(payload[:4])
	if int(channelLength) > len(payload)-4 {
		return core.Fail(core.E("stream.tcp", "invalid frame", nil))
	}
	channel := string(payload[4 : 4+int(channelLength)])
	frame := append([]byte(nil), payload[4+int(channelLength):]...)
	return core.Ok(tcpFrame{channel: channel, frame: frame})
}

func encodeTCPFrame(channel string, frame []byte) []byte {
	channelBytes := []byte(channel)
	payloadLength := uint32(4 + len(channelBytes) + len(frame))
	buffer := make([]byte, 4+payloadLength)
	binary.BigEndian.PutUint32(buffer[:4], payloadLength)
	binary.BigEndian.PutUint32(buffer[4:8], uint32(len(channelBytes)))
	copy(buffer[8:], channelBytes)
	copy(buffer[8+len(channelBytes):], frame)
	return buffer
}

func writeAll(conn net.Conn, payload []byte) core.Result {
	for len(payload) > 0 {
		written, err := conn.Write(payload)
		if err != nil {
			return core.Fail(err)
		}
		if written <= 0 {
			return core.Fail(io.ErrShortWrite)
		}
		payload = payload[written:]
	}
	return core.Ok(nil)
}

func (adapter *Adapter) writeHandshake(conn net.Conn) core.Result {
	if conn == nil {
		return core.Fail(core.E("stream.tcp", "nil connection", nil))
	}
	if len(adapter.config.HandshakeFrame) == 0 && adapter.config.HandshakeChannel == "" {
		return core.Ok(nil)
	}
	return writeAll(conn, encodeTCPFrame(adapter.config.HandshakeChannel, adapter.config.HandshakeFrame))
}

func isClosedNetworkError(err error) bool {
	if err == nil {
		return false
	}
	return err == net.ErrClosed
}
