---
module: dappco.re/go/core/stream
repo: core/go-stream
lang: go
tier: lib
depends:
  - code/core/go
tags:
  - streaming
  - websocket
  - sse
  - zeromq
  - tcp
  - redis
  - realtime
  - transport
---
# go-stream RFC — Unified Stream Primitive

> An agent should be able to implement this library from this document alone.

**Module:** `dappco.re/go/core/stream`
**Repository:** `core/go-stream`
**Supersedes:** `dappco.re/go/ws` (`core/go-ws`)
**Files:** 18

---

## 1. Overview

go-stream is the transport-agnostic event and data pipe for the CoreGO ecosystem. It
generalises the WebSocket hub from go-ws into a pluggable adapter model so that the
same `Stream` interface works over WebSocket, SSE, Redis pub/sub, ZeroMQ, and raw TCP.

Consumers (`core/api`, `go-pool`, `go-miner`, `go-p2p`, `core/mcp`) call `Stream` —
they never import a specific transport. Transport adapters are wired at startup.

The go-ws `Hub`, `Client`, `RedisBridge`, and `Authenticator` types are preserved with
identical public APIs and re-exported from the `stream/ws` sub-package so existing
callers do not break.

**What each transport is for:**

| Transport | Direction | Primary consumers |
|-----------|-----------|-------------------|
| WebSocket | bidirectional | browser clients, dashboard live updates |
| SSE | server-push only | pool /live_stats, agent events, core/api live endpoints |
| Redis pub/sub | cross-instance | cluster coordination (multi-instance broadcast) |
| ZeroMQ | high-throughput IPC | block notifications from daemon, go-pool → go-proxy |
| Raw TCP | bidirectional framed | VPN tunnels (go-p2p), stratum wire (go-proxy) |

---

## 2. File Map

| File | Package | Purpose |
|------|---------|---------|
| `stream.go` | `stream` | `Stream` interface, `Frame`, `Channel`, `Pipe`, `Envelope` |
| `hub.go` | `stream` | `Hub` — central channel-based broker, transport-agnostic |
| `hub_config.go` | `stream` | `HubConfig`, `ChannelAuthoriser`, `ConnectionState` |
| `auth.go` | `stream` | `Authenticator`, `ConnAuthenticator`, built-in implementations |
| `errors.go` | `stream` | Sentinel errors via `core.E()` |
| `adapter/ws/ws.go` | `ws` | WebSocket adapter — HTTP upgrade, per-client read/write pumps |
| `adapter/sse/sse.go` | `sse` | SSE adapter — `text/event-stream` HTTP handler, per-connection writer |
| `adapter/redis/redis.go` | `redis` | Redis pub/sub bridge (echo-safe, envelope pattern) |
| `adapter/zmq/zmq.go` | `zmq` | ZeroMQ adapter — PUSH/PULL and PUB/SUB socket modes |
| `adapter/tcp/tcp.go` | `tcp` | Raw TCP adapter — length-prefixed framing, dial and listen |
| `adapter/ws/reconnect.go` | `ws` | `ReconnectingClient` — client-side reconnecting WebSocket |
| `adapter/tcp/reconnect.go` | `tcp` | `ReconnectingTCP` — client-side reconnecting TCP with backoff |
| `message.go` | `stream` | `Message`, `MessageType` constants (process_output, event, error, …) |
| `stats.go` | `stream` | `HubStats`, per-channel subscriber counts |
| `hub_test.go` | `stream` | Hub unit tests |
| `adapter/ws/ws_test.go` | `ws` | WebSocket adapter tests |
| `adapter/sse/sse_test.go` | `sse` | SSE adapter tests |
| `adapter/redis/redis_test.go` | `redis` | Redis bridge tests |

---

## 3. Data Flow

### Hub-centric model (WebSocket and SSE)

```
HTTP request
  → Adapter.ServeHTTP() / Adapter.Handler()
      → auth check (Authenticator.Authenticate(r))
          → 401 if denied
      → conn upgrade (WS) or response header flush (SSE)
      → Peer created: {id, send chan []byte, subscriptions}
      → Hub.register <- peer

Hub.Run(ctx) select loop:
  case register:   add to hub.peers map, call OnConnect
  case unregister: remove, close send chan, call OnDisconnect
  case broadcast:  range hub.peers → trySend(peer.send, frame)

Hub.SendToChannel(ch, frame):
  hub.channels[ch] → range subscribers → trySend(peer.send, frame)

WS peer writePump: drain peer.send → conn.WriteMessage
SSE peer writePump: drain peer.send → fmt-free write "data: {frame}\n\n"
```

### Cross-instance (Redis bridge)

```
Publish(channel, data):
  → RedisBridge.Publish(prefix+":channel:"+channel, Envelope{sourceID, data})
  → Redis pub/sub

RedisBridge.listen():
  → receive Envelope from Redis
  → drop if envelope.SourceID == self.sourceID   (echo prevention)
  → hub.SendToChannel(channel, frame)             (local delivery)
```

### IPC / daemon events (ZMQ)

```
Daemon → ZMQ PUSH socket → go-pool ZMQ PULL listener
  → decode block template bytes
  → hub.Broadcast(frame)                          (all subscribers)
  → hub.SendToChannel("block", frame)             (channel subscribers)
```

### VPN / stratum (Raw TCP)

```
TCPAdapter.Listen(addr):
  → net.Listen → accept loop
  → conn auth: ConnAuthenticator.AuthenticateConn(handshake []byte)
  → Peer{send chan, conn}
  → Hub.register <- peer
  → readPump: length-prefix decode → hub dispatch
  → writePump: drain send → length-prefix encode → conn.Write
```

---

## 4. Stream Interface

```go
// Stream is the transport-agnostic event and data pipe.
// Consumers never import a specific adapter — they call Stream.
//
//   // Wire at startup:
//   hub := stream.NewHub()
//   ws.Mount(hub, ws.Config{...})
//
//   // At call site:
//   var s stream.Stream = hub
//   s.Publish("hashrate", frame)
//   s.Subscribe("block", handler)
type Stream interface {
    // Publish sends frame to all subscribers of channel.
    // Returns core.E if the hub is not running.
    //
    //   hub.Publish("hashrate", []byte(`{"h":123456}`))
    Publish(channel string, frame []byte) error

    // Subscribe registers handler for all frames arriving on channel.
    // Returns an unsubscribe function. Safe to call from multiple goroutines.
    //
    //   unsub := hub.Subscribe("block", func(f []byte) { ... })
    //   defer unsub()
    Subscribe(channel string, handler func([]byte)) func()

    // Broadcast sends frame to every connected peer regardless of subscriptions.
    //
    //   hub.Broadcast([]byte(`{"type":"shutdown"}`))
    Broadcast(frame []byte) error

    // Pipe connects this stream to dst: every frame published here is forwarded to dst.
    // Returns a stop function. Useful for bridging two hubs or two transport adapters.
    //
    //   stop := hub.Pipe(remoteHub)
    //   defer stop()
    Pipe(dst Stream) func()

    // Stats returns a snapshot of current hub state.
    //
    //   s := hub.Stats()
    //   core.Print("stream", "peers=%d channels=%d", s.Peers, s.Channels)
    Stats() HubStats
}
```

---

## 5. Hub

```go
// Hub is the central channel-based broker. Transport adapters register peers into
// the hub; the hub serialises all state mutations through Go channels.
//
//   hub := stream.NewHub()
//   go hub.Run(ctx)
//
//   // Mount adapters
//   wsAdapter := ws.New(ws.Config{Authenticator: auth})
//   wsAdapter.Mount(hub)
//   http.Handle("/stream/ws", wsAdapter.Handler())
//
//   sse := sse.New()
//   sse.Mount(hub)
//   http.Handle("/stream/events", sse.Handler())
type Hub struct {
    peers      map[*Peer]bool
    broadcast  chan []byte
    register   chan *Peer
    unregister chan *Peer
    channels   map[string]map[*Peer]bool
    handlers   map[string][]func([]byte)  // Subscribe() callbacks
    config     HubConfig
    done       chan struct{}
    doneOnce   sync.Once
    running    bool
    mu         sync.RWMutex
}

// NewHub creates a hub with default configuration.
//
//   hub := stream.NewHub()
//   go hub.Run(ctx)
func NewHub() *Hub {}

// NewHubWithConfig creates a hub with the given configuration.
//
//   hub := stream.NewHubWithConfig(stream.HubConfig{
//       HeartbeatInterval: 30 * time.Second,
//       OnConnect: func(p *stream.Peer) { log.Println("connected", p.ID) },
//   })
func NewHubWithConfig(config HubConfig) *Hub {}

// Run starts the hub's select loop. Call in a goroutine. Exits when ctx is cancelled.
//
//   go hub.Run(ctx)
func (h *Hub) Run(ctx context.Context) {}

// SendToChannel delivers frame to all peers subscribed to channel.
// Returns nil if channel has no subscribers (not an error).
//
//   hub.SendToChannel("process:abc123", frame)
func (h *Hub) SendToChannel(channel string, frame []byte) error {}

// Subscribe registers a handler function invoked for every frame arriving on channel.
// Returns an unsubscribe function. Multiple handlers per channel are allowed.
// Handlers run in the hub's goroutine — keep them non-blocking.
//
//   unsub := hub.Subscribe("block", func(f []byte) { ... })
//   defer unsub()
func (h *Hub) Subscribe(channel string, handler func([]byte)) func() {}

// SubscribePeer adds peer to a named channel. Used by transport adapters when
// a peer requests channel subscription (WebSocket TypeSubscribe message, etc.).
//
//   hub.SubscribePeer(peer, "hashrate")
func (h *Hub) SubscribePeer(peer *Peer, channel string) error {}

// UnsubscribePeer removes peer from a named channel.
//
//   hub.UnsubscribePeer(peer, "hashrate")
func (h *Hub) UnsubscribePeer(peer *Peer, channel string) {}

// Publish sends frame to all subscribers of channel. Satisfies Stream interface.
// Delegates to SendToChannel internally.
//
//   hub.Publish("hashrate", frame)
func (h *Hub) Publish(channel string, frame []byte) error {}

// Broadcast sends frame to every connected peer regardless of subscriptions.
// Satisfies Stream interface. Enqueues onto the hub's broadcast channel.
//
//   hub.Broadcast([]byte(`{"type":"shutdown"}`))
func (h *Hub) Broadcast(frame []byte) error {}

// Pipe connects this hub to dst: every frame published here is forwarded to dst.
// Returns a stop function. Satisfies Stream interface.
//
//   stop := hub.Pipe(remoteHub)
//   defer stop()
func (h *Hub) Pipe(dst Stream) func() {}
```

---

## 5.1 ConnectionState

```go
// ConnectionState represents the lifecycle state of a reconnecting client.
type ConnectionState int

const (
    StateDisconnected ConnectionState = iota
    StateConnecting
    StateConnected
)
```

---

## 6. Peer

`Peer` replaces the go-ws `Client`. The rename reflects transport-agnosticity — a peer may be a browser WebSocket, an SSE connection, a ZMQ socket, or a TCP tunnel.

```go
// Peer represents one connected endpoint. Created by a transport adapter.
//
//   // Adapter creates peer:
//   peer := &stream.Peer{
//       ID:     uuid.New(),
//       UserID: authResult.UserID,
//       Claims: authResult.Claims,
//       send:   make(chan []byte, 256),
//   }
//   hub.register <- peer
type Peer struct {
    // ID is a random UUID assigned on creation.
    ID string

    // UserID is the authenticated user identifier. Empty when no auth is configured.
    UserID string

    // Claims holds arbitrary auth metadata (roles, tenant ID, scopes).
    Claims map[string]any

    // Transport identifies the adapter type for logging and metrics.
    // Values: "ws", "sse", "tcp", "zmq"
    Transport string

    send          chan []byte
    subscriptions map[string]bool
    mu            sync.RWMutex
    closeOnce     sync.Once
}

// Subscriptions returns a copy of this peer's current channel subscriptions.
//
//   channels := peer.Subscriptions()   // ["hashrate", "block"]
func (p *Peer) Subscriptions() []string {}

// Send enqueues frame for delivery. Non-blocking: drops and returns false if buffer full.
//
//   ok := peer.Send(frame)
func (p *Peer) Send(frame []byte) bool {}

// Close signals the transport adapter to shut down this connection.
//
//   peer.Close()
func (p *Peer) Close() {}
```

---

## 7. HubConfig

```go
// HubConfig controls hub behaviour and lifecycle callbacks.
type HubConfig struct {
    // HeartbeatInterval is the server-side ping interval for WebSocket peers.
    // Defaults to 30 seconds. Ignored by SSE and TCP adapters.
    HeartbeatInterval time.Duration

    // PongTimeout is the deadline after a ping before the WS connection is closed.
    // Must be greater than HeartbeatInterval. Defaults to 60 seconds.
    PongTimeout time.Duration

    // WriteTimeout is the per-write deadline for WS and TCP adapters.
    // Defaults to 10 seconds.
    WriteTimeout time.Duration

    // OnConnect is called when a peer registers. Optional.
    //
    //   OnConnect: func(p *stream.Peer) { metrics.Inc("peers") },
    OnConnect func(peer *Peer)

    // OnDisconnect is called when a peer unregisters. Optional.
    OnDisconnect func(peer *Peer)

    // ChannelAuthoriser optionally decides whether a peer may subscribe to a channel.
    // Return true to allow. When nil, all subscriptions are allowed.
    //
    //   ChannelAuthoriser: func(p *stream.Peer, ch string) bool {
    //       return p.Claims["role"] == "admin" || ch == "public"
    //   },
    ChannelAuthoriser func(peer *Peer, channel string) bool
}

// DefaultHubConfig returns sensible defaults.
//
//   cfg := stream.DefaultHubConfig()
func DefaultHubConfig() HubConfig {}
```

---

## 8. Auth

### 8.1 HTTP Authenticator (WebSocket and SSE)

```go
// Authenticator validates an HTTP request during the WebSocket upgrade or SSE
// connection. Implementations may inspect headers, query parameters, or cookies.
type Authenticator interface {
    Authenticate(r *http.Request) AuthResult
}

// AuthResult holds the outcome of an authentication attempt.
type AuthResult struct {
    // Valid indicates whether authentication succeeded.
    Valid bool

    // UserID is the authenticated user's identifier.
    UserID string

    // Claims holds arbitrary metadata (roles, scopes, tenant ID).
    Claims map[string]any

    // Error holds the reason for failure, if any.
    Error error
}

// AuthenticatorFunc adapts a plain function to the Authenticator interface.
//
//   auth := stream.AuthenticatorFunc(func(r *http.Request) stream.AuthResult {
//       token := r.Header.Get("X-Api-Key")
//       if token == "" { return stream.AuthResult{Valid: false} }
//       return stream.AuthResult{Valid: true, UserID: lookupUser(token)}
//   })
type AuthenticatorFunc func(r *http.Request) AuthResult

// Authenticate calls f(r).
func (f AuthenticatorFunc) Authenticate(r *http.Request) AuthResult {}

// APIKeyAuthenticator validates Authorization: Bearer <key> against a static map.
//
//   auth := stream.NewAPIKeyAuth(map[string]string{"sk-prod-1": "user-42"})
type APIKeyAuthenticator struct {
    Keys map[string]string
}

func NewAPIKeyAuth(keys map[string]string) *APIKeyAuthenticator {}
func (a *APIKeyAuthenticator) Authenticate(r *http.Request) AuthResult {}

// BearerTokenAuth delegates bearer token validation to a caller-supplied function.
//
//   auth := &stream.BearerTokenAuth{
//       Validate: func(token string) stream.AuthResult {
//           claims, err := jwt.Parse(token, keyFunc)
//           if err != nil { return stream.AuthResult{Valid: false, Error: err} }
//           return stream.AuthResult{Valid: true, UserID: claims.Subject}
//       },
//   }
type BearerTokenAuth struct {
    Validate func(token string) AuthResult
}

func (b *BearerTokenAuth) Authenticate(r *http.Request) AuthResult {}

// QueryTokenAuth extracts a ?token= query parameter and validates via caller function.
// Use when browser clients cannot set headers (native WebSocket API).
//
//   auth := &stream.QueryTokenAuth{
//       Validate: func(token string) stream.AuthResult { ... },
//   }
type QueryTokenAuth struct {
    Validate func(token string) AuthResult
}

func (q *QueryTokenAuth) Authenticate(r *http.Request) AuthResult {}
```

### 8.2 Connection Authenticator (TCP and ZMQ)

```go
// ConnAuthenticator validates a raw connection handshake for TCP and ZMQ adapters.
// The handshake is the first message received on the connection (up to 4 KB).
// Implementations decode credentials from the raw bytes.
//
//   // HMAC-based TCP auth:
//   auth := stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
//       var h tcp.Handshake
//       if r := core.JSONUnmarshal(handshake, &h); !r.OK {
//           return stream.AuthResult{Valid: false}
//       }
//       return verifyHMAC(h.Token, h.Timestamp)
//   })
type ConnAuthenticator interface {
    AuthenticateConn(handshake []byte) AuthResult
}

// ConnAuthenticatorFunc adapts a plain function to ConnAuthenticator.
type ConnAuthenticatorFunc func(handshake []byte) AuthResult

func (f ConnAuthenticatorFunc) AuthenticateConn(handshake []byte) AuthResult {}
```

### 8.3 Sentinel errors

```go
var (
    // ErrMissingAuthHeader is returned when no Authorization header is present.
    ErrMissingAuthHeader = core.E("stream.auth", "missing Authorization header", nil)

    // ErrMalformedAuthHeader is returned when the header is not "Bearer <token>".
    ErrMalformedAuthHeader = core.E("stream.auth", "malformed Authorization header", nil)

    // ErrInvalidAPIKey is returned when the API key is not in the key map.
    ErrInvalidAPIKey = core.E("stream.auth", "invalid API key", nil)

    // ErrHandshakeTimeout is returned when the TCP/ZMQ peer did not send a
    // handshake within the configured deadline.
    ErrHandshakeTimeout = core.E("stream.auth", "handshake timeout", nil)

    // ErrAuthRejected is returned when ConnAuthenticator denies the handshake.
    ErrAuthRejected = core.E("stream.auth", "connection rejected by authenticator", nil)
)
```

---

## 9. Message Envelope

go-stream preserves the go-ws JSON message format for backward compatibility with
existing browser clients. The envelope is used by the WebSocket adapter only — SSE,
TCP, and ZMQ adapters carry raw `[]byte` frames and choose their own serialisation.

```go
// MessageType identifies the purpose of a WebSocket message.
type MessageType string

const (
    TypeProcessOutput MessageType = "process_output" // real-time process output line
    TypeProcessStatus MessageType = "process_status" // process status change (running/exited)
    TypeEvent         MessageType = "event"           // generic named event
    TypeError         MessageType = "error"           // error message
    TypePing          MessageType = "ping"             // client → server keepalive
    TypePong          MessageType = "pong"             // server → client keepalive response
    TypeSubscribe     MessageType = "subscribe"        // client requests channel subscription
    TypeUnsubscribe   MessageType = "unsubscribe"      // client cancels channel subscription
)

// Message is the JSON envelope for WebSocket frames. Preserved from go-ws.
//
//   msg := stream.Message{
//       Type:    stream.TypeEvent,
//       Channel: "hashrate",
//       Data:    map[string]any{"h": 1234567},
//   }
//   frame, _ := core.JSONMarshal(msg)
//   hub.Publish("hashrate", frame.Value.([]byte))
type Message struct {
    Type      MessageType `json:"type"`
    Channel   string      `json:"channel,omitempty"`
    ProcessID string      `json:"processId,omitempty"`
    Data      any         `json:"data,omitempty"`
    Timestamp time.Time   `json:"timestamp"`
}
```

---

## 10. WebSocket Adapter (`adapter/ws`)

The WebSocket adapter is the go-ws `Hub.Handler()` and client machinery extracted into
the `adapter/ws` sub-package. It wires gorilla/websocket onto the stream Hub.

```go
// Config configures the WebSocket adapter.
//
//   cfg := ws.Config{
//       Authenticator: stream.NewAPIKeyAuth(keys),
//       OnAuthFailure: func(r *http.Request, res stream.AuthResult) {
//           log.Printf("ws auth fail from %s", r.RemoteAddr)
//       },
//   }
type Config struct {
    // Authenticator is called during HTTP upgrade. When nil, all connections accepted.
    Authenticator stream.Authenticator

    // OnAuthFailure is called when Authenticator rejects a connection.
    OnAuthFailure func(r *http.Request, result stream.AuthResult)

    // ReadBufferSize and WriteBufferSize are passed to the gorilla upgrader.
    // Default: 1024 each.
    ReadBufferSize  int
    WriteBufferSize int

    // CheckOrigin overrides the upgrader's origin check. When nil, all origins accepted.
    CheckOrigin func(r *http.Request) bool
}

// Adapter is the WebSocket transport adapter for a stream.Hub.
//
//   adapter := ws.New(ws.Config{...})
//   adapter.Mount(hub)
//   http.Handle("/ws", adapter.Handler())
type Adapter struct {
    hub    *stream.Hub
    config Config
}

// New creates a WebSocket adapter. Call Mount before serving requests.
//
//   adapter := ws.New(ws.Config{Authenticator: auth})
func New(config Config) *Adapter {}

// Mount wires the adapter to a hub. Must be called before Handler().
//
//   adapter.Mount(hub)
func (a *Adapter) Mount(hub *stream.Hub) {}

// Handler returns an http.HandlerFunc for WebSocket connections.
// Compatible with net/http and gin (use gin.WrapF).
//
//   http.Handle("/stream/ws", adapter.Handler())
//
//   // Gin:
//   r.GET("/stream/ws", gin.WrapF(adapter.Handler()))
func (a *Adapter) Handler() http.HandlerFunc {}

// ReconnectConfig configures the client-side reconnecting WebSocket.
//
//   rc := ws.ReconnectConfig{
//       URL:            "ws://localhost:8080/stream/ws",
//       InitialBackoff: 500 * time.Millisecond,
//       MaxBackoff:     30 * time.Second,
//       OnMessage: func(msg stream.Message) {
//           log.Printf("received %s on %s", msg.Type, msg.Channel)
//       },
//   }
//   client := ws.NewReconnectingClient(rc)
//   err := client.Connect(ctx)
type ReconnectConfig struct {
    URL                  string
    InitialBackoff       time.Duration
    MaxBackoff           time.Duration
    BackoffMultiplier    float64
    MaxRetries           int           // 0 = unlimited
    OnConnect            func()
    OnDisconnect         func()
    OnReconnect          func(attempt int)
    OnMessage            func(msg stream.Message)
    Dialer               *websocket.Dialer
    Headers              http.Header
}

// ReconnectingClient is a WebSocket client with automatic reconnection.
// Preserves the go-ws ReconnectingClient API exactly.
//
//   client := ws.NewReconnectingClient(rc)
//   err := client.Connect(ctx)  // blocks until ctx cancelled
type ReconnectingClient struct { /* unexported */ }

func NewReconnectingClient(config ReconnectConfig) *ReconnectingClient {}
func (rc *ReconnectingClient) Connect(ctx context.Context) error         {}
func (rc *ReconnectingClient) Send(msg stream.Message) error             {}
func (rc *ReconnectingClient) State() stream.ConnectionState             {}
func (rc *ReconnectingClient) Close() error                              {}
```

---

## 11. SSE Adapter (`adapter/sse`)

Server-Sent Events: lightweight server-push over HTTP/1.1. No upgrade required.
Used by `core/api` for live stats, agent event streams, and `/live_stats` endpoints.

```go
// Config configures the SSE adapter.
//
//   cfg := sse.Config{
//       Authenticator:   stream.NewAPIKeyAuth(keys),
//       HeartbeatInterval: 15 * time.Second,
//   }
type Config struct {
    // Authenticator is checked before accepting the SSE connection.
    // When nil, all connections accepted.
    Authenticator stream.Authenticator

    // HeartbeatInterval is the interval between SSE comment heartbeats (": ping").
    // Defaults to 15 seconds. Keeps the connection alive through proxies.
    HeartbeatInterval time.Duration

    // RetryMs is the SSE retry field sent to the client in milliseconds.
    // Instructs the browser how long to wait before reconnecting.
    // Defaults to 3000.
    RetryMs int
}

// Adapter is the SSE transport adapter for a stream.Hub.
//
//   adapter := sse.New(sse.Config{})
//   adapter.Mount(hub)
//   http.Handle("/stream/events", adapter.Handler())
type Adapter struct {
    hub    *stream.Hub
    config Config
}

func New(config Config) *Adapter             {}
func (a *Adapter) Mount(hub *stream.Hub)     {}

// Handler returns an http.HandlerFunc that accepts SSE connections.
// Response: Content-Type: text/event-stream, Cache-Control: no-cache
//
//   http.Handle("/stream/events", adapter.Handler())
//
// Each frame published to the hub is sent as:
//   data: {frame bytes}\n\n
//
// Subscription is controlled via query parameter:
//   GET /stream/events?channel=hashrate
// Multiple channels via repeated param:
//   GET /stream/events?channel=hashrate&channel=block
//
//   http.Handle("/stream/events", adapter.Handler())
func (a *Adapter) Handler() http.HandlerFunc {}

// HandlerForChannel returns a handler that auto-subscribes all connections to channel.
// Used when a dedicated endpoint per channel is preferred.
//
//   http.Handle("/stream/hashrate", adapter.HandlerForChannel("hashrate"))
func (a *Adapter) HandlerForChannel(channel string) http.HandlerFunc {}
```

**SSE wire format:**

```
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
X-Accel-Buffering: no

retry: 3000\n\n
: ping\n\n          (heartbeat — keeps proxy connections alive)
data: {bytes}\n\n   (one frame per event)
```

SSE connections are read-only from the client's perspective. Clients request a channel
via the URL query string, not via an in-band subscribe message. This keeps the adapter
stateless per-request and avoids the need for a read pump.

---

## 12. Redis Bridge (`adapter/redis`)

Cross-instance coordination. Identical to go-ws `RedisBridge` but generalised to
operate on `[]byte` frames rather than typed `Message` structs. The echo-prevention
envelope pattern is preserved.

```go
// Config configures the Redis bridge.
//
//   cfg := redis.Config{
//       Addr:   "10.69.69.87:6379",
//       Prefix: "pool",
//   }
type Config struct {
    Addr      string
    Password  string
    DB        int
    Prefix    string       // key prefix for Redis channels. Default: "stream"
    TLSConfig *tls.Config
}

// Bridge connects a Hub to Redis pub/sub for cross-instance messaging.
// Multiple Hub instances on different nodes using the same Redis backend
// coordinate broadcasts and channel messages transparently.
//
//   bridge, err := redis.NewBridge(hub, redis.Config{Addr: "redis:6379", Prefix: "pool"})
//   bridge.Start(ctx)
//   defer bridge.Stop()
type Bridge struct { /* unexported */ }

// NewBridge creates and validates the Redis connection. Does not start listening.
// Returns core.E if hub is nil, address is empty, or Redis ping fails.
//
//   bridge, err := redis.NewBridge(hub, cfg)
func NewBridge(hub *stream.Hub, cfg Config) (*Bridge, error) {}

// Start begins the Redis pub/sub listener. Blocks in a goroutine until Stop() or ctx cancel.
//
//   bridge.Start(ctx)
func (b *Bridge) Start(ctx context.Context) error {}

// Stop cleanly shuts down the bridge. Closes the pub/sub subscription and Redis client.
//
//   defer bridge.Stop()
func (b *Bridge) Stop() error {}

// PublishToChannel publishes frame to a specific hub channel via Redis.
// All Bridge instances on the same Redis receive the frame and deliver locally.
//
//   bridge.PublishToChannel("block", templateBytes)
func (b *Bridge) PublishToChannel(channel string, frame []byte) error {}

// PublishBroadcast publishes frame as a broadcast via Redis.
// All Bridge instances forward it to all local hub peers.
//
//   bridge.PublishBroadcast(shutdownFrame)
func (b *Bridge) PublishBroadcast(frame []byte) error {}

// SourceID returns the random instance identifier. Used in tests to verify echo prevention.
//
//   id := bridge.SourceID()
func (b *Bridge) SourceID() string {}
```

**Redis channel names:**

| Purpose | Redis key |
|---------|-----------|
| Broadcast | `{prefix}:broadcast` |
| Channel X | `{prefix}:channel:{X}` |

The bridge uses `PSUBSCRIBE {prefix}:broadcast {prefix}:channel:*` — one subscription
covers all channels. Envelope format:

```go
// envelope wraps a frame with a sourceID to prevent infinite echo loops.
// Serialised as JSON on the Redis wire; unexported.
type envelope struct {
    SourceID string `json:"s"`
    Frame    []byte `json:"f"`
}
```

---

## 13. ZeroMQ Adapter (`adapter/zmq`)

High-throughput IPC for daemon → go-pool block notifications and go-pool → go-proxy
job broadcasts. Uses `go-zeromq/zmq4` (pure Go, no CGO).

```go
// Mode selects the ZMQ socket pattern.
type Mode int

const (
    // ModePubSub uses PUB/SUB sockets.
    // Publisher calls Publish(channel, frame) — topic = channel name.
    // Subscriber receives frames for subscribed topics.
    ModePubSub Mode = iota

    // ModePushPull uses PUSH/PULL sockets.
    // Pusher sends frames; Puller receives and forwards to hub.
    // Useful for daemon → pool block notification (single consumer).
    ModePushPull
)

// Config configures the ZMQ adapter.
//
//   cfg := zmq.Config{
//       Mode:     zmq.ModePubSub,
//       Endpoint: "tcp://127.0.0.1:5555",
//       Role:     zmq.RoleSubscriber,
//   }
type Config struct {
    Mode     Mode
    Endpoint string   // ZMQ endpoint, e.g. "tcp://127.0.0.1:5555" or "ipc:///tmp/pool.sock"
    Role     Role     // Publisher/Subscriber or Pusher/Puller
    Topics   []string // PubSub subscriber: topics to subscribe. Empty = all.
}

// Role is the ZMQ socket role.
type Role int

const (
    RolePublisher  Role = iota // PubSub: sends frames
    RoleSubscriber             // PubSub: receives frames → hub dispatch
    RolePusher                 // PushPull: sends frames
    RolePuller                 // PushPull: receives frames → hub dispatch
)

// Adapter is the ZMQ transport adapter.
//
//   adapter := zmq.New(zmq.Config{
//       Mode:     zmq.ModePubSub,
//       Endpoint: "tcp://127.0.0.1:5555",
//       Role:     zmq.RoleSubscriber,
//       Topics:   []string{"block"},
//   })
//   adapter.Mount(hub)
//   adapter.Start(ctx)
type Adapter struct { /* unexported */ }

func New(config Config) *Adapter         {}
func (a *Adapter) Mount(hub *stream.Hub) {}

// Start opens the ZMQ socket and begins receive/dispatch. Blocks until ctx cancelled.
// For publisher/pusher roles, Start is still required to open the socket.
//
//   go adapter.Start(ctx)
func (a *Adapter) Start(ctx context.Context) error {}

// Publish sends frame with topic (channel name) via the ZMQ socket.
// Only valid when Role is RolePublisher or RolePusher.
//
//   adapter.Publish("block", templateBytes)
func (a *Adapter) Publish(channel string, frame []byte) error {}
```

**ZMQ PubSub wire format:**

```
[topic_bytes][0x00][frame_bytes]
```

The topic is the channel name as UTF-8 bytes. The null byte is the ZMQ topic delimiter.
On receipt, the adapter strips the topic prefix, determines the hub channel, and calls
`hub.Publish(channel, frame)`.

---

## 14. Raw TCP Adapter (`adapter/tcp`)

Length-prefixed framing over plain or TLS TCP. Used by go-p2p VPN tunnels and
go-proxy stratum sessions where WebSocket overhead is undesirable.

```go
// FrameHeader is a 4-byte big-endian uint32 length prefix.
// Maximum frame size: 65535 bytes (enforced at read time).
const MaxFrameSize = 65535

// Config configures the TCP adapter.
//
//   // Listen mode (server):
//   cfg := tcp.Config{
//       Addr:              ":9000",
//       ConnAuthenticator: myAuth,
//       TLS:               &tls.Config{Certificates: []tls.Certificate{cert}},
//   }
//
//   // Dial mode (client):
//   cfg := tcp.Config{
//       Addr: "10.69.69.165:9000",
//   }
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
//   adapter := tcp.New(tcp.Config{Addr: ":9000", ConnAuthenticator: auth})
//   adapter.Mount(hub)
//   adapter.Listen(ctx)
type Adapter struct { /* unexported */ }

func New(config Config) *Adapter         {}
func (a *Adapter) Mount(hub *stream.Hub) {}

// Listen starts the TCP accept loop. Blocks until ctx cancelled.
// Each accepted connection runs readPump and writePump goroutines.
//
//   go adapter.Listen(ctx)
func (a *Adapter) Listen(ctx context.Context) error {}

// Dial connects to a remote TCP stream endpoint. Returns a Peer that can send/receive.
// Reconnection is handled by ReconnectingTCP, not by Dial itself.
//
//   peer, err := adapter.Dial(ctx, hub)
func (a *Adapter) Dial(ctx context.Context, hub *stream.Hub) (*stream.Peer, error) {}

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
//
//   client := tcp.NewReconnectingTCP(tcp.ReconnectConfig{
//       Addr:           "10.69.69.165:9000",
//       InitialBackoff: 1 * time.Second,
//       OnMessage: func(ch string, frame []byte) {
//           log.Printf("received on %s: %d bytes", ch, len(frame))
//       },
//   })
//   err := client.Connect(ctx)
type ReconnectingTCP struct { /* unexported */ }

func NewReconnectingTCP(config ReconnectConfig) *ReconnectingTCP {}
func (rc *ReconnectingTCP) Connect(ctx context.Context) error     {}
func (rc *ReconnectingTCP) Send(channel string, frame []byte) error {}
func (rc *ReconnectingTCP) Close() error                           {}
```

**TCP wire format (framing):**

```
[4 bytes big-endian uint32: payload length]
[4 bytes: channel name length (uint32)]
[channel name bytes]
[frame bytes]
```

This compact framing lets the receiver restore both channel and payload without
scanning for delimiters. Channel is included in every frame so a single TCP connection
can carry multiple logical channels.

---

## 15. Pipe

```go
// Pipe connects src to dst: every frame published on src is forwarded to dst.
// Returns a stop function. Safe to call from multiple goroutines.
//
// Pipe is the primary composition primitive — use it to bridge adapters without
// writing custom glue code:
//
//   // Forward ZMQ daemon events to all WebSocket browser clients:
//   stop := stream.Pipe(zmqHub, wsHub)
//   defer stop()
//
//   // Mirror a local hub to a remote hub via TCP:
//   stop := stream.Pipe(localHub, remoteHub)
//   defer stop()
func Pipe(src Stream, dst Stream) func() {}
```

Pipe subscribes to `src` (all channels via `"*"` wildcard) and calls `dst.Publish`
for each frame received. The stop function cancels the subscription.

---

## 16. Stats

```go
// HubStats is a snapshot of hub state at a point in time.
type HubStats struct {
    // Peers is the number of currently connected peers across all transports.
    Peers int `json:"peers"`

    // Channels is the number of active named channels with at least one subscriber.
    Channels int `json:"channels"`

    // SubscriberCount maps channel name to subscriber count.
    SubscriberCount map[string]int `json:"subscriber_count"`
}

// Stats returns a snapshot of current hub state.
//
//   s := hub.Stats()
//   log.Printf("peers=%d channels=%d", s.Peers, s.Channels)
func (h *Hub) Stats() HubStats {}

// PeerCount returns the number of connected peers.
//
//   n := hub.PeerCount()
func (h *Hub) PeerCount() int {}

// ChannelCount returns the number of active channels.
//
//   n := hub.ChannelCount()
func (h *Hub) ChannelCount() int {}

// ChannelSubscriberCount returns the subscriber count for a channel.
// Returns 0 if the channel has no subscribers.
//
//   n := hub.ChannelSubscriberCount("hashrate")
func (h *Hub) ChannelSubscriberCount(channel string) int {}

// AllPeers returns an iterator for all connected peers.
//
//   for peer := range hub.AllPeers() { log.Println(peer.UserID) }
func (h *Hub) AllPeers() iter.Seq[*Peer] {}

// AllChannels returns an iterator for all active channels.
//
//   for ch := range hub.AllChannels() { log.Println(ch) }
func (h *Hub) AllChannels() iter.Seq[string] {}
```

---

## 17. Consumer Usage Patterns

### core/api — SSE live stats endpoint

```go
// Gin route: GET /1/live_stats
//
//   hub := stream.NewHub()
//   go hub.Run(ctx)
//   sseAdapter := sse.New(sse.Config{Authenticator: apiKeyAuth})
//   sseAdapter.Mount(hub)
//   r.GET("/1/live_stats", gin.WrapF(sseAdapter.HandlerForChannel("hashrate")))
//
//   // Publish from pool stats ticker:
//   hub.Publish("hashrate", frame)
```

### go-pool — block template broadcast

```go
// ZMQ subscriber receives new blocks from daemon; broadcasts to all WebSocket clients:
//
//   zmqHub := stream.NewHub()
//   go zmqHub.Run(ctx)
//
//   wsHub := stream.NewHub()
//   go wsHub.Run(ctx)
//
//   // Forward ZMQ frames to WS clients
//   stop := stream.Pipe(zmqHub, wsHub)
//   defer stop()
//
//   zmqAdapter := zmq.New(zmq.Config{
//       Mode:     zmq.ModePubSub,
//       Role:     zmq.RoleSubscriber,
//       Endpoint: "tcp://127.0.0.1:18083",
//       Topics:   []string{"json-full-txpool"},
//   })
//   zmqAdapter.Mount(zmqHub)
//   go zmqAdapter.Start(ctx)
```

### go-p2p — VPN tunnel

```go
// TCP adapter with TLS and HMAC handshake auth:
//
//   hub := stream.NewHub()
//   go hub.Run(ctx)
//
//   tcpAdapter := tcp.New(tcp.Config{
//       Addr: ":51820",
//       TLS:  &tls.Config{Certificates: []tls.Certificate{cert}},
//       ConnAuthenticator: stream.ConnAuthenticatorFunc(func(h []byte) stream.AuthResult {
//           return verifyHMAC(h, sharedSecret)
//       }),
//   })
//   tcpAdapter.Mount(hub)
//   go tcpAdapter.Listen(ctx)
//
//   // Send VPN packet:
//   hub.Publish("vpn:peer-abc123", encryptedPacket)
```

### core/mcp — agent-to-agent event stream

```go
// MCP server uses a stream hub as the event bus for agent session notifications:
//
//   hub := stream.NewHub()
//   go hub.Run(ctx)
//
//   // MCP handler publishes agent events:
//   hub.Publish("agent:session-xyz", eventFrame)
//
//   // Other agents subscribe:
//   unsub := hub.Subscribe("agent:session-xyz", func(f []byte) {
//       var evt AgentEvent
//       core.JSONUnmarshal(f, &evt)
//       handleEvent(evt)
//   })
//   defer unsub()
```

---

## 18. Migration from go-ws

go-ws callers migrate to go-stream as follows:

| go-ws | go-stream |
|-------|-----------|
| `import "dappco.re/go/ws"` | `import "dappco.re/go/core/stream"` + `"dappco.re/go/core/stream/adapter/ws"` |
| `ws.NewHub()` | `stream.NewHub()` |
| `ws.NewHubWithConfig(ws.HubConfig{...})` | `stream.NewHubWithConfig(stream.HubConfig{...})` |
| `hub.Handler()` | `wsAdapter.Handler()` (after `wsAdapter.Mount(hub)`) |
| `hub.Broadcast(msg)` | `hub.Broadcast(frame)` (caller marshals `stream.Message` to `[]byte`) |
| `hub.SendToChannel(ch, msg)` | `hub.Publish(ch, frame)` |
| `hub.Subscribe(client, ch)` | `hub.SubscribePeer(peer, ch)` (adapter calls; handlers via `hub.Subscribe`) |
| `ws.Client` | `stream.Peer` |
| `ws.RedisBridge` | `redis.Bridge` (`adapter/redis`) |
| `ws.NewReconnectingClient(cfg)` | `ws.NewReconnectingClient(cfg)` (same, in `adapter/ws`) |
| `ws.Authenticator` | `stream.Authenticator` |
| `ws.AuthResult` | `stream.AuthResult` |

The `stream.Message` type and all `MessageType` constants are preserved with identical
JSON tags. Existing browser clients require no changes.

---

## 19. Test Naming

Tests follow `TestFilename_Function_{Good,Bad,Ugly}` — all three categories mandatory.

```
hub_test.go:
  TestHub_Run_Good           — hub starts, accepts peers, shuts down cleanly
  TestHub_Run_Bad            — Run called twice; second call is a no-op
  TestHub_Run_Ugly           — ctx cancelled mid-broadcast; no goroutine leak

  TestHub_Publish_Good       — frame reaches all subscribers of channel
  TestHub_Publish_Bad        — publish to channel with no subscribers returns nil (not error)
  TestHub_Publish_Ugly       — hub not running; publish returns core.E

  TestHub_Subscribe_Good     — handler invoked for matching channel
  TestHub_Subscribe_Bad      — subscribe with empty channel name; returns core.E
  TestHub_Subscribe_Ugly     — handler panics; hub recovers, other handlers unaffected

  TestHub_Pipe_Good          — frames from src appear on dst
  TestHub_Pipe_Bad           — stop() called; no further frames forwarded
  TestHub_Pipe_Ugly          — src and dst are the same hub (self-pipe); no infinite loop

adapter/ws/ws_test.go:
  TestAdapter_Handler_Good   — WebSocket upgrade, subscribe, receive frame
  TestAdapter_Handler_Bad    — missing auth header returns 401
  TestAdapter_Handler_Ugly   — client drops connection mid-frame; hub removes peer

adapter/sse/sse_test.go:
  TestAdapter_Handler_Good   — SSE connection receives frame via data: line
  TestAdapter_Handler_Bad    — auth failure returns 401
  TestAdapter_Handler_Ugly   — client disconnects; heartbeat goroutine exits cleanly

adapter/redis/redis_test.go:
  TestBridge_Publish_Good    — frame published to Redis arrives on second hub via bridge
  TestBridge_Publish_Bad     — publish before Start returns core.E
  TestBridge_Publish_Ugly    — self-echo prevention: frame from own sourceID is dropped
```

---

## 20. Reference Material

| Resource | Location |
|----------|----------|
| Core Go RFC | `code/core/go/RFC.md` |
| API RFC | `code/core/go/api/RFC.md` |
| MCP RFC | `code/core/mcp/RFC.md` |
| go-pool RFC | `code/core/go/pool/RFC.md` |
| go-proxy RFC | `code/core/go/proxy/RFC.md` |
| go-p2p RFC | `code/core/go/p2p/RFC.md` |
| go-io RFC | `code/core/go/io/RFC.md` |
| GUI RFC | `code/core/gui/RFC.md` |

