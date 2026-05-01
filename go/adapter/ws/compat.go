// SPDX-License-Identifier: EUPL-1.2

// Package ws preserves the legacy go-ws compatibility surface while the new
// transport-agnostic stream package does the actual work.
package ws

import (
	"dappco.re/go"
	"dappco.re/go/stream"
	"dappco.re/go/stream/adapter/redis"
)

// Stream preserves the transport-agnostic stream interface for legacy callers.
type Stream = stream.Stream

// Frame preserves the legacy raw payload alias.
type Frame = stream.Frame

// Channel preserves the legacy channel name alias.
type Channel = stream.Channel

// Hub preserves the legacy go-ws Hub type name.
type Hub = stream.Hub

// HubConfig preserves the legacy go-ws HubConfig type name.
type HubConfig = stream.HubConfig

// ChannelAuthoriser preserves the legacy go-ws channel authoriser type name.
type ChannelAuthoriser = stream.ChannelAuthoriser

// HubStats preserves the legacy hub stats type name.
type HubStats = stream.HubStats

// Peer preserves the transport-agnostic peer type under the legacy package.
type Peer = stream.Peer

// Client preserves the legacy go-ws Client type name.
type Client = stream.Peer

// Authenticator preserves the legacy go-ws Authenticator type name.
type Authenticator = stream.Authenticator

// AuthenticatorFunc preserves the legacy go-ws AuthenticatorFunc helper.
type AuthenticatorFunc = stream.AuthenticatorFunc

// AuthResult preserves the legacy go-ws AuthResult type name.
type AuthResult = stream.AuthResult

// APIKeyAuthenticator preserves the legacy API key authenticator type name.
type APIKeyAuthenticator = stream.APIKeyAuthenticator

// BearerTokenAuth preserves the legacy bearer-token authenticator type name.
type BearerTokenAuth = stream.BearerTokenAuth

// QueryTokenAuth preserves the legacy query-token authenticator type name.
type QueryTokenAuth = stream.QueryTokenAuth

// ConnAuthenticator preserves the legacy raw-connection authenticator name.
type ConnAuthenticator = stream.ConnAuthenticator

// ConnAuthenticatorFunc preserves the legacy raw-connection helper name.
type ConnAuthenticatorFunc = stream.ConnAuthenticatorFunc

// ConnectionState preserves the reconnecting client connection state type.
type ConnectionState = stream.ConnectionState

// Message preserves the legacy go-ws WebSocket message envelope.
type Message = stream.Message

// MessageType preserves the legacy go-ws message type name.
type MessageType = stream.MessageType

const (
	// TypeProcessOutput preserves the legacy message type constant.
	TypeProcessOutput = stream.TypeProcessOutput
	// TypeProcessStatus preserves the legacy message type constant.
	TypeProcessStatus = stream.TypeProcessStatus
	// TypeEvent preserves the legacy message type constant.
	TypeEvent = stream.TypeEvent
	// TypeError preserves the legacy message type constant.
	TypeError = stream.TypeError
	// TypePing preserves the legacy message type constant.
	TypePing = stream.TypePing
	// TypePong preserves the legacy message type constant.
	TypePong = stream.TypePong
	// TypeSubscribe preserves the legacy message type constant.
	TypeSubscribe = stream.TypeSubscribe
	// TypeUnsubscribe preserves the legacy message type constant.
	TypeUnsubscribe = stream.TypeUnsubscribe
	// StateDisconnected preserves the reconnecting client disconnected state.
	StateDisconnected = stream.StateDisconnected
	// StateConnecting preserves the reconnecting client connecting state.
	StateConnecting = stream.StateConnecting
	// StateConnected preserves the reconnecting client connected state.
	StateConnected = stream.StateConnected
)

var (
	// ErrMissingAuthHeader preserves the legacy missing-header sentinel error.
	ErrMissingAuthHeader = stream.ErrMissingAuthHeader
	// ErrMalformedAuthHeader preserves the legacy malformed-header sentinel error.
	ErrMalformedAuthHeader = stream.ErrMalformedAuthHeader
	// ErrInvalidAPIKey preserves the legacy invalid API key sentinel error.
	ErrInvalidAPIKey = stream.ErrInvalidAPIKey
	// ErrHandshakeTimeout preserves the legacy handshake timeout sentinel error.
	ErrHandshakeTimeout = stream.ErrHandshakeTimeout
	// ErrAuthRejected preserves the legacy authenticator rejection sentinel error.
	ErrAuthRejected = stream.ErrAuthRejected
	// ErrHubNotRunning preserves the legacy hub lifecycle sentinel error.
	ErrHubNotRunning = stream.ErrHubNotRunning
	// ErrEmptyChannel preserves the legacy empty-channel sentinel error.
	ErrEmptyChannel = stream.ErrEmptyChannel
)

// RedisBridge preserves the legacy go-ws RedisBridge type name.
type RedisBridge = redis.Bridge

// bridge, err := ws.NewRedisBridge(hub, redis.Config{Addr: "redis:6379", Prefix: "pool"})
func NewRedisBridge(hub *stream.Hub, config redis.Config) core.Result {
	return redis.NewBridge(hub, config)
}

// auth := ws.NewAPIKeyAuth(map[string]string{"sk-live": "user-42"})
func NewAPIKeyAuth(keys map[string]string) *APIKeyAuthenticator {
	return stream.NewAPIKeyAuth(keys)
}

// hub := ws.NewHub()
func NewHub() *Hub {
	return stream.NewHub()
}

// hub := ws.NewHubWithConfig(stream.HubConfig{HeartbeatInterval: 30 * time.Second})
func NewHubWithConfig(config HubConfig) *Hub {
	return stream.NewHubWithConfig(config)
}

// config := ws.DefaultHubConfig()
func DefaultHubConfig() HubConfig {
	return stream.DefaultHubConfig()
}

// peer := ws.NewPeer("ws")
func NewPeer(transport string) *Peer {
	return stream.NewPeer(transport)
}

// stop := ws.Pipe(sourceHub, destinationHub)
func Pipe(source Stream, destination Stream) func() {
	return stream.Pipe(source, destination)
}
