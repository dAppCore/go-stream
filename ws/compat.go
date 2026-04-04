// SPDX-License-Identifier: EUPL-1.2

// Package ws preserves the legacy go-ws compatibility surface while the new
// transport-agnostic stream package does the actual work.
package ws

import (
	"net/http"
	"sync"

	"dappco.re/go/core"
	"dappco.re/go/stream"
	adapterredis "dappco.re/go/stream/adapter/redis"
	adapterws "dappco.re/go/stream/adapter/ws"
)

// Stream preserves the transport-agnostic stream interface for legacy callers.
type Stream = stream.Stream

// Frame preserves the legacy raw payload alias.
type Frame = stream.Frame

// Channel preserves the legacy channel name alias.
type Channel = stream.Channel

// HubConfig preserves the legacy go-ws HubConfig type name.
type HubConfig = stream.HubConfig

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

// Adapter preserves the legacy WebSocket adapter type name.
type Adapter = adapterws.Adapter

// Config preserves the legacy WebSocket adapter configuration type name.
type Config = adapterws.Config

// ReconnectConfig preserves the legacy reconnecting WebSocket configuration type name.
type ReconnectConfig = adapterws.ReconnectConfig

// RedisBridge preserves the legacy go-ws RedisBridge type name.
type RedisBridge = adapterredis.Bridge

// Hub preserves the legacy go-ws Hub surface while embedding the new stream hub.
//
//	hub := ws.NewHub()
//	go hub.Run(ctx)
//	http.Handle("/stream/ws", hub.Handler())
type Hub struct {
	*stream.Hub

	adapterOnce sync.Once
	adapter     *adapterws.Adapter
}

// NewRedisBridge creates the legacy Redis bridge wrapper.
func NewRedisBridge(hub any, config adapterredis.Config) (*RedisBridge, error) {
	switch typedHub := hub.(type) {
	case *Hub:
		if typedHub == nil {
			return adapterredis.NewBridge(nil, config)
		}
		return adapterredis.NewBridge(typedHub.Hub, config)
	case *stream.Hub:
		return adapterredis.NewBridge(typedHub, config)
	default:
		return nil, core.E("stream.ws", "unsupported hub type", nil)
	}
}

// NewAPIKeyAuth creates the legacy-compatible API key authenticator wrapper.
func NewAPIKeyAuth(keys map[string]string) *APIKeyAuthenticator {
	return stream.NewAPIKeyAuth(keys)
}

// NewHub creates a legacy-compatible hub.
func NewHub() *Hub {
	return &Hub{Hub: stream.NewHub()}
}

// NewHubWithConfig creates a legacy-compatible hub with explicit configuration.
func NewHubWithConfig(config HubConfig) *Hub {
	return &Hub{Hub: stream.NewHubWithConfig(config)}
}

// DefaultHubConfig returns the default hub configuration for legacy callers.
func DefaultHubConfig() HubConfig {
	return stream.DefaultHubConfig()
}

// NewPeer creates a legacy-compatible peer with a buffered send queue.
func NewPeer(transport string) *Peer {
	return stream.NewPeer(transport)
}

// Pipe preserves the legacy stream pipe composition helper.
func Pipe(src Stream, dst Stream) func() {
	return stream.Pipe(src, dst)
}

// New creates a legacy-compatible WebSocket adapter.
func New(config Config) *Adapter {
	return adapterws.New(config)
}

// NewReconnectingClient creates the legacy reconnecting WebSocket client.
func NewReconnectingClient(config ReconnectConfig) *adapterws.ReconnectingClient {
	return adapterws.NewReconnectingClient(config)
}

// Handler preserves the old hub-bound WebSocket handler entrypoint.
//
//	http.Handle("/stream/ws", hub.Handler())
func (hub *Hub) Handler() http.HandlerFunc {
	if hub == nil {
		return func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "stream hub not mounted", http.StatusInternalServerError)
		}
	}
	return hub.compatAdapter().Handler()
}

// HandlerForChannel preserves the old dedicated-channel handler entrypoint.
//
//	http.Handle("/stream/hashrate", hub.HandlerForChannel("hashrate"))
func (hub *Hub) HandlerForChannel(channel string) http.HandlerFunc {
	if hub == nil {
		return func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "stream hub not mounted", http.StatusInternalServerError)
		}
	}
	return hub.compatAdapter().HandlerForChannel(channel)
}

func (hub *Hub) compatAdapter() *adapterws.Adapter {
	hub.adapterOnce.Do(func() {
		adapter := adapterws.New(adapterws.Config{})
		adapter.Mount(hub.Hub)
		hub.adapter = adapter
	})
	return hub.adapter
}

var _ Stream = (*Hub)(nil)
