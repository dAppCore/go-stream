// SPDX-License-Identifier: EUPL-1.2

// Package ws preserves the legacy go-ws compatibility surface while the new
// transport-agnostic stream package does the actual work.
package ws

import (
	"dappco.re/go/stream"
	"dappco.re/go/stream/adapter/redis"
)

// Hub preserves the legacy go-ws Hub type name.
type Hub = stream.Hub

// HubConfig preserves the legacy go-ws HubConfig type name.
type HubConfig = stream.HubConfig

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

// ConnAuthenticator preserves the legacy raw-connection authenticator name.
type ConnAuthenticator = stream.ConnAuthenticator

// ConnAuthenticatorFunc preserves the legacy raw-connection helper name.
type ConnAuthenticatorFunc = stream.ConnAuthenticatorFunc

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
)

// RedisBridge preserves the legacy go-ws RedisBridge type name.
type RedisBridge = redis.Bridge

// NewRedisBridge creates the legacy Redis bridge wrapper.
func NewRedisBridge(hub *stream.Hub, config redis.Config) (*RedisBridge, error) {
	return redis.NewBridge(hub, config)
}

// NewHub creates a legacy-compatible hub.
func NewHub() *Hub {
	return stream.NewHub()
}

// NewHubWithConfig creates a legacy-compatible hub with explicit configuration.
func NewHubWithConfig(config HubConfig) *Hub {
	return stream.NewHubWithConfig(config)
}

// DefaultHubConfig returns the default hub configuration for legacy callers.
func DefaultHubConfig() HubConfig {
	return stream.DefaultHubConfig()
}
