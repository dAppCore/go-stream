// SPDX-License-Identifier: EUPL-1.2

package stream

import "dappco.re/go/core"

// Sentinel errors for the stream package. All errors use core.E().
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

	// ErrHubNotRunning is returned when Publish or Broadcast is called before Run.
	ErrHubNotRunning = core.E("stream.hub", "hub not running", nil)

	// ErrEmptyChannel is returned when Subscribe is called with an empty channel name.
	ErrEmptyChannel = core.E("stream.hub", "empty channel", nil)
)
