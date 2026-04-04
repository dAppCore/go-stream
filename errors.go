// SPDX-License-Identifier: EUPL-1.2

package stream

// Sentinel errors for the stream package. All errors use core.E().
//
// TODO: Replace with core.E() calls once dappco.re/go/core is wired.
var (
	// ErrMissingAuthHeader is returned when no Authorization header is present.
	ErrMissingAuthHeader error

	// ErrMalformedAuthHeader is returned when the header is not "Bearer <token>".
	ErrMalformedAuthHeader error

	// ErrInvalidAPIKey is returned when the API key is not in the key map.
	ErrInvalidAPIKey error

	// ErrHandshakeTimeout is returned when the TCP/ZMQ peer did not send a
	// handshake within the configured deadline.
	ErrHandshakeTimeout error

	// ErrAuthRejected is returned when ConnAuthenticator denies the handshake.
	ErrAuthRejected error

	// ErrHubNotRunning is returned when Publish or Broadcast is called before Run.
	ErrHubNotRunning error

	// ErrEmptyChannel is returned when Subscribe is called with an empty channel name.
	ErrEmptyChannel error
)
