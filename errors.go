// SPDX-License-Identifier: EUPL-1.2

package stream

import "dappco.re/go"

//	if err := hub.Publish("hashrate", frame); err == ErrHubNotRunning {
//	    return
//	}
var (
	// if err := auth.Authenticate(request); err == stream.ErrMissingAuthHeader {
	//     http.Error(w, "missing auth", http.StatusUnauthorized)
	// }
	ErrMissingAuthHeader = core.E("stream.auth", "missing Authorization header", nil)

	// if err := auth.Authenticate(request); err == stream.ErrMalformedAuthHeader {
	//     http.Error(w, "bad auth header", http.StatusUnauthorized)
	// }
	ErrMalformedAuthHeader = core.E("stream.auth", "malformed Authorization header", nil)

	// if err := auth.Authenticate(request); err == stream.ErrInvalidAPIKey {
	//     http.Error(w, "unknown key", http.StatusUnauthorized)
	// }
	ErrInvalidAPIKey = core.E("stream.auth", "invalid API key", nil)

	// if err := adapter.Listen(ctx); err == stream.ErrHandshakeTimeout {
	//     return
	// }
	ErrHandshakeTimeout = core.E("stream.auth", "handshake timeout", nil)

	// if err := adapter.Listen(ctx); err == stream.ErrAuthRejected {
	//     return
	// }
	ErrAuthRejected = core.E("stream.auth", "connection rejected by authenticator", nil)

	// if err := hub.Publish("hashrate", frame); err == stream.ErrHubNotRunning {
	//     go hub.Run(ctx)
	// }
	ErrHubNotRunning = core.E("stream.hub", "hub not running", nil)

	// if _, err := hub.SubscribeE("", func([]byte) {}); err == stream.ErrEmptyChannel {
	//     return
	// }
	ErrEmptyChannel = core.E("stream.hub", "empty channel", nil)
)
