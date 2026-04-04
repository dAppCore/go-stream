// SPDX-License-Identifier: EUPL-1.2

package stream

import "net/http"

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
//	auth := stream.AuthenticatorFunc(func(r *http.Request) stream.AuthResult {
//	    token := r.Header.Get("X-Api-Key")
//	    if token == "" { return stream.AuthResult{Valid: false} }
//	    return stream.AuthResult{Valid: true, UserID: lookupUser(token)}
//	})
type AuthenticatorFunc func(r *http.Request) AuthResult

// Authenticate calls f(r).
func (f AuthenticatorFunc) Authenticate(r *http.Request) AuthResult {
	return f(r)
}

// APIKeyAuthenticator validates Authorization: Bearer <key> against a static map.
//
//	auth := stream.NewAPIKeyAuth(map[string]string{"sk-prod-1": "user-42"})
type APIKeyAuthenticator struct {
	Keys map[string]string
}

// NewAPIKeyAuth creates an API key authenticator from a key-to-user map.
//
//	auth := stream.NewAPIKeyAuth(map[string]string{"sk-prod-1": "user-42"})
func NewAPIKeyAuth(keys map[string]string) *APIKeyAuthenticator {
	return nil
}

// Authenticate validates the request's Authorization Bearer token against the key map.
func (a *APIKeyAuthenticator) Authenticate(r *http.Request) AuthResult {
	return AuthResult{}
}

// BearerTokenAuth delegates bearer token validation to a caller-supplied function.
//
//	auth := &stream.BearerTokenAuth{
//	    Validate: func(token string) stream.AuthResult {
//	        claims, err := jwt.Parse(token, keyFunc)
//	        if err != nil { return stream.AuthResult{Valid: false, Error: err} }
//	        return stream.AuthResult{Valid: true, UserID: claims.Subject}
//	    },
//	}
type BearerTokenAuth struct {
	Validate func(token string) AuthResult
}

// Authenticate extracts the Bearer token and delegates to Validate.
func (b *BearerTokenAuth) Authenticate(r *http.Request) AuthResult {
	return AuthResult{}
}

// QueryTokenAuth extracts a ?token= query parameter and validates via caller function.
// Use when browser clients cannot set headers (native WebSocket API).
//
//	auth := &stream.QueryTokenAuth{
//	    Validate: func(token string) stream.AuthResult { ... },
//	}
type QueryTokenAuth struct {
	Validate func(token string) AuthResult
}

// Authenticate extracts the token query parameter and delegates to Validate.
func (q *QueryTokenAuth) Authenticate(r *http.Request) AuthResult {
	return AuthResult{}
}

// ConnAuthenticator validates a raw connection handshake for TCP and ZMQ adapters.
// The handshake is the first message received on the connection (up to 4 KB).
//
//	auth := stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
//	    var h tcp.Handshake
//	    if r := core.JSONUnmarshal(handshake, &h); !r.OK {
//	        return stream.AuthResult{Valid: false}
//	    }
//	    return verifyHMAC(h.Token, h.Timestamp)
//	})
type ConnAuthenticator interface {
	AuthenticateConn(handshake []byte) AuthResult
}

// ConnAuthenticatorFunc adapts a plain function to ConnAuthenticator.
type ConnAuthenticatorFunc func(handshake []byte) AuthResult

// AuthenticateConn calls f(handshake).
func (f ConnAuthenticatorFunc) AuthenticateConn(handshake []byte) AuthResult {
	return f(handshake)
}
