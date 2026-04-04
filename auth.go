// SPDX-License-Identifier: EUPL-1.2

package stream

import (
	"net/http"

	"dappco.re/go/core"
)

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
	if keys == nil {
		keys = map[string]string{}
	}
	copied := make(map[string]string, len(keys))
	for key, userID := range keys {
		copied[key] = userID
	}
	return &APIKeyAuthenticator{Keys: copied}
}

// Authenticate validates the request's Authorization Bearer token against the key map.
func (a *APIKeyAuthenticator) Authenticate(r *http.Request) AuthResult {
	if a == nil {
		return AuthResult{Valid: false}
	}
	header := r.Header.Get("Authorization")
	if header == "" {
		return AuthResult{Valid: false, Error: ErrMissingAuthHeader}
	}
	if !core.HasPrefix(header, "Bearer ") {
		return AuthResult{Valid: false, Error: ErrMalformedAuthHeader}
	}
	token := core.TrimPrefix(header, "Bearer ")
	if token == "" {
		return AuthResult{Valid: false, Error: ErrMalformedAuthHeader}
	}
	userID, ok := a.Keys[token]
	if !ok {
		return AuthResult{Valid: false, Error: ErrInvalidAPIKey}
	}
	return AuthResult{Valid: true, UserID: userID}
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
	if b == nil || b.Validate == nil {
		return AuthResult{Valid: false}
	}
	header := r.Header.Get("Authorization")
	if header == "" {
		return AuthResult{Valid: false, Error: ErrMissingAuthHeader}
	}
	if !core.HasPrefix(header, "Bearer ") {
		return AuthResult{Valid: false, Error: ErrMalformedAuthHeader}
	}
	token := core.TrimPrefix(header, "Bearer ")
	if token == "" {
		return AuthResult{Valid: false, Error: ErrMalformedAuthHeader}
	}
	return b.Validate(token)
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
	if q == nil || q.Validate == nil {
		return AuthResult{Valid: false}
	}
	token := r.URL.Query().Get("token")
	if token == "" {
		return AuthResult{Valid: false}
	}
	return q.Validate(token)
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
