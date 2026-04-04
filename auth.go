// SPDX-License-Identifier: EUPL-1.2

package stream

import (
	"net/http"

	"dappco.re/go/core"
)

// Authenticator validates an HTTP request during the WebSocket upgrade or SSE
// connection.
//
//	auth := stream.NewAPIKeyAuth(map[string]string{"sk-prod-1": "user-42"})
//	result := auth.Authenticate(r)
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

//	auth := stream.AuthenticatorFunc(func(r *http.Request) stream.AuthResult {
//	    token := r.Header.Get("X-Api-Key")
//	    if token == "" {
//	        return stream.AuthResult{Valid: false}
//	    }
//	    return stream.AuthResult{Valid: true, UserID: lookupUser(token)}
//	})
type AuthenticatorFunc func(r *http.Request) AuthResult

//	auth := stream.AuthenticatorFunc(func(r *http.Request) stream.AuthResult {
//	    return stream.AuthResult{Valid: true, UserID: r.Header.Get("X-User")}
//	})
func (f AuthenticatorFunc) Authenticate(r *http.Request) AuthResult {
	if f == nil || r == nil {
		return AuthResult{Valid: false}
	}
	return f(r)
}

// APIKeyAuthenticator validates `Authorization: Bearer <key>` against a static map.
//
//	auth := stream.NewAPIKeyAuth(map[string]string{"sk-prod-1": "user-42"})
//	result := auth.Authenticate(r)
type APIKeyAuthenticator struct {
	Keys map[string]string
}

// auth := stream.NewAPIKeyAuth(map[string]string{"sk-prod-1": "user-42"})
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

// auth := stream.NewAPIKeyAuth(map[string]string{"sk-prod-1": "user-42"})
// result := auth.Authenticate(r)
func (a *APIKeyAuthenticator) Authenticate(r *http.Request) AuthResult {
	if a == nil || r == nil {
		return AuthResult{Valid: false}
	}
	token, result := bearerTokenFromRequest(r)
	if !result.Valid {
		return result
	}
	userID, ok := a.Keys[token]
	if !ok {
		return AuthResult{Valid: false, Error: ErrInvalidAPIKey}
	}
	return AuthResult{Valid: true, UserID: userID}
}

//	auth := &stream.BearerTokenAuth{
//	    Validate: func(token string) stream.AuthResult {
//	        claims, err := jwt.Parse(token, keyFunc)
//	        if err != nil {
//	            return stream.AuthResult{Valid: false, Error: err}
//	        }
//	        return stream.AuthResult{Valid: true, UserID: claims.Subject}
//	    },
//	}
type BearerTokenAuth struct {
	Validate func(token string) AuthResult
}

// auth := &stream.BearerTokenAuth{Validate: validateJWT}
// result := auth.Authenticate(r)
func (b *BearerTokenAuth) Authenticate(r *http.Request) AuthResult {
	if b == nil || b.Validate == nil || r == nil {
		return AuthResult{Valid: false}
	}
	token, result := bearerTokenFromRequest(r)
	if !result.Valid {
		return result
	}
	return b.Validate(token)
}

//	auth := &stream.QueryTokenAuth{
//	    Validate: func(token string) stream.AuthResult {
//	        return lookupToken(token)
//	    },
//	}
type QueryTokenAuth struct {
	Validate func(token string) AuthResult
}

// auth := &stream.QueryTokenAuth{Validate: lookupToken}
// result := auth.Authenticate(r)
func (q *QueryTokenAuth) Authenticate(r *http.Request) AuthResult {
	if q == nil || q.Validate == nil || r == nil {
		return AuthResult{Valid: false}
	}
	token := r.URL.Query().Get("token")
	if token == "" {
		return AuthResult{Valid: false}
	}
	return q.Validate(token)
}

//	auth := stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
//	    if len(handshake) == 0 {
//	        return stream.AuthResult{Valid: false}
//	    }
//	    return stream.AuthResult{Valid: true, UserID: "peer-1"}
//	})
type ConnAuthenticator interface {
	AuthenticateConn(handshake []byte) AuthResult
}

//	auth := stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
//	    return stream.AuthResult{Valid: true}
//	})
type ConnAuthenticatorFunc func(handshake []byte) AuthResult

//	auth := stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
//	    return stream.AuthResult{Valid: true}
//	})
func (f ConnAuthenticatorFunc) AuthenticateConn(handshake []byte) AuthResult {
	if f == nil {
		return AuthResult{Valid: false}
	}
	return f(handshake)
}

func bearerTokenFromRequest(r *http.Request) (string, AuthResult) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return "", AuthResult{Valid: false, Error: ErrMissingAuthHeader}
	}
	if !core.HasPrefix(header, "Bearer ") {
		return "", AuthResult{Valid: false, Error: ErrMalformedAuthHeader}
	}
	token := core.TrimPrefix(header, "Bearer ")
	if token == "" {
		return "", AuthResult{Valid: false, Error: ErrMalformedAuthHeader}
	}
	return token, AuthResult{Valid: true}
}
