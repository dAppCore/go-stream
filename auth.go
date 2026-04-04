// SPDX-License-Identifier: EUPL-1.2

package stream

import (
	"net/http"

	"dappco.re/go/core"
)

// Authenticator checks an HTTP request during connection setup.
//
//	authenticator := stream.NewAPIKeyAuth(map[string]string{"sk-prod-1": "user-42"})
//	result := authenticator.Authenticate(request)
type Authenticator interface {
	Authenticate(request *http.Request) AuthResult
}

// AuthResult is the outcome of an authentication attempt.
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

//	authenticator := stream.AuthenticatorFunc(func(request *http.Request) stream.AuthResult {
//	    token := request.Header.Get("X-Api-Key")
//	    if token == "" {
//	        return stream.AuthResult{Valid: false}
//	    }
//	    return stream.AuthResult{Valid: true, UserID: lookupUser(token)}
//	})
type AuthenticatorFunc func(request *http.Request) AuthResult

//	authenticator := stream.AuthenticatorFunc(func(request *http.Request) stream.AuthResult {
//	    return stream.AuthResult{Valid: true, UserID: request.Header.Get("X-User")}
//	})
func (function AuthenticatorFunc) Authenticate(request *http.Request) AuthResult {
	if function == nil || request == nil {
		return AuthResult{Valid: false}
	}
	return function(request)
}

// APIKeyAuthenticator validates `Authorization: Bearer <key>` against a static map.
//
//	auth := stream.NewAPIKeyAuth(map[string]string{"sk-prod-1": "user-42"})
//	result := auth.Authenticate(r)
type APIKeyAuthenticator struct {
	Keys map[string]string
}

// authenticator := stream.NewAPIKeyAuth(map[string]string{"sk-prod-1": "user-42"})
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

// authenticator := stream.NewAPIKeyAuth(map[string]string{"sk-prod-1": "user-42"})
// request.Header.Set("Authorization", "Bearer sk-prod-1")
// result := authenticator.Authenticate(request)
func (authenticator *APIKeyAuthenticator) Authenticate(request *http.Request) AuthResult {
	if authenticator == nil || request == nil {
		return AuthResult{Valid: false}
	}
	token, result := bearerTokenFromRequest(request)
	if !result.Valid {
		return result
	}
	userID, ok := authenticator.Keys[token]
	if !ok {
		return AuthResult{Valid: false, Error: ErrInvalidAPIKey}
	}
	return AuthResult{Valid: true, UserID: userID}
}

//	authenticator := &stream.BearerTokenAuth{
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

// authenticator := &stream.BearerTokenAuth{Validate: validateJWT}
// result := authenticator.Authenticate(request)
func (authenticator *BearerTokenAuth) Authenticate(request *http.Request) AuthResult {
	if authenticator == nil || authenticator.Validate == nil || request == nil {
		return AuthResult{Valid: false}
	}
	token, result := bearerTokenFromRequest(request)
	if !result.Valid {
		return result
	}
	return authenticator.Validate(token)
}

//	authenticator := &stream.QueryTokenAuth{
//	    Validate: func(token string) stream.AuthResult {
//	        return lookupToken(token)
//	    },
//	}
type QueryTokenAuth struct {
	Validate func(token string) AuthResult
}

// authenticator := &stream.QueryTokenAuth{Validate: lookupToken}
// result := authenticator.Authenticate(request)
func (authenticator *QueryTokenAuth) Authenticate(request *http.Request) AuthResult {
	if authenticator == nil || authenticator.Validate == nil || request == nil {
		return AuthResult{Valid: false}
	}
	token := request.URL.Query().Get("token")
	if token == "" {
		return AuthResult{Valid: false}
	}
	return authenticator.Validate(token)
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
func (function ConnAuthenticatorFunc) AuthenticateConn(handshake []byte) AuthResult {
	if function == nil {
		return AuthResult{Valid: false}
	}
	return function(handshake)
}

func bearerTokenFromRequest(request *http.Request) (string, AuthResult) {
	header := request.Header.Get("Authorization")
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
