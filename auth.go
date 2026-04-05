// SPDX-License-Identifier: EUPL-1.2

package stream

import (
	"net/http"

	"dappco.re/go/core"
)

//	authenticator := stream.AuthenticatorFunc(func(request *http.Request) stream.AuthResult {
//		return stream.AuthResult{Valid: true, UserID: "user-42"}
//	})
type Authenticator interface {
	Authenticate(request *http.Request) AuthResult
}

//	result := stream.AuthResult{
//		Valid:  true,
//		UserID: "user-42",
//		Claims: map[string]any{"role": "admin"},
//	}
type AuthResult struct {
	Valid bool

	UserID string

	Claims map[string]any

	Error error
}

//	authenticator := stream.AuthenticatorFunc(func(request *http.Request) stream.AuthResult {
//		return stream.AuthResult{Valid: true, UserID: "user-42"}
//	})
type AuthenticatorFunc func(request *http.Request) AuthResult

func (authenticatorFunc AuthenticatorFunc) Authenticate(request *http.Request) AuthResult {
	if authenticatorFunc == nil || request == nil {
		return AuthResult{Valid: false}
	}
	return authenticatorFunc(request)
}

// auth := stream.NewAPIKeyAuth(map[string]string{"sk-live": "user-42"})
// request := httptest.NewRequest("GET", "/stream/ws", nil)
// request.Header.Set("Authorization", "Bearer sk-live")
// result := auth.Authenticate(request)
type APIKeyAuthenticator struct {
	Keys map[string]string
}

// authenticator := stream.NewAPIKeyAuth(map[string]string{"sk-live": "user-42"})
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

// authenticator := stream.NewAPIKeyAuth(map[string]string{"sk-live": "user-42"})
// request.Header.Set("Authorization", "Bearer sk-live")
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
//		Validate: func(token string) stream.AuthResult {
//			if token == "sk-live" {
//				return stream.AuthResult{Valid: true, UserID: "user-42"}
//			}
//			return stream.AuthResult{Valid: false}
//		},
//	}
type BearerTokenAuth struct {
	Validate func(token string) AuthResult
}

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
//		Validate: func(token string) stream.AuthResult {
//			if token == "sk-live" {
//				return stream.AuthResult{Valid: true, UserID: "user-42"}
//			}
//			return stream.AuthResult{Valid: false}
//		},
//	}
type QueryTokenAuth struct {
	Validate func(token string) AuthResult
}

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
//		if string(handshake) == "hello" {
//			return stream.AuthResult{Valid: true, UserID: "peer-1"}
//		}
//		return stream.AuthResult{Valid: false}
//	})
type ConnAuthenticator interface {
	AuthenticateConn(handshake []byte) AuthResult
}

//	auth := stream.ConnAuthenticatorFunc(func(handshake []byte) stream.AuthResult {
//		if string(handshake) == "hello" {
//			return stream.AuthResult{Valid: true, UserID: "peer-1"}
//		}
//		return stream.AuthResult{Valid: false}
//	})
type ConnAuthenticatorFunc func(handshake []byte) AuthResult

func (connAuthenticatorFunc ConnAuthenticatorFunc) AuthenticateConn(handshake []byte) AuthResult {
	if connAuthenticatorFunc == nil {
		return AuthResult{Valid: false}
	}
	return connAuthenticatorFunc(handshake)
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
