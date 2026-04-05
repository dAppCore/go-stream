// SPDX-License-Identifier: EUPL-1.2

package stream

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuth_APIKeyAuthenticator_ClaimsInitialized_Good(t *testing.T) {
	authenticator := NewAPIKeyAuth(map[string]string{"sk-live": "user-42"})
	request := httptest.NewRequest(http.MethodGet, "/stream/ws", nil)
	request.Header.Set("Authorization", "Bearer sk-live")

	result := authenticator.Authenticate(request)
	if !result.Valid {
		t.Fatal("Authenticate() result.Valid = false, want true")
	}
	if result.Claims == nil {
		t.Fatal("Authenticate() result.Claims = nil, want empty map")
	}
	if len(result.Claims) != 0 {
		t.Fatalf("len(Authenticate().Claims) = %d, want 0", len(result.Claims))
	}
}

func TestAuth_AuthenticatorFunc_ClaimsInitialized_Good(t *testing.T) {
	authenticator := AuthenticatorFunc(func(request *http.Request) AuthResult {
		return AuthResult{Valid: true, UserID: "user-42"}
	})
	request := httptest.NewRequest(http.MethodGet, "/stream/ws", nil)

	result := authenticator.Authenticate(request)
	if !result.Valid {
		t.Fatal("Authenticate() result.Valid = false, want true")
	}
	if result.Claims == nil {
		t.Fatal("Authenticate() result.Claims = nil, want empty map")
	}
	if len(result.Claims) != 0 {
		t.Fatalf("len(Authenticate().Claims) = %d, want 0", len(result.Claims))
	}
}

func TestAuth_ConnAuthenticatorFunc_ClaimsInitialized_Good(t *testing.T) {
	authenticator := ConnAuthenticatorFunc(func(handshake []byte) AuthResult {
		return AuthResult{Valid: true, UserID: "peer-1"}
	})

	result := authenticator.AuthenticateConn([]byte("hello"))
	if !result.Valid {
		t.Fatal("AuthenticateConn() result.Valid = false, want true")
	}
	if result.Claims == nil {
		t.Fatal("AuthenticateConn() result.Claims = nil, want empty map")
	}
	if len(result.Claims) != 0 {
		t.Fatalf("len(AuthenticateConn().Claims) = %d, want 0", len(result.Claims))
	}
}

func TestAuth_APIKeyAuthenticator_Bad(t *testing.T) {
	authenticator := NewAPIKeyAuth(map[string]string{"sk-live": "user-42"})

	// Missing Authorization header.
	request := httptest.NewRequest(http.MethodGet, "/stream/ws", nil)
	result := authenticator.Authenticate(request)
	if result.Valid {
		t.Fatal("Authenticate() without header: result.Valid = true, want false")
	}
	if result.Error != ErrMissingAuthHeader {
		t.Fatalf("Authenticate() without header: error = %v, want %v", result.Error, ErrMissingAuthHeader)
	}

	// Malformed Authorization header (not "Bearer <token>").
	request = httptest.NewRequest(http.MethodGet, "/stream/ws", nil)
	request.Header.Set("Authorization", "Basic sk-live")
	result = authenticator.Authenticate(request)
	if result.Valid {
		t.Fatal("Authenticate() with Basic scheme: result.Valid = true, want false")
	}
	if result.Error != ErrMalformedAuthHeader {
		t.Fatalf("Authenticate() with Basic scheme: error = %v, want %v", result.Error, ErrMalformedAuthHeader)
	}

	// Unknown API key.
	request = httptest.NewRequest(http.MethodGet, "/stream/ws", nil)
	request.Header.Set("Authorization", "Bearer sk-unknown")
	result = authenticator.Authenticate(request)
	if result.Valid {
		t.Fatal("Authenticate() with unknown key: result.Valid = true, want false")
	}
	if result.Error != ErrInvalidAPIKey {
		t.Fatalf("Authenticate() with unknown key: error = %v, want %v", result.Error, ErrInvalidAPIKey)
	}
}

func TestAuth_APIKeyAuthenticator_Ugly(t *testing.T) {
	// Nil authenticator returns invalid result without panic.
	var authenticator *APIKeyAuthenticator
	request := httptest.NewRequest(http.MethodGet, "/stream/ws", nil)
	request.Header.Set("Authorization", "Bearer sk-live")

	result := authenticator.Authenticate(request)
	if result.Valid {
		t.Fatal("nil authenticator: result.Valid = true, want false")
	}

	// Nil request returns invalid result without panic.
	validAuth := NewAPIKeyAuth(map[string]string{"sk-live": "user-42"})
	result = validAuth.Authenticate(nil)
	if result.Valid {
		t.Fatal("nil request: result.Valid = true, want false")
	}
}

func TestAuth_BearerTokenAuth_Good(t *testing.T) {
	authenticator := &BearerTokenAuth{
		Validate: func(token string) AuthResult {
			if token == "jwt-valid" {
				return AuthResult{Valid: true, UserID: "user-99", Claims: map[string]any{"role": "admin"}}
			}
			return AuthResult{Valid: false}
		},
	}

	request := httptest.NewRequest(http.MethodGet, "/stream/ws", nil)
	request.Header.Set("Authorization", "Bearer jwt-valid")
	result := authenticator.Authenticate(request)
	if !result.Valid {
		t.Fatal("Authenticate() result.Valid = false, want true")
	}
	if result.UserID != "user-99" {
		t.Fatalf("result.UserID = %q, want %q", result.UserID, "user-99")
	}
	if result.Claims["role"] != "admin" {
		t.Fatalf("result.Claims[role] = %v, want %q", result.Claims["role"], "admin")
	}
}

func TestAuth_BearerTokenAuth_Bad(t *testing.T) {
	authenticator := &BearerTokenAuth{
		Validate: func(token string) AuthResult {
			return AuthResult{Valid: false}
		},
	}

	// Valid header but rejected by validator.
	request := httptest.NewRequest(http.MethodGet, "/stream/ws", nil)
	request.Header.Set("Authorization", "Bearer bad-token")
	result := authenticator.Authenticate(request)
	if result.Valid {
		t.Fatal("Authenticate() with rejected token: result.Valid = true, want false")
	}
}

func TestAuth_BearerTokenAuth_Ugly(t *testing.T) {
	// Nil Validate function returns invalid without panic.
	authenticator := &BearerTokenAuth{}
	request := httptest.NewRequest(http.MethodGet, "/stream/ws", nil)
	request.Header.Set("Authorization", "Bearer test")
	result := authenticator.Authenticate(request)
	if result.Valid {
		t.Fatal("nil Validate: result.Valid = true, want false")
	}
}

func TestAuth_QueryTokenAuth_Good(t *testing.T) {
	authenticator := &QueryTokenAuth{
		Validate: func(token string) AuthResult {
			if token == "ws-token-1" {
				return AuthResult{Valid: true, UserID: "browser-user"}
			}
			return AuthResult{Valid: false}
		},
	}

	request := httptest.NewRequest(http.MethodGet, "/stream/ws?token=ws-token-1", nil)
	result := authenticator.Authenticate(request)
	if !result.Valid {
		t.Fatal("Authenticate() result.Valid = false, want true")
	}
	if result.UserID != "browser-user" {
		t.Fatalf("result.UserID = %q, want %q", result.UserID, "browser-user")
	}
}

func TestAuth_QueryTokenAuth_Bad(t *testing.T) {
	authenticator := &QueryTokenAuth{
		Validate: func(token string) AuthResult {
			return AuthResult{Valid: false}
		},
	}

	// Missing token query parameter.
	request := httptest.NewRequest(http.MethodGet, "/stream/ws", nil)
	result := authenticator.Authenticate(request)
	if result.Valid {
		t.Fatal("Authenticate() without token param: result.Valid = true, want false")
	}
}

func TestAuth_QueryTokenAuth_Ugly(t *testing.T) {
	// Nil Validate function returns invalid without panic.
	authenticator := &QueryTokenAuth{}
	request := httptest.NewRequest(http.MethodGet, "/stream/ws?token=test", nil)
	result := authenticator.Authenticate(request)
	if result.Valid {
		t.Fatal("nil Validate: result.Valid = true, want false")
	}

	// Nil authenticator returns invalid without panic.
	var nilAuth AuthenticatorFunc
	result = nilAuth.Authenticate(httptest.NewRequest(http.MethodGet, "/stream/ws", nil))
	if result.Valid {
		t.Fatal("nil AuthenticatorFunc: result.Valid = true, want false")
	}
}

func TestAuth_ConnAuthenticatorFunc_Bad(t *testing.T) {
	authenticator := ConnAuthenticatorFunc(func(handshake []byte) AuthResult {
		return AuthResult{Valid: false}
	})

	result := authenticator.AuthenticateConn([]byte("invalid-handshake"))
	if result.Valid {
		t.Fatal("AuthenticateConn() with invalid handshake: result.Valid = true, want false")
	}
}

func TestAuth_ConnAuthenticatorFunc_Ugly(t *testing.T) {
	// Nil ConnAuthenticatorFunc returns invalid without panic.
	var authenticator ConnAuthenticatorFunc
	result := authenticator.AuthenticateConn([]byte("hello"))
	if result.Valid {
		t.Fatal("nil ConnAuthenticatorFunc: result.Valid = true, want false")
	}
}
