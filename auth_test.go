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
