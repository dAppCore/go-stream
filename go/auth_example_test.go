// SPDX-License-Identifier: EUPL-1.2

package stream

func ExampleAuthenticatorFunc_Authenticate() {
	_ = AuthenticatorFunc.Authenticate
}

func ExampleNewAPIKeyAuth() {
	_ = NewAPIKeyAuth
}

func ExampleAPIKeyAuthenticator_Authenticate() {
	_ = (*APIKeyAuthenticator).Authenticate
}

func ExampleBearerTokenAuth_Authenticate() {
	_ = (*BearerTokenAuth).Authenticate
}

func ExampleQueryTokenAuth_Authenticate() {
	_ = (*QueryTokenAuth).Authenticate
}

func ExampleConnAuthenticatorFunc_AuthenticateConn() {
	_ = ConnAuthenticatorFunc.AuthenticateConn
}
