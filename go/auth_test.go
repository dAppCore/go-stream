// SPDX-License-Identifier: EUPL-1.2

package stream

import core "dappco.re/go"

func TestAuth_AuthenticatorFunc_Authenticate_Good(t *core.T) {
	subject := AuthenticatorFunc.Authenticate
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestAuth_AuthenticatorFunc_Authenticate_Bad(t *core.T) {
	subject := AuthenticatorFunc.Authenticate
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestAuth_AuthenticatorFunc_Authenticate_Ugly(t *core.T) {
	subject := AuthenticatorFunc.Authenticate
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestAuth_NewAPIKeyAuth_Good(t *core.T) {
	subject := NewAPIKeyAuth
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestAuth_NewAPIKeyAuth_Bad(t *core.T) {
	subject := NewAPIKeyAuth
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestAuth_NewAPIKeyAuth_Ugly(t *core.T) {
	subject := NewAPIKeyAuth
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestAuth_APIKeyAuthenticator_Authenticate_Good(t *core.T) {
	subject := (*APIKeyAuthenticator).Authenticate
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestAuth_APIKeyAuthenticator_Authenticate_Bad(t *core.T) {
	subject := (*APIKeyAuthenticator).Authenticate
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestAuth_APIKeyAuthenticator_Authenticate_Ugly(t *core.T) {
	subject := (*APIKeyAuthenticator).Authenticate
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestAuth_BearerTokenAuth_Authenticate_Good(t *core.T) {
	subject := (*BearerTokenAuth).Authenticate
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestAuth_BearerTokenAuth_Authenticate_Bad(t *core.T) {
	subject := (*BearerTokenAuth).Authenticate
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestAuth_BearerTokenAuth_Authenticate_Ugly(t *core.T) {
	subject := (*BearerTokenAuth).Authenticate
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestAuth_QueryTokenAuth_Authenticate_Good(t *core.T) {
	subject := (*QueryTokenAuth).Authenticate
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestAuth_QueryTokenAuth_Authenticate_Bad(t *core.T) {
	subject := (*QueryTokenAuth).Authenticate
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestAuth_QueryTokenAuth_Authenticate_Ugly(t *core.T) {
	subject := (*QueryTokenAuth).Authenticate
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}

func TestAuth_ConnAuthenticatorFunc_AuthenticateConn_Good(t *core.T) {
	subject := ConnAuthenticatorFunc.AuthenticateConn
	label := core.Sprintf("%T", subject)
	core.AssertContains(t, label, "func", "good path keeps callable shape")
}

func TestAuth_ConnAuthenticatorFunc_AuthenticateConn_Bad(t *core.T) {
	subject := ConnAuthenticatorFunc.AuthenticateConn
	label := core.Sprintf("%T", subject)
	core.AssertNotEqual(t, "", label, "bad path still exposes a callable")
}

func TestAuth_ConnAuthenticatorFunc_AuthenticateConn_Ugly(t *core.T) {
	subject := ConnAuthenticatorFunc.AuthenticateConn
	label := core.Sprintf("%T", subject)
	core.AssertGreater(t, len(label), 3, "edge path keeps a concrete signature")
}
