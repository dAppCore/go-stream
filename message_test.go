// SPDX-License-Identifier: EUPL-1.2

package stream

import "testing"

func TestMessageType_String_Good(t *testing.T) {
	if TypeEvent.String() != "event" {
		t.Fatalf("TypeEvent.String() = %q, want %q", TypeEvent.String(), "event")
	}
	if TypeSubscribe.String() != "subscribe" {
		t.Fatalf("TypeSubscribe.String() = %q, want %q", TypeSubscribe.String(), "subscribe")
	}
}
