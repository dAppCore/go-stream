// SPDX-License-Identifier: EUPL-1.2

package stream

import (
	"testing"
	"time"
)

func TestAX7_MessageType_String_Good(t *testing.T) {
	cases := []struct {
		messageType MessageType
		expected    string
	}{
		{TypeProcessOutput, "process_output"},
		{TypeProcessStatus, "process_status"},
		{TypeEvent, "event"},
		{TypeError, "error"},
		{TypePing, "ping"},
		{TypePong, "pong"},
		{TypeSubscribe, "subscribe"},
		{TypeUnsubscribe, "unsubscribe"},
	}
	for _, testCase := range cases {
		if testCase.messageType.String() != testCase.expected {
			t.Fatalf("%q.String() = %q, want %q", testCase.messageType, testCase.messageType.String(), testCase.expected)
		}
	}
}

func TestAX7_MessageType_String_Bad(t *testing.T) {
	// Unknown MessageType returns its raw string value.
	unknown := MessageType("nonexistent")
	if unknown.String() != "nonexistent" {
		t.Fatalf("unknown MessageType.String() = %q, want %q", unknown.String(), "nonexistent")
	}
}

func TestAX7_MessageType_String_Ugly(t *testing.T) {
	// Empty MessageType returns empty string.
	empty := MessageType("")
	if empty.String() != "" {
		t.Fatalf("empty MessageType.String() = %q, want %q", empty.String(), "")
	}
}

func TestMessage_Fields_Good(t *testing.T) {
	timestamp := time.Date(2026, 4, 5, 12, 0, 0, 0, time.UTC)
	message := Message{
		Type:      TypeEvent,
		Channel:   "hashrate",
		ProcessID: "agent-42",
		Data:      map[string]any{"h": 1234567},
		Timestamp: timestamp,
	}
	if message.Type != TypeEvent {
		t.Fatalf("message.Type = %q, want %q", message.Type, TypeEvent)
	}
	if message.Channel != "hashrate" {
		t.Fatalf("message.Channel = %q, want %q", message.Channel, "hashrate")
	}
	if message.ProcessID != "agent-42" {
		t.Fatalf("message.ProcessID = %q, want %q", message.ProcessID, "agent-42")
	}
	if message.Timestamp != timestamp {
		t.Fatalf("message.Timestamp = %v, want %v", message.Timestamp, timestamp)
	}
}

func TestMessage_Fields_Bad(t *testing.T) {
	// Zero-value Message has empty fields — no panic.
	message := Message{}
	if message.Type != "" {
		t.Fatalf("zero Message.Type = %q, want empty", message.Type)
	}
	if message.Channel != "" {
		t.Fatalf("zero Message.Channel = %q, want empty", message.Channel)
	}
	if message.Timestamp.IsZero() != true {
		t.Fatal("zero Message.Timestamp.IsZero() = false, want true")
	}
}

func TestMessage_Fields_Ugly(t *testing.T) {
	// Message with nil Data does not panic on access.
	message := Message{Type: TypeError, Data: nil}
	if message.Data != nil {
		t.Fatalf("Message.Data = %v, want nil", message.Data)
	}
}
