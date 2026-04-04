// SPDX-License-Identifier: EUPL-1.2

package stream

import "time"

// MessageType is used with stream.Message for WebSocket envelopes.
//
//	msg := stream.Message{Type: stream.TypeEvent, Channel: "hashrate"}
type MessageType string

const (
	TypeProcessOutput MessageType = "process_output" // real-time process output line
	TypeProcessStatus MessageType = "process_status" // process status change (running/exited)
	TypeEvent         MessageType = "event"          // generic named event
	TypeError         MessageType = "error"          // error message
	TypePing          MessageType = "ping"           // client → server keepalive
	TypePong          MessageType = "pong"           // server → client keepalive response
	TypeSubscribe     MessageType = "subscribe"      // client requests channel subscription
	TypeUnsubscribe   MessageType = "unsubscribe"    // client cancels channel subscription
)

// Message is the JSON envelope for WebSocket frames.
//
//	msg := stream.Message{
//	    Type:    stream.TypeEvent,
//	    Channel: "hashrate",
//	    Data:    map[string]any{"h": 1234567},
//	}
type Message struct {
	Type      MessageType `json:"type"`
	Channel   string      `json:"channel,omitempty"`
	ProcessID string      `json:"processId,omitempty"`
	Data      any         `json:"data,omitempty"`
	Timestamp time.Time   `json:"timestamp"`
}
