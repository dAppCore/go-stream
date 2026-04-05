// SPDX-License-Identifier: EUPL-1.2

package stream

import "time"

// messageType := stream.TypeEvent
type MessageType string

// String returns the canonical wire value for the message type.
func (messageType MessageType) String() string {
	return string(messageType)
}

const (
	// message := stream.Message{Type: stream.TypeProcessOutput, ProcessID: "build-123"}
	TypeProcessOutput MessageType = "process_output"
	// message := stream.Message{Type: stream.TypeProcessStatus, ProcessID: "build-123"}
	TypeProcessStatus MessageType = "process_status"
	// message := stream.Message{Type: stream.TypeEvent, Channel: "hashrate"}
	TypeEvent MessageType = "event"
	// message := stream.Message{Type: stream.TypeError, Data: "unauthorised"}
	TypeError MessageType = "error"
	// message := stream.Message{Type: stream.TypePing, ProcessID: "client-1"}
	TypePing MessageType = "ping"
	// reply := stream.Message{Type: stream.TypePong, ProcessID: "client-1"}
	TypePong MessageType = "pong"
	// message := stream.Message{Type: stream.TypeSubscribe, Channel: "block"}
	TypeSubscribe MessageType = "subscribe"
	// message := stream.Message{Type: stream.TypeUnsubscribe, Channel: "block"}
	TypeUnsubscribe MessageType = "unsubscribe"
)

//	msg := stream.Message{
//	    Type:      stream.TypeEvent,
//	    Channel:   "hashrate",
//	    Data:      map[string]any{"h": 1234567},
//	    Timestamp: time.Now().UTC(),
//	}
//
// frame, _ := core.JSONMarshal(msg)
// _ = frame
type Message struct {
	Type      MessageType `json:"type"`
	Channel   string      `json:"channel,omitempty"`
	ProcessID string      `json:"processId,omitempty"`
	Data      any         `json:"data,omitempty"`
	Timestamp time.Time   `json:"timestamp"`
}
