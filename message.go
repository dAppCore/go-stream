// SPDX-License-Identifier: EUPL-1.2

package stream

import "time"

// messageType := stream.TypeEvent
type MessageType string

const (
	TypeProcessOutput MessageType = "process_output" // message := stream.Message{Type: stream.TypeProcessOutput, ProcessID: "build-123"}
	TypeProcessStatus MessageType = "process_status" // message := stream.Message{Type: stream.TypeProcessStatus, ProcessID: "build-123"}
	TypeEvent         MessageType = "event"          // message := stream.Message{Type: stream.TypeEvent, Channel: "hashrate"}
	TypeError         MessageType = "error"          // message := stream.Message{Type: stream.TypeError, Data: "unauthorised"}
	TypePing          MessageType = "ping"           // message := stream.Message{Type: stream.TypePing, ProcessID: "client-1"}
	TypePong          MessageType = "pong"           // reply := stream.Message{Type: stream.TypePong, ProcessID: "client-1"}
	TypeSubscribe     MessageType = "subscribe"      // message := stream.Message{Type: stream.TypeSubscribe, Channel: "block"}
	TypeUnsubscribe   MessageType = "unsubscribe"    // message := stream.Message{Type: stream.TypeUnsubscribe, Channel: "block"}
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
