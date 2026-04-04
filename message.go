// SPDX-License-Identifier: EUPL-1.2

package stream

import "time"

// messageType := stream.TypeEvent
type MessageType string

const (
	TypeProcessOutput MessageType = "process_output" // stream a process line to clients
	TypeProcessStatus MessageType = "process_status" // signal a process transition such as running or exited
	TypeEvent         MessageType = "event"          // generic named event payload
	TypeError         MessageType = "error"          // report an error envelope
	TypePing          MessageType = "ping"           // client keepalive ping
	TypePong          MessageType = "pong"           // server keepalive pong
	TypeSubscribe     MessageType = "subscribe"      // request subscription to a channel
	TypeUnsubscribe   MessageType = "unsubscribe"    // cancel a channel subscription
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
