// SPDX-License-Identifier: EUPL-1.2

// Behaviour-level tests for the SSE adapter: handshake headers, auth
// gating, hub-not-mounted / not-running guards, event-frame writing,
// and clean teardown on client disconnect.
package sse

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"time"

	core "dappco.re/go"
	"dappco.re/go/stream"
)

// runHub starts a hub and returns it with a stop function.
//
//	hub, stop := runHub(t)
//	defer stop()
func runHub(t *core.T) (*stream.Hub, func()) {
	t.Helper()
	hub := stream.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	go hub.Run(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for !hub.Running() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !hub.Running() {
		t.Fatalf("hub did not start")
	}
	return hub, cancel
}

func TestSse_NewDefaults_Behaviour(t *core.T) {
	adapter := New(Config{})
	core.AssertEqual(t, 15*time.Second, adapter.config.HeartbeatInterval, "heartbeat defaulted")
	core.AssertEqual(t, 3000, adapter.config.RetryMs, "retry defaulted")

	custom := New(Config{HeartbeatInterval: 5 * time.Second, RetryMs: 1000})
	core.AssertEqual(t, 5*time.Second, custom.config.HeartbeatInterval, "heartbeat honoured")
	core.AssertEqual(t, 1000, custom.config.RetryMs, "retry honoured")
}

func TestSse_HubNotMounted_Behaviour(t *core.T) {
	adapter := New(Config{})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/stream/events", nil)
	adapter.ServeHTTP(recorder, request)
	core.AssertEqual(t, http.StatusInternalServerError, recorder.Code, "no hub mounted returns 500")
}

func TestSse_AuthRejected_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	var failureSeen bool
	adapter := New(Config{
		Authenticator: stream.NewAPIKeyAuth(map[string]string{"sk-live": "user-42"}),
		OnAuthFailure: func(*http.Request, stream.AuthResult) { failureSeen = true },
	})
	adapter.Mount(hub)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/stream/events?channel=hashrate", nil)
	adapter.ServeHTTP(recorder, request)
	core.AssertEqual(t, http.StatusUnauthorized, recorder.Code, "missing key returns 401")
	core.AssertTrue(t, failureSeen, "OnAuthFailure invoked")
}

func TestSse_StreamsEventFrame_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()

	adapter := New(Config{HeartbeatInterval: time.Hour})
	adapter.Mount(hub)

	server := httptest.NewServer(adapter.HandlerForChannel("hashrate"))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	response, err := http.DefaultClient.Do(request)
	core.AssertNoError(t, err, "request opens")
	defer response.Body.Close()

	core.AssertEqual(t, "text/event-stream", response.Header.Get("Content-Type"), "sse content type")
	core.AssertEqual(t, "no-cache", response.Header.Get("Cache-Control"), "no-cache header")

	reader := bufio.NewReader(response.Body)
	// First line is the retry directive.
	retryLine, _ := reader.ReadString('\n')
	core.AssertContains(t, retryLine, "retry:", "retry directive sent first")

	// Wait for the peer to be registered, then publish a frame.
	deadline := time.Now().Add(2 * time.Second)
	for hub.ChannelSubscriberCount("hashrate") == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	hub.Publish("hashrate", []byte("payload"))

	var sawData bool
	for {
		line, readErr := reader.ReadString('\n')
		if core.Contains(line, "payload") {
			sawData = true
			break
		}
		if readErr != nil {
			break
		}
	}
	core.AssertTrue(t, sawData, "published frame delivered as SSE data line")
}

func TestSse_ForbiddenChannel_Behaviour(t *core.T) {
	hub := stream.NewHubWithConfig(stream.HubConfig{
		ChannelAuthoriser: func(*stream.Peer, string) bool { return false },
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for !hub.Running() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	adapter := New(Config{})
	adapter.Mount(hub)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/stream/events?channel=secret", nil)
	adapter.ServeHTTP(recorder, request)
	core.AssertEqual(t, http.StatusForbidden, recorder.Code, "unauthorised channel returns 403")
}

func TestSse_StreamingUnsupported_Behaviour(t *core.T) {
	hub, stop := runHub(t)
	defer stop()
	adapter := New(Config{})
	adapter.Mount(hub)

	// nonFlusher implements ResponseWriter but not http.Flusher.
	recorder := &nonFlusher{header: http.Header{}}
	request := httptest.NewRequest(http.MethodGet, "/stream/events", nil)
	adapter.ServeHTTP(recorder, request)
	core.AssertEqual(t, http.StatusInternalServerError, recorder.status, "non-flusher writer returns 500")
}

// nonFlusher is a ResponseWriter that does NOT implement http.Flusher,
// to exercise the streaming-unsupported guard.
//
//	w := &nonFlusher{header: http.Header{}}
type nonFlusher struct {
	header http.Header
	status int
}

func (w *nonFlusher) Header() http.Header { return w.header }
func (w *nonFlusher) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return len(b), nil
}
func (w *nonFlusher) WriteHeader(status int) { w.status = status }
