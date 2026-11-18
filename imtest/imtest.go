// Package imtest provides offline HTTP and WebSocket test harnesses for channel adapters.
package imtest

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"
)

// RoundTripFunc adapts a function to http.RoundTripper.
type RoundTripFunc func(req *http.Request) (*http.Response, error)

func (f RoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// Client wraps a RoundTripFunc into an *http.Client that never touches the network.
func Client(fn RoundTripFunc) *http.Client {
	return &http.Client{Transport: fn}
}

// JSONResponse builds an *http.Response with a JSON-encoded body and optional header pairs.
func JSONResponse(status int, body any, headerKV ...string) *http.Response {
	var raw []byte
	switch v := body.(type) {
	case string:
		raw = []byte(v)
	case []byte:
		raw = v
	default:
		raw, _ = json.Marshal(body)
	}
	h := make(http.Header)
	h.Set("Content-Type", "application/json; charset=utf-8")
	for i := 0; i+1 < len(headerKV); i += 2 {
		h.Set(headerKV[i], headerKV[i+1])
	}
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     h,
		Body:       io.NopCloser(bytes.NewReader(raw)),
	}
}

// BytesResponse builds an *http.Response with raw bytes and optional header pairs.
func BytesResponse(status int, raw []byte, headerKV ...string) *http.Response {
	h := make(http.Header)
	for i := 0; i+1 < len(headerKV); i += 2 {
		h.Set(headerKV[i], headerKV[i+1])
	}
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     h,
		Body:       io.NopCloser(bytes.NewReader(raw)),
	}
}

// WSServer starts a local loopback WebSocket server and registers t.Cleanup to close it.
// Returns the ws:// URL for adapters to dial.
func WSServer(t *testing.T, handler func(conn *websocket.Conn, r *http.Request)) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		handler(conn, r)
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}
