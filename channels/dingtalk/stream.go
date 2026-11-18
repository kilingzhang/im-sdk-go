package dingtalk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	imsdk "github.com/kilingzhang/im-sdk-go"
)

// TopicRobot is DingTalk's Stream callback topic for inbound bot messages.
const TopicRobot = "/v1.0/im/bot/messages/get"

// StreamEvent represents one inbound CALLBACK message over DingTalk Stream.
type StreamEvent struct {
	MessageID string
	Topic     string
	Data      []byte
}

// StreamClient abstracts the DingTalk Stream WebSocket connection so tests can
// either exercise the pure Go WebSocket client or inject a fake stream.
type StreamClient interface {
	Connect(ctx context.Context, onCallback func(StreamEvent), onState func(imsdk.ConnectionState)) error
	AckCallback(ctx context.Context, messageID string, response any) error
	Close() error
}

type dwStreamClient struct {
	clientID          string
	clientSecret      string
	apiBaseURL        string
	httpClient        *http.Client
	dialWS            func(ctx context.Context, url string, opts *websocket.DialOptions) (*websocket.Conn, *http.Response, error)
	reconnectInterval time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	wsConn *websocket.Conn
}

type dwDownstream struct {
	SpecVersion string            `json:"specVersion"`
	Type        string            `json:"type"`
	Headers     map[string]string `json:"headers"`
	Data        string            `json:"data"`
}

func newDWStreamClient(clientID, clientSecret, apiBaseURL string, httpClient *http.Client, dialWS func(ctx context.Context, url string, opts *websocket.DialOptions) (*websocket.Conn, *http.Response, error), reconnectInterval time.Duration) *dwStreamClient {
	if reconnectInterval <= 0 {
		reconnectInterval = time.Second
	}
	return &dwStreamClient{
		clientID:          clientID,
		clientSecret:      clientSecret,
		apiBaseURL:        apiBaseURL,
		httpClient:        httpClient,
		dialWS:            dialWS,
		reconnectInterval: reconnectInterval,
	}
}

func (s *dwStreamClient) openTicketURL(ctx context.Context) (string, error) {
	payload := map[string]any{
		"clientId":     s.clientID,
		"clientSecret": s.clientSecret,
		"ua":           "im-sdk-go",
		"subscriptions": []map[string]string{
			{"type": "EVENT", "topic": "*"},
			{"type": "CALLBACK", "topic": TopicRobot},
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	rctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(rctx, http.MethodPost, s.apiBaseURL+"/v1.0/gateway/connections/open", bytes.NewReader(raw))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var out struct {
		Endpoint string `json:"endpoint"`
		Ticket   string `json:"ticket"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if resp.StatusCode >= 400 || out.Endpoint == "" || out.Ticket == "" {
		return "", fmt.Errorf("dingtalk: gateway open failed (status %d)", resp.StatusCode)
	}
	sep := "?"
	if strings.Contains(out.Endpoint, "?") {
		sep = "&"
	}
	return out.Endpoint + sep + "ticket=" + url.QueryEscape(out.Ticket), nil
}

func (s *dwStreamClient) Connect(ctx context.Context, onCallback func(StreamEvent), onState func(imsdk.ConnectionState)) error {
	wsURL, err := s.openTicketURL(ctx)
	if err != nil {
		return err
	}
	wsConn, _, err := s.dialWS(ctx, wsURL, nil)
	if err != nil {
		return err
	}
	wsConn.SetReadLimit(16 << 20)

	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.mu.Lock()
	s.wsConn = wsConn
	s.mu.Unlock()

	if onState != nil {
		onState(imsdk.ConnectionState{Kind: imsdk.StateConnected})
	}

	s.wg.Add(1)
	go s.supervisor(wsConn, onCallback, onState)
	return nil
}

func (s *dwStreamClient) supervisor(initial *websocket.Conn, onCallback func(StreamEvent), onState func(imsdk.ConnectionState)) {
	defer s.wg.Done()
	wsConn := initial
	for {
		s.readLoop(wsConn, onCallback)
		_ = wsConn.CloseNow()
		if s.ctx.Err() != nil {
			return
		}

		attempt := 0
		for {
			if s.ctx.Err() != nil {
				return
			}
			attempt++
			if onState != nil {
				onState(imsdk.ConnectionState{Kind: imsdk.StateReconnecting, Attempt: attempt})
			}
			timer := time.NewTimer(s.reconnectInterval)
			select {
			case <-s.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}

			wsURL, err := s.openTicketURL(s.ctx)
			if err != nil {
				continue
			}
			nextConn, _, err := s.dialWS(s.ctx, wsURL, nil)
			if err != nil {
				continue
			}
			nextConn.SetReadLimit(16 << 20)
			s.mu.Lock()
			s.wsConn = nextConn
			s.mu.Unlock()
			wsConn = nextConn
			if onState != nil {
				onState(imsdk.ConnectionState{Kind: imsdk.StateConnected})
			}
			break
		}
	}
}

func (s *dwStreamClient) readLoop(wsConn *websocket.Conn, onCallback func(StreamEvent)) {
	for {
		_, raw, err := wsConn.Read(s.ctx)
		if err != nil {
			return
		}
		var msg dwDownstream
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}
		switch msg.Type {
		case "SYSTEM":
			switch msg.Headers["topic"] {
			case "ping":
				_ = s.writeJSON(wsConn, map[string]any{
					"code":    200,
					"headers": msg.Headers,
					"message": "OK",
					"data":    msg.Data,
				})
			case "disconnect":
				return
			}

		case "EVENT":
			ackData, _ := json.Marshal(map[string]string{"status": "SUCCESS"})
			_ = s.writeJSON(wsConn, map[string]any{
				"code": 200,
				"headers": map[string]string{
					"contentType": "application/json",
					"messageId":   msg.Headers["messageId"],
				},
				"message": "OK",
				"data":    string(ackData),
			})

		case "CALLBACK":
			if onCallback != nil {
				onCallback(StreamEvent{
					MessageID: msg.Headers["messageId"],
					Topic:     msg.Headers["topic"],
					Data:      []byte(msg.Data),
				})
			}
		}
	}
}

func (s *dwStreamClient) AckCallback(ctx context.Context, messageID string, response any) error {
	if messageID == "" {
		return errors.New("dingtalk: messageId is required")
	}
	s.mu.Lock()
	wsConn := s.wsConn
	s.mu.Unlock()
	if wsConn == nil {
		return errors.New("dingtalk: stream socket not connected")
	}
	dataBytes, err := json.Marshal(map[string]any{"response": response})
	if err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]any{
		"code": 200,
		"headers": map[string]string{
			"contentType": "application/json",
			"messageId":   messageID,
		},
		"message": "OK",
		"data":    string(dataBytes),
	})
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return wsConn.Write(wctx, websocket.MessageText, payload)
}

func (s *dwStreamClient) writeJSON(wsConn *websocket.Conn, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
	return wsConn.Write(wctx, websocket.MessageText, raw)
}

func (s *dwStreamClient) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Lock()
	ws := s.wsConn
	s.wsConn = nil
	s.mu.Unlock()
	if ws != nil {
		_ = ws.CloseNow()
	}
	s.wg.Wait()
	return nil
}
