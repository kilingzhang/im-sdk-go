package qq

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	imsdk "github.com/kilingzhang/im-sdk-go"
)

// Gateway OpCodes and Intents for the QQ Open Platform WebSocket Gateway.
const (
	OpDispatch       = 0
	OpHeartbeat      = 1
	OpIdentify       = 2
	OpResume         = 6
	OpReconnect      = 7
	OpInvalidSession = 9
	OpHello          = 10
	OpHeartbeatAck   = 11

	IntentGuilds              = 1 << 0
	IntentGuildMembers        = 1 << 1
	IntentDirectMessage       = 1 << 12
	IntentGroupAndC2C         = 1 << 25
	IntentInteraction         = 1 << 26
	IntentPublicGuildMessages = 1 << 30

	FullIntents = IntentGuilds |
		IntentGuildMembers |
		IntentPublicGuildMessages |
		IntentDirectMessage |
		IntentGroupAndC2C |
		IntentInteraction

	closeAuthFailed          = 4004
	closeInvalidSession      = 4006
	closeSeqOutOfRange       = 4007
	closeRateLimited         = 4008
	closeSessionTimeout      = 4009
	closeServerErrorStart    = 4900
	closeServerErrorEnd      = 4913
	closeInsufficientIntents = 4914
	closeDisallowedIntents   = 4915
)

type gatewayFrame struct {
	Op int             `json:"op"`
	D  json.RawMessage `json:"d,omitempty"`
	S  *int64          `json:"s,omitempty"`
	T  string          `json:"t,omitempty"`
}

type replyTarget struct {
	scope    string // "c2c" or "group"
	targetID string
	msgID    string
}

type qqAttachment struct {
	ContentType string `json:"content_type"`
	URL         string `json:"url"`
	Filename    string `json:"filename"`
	Size        *int64 `json:"size"`
}

// InboundPayload represents a normalized QQ Gateway message event.
type InboundPayload struct {
	Kind        string
	SenderID    string
	SenderName  string
	SenderIsBot bool
	Content     string
	MessageID   string
	Timestamp   string
	GroupOpenID string
	Attachments []qqAttachment
	ReplyTarget replyTarget
}

type gatewayCallbacks struct {
	onReady   func()
	onResumed func()
	onMessage func(InboundPayload)
	onState   func(imsdk.ConnectionState)
}

type gatewayClient struct {
	dialWS         func(ctx context.Context, url string, opts *websocket.DialOptions) (*websocket.Conn, *http.Response, error)
	getToken       func(ctx context.Context) (string, error)
	clearToken     func()
	getGatewayURL  func(ctx context.Context) (string, error)
	reconnectDelay time.Duration
	cb             gatewayCallbacks

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu           sync.Mutex
	wsConn       *websocket.Conn
	sessionID    string
	lastSeq      *int64
	refreshToken bool
}

func newGatewayClient(
	dialWS func(ctx context.Context, url string, opts *websocket.DialOptions) (*websocket.Conn, *http.Response, error),
	getToken func(ctx context.Context) (string, error),
	clearToken func(),
	getGatewayURL func(ctx context.Context) (string, error),
	reconnectDelay time.Duration,
	cb gatewayCallbacks,
) *gatewayClient {
	if reconnectDelay <= 0 {
		reconnectDelay = 500 * time.Millisecond
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &gatewayClient{
		dialWS:         dialWS,
		getToken:       getToken,
		clearToken:     clearToken,
		getGatewayURL:  getGatewayURL,
		reconnectDelay: reconnectDelay,
		cb:             cb,
		ctx:            ctx,
		cancel:         cancel,
	}
}

func (g *gatewayClient) start(ctx context.Context) error {
	tok, err := g.getToken(ctx)
	if err != nil {
		return err
	}
	gwURL, err := g.getGatewayURL(ctx)
	if err != nil {
		return err
	}
	wsConn, _, err := g.dialWS(ctx, gwURL, nil)
	if err != nil {
		return err
	}
	wsConn.SetReadLimit(16 << 20)
	g.mu.Lock()
	g.wsConn = wsConn
	g.mu.Unlock()

	g.wg.Add(1)
	go g.supervisor(wsConn, tok)
	return nil
}

func (g *gatewayClient) supervisor(initial *websocket.Conn, initialTok string) {
	defer g.wg.Done()
	wsConn := initial
	tok := initialTok

	for {
		fatal := g.serveConn(wsConn, tok)
		_ = wsConn.CloseNow()
		if g.ctx.Err() != nil || fatal {
			return
		}

		attempt := 0
		for {
			if g.ctx.Err() != nil {
				return
			}
			attempt++
			if g.cb.onState != nil {
				g.cb.onState(imsdk.ConnectionState{Kind: imsdk.StateReconnecting, Attempt: attempt})
			}

			g.mu.Lock()
			needRefresh := g.refreshToken
			g.refreshToken = false
			g.mu.Unlock()
			if needRefresh && g.clearToken != nil {
				g.clearToken()
			}

			timer := time.NewTimer(g.reconnectDelay)
			select {
			case <-g.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}

			nextTok, err := g.getToken(g.ctx)
			if err != nil {
				continue
			}
			gwURL, err := g.getGatewayURL(g.ctx)
			if err != nil {
				continue
			}
			nextConn, _, err := g.dialWS(g.ctx, gwURL, nil)
			if err != nil {
				continue
			}
			nextConn.SetReadLimit(16 << 20)
			g.mu.Lock()
			g.wsConn = nextConn
			g.mu.Unlock()
			wsConn = nextConn
			tok = nextTok
			break
		}
	}
}

func (g *gatewayClient) serveConn(wsConn *websocket.Conn, tok string) (fatal bool) {
	connCtx, cancel := context.WithCancel(g.ctx)
	defer cancel()

	var writeMu sync.Mutex
	sendJSON := func(v any) {
		raw, err := json.Marshal(v)
		if err != nil {
			return
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		wctx, wcancel := context.WithTimeout(connCtx, 10*time.Second)
		defer wcancel()
		_ = wsConn.Write(wctx, websocket.MessageText, raw)
	}

	var hbCancel context.CancelFunc
	defer func() {
		if hbCancel != nil {
			hbCancel()
		}
	}()

	for {
		_, raw, err := wsConn.Read(connCtx)
		if err != nil {
			code := int(websocket.CloseStatus(err))
			switch {
			case code == closeInsufficientIntents || code == closeDisallowedIntents:
				if g.cb.onState != nil {
					g.cb.onState(imsdk.ConnectionState{Kind: imsdk.StateError, Code: imsdk.ConnErrUnknown})
				}
				return true
			case code == closeAuthFailed:
				g.mu.Lock()
				g.refreshToken = true
				g.mu.Unlock()
			case code == closeInvalidSession || code == closeSeqOutOfRange || code == closeSessionTimeout ||
				(code >= closeServerErrorStart && code <= closeServerErrorEnd):
				g.mu.Lock()
				g.sessionID = ""
				g.lastSeq = nil
				g.refreshToken = true
				g.mu.Unlock()
			}
			return false
		}

		var frame gatewayFrame
		if json.Unmarshal(raw, &frame) != nil {
			continue
		}
		if frame.S != nil {
			seqCopy := *frame.S
			g.mu.Lock()
			g.lastSeq = &seqCopy
			g.mu.Unlock()
		}

		switch frame.Op {
		case OpHello:
			var hello struct {
				HeartbeatInterval int `json:"heartbeat_interval"`
			}
			_ = json.Unmarshal(frame.D, &hello)

			g.mu.Lock()
			sid := g.sessionID
			seq := g.lastSeq
			g.mu.Unlock()

			if sid != "" && seq != nil {
				sendJSON(map[string]any{
					"op": OpResume,
					"d": map[string]any{
						"token":      "QQBot " + tok,
						"session_id": sid,
						"seq":        *seq,
					},
				})
			} else {
				sendJSON(map[string]any{
					"op": OpIdentify,
					"d": map[string]any{
						"token":   "QQBot " + tok,
						"intents": FullIntents,
						"shard":   [2]int{0, 1},
					},
				})
			}

			if hbCancel != nil {
				hbCancel()
			}
			interval := time.Duration(hello.HeartbeatInterval) * time.Millisecond
			if interval <= 0 {
				interval = 40 * time.Second
			}
			hbCtx, cancelHB := context.WithCancel(connCtx)
			hbCancel = cancelHB
			go func() {
				ticker := time.NewTicker(interval)
				defer ticker.Stop()
				for {
					select {
					case <-hbCtx.Done():
						return
					case <-ticker.C:
						g.mu.Lock()
						var seqVal any
						if g.lastSeq != nil {
							seqVal = *g.lastSeq
						}
						g.mu.Unlock()
						sendJSON(map[string]any{
							"op": OpHeartbeat,
							"d":  seqVal,
						})
					}
				}
			}()

		case OpDispatch:
			g.handleDispatch(frame.T, frame.D)

		case OpHeartbeatAck:

		case OpReconnect:
			return false

		case OpInvalidSession:
			var canResume bool
			_ = json.Unmarshal(frame.D, &canResume)
			if !canResume {
				g.mu.Lock()
				g.sessionID = ""
				g.lastSeq = nil
				g.refreshToken = true
				g.mu.Unlock()
			}
			return false
		}
	}
}

func (g *gatewayClient) handleDispatch(eventType string, data json.RawMessage) {
	switch eventType {
	case "READY":
		var d struct {
			SessionID string `json:"session_id"`
		}
		_ = json.Unmarshal(data, &d)
		g.mu.Lock()
		g.sessionID = d.SessionID
		g.mu.Unlock()
		if g.cb.onReady != nil {
			g.cb.onReady()
		}

	case "RESUMED":
		if g.cb.onResumed != nil {
			g.cb.onResumed()
		}

	case "C2C_MESSAGE_CREATE":
		var ev struct {
			ID        string `json:"id"`
			Content   string `json:"content"`
			Timestamp string `json:"timestamp"`
			Author    struct {
				UserOpenID string `json:"user_openid"`
			} `json:"author"`
			Attachments []qqAttachment `json:"attachments"`
		}
		if json.Unmarshal(data, &ev) != nil {
			return
		}
		if g.cb.onMessage != nil {
			g.cb.onMessage(InboundPayload{
				Kind:        "c2c",
				SenderID:    ev.Author.UserOpenID,
				Content:     ev.Content,
				MessageID:   ev.ID,
				Timestamp:   ev.Timestamp,
				Attachments: ev.Attachments,
				ReplyTarget: replyTarget{
					scope:    "c2c",
					targetID: ev.Author.UserOpenID,
					msgID:    ev.ID,
				},
			})
		}

	case "GROUP_AT_MESSAGE_CREATE", "GROUP_MESSAGE_CREATE":
		var ev struct {
			ID          string `json:"id"`
			Content     string `json:"content"`
			Timestamp   string `json:"timestamp"`
			GroupOpenID string `json:"group_openid"`
			Author      struct {
				MemberOpenID string `json:"member_openid"`
				Username     string `json:"username"`
				Bot          bool   `json:"bot"`
			} `json:"author"`
			Attachments []qqAttachment `json:"attachments"`
		}
		if json.Unmarshal(data, &ev) != nil {
			return
		}
		if g.cb.onMessage != nil {
			g.cb.onMessage(InboundPayload{
				Kind:        "group",
				SenderID:    ev.Author.MemberOpenID,
				SenderName:  ev.Author.Username,
				SenderIsBot: ev.Author.Bot,
				Content:     ev.Content,
				MessageID:   ev.ID,
				Timestamp:   ev.Timestamp,
				GroupOpenID: ev.GroupOpenID,
				Attachments: ev.Attachments,
				ReplyTarget: replyTarget{
					scope:    "group",
					targetID: ev.GroupOpenID,
					msgID:    ev.ID,
				},
			})
		}
	}
}

func (g *gatewayClient) close() {
	g.cancel()
	g.mu.Lock()
	ws := g.wsConn
	g.wsConn = nil
	g.mu.Unlock()
	if ws != nil {
		_ = ws.CloseNow()
	}
	g.wg.Wait()
}

var _ = errors.New
