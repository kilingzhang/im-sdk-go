package wecom

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const (
	cmdSubscribe         = "aibot_subscribe"
	cmdHeartbeat         = "ping"
	cmdRespondMsg        = "aibot_respond_msg"
	cmdSendMsg           = "aibot_send_msg"
	cmdUploadMediaInit   = "aibot_upload_media_init"
	cmdUploadMediaChunk  = "aibot_upload_media_chunk"
	cmdUploadMediaFinish = "aibot_upload_media_finish"
	cmdMsgCallback       = "aibot_msg_callback"
	cmdEventCallback     = "aibot_event_callback"

	eventDisconnected = "disconnected_event"

	defaultHeartbeatInterval = 30 * time.Second
	defaultReconnectDelay    = 1 * time.Second
	maxReconnectDelay        = 30 * time.Second
	defaultReplyAckTimeout   = 5 * time.Second
	defaultRequestTimeout    = 10 * time.Second
	maxReplyQueueSize        = 500
	maxMissedPong            = 2
	maxReconnectAttempts     = 10
	maxAuthFailureAttempts   = 5

	mediaChunkSize = 512 * 1024
	maxMediaChunks = 100
)

// Headers carries the correlation request ID on a WeCom WebSocket frame.
type Headers struct {
	ReqID string `json:"req_id"`
}

// Frame is an inbound or outbound WeCom intelligent bot WebSocket frame.
type Frame struct {
	Cmd     string   `json:"cmd,omitempty"`
	Headers Headers  `json:"headers"`
	Body    *Message `json:"body,omitempty"`
}

// UserRef identifies the author of an inbound WeCom message.
type UserRef struct {
	UserID string `json:"userid,omitempty"`
}

// TextContent carries plain or markdown text in an inbound WeCom message.
type TextContent struct {
	Content string `json:"content,omitempty"`
}

// MediaRef carries an encrypted media URL and optional AES-256 key.
type MediaRef struct {
	URL      string `json:"url,omitempty"`
	AESKey   string `json:"aeskey,omitempty"`
	Filename string `json:"filename,omitempty"`
}

// MixedItem represents one text or image part inside a WeCom mixed message.
type MixedItem struct {
	MsgType string       `json:"msgtype,omitempty"`
	Text    *TextContent `json:"text,omitempty"`
	Image   *MediaRef    `json:"image,omitempty"`
}

// MixedContent groups items inside a WeCom mixed message.
type MixedContent struct {
	MsgItem []MixedItem `json:"msg_item,omitempty"`
}

// EventContent describes an inbound WeCom event callback payload.
type EventContent struct {
	EventType string `json:"eventtype,omitempty"`
}

// Message represents the body of an inbound WeCom callback frame.
type Message struct {
	MsgID      string        `json:"msgid,omitempty"`
	ChatType   string        `json:"chattype,omitempty"`
	ChatID     string        `json:"chatid,omitempty"`
	From       *UserRef      `json:"from,omitempty"`
	CreateTime int64         `json:"create_time,omitempty"`
	MsgType    string        `json:"msgtype,omitempty"`
	Text       *TextContent  `json:"text,omitempty"`
	Voice      *TextContent  `json:"voice,omitempty"`
	Image      *MediaRef     `json:"image,omitempty"`
	File       *MediaRef     `json:"file,omitempty"`
	Mixed      *MixedContent `json:"mixed,omitempty"`
	Event      *EventContent `json:"event,omitempty"`
}

// ClientCallbacks receives asynchronous lifecycle and message events from a Client.
type ClientCallbacks struct {
	OnMessage       func(Frame)
	OnAuthenticated func()
	OnReconnecting  func(attempt int)
	OnError         func(error)
}

// Client abstracts the WeCom intelligent bot WebSocket + HTTP media transport.
type Client interface {
	SetCallbacks(cb ClientCallbacks)
	Connect(ctx context.Context) error
	Close() error
	ReplyStream(ctx context.Context, frame Frame, streamID string, content string, finish bool) error
	SendMessage(ctx context.Context, chatID string, body map[string]any) error
	UploadMedia(ctx context.Context, data []byte, mediaType string, filename string) (string, error)
	SendMediaMessage(ctx context.Context, chatID string, mediaType string, mediaID string) error
	DownloadFile(ctx context.Context, fileURL string, aesKey string) ([]byte, string, error)
}

// WSClientOptions configures a pure-Go WeCom WSClient.
type WSClientOptions struct {
	BotID             string
	Secret            string
	WSURL             string
	HTTPClient        *http.Client
	HeartbeatInterval time.Duration
	ReconnectDelay    time.Duration
	ReplyAckTimeout   time.Duration
	RequestTimeout    time.Duration
}

type rawFrame struct {
	Cmd     string          `json:"cmd,omitempty"`
	Headers Headers         `json:"headers"`
	Body    json.RawMessage `json:"body,omitempty"`
	ErrCode int             `json:"errcode,omitempty"`
	ErrMsg  string          `json:"errmsg,omitempty"`
}

type replyOutcome struct {
	frame rawFrame
	err   error
}

type queuedReply struct {
	wire  []byte
	resCh chan replyOutcome
}

type pendingAck struct {
	item  *queuedReply
	timer *time.Timer
	seq   uint64
}

// WSClient implements Client over @wecom/aibot-node-sdk's WebSocket and HTTP wire protocol.
type WSClient struct {
	botID             string
	secret            string
	wsURL             string
	httpClient        *http.Client
	heartbeatInterval time.Duration
	reconnectDelay    time.Duration
	replyAckTimeout   time.Duration
	requestTimeout    time.Duration

	lifeCtx    context.Context
	lifeCancel context.CancelFunc
	wg         sync.WaitGroup

	mu                      sync.Mutex
	cb                      ClientCallbacks
	started                 bool
	manualClose             bool
	conn                    *websocket.Conn
	reconnectTimer          *time.Timer
	reconnectAttempts       int
	authFailureAttempts     int
	lastCloseWasAuthFailure bool
	missedPongCount         int
	pendingAckSeq           uint64
	pendingAcks             map[string]*pendingAck
	replyQueues             map[string][]*queuedReply
	initDone                chan error
	initResolved            bool
}

// NewWSClient creates a pure-Go WeCom intelligent bot WebSocket client.
func NewWSClient(opts WSClientOptions) *WSClient {
	wsURL := strings.TrimSpace(opts.WSURL)
	if wsURL == "" {
		wsURL = DefaultWSURL
	}
	hb := opts.HeartbeatInterval
	if hb <= 0 {
		hb = defaultHeartbeatInterval
	}
	reconn := opts.ReconnectDelay
	if reconn <= 0 {
		reconn = defaultReconnectDelay
	}
	ackTimeout := opts.ReplyAckTimeout
	if ackTimeout <= 0 {
		ackTimeout = defaultReplyAckTimeout
	}
	reqTimeout := opts.RequestTimeout
	if reqTimeout <= 0 {
		reqTimeout = defaultRequestTimeout
	}

	var dlClient http.Client
	if opts.HTTPClient != nil {
		dlClient = *opts.HTTPClient
	}
	dlClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	return &WSClient{
		botID:             opts.BotID,
		secret:            opts.Secret,
		wsURL:             wsURL,
		httpClient:        &dlClient,
		heartbeatInterval: hb,
		reconnectDelay:    reconn,
		replyAckTimeout:   ackTimeout,
		requestTimeout:    reqTimeout,
		lifeCtx:           lifeCtx,
		lifeCancel:        lifeCancel,
		pendingAcks:       make(map[string]*pendingAck),
		replyQueues:       make(map[string][]*queuedReply),
		initDone:          make(chan error, 1),
	}
}

func generateReqID(prefix string) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s_%d_%s", prefix, time.Now().UnixMilli(), hex.EncodeToString(b[:]))
}

func stripControlBytes(raw []byte) []byte {
	out := make([]byte, 0, len(raw))
	for _, b := range raw {
		if (b <= 0x08) || (b >= 0x0B && b <= 0x0D) || (b >= 0x0E && b <= 0x1F) {
			continue
		}
		out = append(out, b)
	}
	return out
}

func (c *WSClient) SetCallbacks(cb ClientCallbacks) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cb = cb
}

func (c *WSClient) resolveInitLocked(err error) {
	if c.initResolved {
		return
	}
	c.initResolved = true
	c.initDone <- err
}

func (c *WSClient) Connect(ctx context.Context) error {
	c.mu.Lock()
	if !c.started {
		c.started = true
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.dialSession()
		}()
	}
	c.mu.Unlock()

	select {
	case err := <-c.initDone:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *WSClient) dialSession() {
	c.mu.Lock()
	if c.manualClose || c.lifeCtx.Err() != nil {
		c.mu.Unlock()
		return
	}
	c.lastCloseWasAuthFailure = false
	c.missedPongCount = 0
	c.mu.Unlock()

	dialCtx, cancel := context.WithTimeout(c.lifeCtx, c.requestTimeout)
	conn, _, err := websocket.Dial(dialCtx, c.wsURL, nil)
	cancel()
	if err != nil {
		c.mu.Lock()
		cb := c.cb
		manual := c.manualClose || c.lifeCtx.Err() != nil
		c.mu.Unlock()
		if manual {
			return
		}
		if cb.OnError != nil {
			cb.OnError(err)
		}
		c.scheduleReconnect("dial failed")
		return
	}

	conn.SetReadLimit(16 << 20)

	c.mu.Lock()
	if c.manualClose || c.lifeCtx.Err() != nil {
		c.mu.Unlock()
		_ = conn.CloseNow()
		return
	}
	c.conn = conn
	c.mu.Unlock()

	authFrame := map[string]any{
		"cmd":     cmdSubscribe,
		"headers": map[string]string{"req_id": generateReqID(cmdSubscribe)},
		"body": map[string]string{
			"bot_id": c.botID,
			"secret": c.secret,
		},
	}
	if raw, mErr := json.Marshal(authFrame); mErr == nil {
		writeCtx, wCancel := context.WithTimeout(c.lifeCtx, c.requestTimeout)
		_ = conn.Write(writeCtx, websocket.MessageText, raw)
		wCancel()
	}

	stopHB := make(chan struct{})
	var stopHBOnce sync.Once
	closeHB := func() {
		stopHBOnce.Do(func() { close(stopHB) })
	}
	defer closeHB()

	for {
		_, data, rErr := conn.Read(c.lifeCtx)
		if rErr != nil {
			closeHB()
			_ = conn.CloseNow()
			c.mu.Lock()
			if c.conn == conn {
				c.conn = nil
			}
			manual := c.manualClose || c.lifeCtx.Err() != nil
			c.clearPendingLocked("WebSocket connection closed")
			c.mu.Unlock()
			if !manual {
				c.scheduleReconnect(rErr.Error())
			}
			return
		}

		cleaned := stripControlBytes(data)
		var rf rawFrame
		if err := json.Unmarshal(cleaned, &rf); err != nil {
			continue
		}
		if c.handleFrame(conn, rf, stopHB, closeHB) {
			return
		}
	}
}

func (c *WSClient) handleFrame(conn *websocket.Conn, rf rawFrame, stopHB chan struct{}, closeHB func()) (stopLoop bool) {
	switch rf.Cmd {
	case cmdMsgCallback:
		var msg Message
		if len(rf.Body) == 0 || json.Unmarshal(rf.Body, &msg) != nil || msg.MsgType == "" {
			return false
		}
		c.mu.Lock()
		cb := c.cb
		c.mu.Unlock()
		if cb.OnMessage != nil {
			cb.OnMessage(Frame{
				Cmd:     rf.Cmd,
				Headers: rf.Headers,
				Body:    &msg,
			})
		}
		return false

	case cmdEventCallback:
		var msg Message
		if len(rf.Body) > 0 {
			_ = json.Unmarshal(rf.Body, &msg)
		}
		if msg.Event != nil && msg.Event.EventType == eventDisconnected {
			closeHB()
			c.mu.Lock()
			c.manualClose = true
			if c.conn == conn {
				c.conn = nil
			}
			c.clearPendingLocked("Server disconnected due to new connection")
			c.mu.Unlock()
			_ = conn.CloseNow()
			return true
		}
		return false
	}

	reqID := rf.Headers.ReqID
	if strings.HasPrefix(reqID, cmdSubscribe) {
		if rf.ErrCode != 0 {
			authErr := &AuthError{ErrCode: rf.ErrCode, ErrMsg: rf.ErrMsg}
			c.mu.Lock()
			c.lastCloseWasAuthFailure = true
			cb := c.cb
			c.resolveInitLocked(authErr)
			c.mu.Unlock()
			if cb.OnError != nil {
				cb.OnError(authErr)
			}
			_ = conn.CloseNow()
			return false
		}

		c.mu.Lock()
		c.reconnectAttempts = 0
		c.authFailureAttempts = 0
		cb := c.cb
		c.resolveInitLocked(nil)
		c.mu.Unlock()

		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.heartbeatLoop(conn, stopHB)
		}()

		if cb.OnAuthenticated != nil {
			cb.OnAuthenticated()
		}
		return false
	}

	if strings.HasPrefix(reqID, cmdHeartbeat) {
		if rf.ErrCode == 0 {
			c.mu.Lock()
			c.missedPongCount = 0
			c.mu.Unlock()
		}
		return false
	}

	c.handleReplyAck(reqID, rf)
	return false
}

func (c *WSClient) heartbeatLoop(conn *websocket.Conn, stopHB <-chan struct{}) {
	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stopHB:
			return
		case <-c.lifeCtx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			if c.conn != conn || c.manualClose {
				c.mu.Unlock()
				return
			}
			if c.missedPongCount >= maxMissedPong {
				c.mu.Unlock()
				_ = conn.CloseNow()
				return
			}
			c.missedPongCount++
			c.mu.Unlock()

			ping := map[string]any{
				"cmd":     cmdHeartbeat,
				"headers": map[string]string{"req_id": generateReqID(cmdHeartbeat)},
			}
			if raw, err := json.Marshal(ping); err == nil {
				wCtx, cancel := context.WithTimeout(c.lifeCtx, c.requestTimeout)
				_ = conn.Write(wCtx, websocket.MessageText, raw)
				cancel()
			}
		}
	}
}

func (c *WSClient) scheduleReconnect(_ string) {
	c.mu.Lock()
	if c.manualClose || c.lifeCtx.Err() != nil {
		c.mu.Unlock()
		return
	}
	cb := c.cb
	var attempt int
	if c.lastCloseWasAuthFailure {
		if c.authFailureAttempts >= maxAuthFailureAttempts {
			err := &AuthError{ErrCode: -1, ErrMsg: fmt.Sprintf("max auth failure attempts exceeded (%d)", maxAuthFailureAttempts)}
			c.resolveInitLocked(err)
			c.mu.Unlock()
			if cb.OnError != nil {
				cb.OnError(err)
			}
			return
		}
		c.authFailureAttempts++
		attempt = c.authFailureAttempts
	} else {
		if c.reconnectAttempts >= maxReconnectAttempts {
			err := fmt.Errorf("wecom: max reconnect attempts exceeded (%d)", maxReconnectAttempts)
			c.resolveInitLocked(err)
			c.mu.Unlock()
			if cb.OnError != nil {
				cb.OnError(err)
			}
			return
		}
		c.reconnectAttempts++
		attempt = c.reconnectAttempts
	}

	delay := c.reconnectDelay
	for i := 1; i < attempt && delay < maxReconnectDelay; i++ {
		delay *= 2
	}
	if delay > maxReconnectDelay {
		delay = maxReconnectDelay
	}
	if c.reconnectTimer != nil {
		c.reconnectTimer.Stop()
	}
	c.reconnectTimer = time.AfterFunc(delay, func() {
		c.mu.Lock()
		c.reconnectTimer = nil
		stopped := c.manualClose || c.lifeCtx.Err() != nil
		c.mu.Unlock()
		if stopped {
			return
		}
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.dialSession()
		}()
	})
	c.mu.Unlock()

	if cb.OnReconnecting != nil {
		cb.OnReconnecting(attempt)
	}
}

func (c *WSClient) sendReply(ctx context.Context, reqID, cmd string, body any) (rawFrame, error) {
	if err := ctx.Err(); err != nil {
		return rawFrame{}, err
	}
	envelope := map[string]any{
		"cmd":     cmd,
		"headers": map[string]string{"req_id": reqID},
		"body":    body,
	}
	wire, err := json.Marshal(envelope)
	if err != nil {
		return rawFrame{}, err
	}

	item := &queuedReply{
		wire:  wire,
		resCh: make(chan replyOutcome, 1),
	}

	c.mu.Lock()
	if c.manualClose || c.lifeCtx.Err() != nil {
		c.mu.Unlock()
		return rawFrame{}, errors.New("wecom: client closed")
	}
	q := c.replyQueues[reqID]
	if len(q) >= maxReplyQueueSize {
		c.mu.Unlock()
		return rawFrame{}, fmt.Errorf("wecom: reply queue for %s exceeds %d", reqID, maxReplyQueueSize)
	}
	q = append(q, item)
	c.replyQueues[reqID] = q
	startNow := len(q) == 1
	c.mu.Unlock()

	if startNow {
		c.dispatchNextReply(reqID)
	}

	select {
	case out := <-item.resCh:
		return out.frame, out.err
	case <-ctx.Done():
		return rawFrame{}, ctx.Err()
	}
}

func (c *WSClient) dispatchNextReply(reqID string) {
	for {
		c.mu.Lock()
		q := c.replyQueues[reqID]
		if len(q) == 0 {
			delete(c.replyQueues, reqID)
			c.mu.Unlock()
			return
		}
		if _, busy := c.pendingAcks[reqID]; busy {
			c.mu.Unlock()
			return
		}
		head := q[0]
		ws := c.conn
		if ws == nil {
			q = q[1:]
			if len(q) == 0 {
				delete(c.replyQueues, reqID)
			} else {
				c.replyQueues[reqID] = q
			}
			c.mu.Unlock()
			head.resCh <- replyOutcome{err: errors.New("WebSocket not connected, unable to send data")}
			continue
		}

		c.pendingAckSeq++
		seq := c.pendingAckSeq
		timer := time.AfterFunc(c.replyAckTimeout, func() {
			c.onAckTimeout(reqID, seq)
		})
		c.pendingAcks[reqID] = &pendingAck{
			item:  head,
			timer: timer,
			seq:   seq,
		}
		c.mu.Unlock()

		wCtx, cancel := context.WithTimeout(c.lifeCtx, c.requestTimeout)
		wErr := ws.Write(wCtx, websocket.MessageText, head.wire)
		cancel()
		if wErr == nil {
			return
		}

		c.mu.Lock()
		cur, stillPending := c.pendingAcks[reqID]
		if !stillPending || cur.seq != seq {
			c.mu.Unlock()
			return
		}
		cur.timer.Stop()
		delete(c.pendingAcks, reqID)
		if q := c.replyQueues[reqID]; len(q) > 0 && q[0] == head {
			q = q[1:]
			if len(q) == 0 {
				delete(c.replyQueues, reqID)
			} else {
				c.replyQueues[reqID] = q
			}
		}
		c.mu.Unlock()
		head.resCh <- replyOutcome{err: wErr}
	}
}

func (c *WSClient) onAckTimeout(reqID string, seq uint64) {
	c.mu.Lock()
	cur, ok := c.pendingAcks[reqID]
	if !ok || cur.seq != seq {
		c.mu.Unlock()
		return
	}
	delete(c.pendingAcks, reqID)
	if q := c.replyQueues[reqID]; len(q) > 0 && q[0] == cur.item {
		q = q[1:]
		if len(q) == 0 {
			delete(c.replyQueues, reqID)
		} else {
			c.replyQueues[reqID] = q
		}
	}
	c.mu.Unlock()

	cur.item.resCh <- replyOutcome{
		err: fmt.Errorf("Reply ack timeout (%s) for reqId: %s", c.replyAckTimeout, reqID),
	}
	c.dispatchNextReply(reqID)
}

func (c *WSClient) handleReplyAck(reqID string, rf rawFrame) {
	c.mu.Lock()
	cur, ok := c.pendingAcks[reqID]
	if !ok {
		c.mu.Unlock()
		return
	}
	cur.timer.Stop()
	delete(c.pendingAcks, reqID)
	if q := c.replyQueues[reqID]; len(q) > 0 && q[0] == cur.item {
		q = q[1:]
		if len(q) == 0 {
			delete(c.replyQueues, reqID)
		} else {
			c.replyQueues[reqID] = q
		}
	}
	c.mu.Unlock()

	if rf.ErrCode != 0 {
		cur.item.resCh <- replyOutcome{err: &AckError{ErrCode: rf.ErrCode, ErrMsg: rf.ErrMsg}}
	} else {
		cur.item.resCh <- replyOutcome{frame: rf}
	}
	c.dispatchNextReply(reqID)
}

func (c *WSClient) clearPendingLocked(reason string) {
	notified := make(map[*queuedReply]struct{})
	for reqID, p := range c.pendingAcks {
		p.timer.Stop()
		notified[p.item] = struct{}{}
		p.item.resCh <- replyOutcome{err: fmt.Errorf("%s, reply for reqId: %s cancelled", reason, reqID)}
	}
	clear(c.pendingAcks)
	for reqID, q := range c.replyQueues {
		for _, item := range q {
			if _, done := notified[item]; done {
				continue
			}
			item.resCh <- replyOutcome{err: fmt.Errorf("%s, reply for reqId: %s cancelled", reason, reqID)}
		}
	}
	clear(c.replyQueues)
}

func (c *WSClient) ReplyStream(ctx context.Context, frame Frame, streamID string, content string, finish bool) error {
	body := map[string]any{
		"msgtype": "stream",
		"stream": map[string]any{
			"id":      streamID,
			"finish":  finish,
			"content": content,
		},
	}
	_, err := c.sendReply(ctx, frame.Headers.ReqID, cmdRespondMsg, body)
	return err
}

func (c *WSClient) SendMessage(ctx context.Context, chatID string, body map[string]any) error {
	full := make(map[string]any, len(body)+1)
	maps.Copy(full, body)
	full["chatid"] = chatID
	_, err := c.sendReply(ctx, generateReqID(cmdSendMsg), cmdSendMsg, full)
	return err
}

func (c *WSClient) SendMediaMessage(ctx context.Context, chatID string, mediaType string, mediaID string) error {
	return c.SendMessage(ctx, chatID, map[string]any{
		"msgtype": mediaType,
		mediaType: map[string]string{"media_id": mediaID},
	})
}

func (c *WSClient) UploadMedia(ctx context.Context, data []byte, mediaType string, filename string) (string, error) {
	totalSize := len(data)
	totalChunks := 1
	if totalSize > 0 {
		totalChunks = (totalSize + mediaChunkSize - 1) / mediaChunkSize
	}
	if totalChunks > maxMediaChunks {
		return "", fmt.Errorf("wecom: file too large (%d chunks exceeds maximum %d)", totalChunks, maxMediaChunks)
	}
	sum := md5.Sum(data)
	md5Hex := hex.EncodeToString(sum[:])

	initFrame, err := c.sendReply(ctx, generateReqID(cmdUploadMediaInit), cmdUploadMediaInit, map[string]any{
		"type":         mediaType,
		"filename":     filename,
		"total_size":   totalSize,
		"total_chunks": totalChunks,
		"md5":          md5Hex,
	})
	if err != nil {
		return "", err
	}
	var initBody struct {
		UploadID string `json:"upload_id"`
	}
	if len(initFrame.Body) > 0 {
		_ = json.Unmarshal(initFrame.Body, &initBody)
	}
	if initBody.UploadID == "" {
		return "", errors.New("wecom: upload init returned no upload_id")
	}

	concurrency := totalChunks
	if totalChunks > 10 {
		concurrency = 2
	} else if totalChunks > 4 {
		concurrency = 3
	}

	uploadChunk := func(idx int) error {
		start := idx * mediaChunkSize
		end := min(start+mediaChunkSize, totalSize)
		var chunk []byte
		if start < end {
			chunk = data[start:end]
		}
		b64 := base64.StdEncoding.EncodeToString(chunk)
		var lastErr error
		for attempt := range 3 {
			_, cErr := c.sendReply(ctx, generateReqID(cmdUploadMediaChunk), cmdUploadMediaChunk, map[string]any{
				"upload_id":   initBody.UploadID,
				"chunk_index": idx,
				"base64_data": b64,
			})
			if cErr == nil {
				return nil
			}
			lastErr = cErr
			if attempt < 2 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Duration(attempt+1) * 50 * time.Millisecond):
				}
			}
		}
		return fmt.Errorf("wecom: chunk %d upload failed: %w", idx, lastErr)
	}

	if totalChunks <= 1 {
		if err := uploadChunk(0); err != nil {
			return "", err
		}
	} else {
		var idxMu sync.Mutex
		nextIdx := 0
		var errMu sync.Mutex
		var firstErr error

		var cwg sync.WaitGroup
		for range concurrency {
			cwg.Add(1)
			go func() {
				defer cwg.Done()
				for {
					idxMu.Lock()
					if nextIdx >= totalChunks {
						idxMu.Unlock()
						return
					}
					idx := nextIdx
					nextIdx++
					idxMu.Unlock()

					if err := uploadChunk(idx); err != nil {
						errMu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						errMu.Unlock()
						return
					}
				}
			}()
		}
		cwg.Wait()
		if firstErr != nil {
			return "", firstErr
		}
	}

	finishFrame, err := c.sendReply(ctx, generateReqID(cmdUploadMediaFinish), cmdUploadMediaFinish, map[string]any{
		"upload_id": initBody.UploadID,
	})
	if err != nil {
		return "", err
	}
	var finishBody struct {
		MediaID string `json:"media_id"`
	}
	if len(finishFrame.Body) > 0 {
		_ = json.Unmarshal(finishFrame.Body, &finishBody)
	}
	if finishBody.MediaID == "" {
		return "", errors.New("wecom: upload finish returned no media_id")
	}
	return finishBody.MediaID, nil
}

func (c *WSClient) DownloadFile(ctx context.Context, fileURL string, aesKey string) ([]byte, string, error) {
	dlCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(dlCtx, http.MethodGet, fileURL, nil)
	if err != nil {
		return nil, "", err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("wecom: file download HTTP %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}
	filename := parseContentDispositionFilename(resp.Header.Get("Content-Disposition"))
	if strings.TrimSpace(aesKey) == "" {
		return raw, filename, nil
	}
	decrypted, err := DecryptFile(raw, aesKey)
	if err != nil {
		return nil, "", err
	}
	return decrypted, filename, nil
}

func parseContentDispositionFilename(cd string) string {
	if strings.TrimSpace(cd) == "" {
		return ""
	}
	if _, params, err := mime.ParseMediaType(cd); err == nil {
		if fn := params["filename"]; fn != "" {
			if decoded, uErr := url.QueryUnescape(fn); uErr == nil {
				return decoded
			}
			return fn
		}
	}
	return ""
}

func (c *WSClient) Close() error {
	c.mu.Lock()
	if c.manualClose {
		c.mu.Unlock()
		c.wg.Wait()
		return nil
	}
	c.manualClose = true
	if c.reconnectTimer != nil {
		c.reconnectTimer.Stop()
		c.reconnectTimer = nil
	}
	ws := c.conn
	c.conn = nil
	c.clearPendingLocked("Connection manually closed")
	c.resolveInitLocked(errors.New("wecom: connection closed"))
	c.mu.Unlock()

	c.lifeCancel()
	if ws != nil {
		_ = ws.CloseNow()
	}
	c.wg.Wait()
	return nil
}
