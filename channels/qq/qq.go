// Package qq implements the QQ Open Platform bot channel adapter with QR
// connector binding, WebSocket Gateway (Hello/Heartbeat/Identify/Resume),
// C2C and group passive-to-active reply fallback, media uploads, and DM typing
// renewal.
package qq

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	imsdk "github.com/kilingzhang/im-sdk-go"
)

const (
	defaultQRBaseURL    = "https://q.qq.com"
	defaultTokenBaseURL = "https://bots.qq.com"
	defaultAPIBaseURL   = "https://api.sgroup.qq.com"

	qrTTL               = 5 * time.Minute
	typingSeconds       = 60
	defaultTypingRenew  = 50 * time.Second
	defaultTypingWindow = 270 * time.Second // 4.5 minutes
	defaultQRPoll       = 2 * time.Second
	defaultReadyTimeout = 10 * time.Second

	bindStatusNone      = 0
	bindStatusPending   = 1
	bindStatusCompleted = 2
	bindStatusExpired   = 3

	mediaTypeImage = 1
	mediaTypeFile  = 4
)

var (
	imageExtensions = map[string]struct{}{
		".png": {}, ".jpg": {}, ".jpeg": {}, ".gif": {}, ".webp": {}, ".bmp": {},
	}

	capabilities = imsdk.Capabilities{
		QuoteReply:      false,
		Reactions:       false,
		TypingIndicator: true,
		FileUpload:      true,
		FileDownload:    true,
		ListMembers:     false,
		Mentions:        false,
		MaxTextLength:   2000,
	}
)

// Options configures a QQ Channel adapter.
type Options struct {
	HTTPClient          *http.Client
	DialWS              func(ctx context.Context, url string, opts *websocket.DialOptions) (*websocket.Conn, *http.Response, error)
	QRBaseURL           string
	TokenBaseURL        string
	APIBaseURL          string
	QRPollInterval      time.Duration
	ReadyTimeout        time.Duration
	ReconnectInterval   time.Duration
	TypingRenewInterval time.Duration
	TypingWindow        time.Duration
	Now                 func() time.Time
}

type tokenError struct {
	StatusCode int
	MissingTok bool
	Msg        string
	Err        error
}

func (e *tokenError) Error() string { return e.Msg }
func (e *tokenError) Unwrap() error { return e.Err }

type apiError struct {
	StatusCode int
	Path       string
	BizCode    int
	Msg        string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("qq api %s HTTP %d: %s", e.Path, e.StatusCode, e.Msg)
}

// StartFailure classifies why QQ bot startup failed: 4xx HTTP errors or an
// empty access_token mean credentials_revoked; network or 5xx errors mean network.
func StartFailure(err error) imsdk.ConnectionErrorCode {
	if err == nil {
		return imsdk.ConnErrNetwork
	}
	var te *tokenError
	if errors.As(err, &te) {
		if (te.StatusCode >= 400 && te.StatusCode < 500) || te.MissingTok {
			return imsdk.ConnErrCredentialsRevoked
		}
		return imsdk.ConnErrNetwork
	}
	msg := err.Error()
	if strings.HasPrefix(msg, "Token fetch failed: HTTP 4") || strings.HasPrefix(msg, "Failed to get access_token") {
		return imsdk.ConnErrCredentialsRevoked
	}
	return imsdk.ConnErrNetwork
}

type tokenCacheEntry struct {
	token     string
	expiresAt time.Time
}

type channel struct {
	imsdk.BaseChannel
	httpClient        *http.Client
	dialWS            func(ctx context.Context, url string, opts *websocket.DialOptions) (*websocket.Conn, *http.Response, error)
	qrBaseURL         string
	tokenBaseURL      string
	apiBaseURL        string
	qrPollInterval    time.Duration
	readyTimeout      time.Duration
	reconnectInterval time.Duration
	typingRenew       time.Duration
	typingWindow      time.Duration
	now               func() time.Time

	tokenMu sync.Mutex
	tokens  map[string]tokenCacheEntry
}

// New constructs a QQ Channel adapter.
func New(opts Options) imsdk.Channel {
	hc := opts.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}
	dialWS := opts.DialWS
	if dialWS == nil {
		dialWS = websocket.Dial
	}
	qrBase := strings.TrimRight(opts.QRBaseURL, "/")
	if qrBase == "" {
		qrBase = defaultQRBaseURL
	}
	tokBase := strings.TrimRight(opts.TokenBaseURL, "/")
	if tokBase == "" {
		tokBase = defaultTokenBaseURL
	}
	apiBase := strings.TrimRight(opts.APIBaseURL, "/")
	if apiBase == "" {
		apiBase = defaultAPIBaseURL
	}
	qrPoll := opts.QRPollInterval
	if qrPoll <= 0 {
		qrPoll = defaultQRPoll
	}
	readyTimeout := opts.ReadyTimeout
	if readyTimeout <= 0 {
		readyTimeout = defaultReadyTimeout
	}
	typingRenew := opts.TypingRenewInterval
	if typingRenew <= 0 {
		typingRenew = defaultTypingRenew
	}
	typingWindow := opts.TypingWindow
	if typingWindow <= 0 {
		typingWindow = defaultTypingWindow
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	return &channel{
		BaseChannel: imsdk.BaseChannel{
			PlatformValue:       imsdk.PlatformQQ,
			TrustValue:          imsdk.TrustPublic,
			OnboardingValue:     imsdk.OnboardingMethod{Kind: imsdk.OnboardingQR},
			ManualSetupURLValue: "https://q.qq.com/qqbot/",
			ManualFieldsValue: []imsdk.CredentialField{
				{Key: "appId", LabelKey: "qq.appId", Secret: false},
				{Key: "appSecret", LabelKey: "qq.appSecret", Secret: true},
			},
		},
		httpClient:        hc,
		dialWS:            dialWS,
		qrBaseURL:         qrBase,
		tokenBaseURL:      tokBase,
		apiBaseURL:        apiBase,
		qrPollInterval:    qrPoll,
		readyTimeout:      readyTimeout,
		reconnectInterval: opts.ReconnectInterval,
		typingRenew:       typingRenew,
		typingWindow:      typingWindow,
		now:               nowFn,
		tokens:            make(map[string]tokenCacheEntry),
	}
}

func (c *channel) getAccessToken(ctx context.Context, appID, appSecret string) (string, error) {
	id := strings.TrimSpace(appID)
	now := c.now()

	c.tokenMu.Lock()
	if ent, ok := c.tokens[id]; ok {
		rem := ent.expiresAt.Sub(now)
		ahead := min(5*time.Minute, rem/3)
		if ahead < 0 {
			ahead = 0
		}
		if now.Before(ent.expiresAt.Add(-ahead)) {
			c.tokenMu.Unlock()
			return ent.token, nil
		}
	}
	c.tokenMu.Unlock()

	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rawReq, _ := json.Marshal(map[string]string{
		"appId":        id,
		"clientSecret": appSecret,
	})
	req, err := http.NewRequestWithContext(rctx, http.MethodPost, c.tokenBaseURL+"/app/getAppAccessToken", bytes.NewReader(rawReq))
	if err != nil {
		return "", &tokenError{Msg: "Network error getting access_token: " + err.Error(), Err: err}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", &tokenError{Msg: "Network error getting access_token: " + err.Error(), Err: err}
	}
	defer resp.Body.Close()

	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", &tokenError{StatusCode: resp.StatusCode, Msg: fmt.Sprintf("Token fetch failed: HTTP %d", resp.StatusCode), Err: err}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", &tokenError{
			StatusCode: resp.StatusCode,
			Msg:        fmt.Sprintf("Token fetch failed: HTTP %d — %s", resp.StatusCode, string(rawBody)),
		}
	}

	var parsed struct {
		AccessToken string          `json:"access_token"`
		ExpiresIn   json.RawMessage `json:"expires_in"`
	}
	if err := json.Unmarshal(rawBody, &parsed); err != nil {
		return "", &tokenError{StatusCode: resp.StatusCode, Msg: "Failed to parse access_token response: " + err.Error(), Err: err}
	}
	if parsed.AccessToken == "" {
		return "", &tokenError{
			StatusCode: resp.StatusCode,
			MissingTok: true,
			Msg:        "Failed to get access_token: " + string(rawBody),
		}
	}

	expSec := 7200
	if len(parsed.ExpiresIn) > 0 {
		var n int
		if json.Unmarshal(parsed.ExpiresIn, &n) == nil && n > 0 {
			expSec = n
		} else {
			var s string
			if json.Unmarshal(parsed.ExpiresIn, &s) == nil {
				if v, err := strconv.Atoi(s); err == nil && v > 0 {
					expSec = v
				}
			}
		}
	}

	expAt := c.now().Add(time.Duration(expSec) * time.Second)
	c.tokenMu.Lock()
	c.tokens[id] = tokenCacheEntry{token: parsed.AccessToken, expiresAt: expAt}
	c.tokenMu.Unlock()
	return parsed.AccessToken, nil
}

func (c *channel) clearTokenCache(appID string) {
	c.tokenMu.Lock()
	delete(c.tokens, strings.TrimSpace(appID))
	c.tokenMu.Unlock()
}

func (c *channel) apiRequest(ctx context.Context, appID, appSecret, method, apiPath string, body any, out any) error {
	tok, err := c.getAccessToken(ctx, appID, appSecret)
	if err != nil {
		return err
	}
	timeout := 30 * time.Second
	if strings.Contains(apiPath, "/files") {
		timeout = 120 * time.Second
	}
	rctx := ctx
	cancel := func() {}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		rctx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(rctx, method, c.apiBaseURL+apiPath, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "QQBot "+tok)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return &apiError{StatusCode: 0, Path: apiPath, Msg: err.Error()}
	}
	defer resp.Body.Close()

	rawResp, err := io.ReadAll(resp.Body)
	if err != nil {
		return &apiError{StatusCode: resp.StatusCode, Path: apiPath, Msg: err.Error()}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errBody struct {
			Code    int    `json:"code"`
			ErrCode int    `json:"err_code"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(rawResp, &errBody)
		biz := errBody.Code
		if biz == 0 {
			biz = errBody.ErrCode
		}
		msg := errBody.Message
		if msg == "" {
			msg = string(rawResp)
		}
		return &apiError{StatusCode: resp.StatusCode, Path: apiPath, BizCode: biz, Msg: msg}
	}
	if out != nil && len(rawResp) > 0 {
		if err := json.Unmarshal(rawResp, out); err != nil {
			return &apiError{StatusCode: resp.StatusCode, Path: apiPath, Msg: err.Error()}
		}
	}
	return nil
}

// DecryptBindSecret decrypts bot_encrypt_secret using the base64 AES-256-GCM bind key.
func DecryptBindSecret(botEncryptSecret, bindKey string) (string, error) {
	key, err := base64.StdEncoding.DecodeString(bindKey)
	if err != nil {
		return "", err
	}
	raw, err := base64.StdEncoding.DecodeString(botEncryptSecret)
	if err != nil {
		return "", err
	}
	if len(raw) < 12+16 {
		return "", errors.New("qq: encrypted secret too short")
	}
	nonce := raw[:12]
	ciphertextWithTag := raw[12:]

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	plain, err := gcm.Open(nil, nonce, ciphertextWithTag, nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

func (c *channel) postQRJSON(ctx context.Context, path string, body any, out any) error {
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(rctx, http.MethodPost, c.qrBaseURL+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("qq: HTTP %d from %s", resp.StatusCode, path)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *channel) Onboard(ctx context.Context, input imsdk.Credentials, opts imsdk.OnboardingOptions) <-chan imsdk.OnboardingEvent {
	ch := make(chan imsdk.OnboardingEvent, 4)
	go func() {
		defer close(ch)
		c.runOnboard(ctx, input, opts, ch)
	}()
	return ch
}

func (c *channel) runOnboard(ctx context.Context, input imsdk.Credentials, opts imsdk.OnboardingOptions, out chan<- imsdk.OnboardingEvent) {
	name := opts.AgentName
	if name == "" {
		name = "QQ bot"
	}

	if input["appId"] != "" && input["appSecret"] != "" {
		if _, err := c.getAccessToken(ctx, input["appId"], input["appSecret"]); err != nil {
			out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureInvalidCredentials}
			return
		}
		out <- imsdk.OnboardingEvent{
			Kind: imsdk.EventSucceeded,
			Credentials: imsdk.Credentials{
				"appId":     input["appId"],
				"appSecret": input["appSecret"],
				"name":      name,
			},
			Bot: imsdk.BotIdentity{ID: input["appId"], Name: name},
		}
		return
	}

	for {
		if ctx.Err() != nil {
			out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureCancelled}
			return
		}

		var keyBytes [32]byte
		if _, err := rand.Read(keyBytes[:]); err != nil {
			out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureUnknown}
			return
		}
		bindKey := base64.StdEncoding.EncodeToString(keyBytes[:])

		var createResp struct {
			RetCode int    `json:"retcode"`
			Msg     string `json:"msg"`
			Data    struct {
				TaskID string `json:"task_id"`
			} `json:"data"`
		}
		if err := c.postQRJSON(ctx, "/lite/create_bind_task", map[string]string{"key": bindKey}, &createResp); err != nil || createResp.RetCode != 0 || createResp.Data.TaskID == "" {
			code := imsdk.FailureNetwork
			if ctx.Err() != nil {
				code = imsdk.FailureCancelled
			}
			out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: code}
			return
		}

		taskID := createResp.Data.TaskID
		qrURL := fmt.Sprintf("%s/qqbot/openclaw/connect.html?task_id=%s&source=%s&_wv=2",
			c.qrBaseURL, url.QueryEscape(taskID), url.QueryEscape("mingle"))

		out <- imsdk.OnboardingEvent{
			Kind:      imsdk.EventQRReady,
			QRContent: qrURL,
			ExpiresAt: c.now().Add(qrTTL),
		}

		expired := false
		for !expired {
			timer := time.NewTimer(c.qrPollInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureCancelled}
				return
			case <-timer.C:
			}

			var pollResp struct {
				RetCode int    `json:"retcode"`
				Msg     string `json:"msg"`
				Data    struct {
					Status           int             `json:"status"`
					BotAppID         json.RawMessage `json:"bot_appid"`
					BotEncryptSecret string          `json:"bot_encrypt_secret"`
					UserOpenID       string          `json:"user_openid"`
				} `json:"data"`
			}
			if err := c.postQRJSON(ctx, "/lite/poll_bind_result", map[string]string{"task_id": taskID}, &pollResp); err != nil || pollResp.RetCode != 0 {
				continue
			}

			switch pollResp.Data.Status {
			case bindStatusCompleted:
				appID := parseRawStringOrInt(pollResp.Data.BotAppID)
				secret, err := DecryptBindSecret(pollResp.Data.BotEncryptSecret, bindKey)
				if err != nil || appID == "" || secret == "" {
					out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureUnknown}
					return
				}
				out <- imsdk.OnboardingEvent{
					Kind: imsdk.EventSucceeded,
					Credentials: imsdk.Credentials{
						"appId":     appID,
						"appSecret": secret,
						"name":      name,
					},
					Bot:     imsdk.BotIdentity{ID: appID, Name: name},
					OwnerID: pollResp.Data.UserOpenID,
				}
				return

			case bindStatusExpired:
				expired = true
			}
		}
	}
}

func parseRawStringOrInt(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var n int64
	if json.Unmarshal(raw, &n) == nil && n != 0 {
		return strconv.FormatInt(n, 10)
	}
	return ""
}

type connection struct {
	imsdk.BaseConnection
	ch        *channel
	appID     string
	appSecret string
	gw        *gatewayClient

	mu          sync.Mutex
	targets     map[string]replyTarget
	targetOrder []string
	files       map[string]string
	typing      map[string]context.CancelFunc
	closed      bool
}

func (c *channel) Connect(ctx context.Context, creds imsdk.Credentials, events imsdk.ChannelEvents) (imsdk.ChannelConnection, error) {
	appID := creds["appId"]
	appSecret := creds["appSecret"]
	if appID == "" || appSecret == "" {
		return nil, errors.New("qq: appId and appSecret are required")
	}
	name := creds["name"]
	if name == "" {
		name = "QQ bot"
	}

	conn := &connection{
		BaseConnection: imsdk.BaseConnection{
			BotValue:          imsdk.BotIdentity{ID: appID, Name: name},
			CapabilitiesValue: capabilities,
		},
		ch:        c,
		appID:     appID,
		appSecret: appSecret,
		targets:   make(map[string]replyTarget),
		files:     make(map[string]string),
		typing:    make(map[string]context.CancelFunc),
	}

	readyCh := make(chan struct{})
	var readyOnce sync.Once
	var startedMu sync.Mutex
	started := false

	gw := newGatewayClient(
		c.dialWS,
		func(gctx context.Context) (string, error) {
			return c.getAccessToken(gctx, appID, appSecret)
		},
		func() { c.clearTokenCache(appID) },
		func(gctx context.Context) (string, error) {
			var res struct {
				URL string `json:"url"`
			}
			if err := c.apiRequest(gctx, appID, appSecret, http.MethodGet, "/gateway", nil, &res); err != nil {
				return "", err
			}
			if res.URL == "" {
				return "", errors.New("qq: empty gateway URL")
			}
			return res.URL, nil
		},
		c.reconnectInterval,
		gatewayCallbacks{
			onReady: func() {
				startedMu.Lock()
				alreadyStarted := started
				started = true
				startedMu.Unlock()
				readyOnce.Do(func() { close(readyCh) })
				if alreadyStarted && events.OnState != nil {
					events.OnState(imsdk.ConnectionState{Kind: imsdk.StateConnected})
				}
			},
			onResumed: func() {
				if events.OnState != nil {
					events.OnState(imsdk.ConnectionState{Kind: imsdk.StateConnected})
				}
			},
			onMessage: func(msg InboundPayload) {
				inbound, ok := ToInbound(msg, c.now())
				if !ok {
					return
				}
				conn.recordInbound(inbound.PlatformID, msg)
				if events.OnMessage != nil {
					events.OnMessage(*inbound)
				}
			},
			onState: events.OnState,
		},
	)
	conn.gw = gw

	if err := gw.start(ctx); err != nil {
		code := StartFailure(err)
		if code == imsdk.ConnErrCredentialsRevoked && events.OnState != nil {
			events.OnState(imsdk.ConnectionState{Kind: imsdk.StateError, Code: code})
		}
		return nil, err
	}

	timer := time.NewTimer(c.readyTimeout)
	defer timer.Stop()
	select {
	case <-readyCh:
		if events.OnState != nil {
			events.OnState(imsdk.ConnectionState{Kind: imsdk.StateConnected})
		}
	case <-timer.C:
		startedMu.Lock()
		started = true
		startedMu.Unlock()
	case <-ctx.Done():
		gw.close()
		return nil, ctx.Err()
	}

	return conn, nil
}

func (c *connection) recordInbound(platformID string, msg InboundPayload) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.targets[platformID]; !exists {
		c.targetOrder = append(c.targetOrder, platformID)
	}
	c.targets[platformID] = msg.ReplyTarget
	for len(c.targets) > 2000 && len(c.targetOrder) > 0 {
		oldest := c.targetOrder[0]
		c.targetOrder = c.targetOrder[1:]
		delete(c.targets, oldest)
	}
	for i, att := range msg.Attachments {
		if att.URL != "" {
			c.files[fmt.Sprintf("%s:%d", msg.MessageID, i)] = att.URL
		}
	}
}

func (c *connection) targetFor(chat imsdk.ChatRef, replyTo string) replyTarget {
	c.mu.Lock()
	defer c.mu.Unlock()
	if replyTo != "" {
		if t, ok := c.targets[replyTo]; ok {
			return t
		}
	}
	scope := "group"
	if chat.Kind == imsdk.ChatDM {
		scope = "c2c"
	}
	return replyTarget{scope: scope, targetID: chat.PlatformID}
}

func messagePath(scope, targetID string) string {
	if scope == "c2c" {
		return "/v2/users/" + url.PathEscape(targetID) + "/messages"
	}
	return "/v2/groups/" + url.PathEscape(targetID) + "/messages"
}

func mediaUploadPath(scope, targetID string) string {
	if scope == "c2c" {
		return "/v2/users/" + url.PathEscape(targetID) + "/files"
	}
	return "/v2/groups/" + url.PathEscape(targetID) + "/files"
}

func nextMsgSeq(now time.Time) int {
	timePart := int(now.UnixMilli() % 100_000_000)
	n, err := rand.Int(rand.Reader, big.NewInt(65536))
	r := 0
	if err == nil {
		r = int(n.Int64())
	}
	return (timePart ^ r) & 0xffff
}

func (c *connection) sendText(ctx context.Context, target replyTarget, content string) (string, error) {
	body := map[string]any{
		"content":  content,
		"msg_type": 0,
	}
	if target.msgID != "" {
		body["msg_id"] = target.msgID
		body["msg_seq"] = nextMsgSeq(c.ch.now())
	}
	var resp struct {
		ID string `json:"id"`
	}
	if err := c.ch.apiRequest(ctx, c.appID, c.appSecret, http.MethodPost, messagePath(target.scope, target.targetID), body, &resp); err != nil {
		return "", err
	}
	return resp.ID, nil
}

func (c *connection) sendMediaFile(ctx context.Context, target replyTarget, filePath, fileName string, fileType int) (string, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", err
	}
	uploadBody := map[string]any{
		"file_type":    fileType,
		"srv_send_msg": false,
		"file_data":    base64.StdEncoding.EncodeToString(data),
	}
	if fileType == mediaTypeFile && fileName != "" {
		uploadBody["file_name"] = fileName
	}
	var uploaded struct {
		FileInfo string `json:"file_info"`
	}
	if err := c.ch.apiRequest(ctx, c.appID, c.appSecret, http.MethodPost, mediaUploadPath(target.scope, target.targetID), uploadBody, &uploaded); err != nil {
		return "", err
	}
	if uploaded.FileInfo == "" {
		return "", errors.New("qq: empty file_info")
	}

	msgSeq := 1
	if target.msgID != "" {
		msgSeq = nextMsgSeq(c.ch.now())
	}
	msgBody := map[string]any{
		"msg_type": 7,
		"media":    map[string]string{"file_info": uploaded.FileInfo},
		"msg_seq":  msgSeq,
	}
	if target.msgID != "" {
		msgBody["msg_id"] = target.msgID
	}
	var sent struct {
		ID string `json:"id"`
	}
	if err := c.ch.apiRequest(ctx, c.appID, c.appSecret, http.MethodPost, messagePath(target.scope, target.targetID), msgBody, &sent); err != nil {
		return "", err
	}
	return sent.ID, nil
}

func (c *connection) sendTypingNotice(ctx context.Context, target replyTarget, seconds int) error {
	if target.scope != "c2c" {
		return nil
	}
	msgSeq := 1
	if target.msgID != "" {
		msgSeq = nextMsgSeq(c.ch.now())
	}
	body := map[string]any{
		"msg_type":     6,
		"input_notify": map[string]int{"input_type": 1, "input_second": seconds},
		"msg_seq":      msgSeq,
	}
	if target.msgID != "" {
		body["msg_id"] = target.msgID
	}
	return c.ch.apiRequest(ctx, c.appID, c.appSecret, http.MethodPost, messagePath("c2c", target.targetID), body, nil)
}

func (c *connection) Send(ctx context.Context, msg imsdk.OutboundMessage) (imsdk.SendResult, error) {
	target := c.targetFor(msg.Chat, msg.ReplyTo)
	lastID := ""

	if strings.TrimSpace(msg.Text) != "" {
		id, err := c.sendText(ctx, target, msg.Text)
		if err != nil && target.msgID != "" {
			active := target
			active.msgID = ""
			id, err = c.sendText(ctx, active, msg.Text)
		}
		if err != nil {
			var ae *apiError
			if errors.As(err, &ae) && ae.StatusCode == http.StatusTooManyRequests {
				return imsdk.SendResult{
					Outcome:   imsdk.OutcomeFailed,
					Code:      imsdk.SendErrRateLimited,
					Retryable: true,
					Detail:    err.Error(),
				}, nil
			}
			return imsdk.SendResult{
				Outcome:   imsdk.OutcomeFailed,
				Code:      imsdk.SendErrUnknown,
				Retryable: false,
				Detail:    err.Error(),
			}, nil
		}
		lastID = id
	}

	dropped := false
	for _, file := range msg.Files {
		name := file.Name
		if name == "" {
			name = filepath.Base(file.Path)
		}
		ext := strings.ToLower(filepath.Ext(name))
		ft := mediaTypeFile
		if _, isImg := imageExtensions[ext]; isImg {
			ft = mediaTypeImage
		}
		id, err := c.sendMediaFile(ctx, target, file.Path, name, ft)
		if err != nil && target.msgID != "" {
			active := target
			active.msgID = ""
			id, err = c.sendMediaFile(ctx, active, file.Path, name, ft)
		}
		if err != nil {
			dropped = true
			continue
		}
		if id != "" {
			lastID = id
		}
	}

	if lastID == "" {
		lastID = fmt.Sprintf("out-%d", c.ch.now().UnixMilli())
	}
	degraded := []imsdk.Degradation{imsdk.DegradedMarkdownFlattened}
	if dropped {
		degraded = append(degraded, imsdk.DegradedFilesDropped)
	}
	return imsdk.SendResult{
		Outcome:    imsdk.OutcomeSent,
		PlatformID: lastID,
		Degraded:   degraded,
	}, nil
}

func (c *connection) React(ctx context.Context, chat imsdk.ChatRef, platformMsgID string, state imsdk.ReactionState) error {
	c.mu.Lock()
	if cancel, ok := c.typing[platformMsgID]; ok {
		cancel()
		delete(c.typing, platformMsgID)
	}
	if c.closed || state != imsdk.ReactionProcessing || chat.Kind != imsdk.ChatDM {
		c.mu.Unlock()
		return nil
	}
	tctx, tcancel := context.WithCancel(context.Background())
	c.typing[platformMsgID] = tcancel
	c.mu.Unlock()

	target := c.targetFor(chat, platformMsgID)
	started := c.ch.now()
	_ = c.sendTypingNotice(ctx, target, typingSeconds)

	go func() {
		ticker := time.NewTicker(c.ch.typingRenew)
		defer ticker.Stop()
		for {
			select {
			case <-tctx.Done():
				return
			case <-ticker.C:
				if c.ch.now().Sub(started) > c.ch.typingWindow {
					c.mu.Lock()
					delete(c.typing, platformMsgID)
					c.mu.Unlock()
					return
				}
				_ = c.sendTypingNotice(tctx, target, typingSeconds)
			}
		}
	}()
	return nil
}

func (c *connection) SetTyping(ctx context.Context, chat imsdk.ChatRef, typing bool) error {
	if typing && chat.Kind == imsdk.ChatDM {
		_ = c.sendTypingNotice(ctx, c.targetFor(chat, ""), typingSeconds)
	}
	return nil
}

func (c *connection) Download(ctx context.Context, att imsdk.InboundAttachment, toPath string) error {
	c.mu.Lock()
	rawURL, ok := c.files[att.ID]
	c.mu.Unlock()
	if !ok || rawURL == "" {
		return errors.New("qq: attachment is no longer available")
	}
	if strings.HasPrefix(rawURL, "//") {
		rawURL = "https:" + rawURL
	}

	rctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	resp, err := c.ch.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("qq: download failed HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return os.WriteFile(toPath, data, 0o600)
}

func (c *connection) Close() error {
	c.mu.Lock()
	c.closed = true
	for id, cancel := range c.typing {
		cancel()
		delete(c.typing, id)
	}
	c.mu.Unlock()
	if c.gw != nil {
		c.gw.close()
	}
	return nil
}

// ToInbound converts a QQ Gateway message payload into an InboundMessage.
func ToInbound(msg InboundPayload, now time.Time) (*imsdk.InboundMessage, bool) {
	if msg.Kind != "c2c" && msg.Kind != "group" {
		return nil, false
	}
	if msg.SenderIsBot || msg.MessageID == "" || msg.SenderID == "" {
		return nil, false
	}
	isGroup := msg.Kind == "group" && msg.GroupOpenID != ""

	attachments := make([]imsdk.InboundAttachment, 0, len(msg.Attachments))
	for i, a := range msg.Attachments {
		name := a.Filename
		if name == "" {
			prefix := "file"
			if strings.HasPrefix(a.ContentType, "image") {
				prefix = "image"
			}
			name = fmt.Sprintf("%s-%d", prefix, i+1)
		}
		att := imsdk.InboundAttachment{
			ID:   fmt.Sprintf("%s:%d", msg.MessageID, i),
			Name: name,
			Mime: a.ContentType,
		}
		if a.Size != nil {
			att.Size = *a.Size
		}
		attachments = append(attachments, att)
	}

	var chat imsdk.ChatRef
	if isGroup {
		chat = imsdk.ChatRef{PlatformID: msg.GroupOpenID, Kind: imsdk.ChatGroup}
	} else {
		chat = imsdk.ChatRef{PlatformID: msg.SenderID, Kind: imsdk.ChatDM}
	}

	sentAt := now
	if ts, err := time.Parse(time.RFC3339, msg.Timestamp); err == nil {
		sentAt = ts
	}

	return &imsdk.InboundMessage{
		PlatformID:     msg.MessageID,
		Chat:           chat,
		Sender:         imsdk.Sender{ID: msg.SenderID, Name: msg.SenderName, IsBot: false},
		Text:           strings.TrimSpace(msg.Content),
		Attachments:    attachments,
		AddressedToBot: true,
		SentAt:         sentAt,
	}, true
}
