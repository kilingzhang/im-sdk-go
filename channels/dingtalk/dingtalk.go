// Package dingtalk implements the DingTalk robot channel adapter with device QR
// registration, pure Go Stream WebSocket transport, OpenAPI sends, emotion
// thinking tags, and streaming AI cards.
package dingtalk

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"

	imsdk "github.com/kilingzhang/im-sdk-go"
)

const (
	defaultRegistrationBase = "https://oapi.dingtalk.com"
	defaultAPIBase          = "https://api.dingtalk.com"

	// RegistrationSource identifies the device-registration client to DingTalk.
	RegistrationSource = "DING_DWS_CLAW"

	// AICardTemplate is DingTalk's shared streaming markdown AI card schema.
	AICardTemplate = "02fcf2f4-5e02-4a85-b672-46d1f715543e.schema"

	thinkingEmotion       = "🤔思考中"
	textReactionID        = "2659900"
	textReactionBg        = "im_bg_1"
	qrMaxDuration         = 15 * time.Minute
	MaxMediaBytes   int64 = 20 * 1024 * 1024
)

var (
	imageExtensions = map[string]struct{}{
		".jpg": {}, ".jpeg": {}, ".png": {}, ".gif": {}, ".bmp": {},
	}

	capabilities = imsdk.Capabilities{
		QuoteReply:      false,
		Reactions:       true,
		TypingIndicator: false,
		FileUpload:      true,
		FileDownload:    true,
		ListMembers:     false,
		Mentions:        false,
		MaxTextLength:   5000,
	}
)

// Options configures a DingTalk Channel adapter.
type Options struct {
	HTTPClient          *http.Client
	DialWS              func(ctx context.Context, url string, opts *websocket.DialOptions) (*websocket.Conn, *http.Response, error)
	RegistrationBaseURL string
	APIBaseURL          string
	NewStreamClient     func(clientID, clientSecret string) StreamClient
	ReconnectInterval   time.Duration
	Now                 func() time.Time
}

// DingTalkMessage represents the JSON payload delivered on TOPIC_ROBOT.
type DingTalkMessage struct {
	MsgID             string `json:"msgId"`
	ConversationID    string `json:"conversationId"`
	ConversationType  string `json:"conversationType"` // "1" direct, "2" group
	ConversationTitle string `json:"conversationTitle"`
	SenderStaffID     string `json:"senderStaffId"`
	SenderID          string `json:"senderId"`
	SenderNick        string `json:"senderNick"`
	IsInAtList        *bool  `json:"isInAtList"`
	CreateAt          int64  `json:"createAt"`
	RobotCode         string `json:"robotCode"`
	MsgType           string `json:"msgtype"`
	Text              *struct {
		Content string `json:"content"`
	} `json:"text"`
	Content *struct {
		DownloadCode string `json:"downloadCode"`
		FileName     string `json:"fileName"`
		Recognition  string `json:"recognition"`
		RichText     []struct {
			Text         string `json:"text"`
			DownloadCode string `json:"downloadCode"`
			Type         string `json:"type"`
		} `json:"richText"`
	} `json:"content"`
}

type httpError struct {
	StatusCode int
	Code       string
	ErrCode    int
	Message    string
}

func (e *httpError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("dingtalk HTTP %d %s: %s", e.StatusCode, e.Code, e.Message)
	}
	if e.ErrCode != 0 {
		return fmt.Sprintf("dingtalk HTTP %d errcode=%d: %s", e.StatusCode, e.ErrCode, e.Message)
	}
	return fmt.Sprintf("dingtalk HTTP %d: %s", e.StatusCode, e.Message)
}

type tokenCacheEntry struct {
	token string
	until time.Time
}

type channel struct {
	imsdk.BaseChannel
	httpClient        *http.Client
	noRedirectClient  *http.Client
	dialWS            func(ctx context.Context, url string, opts *websocket.DialOptions) (*websocket.Conn, *http.Response, error)
	regBase           string
	apiBase           string
	newStream         func(clientID, clientSecret string) StreamClient
	reconnectInterval time.Duration
	now               func() time.Time

	tokenMu sync.Mutex
	tokens  map[string]tokenCacheEntry
}

// New constructs a DingTalk Channel adapter.
func New(opts Options) imsdk.Channel {
	hc := opts.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}
	noRedirect := *hc
	noRedirect.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("dingtalk: unexpected redirect")
	}

	dialWS := opts.DialWS
	if dialWS == nil {
		dialWS = websocket.Dial
	}
	regBase := strings.TrimRight(opts.RegistrationBaseURL, "/")
	if regBase == "" {
		regBase = defaultRegistrationBase
	}
	apiBase := strings.TrimRight(opts.APIBaseURL, "/")
	if apiBase == "" {
		apiBase = defaultAPIBase
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	ch := &channel{
		BaseChannel: imsdk.BaseChannel{
			PlatformValue: imsdk.PlatformDingTalk,
			TrustValue:    imsdk.TrustOrg,
			OnboardingValue: imsdk.OnboardingMethod{
				Kind:        imsdk.OnboardingQR,
				DesktopLink: true,
			},
			ManualSetupURLValue: "https://open-dev.dingtalk.com/",
			ManualFieldsValue: []imsdk.CredentialField{
				{Key: "clientId", LabelKey: "dingtalk.clientId", Secret: false},
				{Key: "clientSecret", LabelKey: "dingtalk.clientSecret", Secret: true},
			},
		},
		httpClient:        hc,
		noRedirectClient:  &noRedirect,
		dialWS:            dialWS,
		regBase:           regBase,
		apiBase:           apiBase,
		newStream:         opts.NewStreamClient,
		reconnectInterval: opts.ReconnectInterval,
		now:               nowFn,
		tokens:            make(map[string]tokenCacheEntry),
	}
	return ch
}

func (c *channel) doJSON(ctx context.Context, method, fullURL string, headers map[string]string, body any, timeout time.Duration, out any) error {
	rctx := ctx
	cancel := func() {}
	if _, ok := ctx.Deadline(); !ok && timeout > 0 {
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
	req, err := http.NewRequestWithContext(rctx, method, fullURL, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.noRedirectClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	rawResp, err := io.ReadAll(resp.Body)
	if err != nil {
		return &httpError{StatusCode: resp.StatusCode, Message: err.Error()}
	}

	var env struct {
		ErrCode *int   `json:"errcode"`
		Code    string `json:"code"`
		Message string `json:"message"`
		ErrMsg  string `json:"errmsg"`
	}
	_ = json.Unmarshal(rawResp, &env)
	msg := env.Message
	if msg == "" {
		msg = env.ErrMsg
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || (env.ErrCode != nil && *env.ErrCode != 0) {
		ec := 0
		if env.ErrCode != nil {
			ec = *env.ErrCode
		}
		return &httpError{
			StatusCode: resp.StatusCode,
			Code:       env.Code,
			ErrCode:    ec,
			Message:    msg,
		}
	}
	if out != nil && len(rawResp) > 0 {
		if err := json.Unmarshal(rawResp, out); err != nil {
			return err
		}
	}
	return nil
}

func (c *channel) token(ctx context.Context, appKey, appSecret string) (string, error) {
	now := c.now()
	c.tokenMu.Lock()
	if cached, ok := c.tokens[appKey]; ok && now.Before(cached.until) {
		c.tokenMu.Unlock()
		return cached.token, nil
	}
	c.tokenMu.Unlock()

	var resp struct {
		AccessToken string `json:"accessToken"`
		ExpireIn    int    `json:"expireIn"`
	}
	err := c.doJSON(ctx, http.MethodPost, c.apiBase+"/v1.0/oauth2/accessToken", nil, map[string]string{
		"appKey":    appKey,
		"appSecret": appSecret,
	}, 15*time.Second, &resp)
	if err != nil {
		return "", err
	}
	if resp.AccessToken == "" {
		return "", errors.New("dingtalk: empty accessToken")
	}
	expSec := resp.ExpireIn
	if expSec <= 0 {
		expSec = 7200
	}
	ttl := expSec - 60
	if ttl < 60 {
		ttl = 60
	}
	c.tokenMu.Lock()
	c.tokens[appKey] = tokenCacheEntry{
		token: resp.AccessToken,
		until: c.now().Add(time.Duration(ttl) * time.Second),
	}
	c.tokenMu.Unlock()
	return resp.AccessToken, nil
}

func (c *channel) callAPI(ctx context.Context, appKey, appSecret, method, apiPath string, body any, out any) error {
	tok, err := c.token(ctx, appKey, appSecret)
	if err != nil {
		return err
	}
	path := strings.TrimLeft(apiPath, "/")
	return c.doJSON(ctx, method, c.apiBase+"/"+path, map[string]string{
		"x-acs-dingtalk-access-token": tok,
	}, body, 15*time.Second, out)
}

func (c *channel) uploadMedia(ctx context.Context, appKey, appSecret string, data []byte, fileName, mediaType string) (string, error) {
	tok, err := c.token(ctx, appKey, appSecret)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(c.regBase + "/media/upload")
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("access_token", tok)
	q.Set("type", mediaType)
	u.RawQuery = q.Encode()

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("media", fileName)
	if err != nil {
		return "", err
	}
	if _, err := fw.Write(data); err != nil {
		return "", err
	}
	_ = mw.Close()

	rctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodPost, u.String(), &buf)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := c.noRedirectClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var out struct {
		ErrCode *int   `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
		MediaID string `json:"media_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", &httpError{StatusCode: resp.StatusCode, Message: err.Error()}
	}
	if resp.StatusCode >= 400 || (out.ErrCode != nil && *out.ErrCode != 0) || out.MediaID == "" {
		ec := 0
		if out.ErrCode != nil {
			ec = *out.ErrCode
		}
		return "", &httpError{StatusCode: resp.StatusCode, ErrCode: ec, Message: out.ErrMsg}
	}
	return out.MediaID, nil
}

func validDingTalkQRURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	host := strings.ToLower(u.Hostname())
	if u.Scheme != "https" || !(host == "dingtalk.com" || strings.HasSuffix(host, ".dingtalk.com")) {
		return "", errors.New("dingtalk: QR URL outside dingtalk.com")
	}
	return u.String(), nil
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
		name = "DingTalk bot"
	}

	if input["clientId"] != "" && input["clientSecret"] != "" {
		if _, err := c.token(ctx, input["clientId"], input["clientSecret"]); err != nil {
			out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureInvalidCredentials}
			return
		}
		out <- imsdk.OnboardingEvent{
			Kind: imsdk.EventSucceeded,
			Credentials: imsdk.Credentials{
				"clientId":     input["clientId"],
				"clientSecret": input["clientSecret"],
				"name":         name,
			},
			Bot: imsdk.BotIdentity{ID: input["clientId"], Name: name},
		}
		return
	}

	var initResp struct {
		Nonce string `json:"nonce"`
	}
	if err := c.doJSON(ctx, http.MethodPost, c.regBase+"/app/registration/init", nil, map[string]string{
		"source": RegistrationSource,
	}, 15*time.Second, &initResp); err != nil || initResp.Nonce == "" {
		code := imsdk.FailureNetwork
		if ctx.Err() != nil {
			code = imsdk.FailureCancelled
		}
		out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: code}
		return
	}

	var beginResp struct {
		DeviceCode              string  `json:"device_code"`
		VerificationURIComplete string  `json:"verification_uri_complete"`
		ExpiresIn               float64 `json:"expires_in"`
		Interval                float64 `json:"interval"`
	}
	if err := c.doJSON(ctx, http.MethodPost, c.regBase+"/app/registration/begin", nil, map[string]string{
		"nonce": initResp.Nonce,
	}, 15*time.Second, &beginResp); err != nil || beginResp.DeviceCode == "" || beginResp.VerificationURIComplete == "" {
		code := imsdk.FailureNetwork
		if ctx.Err() != nil {
			code = imsdk.FailureCancelled
		}
		out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: code}
		return
	}

	qrURL, err := validDingTalkQRURL(beginResp.VerificationURIComplete)
	if err != nil {
		out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureNetwork}
		return
	}

	expSec := beginResp.ExpiresIn
	if expSec <= 0 {
		expSec = 600
	}
	expDur := time.Duration(expSec * float64(time.Second))
	if expDur > qrMaxDuration {
		expDur = qrMaxDuration
	}
	intervalSec := beginResp.Interval
	if intervalSec <= 0 {
		intervalSec = 5
	}
	interval := time.Duration(intervalSec * float64(time.Second))
	if interval < 5*time.Millisecond {
		interval = 5 * time.Millisecond
	}

	deadline := c.now().Add(expDur)
	out <- imsdk.OnboardingEvent{
		Kind:      imsdk.EventQRReady,
		QRContent: qrURL,
		ExpiresAt: deadline,
	}

	for ctx.Err() == nil && c.now().Before(deadline) {
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureCancelled}
			return
		case <-timer.C:
		}

		var pollResp struct {
			Status       string `json:"status"`
			ClientID     string `json:"client_id"`
			ClientSecret string `json:"client_secret"`
		}
		if err := c.doJSON(ctx, http.MethodPost, c.regBase+"/app/registration/poll", nil, map[string]string{
			"device_code": beginResp.DeviceCode,
		}, 15*time.Second, &pollResp); err != nil {
			continue
		}

		switch strings.ToUpper(pollResp.Status) {
		case "SUCCESS":
			if pollResp.ClientID != "" && pollResp.ClientSecret != "" {
				out <- imsdk.OnboardingEvent{
					Kind: imsdk.EventSucceeded,
					Credentials: imsdk.Credentials{
						"clientId":     pollResp.ClientID,
						"clientSecret": pollResp.ClientSecret,
						"name":         name,
					},
					Bot: imsdk.BotIdentity{ID: pollResp.ClientID, Name: name},
				}
				return
			}
		case "EXPIRED":
			out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureExpired}
			return
		case "FAIL":
			out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureDenied}
			return
		}
	}

	code := imsdk.FailureExpired
	if ctx.Err() != nil {
		code = imsdk.FailureCancelled
	}
	out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: code}
}

type msgOrigin struct {
	conversationID string
	staffID        string
	nick           string
}

type connection struct {
	imsdk.BaseConnection
	ch        *channel
	appKey    string
	appSecret string
	robotCode string
	stream    StreamClient

	mu          sync.Mutex
	origins     map[string]msgOrigin
	originOrder []string
}

func (c *channel) Connect(ctx context.Context, creds imsdk.Credentials, events imsdk.ChannelEvents) (imsdk.ChannelConnection, error) {
	appKey := creds["clientId"]
	appSecret := creds["clientSecret"]
	if appKey == "" || appSecret == "" {
		return nil, errors.New("dingtalk: clientId and clientSecret are required")
	}
	robotCode := creds["robotCode"]
	if robotCode == "" {
		robotCode = appKey
	}
	botName := creds["name"]
	if botName == "" {
		botName = "DingTalk bot"
	}

	if _, err := c.token(ctx, appKey, appSecret); err != nil {
		return nil, err
	}

	var stream StreamClient
	if c.newStream != nil {
		stream = c.newStream(appKey, appSecret)
	} else {
		stream = newDWStreamClient(appKey, appSecret, c.apiBase, c.noRedirectClient, c.dialWS, c.reconnectInterval)
	}

	conn := &connection{
		BaseConnection: imsdk.BaseConnection{
			BotValue:          imsdk.BotIdentity{ID: appKey, Name: botName},
			CapabilitiesValue: capabilities,
		},
		ch:        c,
		appKey:    appKey,
		appSecret: appSecret,
		robotCode: robotCode,
		stream:    stream,
		origins:   make(map[string]msgOrigin),
	}

	err := stream.Connect(ctx, func(ev StreamEvent) {
		if ev.Topic != "" && ev.Topic != TopicRobot {
			return
		}
		_ = stream.AckCallback(context.Background(), ev.MessageID, map[string]any{"success": true})
		var dm DingTalkMessage
		if json.Unmarshal(ev.Data, &dm) != nil {
			return
		}
		inbound, ok := ToInbound(dm, c.now())
		if !ok {
			return
		}
		conn.recordOrigin(inbound.PlatformID, msgOrigin{
			conversationID: dm.ConversationID,
			staffID:        dm.SenderStaffID,
			nick:           dm.SenderNick,
		})
		if events.OnMessage != nil {
			events.OnMessage(*inbound)
		}
	}, events.OnState)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (c *connection) recordOrigin(msgID string, orig msgOrigin) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.origins[msgID]; !exists {
		c.originOrder = append(c.originOrder, msgID)
	}
	c.origins[msgID] = orig
	for len(c.origins) > 2000 && len(c.originOrder) > 0 {
		oldest := c.originOrder[0]
		c.originOrder = c.originOrder[1:]
		delete(c.origins, oldest)
	}
}

func (c *connection) lookupOrigin(msgID string) (msgOrigin, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	orig, ok := c.origins[msgID]
	return orig, ok
}

func (c *connection) sendRobot(ctx context.Context, chat imsdk.ChatRef, msgKey string, param map[string]string) error {
	paramJSON, err := json.Marshal(param)
	if err != nil {
		return err
	}
	if chat.Kind == imsdk.ChatDM {
		return c.ch.callAPI(ctx, c.appKey, c.appSecret, http.MethodPost, "v1.0/robot/oToMessages/batchSend", map[string]any{
			"robotCode": c.robotCode,
			"userIds":   []string{chat.PlatformID},
			"msgKey":    msgKey,
			"msgParam":  string(paramJSON),
		}, nil)
	}
	return c.ch.callAPI(ctx, c.appKey, c.appSecret, http.MethodPost, "v1.0/robot/groupMessages/send", map[string]any{
		"robotCode":          c.robotCode,
		"openConversationId": chat.PlatformID,
		"msgKey":             msgKey,
		"msgParam":           string(paramJSON),
	}, nil)
}

func (c *connection) sendFile(ctx context.Context, chat imsdk.ChatRef, file imsdk.OutboundFile) (bool, error) {
	st, err := os.Stat(file.Path)
	if err != nil {
		return false, err
	}
	if st.Size() == 0 || st.Size() > MaxMediaBytes {
		return false, nil
	}
	name := file.Name
	if name == "" {
		name = filepath.Base(file.Path)
	}
	ext := strings.ToLower(filepath.Ext(name))
	data, err := os.ReadFile(file.Path)
	if err != nil {
		return false, err
	}
	if _, isImg := imageExtensions[ext]; isImg {
		mediaID, err := c.ch.uploadMedia(ctx, c.appKey, c.appSecret, data, name, "image")
		if err != nil {
			return false, err
		}
		if err := c.sendRobot(ctx, chat, "sampleImageMsg", map[string]string{"photoURL": mediaID}); err != nil {
			return false, err
		}
		return true, nil
	}
	mediaID, err := c.ch.uploadMedia(ctx, c.appKey, c.appSecret, data, name, "file")
	if err != nil {
		return false, err
	}
	fileType := strings.TrimPrefix(ext, ".")
	if fileType == "" {
		fileType = "file"
	}
	if err := c.sendRobot(ctx, chat, "sampleFile", map[string]string{
		"mediaId":  mediaID,
		"fileName": name,
		"fileType": fileType,
	}); err != nil {
		return false, err
	}
	return true, nil
}

func (c *connection) Send(ctx context.Context, msg imsdk.OutboundMessage) (imsdk.SendResult, error) {
	var degraded []imsdk.Degradation
	if msg.ReplyTo != "" {
		degraded = append(degraded, imsdk.DegradedQuoteDropped)
	}
	sentAny := false

	handleErr := func(err error) (imsdk.SendResult, error) {
		if sentAny {
			if !slices.Contains(degraded, imsdk.DegradedFilesDropped) {
				degraded = append(degraded, imsdk.DegradedFilesDropped)
			}
			return imsdk.SendResult{
				Outcome:    imsdk.OutcomeSent,
				PlatformID: "out-" + newUUID(),
				Degraded:   degraded,
			}, nil
		}
		var he *httpError
		status := 0
		if errors.As(err, &he) {
			status = he.StatusCode
		}
		switch {
		case status == http.StatusTooManyRequests:
			return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrRateLimited, Retryable: true, Detail: err.Error()}, nil
		case status == http.StatusForbidden:
			return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrForbidden, Retryable: false, Detail: err.Error()}, nil
		case status >= 500:
			return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrUnknown, Retryable: true, Detail: err.Error()}, nil
		default:
			return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrNetwork, Retryable: status == 0, Detail: err.Error()}, nil
		}
	}

	if msg.Text != "" {
		firstLine := strings.SplitN(msg.Text, "\n", 2)[0]
		if utf8.RuneCountInString(firstLine) > 40 {
			firstLine = string([]rune(firstLine)[:40])
		}
		if firstLine == "" {
			firstLine = "Mingle"
		}
		if err := c.sendRobot(ctx, msg.Chat, "sampleMarkdown", map[string]string{
			"title": firstLine,
			"text":  msg.Text,
		}); err != nil {
			return handleErr(err)
		}
		sentAny = true
	}

	for _, file := range msg.Files {
		ok, err := c.sendFile(ctx, msg.Chat, file)
		if err != nil {
			return handleErr(err)
		}
		if ok {
			sentAny = true
		} else if !slices.Contains(degraded, imsdk.DegradedFilesDropped) {
			degraded = append(degraded, imsdk.DegradedFilesDropped)
		}
	}

	if !sentAny {
		return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrTooLarge, Retryable: false}, nil
	}
	return imsdk.SendResult{
		Outcome:    imsdk.OutcomeSent,
		PlatformID: "out-" + newUUID(),
		Degraded:   degraded,
	}, nil
}

func (c *connection) React(ctx context.Context, _ imsdk.ChatRef, platformMsgID string, state imsdk.ReactionState) error {
	orig, ok := c.lookupOrigin(platformMsgID)
	if !ok {
		return nil
	}
	action := "recall"
	if state == imsdk.ReactionProcessing {
		action = "reply"
	}
	_ = c.ch.callAPI(ctx, c.appKey, c.appSecret, http.MethodPost, "v1.0/robot/emotion/"+action, map[string]any{
		"robotCode":          c.robotCode,
		"openMsgId":          platformMsgID,
		"openConversationId": orig.conversationID,
		"emotionType":        2,
		"emotionName":        thinkingEmotion,
		"textEmotion": map[string]string{
			"emotionId":    textReactionID,
			"emotionName":  thinkingEmotion,
			"text":         thinkingEmotion,
			"backgroundId": textReactionBg,
		},
	}, nil)
	return nil
}

func (c *connection) StartLive(ctx context.Context, chat imsdk.ChatRef, live imsdk.LiveOptions) (imsdk.LiveReply, error) {
	outTrackID := "mingle_" + newUUID()
	orig, hasOrig := c.lookupOrigin(live.ReplyTo)
	var atUserIDs map[string]string
	var atStaffID, atNick string
	if chat.Kind != imsdk.ChatDM && hasOrig && orig.staffID != "" {
		atStaffID = orig.staffID
		atNick = orig.nick
		atUserIDs = map[string]string{atStaffID: atNick}
	}

	body := map[string]any{
		"outTrackId":            outTrackID,
		"userIdType":            1,
		"cardTemplateId":        AICardTemplate,
		"cardData":              aiCardData(live.Initial, "2"),
		"callbackType":          "STREAM",
		"imGroupOpenSpaceModel": map[string]bool{"supportForward": true},
		"imRobotOpenSpaceModel": map[string]bool{"supportForward": true},
	}
	if chat.Kind == imsdk.ChatDM {
		body["openSpaceId"] = "dtv1.card//IM_ROBOT." + chat.PlatformID
		body["imRobotOpenDeliverModel"] = map[string]string{
			"spaceType": "IM_ROBOT",
			"robotCode": c.robotCode,
		}
	} else {
		body["openSpaceId"] = "dtv1.card//IM_GROUP." + chat.PlatformID
		deliverModel := map[string]any{"robotCode": c.robotCode}
		if len(atUserIDs) > 0 {
			deliverModel["atUserIds"] = atUserIDs
		}
		body["imGroupOpenDeliverModel"] = deliverModel
	}

	if err := c.ch.callAPI(ctx, c.appKey, c.appSecret, http.MethodPost, "v1.0/card/instances/createAndDeliver", body, nil); err != nil {
		return nil, err
	}

	return &liveReply{
		conn:       c,
		outTrackID: outTrackID,
		atStaffID:  atStaffID,
		atNick:     atNick,
	}, nil
}

type liveReply struct {
	conn       *connection
	outTrackID string
	atStaffID  string
	atNick     string
}

func (l *liveReply) Until() time.Time { return time.Time{} }

func (l *liveReply) stream(ctx context.Context, content string, isFinalize bool) error {
	return l.conn.ch.callAPI(ctx, l.conn.appKey, l.conn.appSecret, http.MethodPut, "v1.0/card/streaming", map[string]any{
		"outTrackId": l.outTrackID,
		"guid":       newUUID(),
		"key":        "msgContent",
		"content":    content,
		"isFull":     true,
		"isFinalize": isFinalize,
		"isError":    false,
	}, nil)
}

func (l *liveReply) Update(ctx context.Context, markdown string) error {
	_ = l.stream(ctx, markdown, false)
	return nil
}

func (l *liveReply) Finish(ctx context.Context, markdown string, _ []imsdk.Mention) (imsdk.SendResult, error) {
	text := markdown
	if l.atStaffID != "" {
		text = fmt.Sprintf(`<a atId="%s">@%s</a> %s`, l.atStaffID, l.atNick, markdown)
	}
	if err := l.stream(ctx, text, true); err != nil {
		return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrUnknown, Retryable: false, Detail: err.Error()}, nil
	}
	err := l.conn.ch.callAPI(ctx, l.conn.appKey, l.conn.appSecret, http.MethodPut, "v1.0/card/instances", map[string]any{
		"outTrackId":        l.outTrackID,
		"cardData":          aiCardData(text, "3"),
		"cardUpdateOptions": map[string]bool{"updateCardDataByKey": true},
	}, nil)
	if err != nil {
		return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrUnknown, Retryable: false, Detail: err.Error()}, nil
	}
	return imsdk.SendResult{
		Outcome:    imsdk.OutcomeSent,
		PlatformID: l.outTrackID,
	}, nil
}

func aiCardData(text, flowStatus string) map[string]any {
	return map[string]any{
		"cardParamMap": map[string]string{
			"flowStatus":        flowStatus,
			"msgContent":        text,
			"staticMsgContent":  "",
			"sys_full_json_obj": `{"order":["msgContent"]}`,
			"config":            `{"autoLayout":true}`,
		},
	}
}

func (c *connection) Download(ctx context.Context, att imsdk.InboundAttachment, toPath string) error {
	var resp struct {
		DownloadURL string `json:"downloadUrl"`
	}
	if err := c.ch.callAPI(ctx, c.appKey, c.appSecret, http.MethodPost, "v1.0/robot/messageFiles/download", map[string]string{
		"downloadCode": att.ID,
		"robotCode":    c.robotCode,
	}, &resp); err != nil {
		return err
	}
	if resp.DownloadURL == "" {
		return errors.New("dingtalk: empty downloadUrl")
	}

	rctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodGet, resp.DownloadURL, nil)
	if err != nil {
		return err
	}
	dlResp, err := c.ch.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer dlResp.Body.Close()
	if dlResp.StatusCode < 200 || dlResp.StatusCode >= 300 {
		return fmt.Errorf("dingtalk: download failed HTTP %d", dlResp.StatusCode)
	}
	data, err := io.ReadAll(dlResp.Body)
	if err != nil {
		return err
	}
	return os.WriteFile(toPath, data, 0o600)
}

func (c *connection) Close() error {
	return c.stream.Close()
}

// ToInbound maps a DingTalk robot callback message into an InboundMessage.
func ToInbound(msg DingTalkMessage, now time.Time) (*imsdk.InboundMessage, bool) {
	if msg.MsgID == "" || msg.ConversationID == "" {
		return nil, false
	}
	dm := msg.ConversationType == "1"
	senderID := msg.SenderStaffID
	if senderID == "" {
		senderID = msg.SenderID
	}
	if senderID == "" {
		return nil, false
	}

	var parts []string
	var attachments []imsdk.InboundAttachment
	if msg.Text != nil && msg.Text.Content != "" {
		parts = append(parts, strings.TrimSpace(msg.Text.Content))
	}
	if msg.MsgType == "audio" && msg.Content != nil && msg.Content.Recognition != "" {
		parts = append(parts, msg.Content.Recognition)
	}
	if msg.MsgType == "picture" && msg.Content != nil && msg.Content.DownloadCode != "" {
		attachments = append(attachments, imsdk.InboundAttachment{
			ID:   msg.Content.DownloadCode,
			Name: "image-" + msg.MsgID + ".png",
			Mime: "image/png",
		})
	}
	if msg.MsgType == "file" && msg.Content != nil && msg.Content.DownloadCode != "" {
		name := msg.Content.FileName
		if name == "" {
			name = "file-" + msg.MsgID
		}
		attachments = append(attachments, imsdk.InboundAttachment{
			ID:   msg.Content.DownloadCode,
			Name: name,
		})
	}
	if msg.Content != nil {
		for _, item := range msg.Content.RichText {
			if item.Text != "" {
				parts = append(parts, item.Text)
			}
			if item.DownloadCode != "" {
				attachments = append(attachments, imsdk.InboundAttachment{
					ID:   item.DownloadCode,
					Name: fmt.Sprintf("image-%s-%d.png", msg.MsgID, len(attachments)),
					Mime: "image/png",
				})
			}
		}
	}

	var chat imsdk.ChatRef
	if dm {
		chat = imsdk.ChatRef{
			PlatformID: senderID,
			Kind:       imsdk.ChatDM,
			Name:       msg.SenderNick,
		}
	} else {
		chat = imsdk.ChatRef{
			PlatformID: msg.ConversationID,
			Kind:       imsdk.ChatGroup,
			Name:       msg.ConversationTitle,
		}
	}

	inAtList := msg.IsInAtList == nil || *msg.IsInAtList
	sentAt := now
	if msg.CreateAt > 0 {
		sentAt = time.UnixMilli(msg.CreateAt)
	}

	return &imsdk.InboundMessage{
		PlatformID:     msg.MsgID,
		Chat:           chat,
		Sender:         imsdk.Sender{ID: senderID, Name: msg.SenderNick, IsBot: false},
		Text:           strings.TrimSpace(strings.Join(parts, "\n")),
		Attachments:    attachments,
		AddressedToBot: dm || inAtList,
		Mentioned:      imsdk.BoolPtr(!dm && inAtList),
		SentAt:         sentAt,
	}, true
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:])
}
