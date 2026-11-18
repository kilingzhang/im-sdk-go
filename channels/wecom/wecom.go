// Package wecom implements the Enterprise WeChat (WeCom) intelligent bot channel adapter.
package wecom

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	imsdk "github.com/kilingzhang/im-sdk-go"
)

const (
	// MaxTextLength is the maximum markdown chunk size before splitting (~4 KB WeCom cap).
	MaxTextLength = 3500
	// QRTTL is the lifetime of a WeCom QR onboarding code.
	QRTTL = 5 * time.Minute
	// PollInterval is the default interval between WeCom QR query_result polls.
	PollInterval = 3 * time.Second
	// FrameTTL bounds how long a cached inbound frame is trusted for passive replies.
	FrameTTL = 30 * time.Minute
	// StreamWindow hands over a live reply 1 minute before WeCom's 10-minute stream limit.
	StreamWindow = 9 * time.Minute

	defaultConnectTimeout = 15 * time.Second
	qrHTTPTimeout         = 10 * time.Second
	maxCachedFiles        = 2000
)

var (
	defaultCapabilities = imsdk.Capabilities{
		QuoteReply:      true,
		Reactions:       false,
		TypingIndicator: false,
		FileUpload:      true,
		FileDownload:    true,
		ListMembers:     false,
		Mentions:        false,
		MaxTextLength:   MaxTextLength,
	}

	manualFields = []imsdk.CredentialField{
		{Key: "botId", LabelKey: "wecom.botId", Secret: false},
		{Key: "secret", LabelKey: "wecom.secret", Secret: true},
	}

	imageExts = map[string]struct{}{
		".png":  {},
		".jpg":  {},
		".jpeg": {},
		".gif":  {},
	}

	leadingMentionRe = regexp.MustCompile(`^@\S+\s*`)
)

// Options configures a WeCom intelligent bot channel adapter.
type Options struct {
	HTTPClient        *http.Client
	WSURL             string
	NewClient         func(botID, secret string) Client
	PollInterval      time.Duration
	ConnectTimeout    time.Duration
	HeartbeatInterval time.Duration
	ReplyAckTimeout   time.Duration
	ReconnectDelay    time.Duration
	Now               func() time.Time
}

type channel struct {
	imsdk.BaseChannel
	httpClient     *http.Client
	newClient      func(botID, secret string) Client
	pollInterval   time.Duration
	connectTimeout time.Duration
	nowFn          func() time.Time
}

// New creates a stateless WeCom intelligent bot channel adapter.
func New(opts Options) imsdk.Channel {
	var hc http.Client
	if opts.HTTPClient != nil {
		hc = *opts.HTTPClient
	}
	hc.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

	pollEvery := opts.PollInterval
	if pollEvery <= 0 {
		pollEvery = PollInterval
	}
	connTimeout := opts.ConnectTimeout
	if connTimeout <= 0 {
		connTimeout = defaultConnectTimeout
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	factory := opts.NewClient
	if factory == nil {
		factory = func(botID, secret string) Client {
			return NewWSClient(WSClientOptions{
				BotID:             botID,
				Secret:            secret,
				WSURL:             opts.WSURL,
				HTTPClient:        opts.HTTPClient,
				HeartbeatInterval: opts.HeartbeatInterval,
				ReconnectDelay:    opts.ReconnectDelay,
				ReplyAckTimeout:   opts.ReplyAckTimeout,
			})
		}
	}

	return &channel{
		BaseChannel: imsdk.BaseChannel{
			PlatformValue:     imsdk.PlatformWeCom,
			TrustValue:        imsdk.TrustOrg,
			OnboardingValue:   imsdk.OnboardingMethod{Kind: imsdk.OnboardingQR},
			ManualFieldsValue: manualFields,
		},
		httpClient:     &hc,
		newClient:      factory,
		pollInterval:   pollEvery,
		connectTimeout: connTimeout,
		nowFn:          nowFn,
	}
}

func platformCode() string {
	switch runtime.GOOS {
	case "windows":
		return "2"
	case "linux":
		return "3"
	default:
		return "1"
	}
}

func (c *channel) getJSON(ctx context.Context, target string, out any) error {
	reqCtx, cancel := context.WithTimeout(ctx, qrHTTPTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("WeCom HTTP %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

func (c *channel) verifyCredentials(ctx context.Context, botID, secret string) bool {
	client := c.newClient(botID, secret)
	defer client.Close()
	vCtx, cancel := context.WithTimeout(ctx, c.connectTimeout)
	defer cancel()
	return client.Connect(vCtx) == nil
}

func (c *channel) Onboard(ctx context.Context, input imsdk.Credentials, opts imsdk.OnboardingOptions) <-chan imsdk.OnboardingEvent {
	ch := make(chan imsdk.OnboardingEvent, 8)
	go func() {
		defer close(ch)
		botName := opts.AgentName
		if botName == "" {
			botName = "WeCom bot"
		}

		if botID, secret := strings.TrimSpace(input["botId"]), strings.TrimSpace(input["secret"]); botID != "" && secret != "" {
			if c.verifyCredentials(ctx, botID, secret) {
				ch <- imsdk.OnboardingEvent{
					Kind: imsdk.EventSucceeded,
					Credentials: imsdk.Credentials{
						"botId":  botID,
						"secret": secret,
						"name":   botName,
					},
					Bot: imsdk.BotIdentity{ID: botID, Name: botName},
				}
				return
			}
			code := imsdk.FailureInvalidCredentials
			if ctx.Err() != nil {
				code = imsdk.FailureCancelled
			}
			ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: code}
			return
		}

		genURL, _ := url.Parse(GenerateURL)
		q := genURL.Query()
		q.Set("source", "mingle")
		q.Set("plat", platformCode())
		genURL.RawQuery = q.Encode()

		var genResp struct {
			Data struct {
				AuthURL string `json:"auth_url"`
				SCode   string `json:"scode"`
			} `json:"data"`
		}
		if err := c.getJSON(ctx, genURL.String(), &genResp); err != nil {
			code := imsdk.FailureNetwork
			if ctx.Err() != nil {
				code = imsdk.FailureCancelled
			}
			ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: code}
			return
		}
		authURL, ok := TrustedAuthURL(genResp.Data.AuthURL)
		scode := strings.TrimSpace(genResp.Data.SCode)
		if !ok || scode == "" {
			code := imsdk.FailureNetwork
			if ctx.Err() != nil {
				code = imsdk.FailureCancelled
			}
			ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: code}
			return
		}

		deadline := c.nowFn().Add(QRTTL)
		select {
		case ch <- imsdk.OnboardingEvent{
			Kind:      imsdk.EventQRReady,
			QRContent: authURL,
			ExpiresAt: deadline,
		}:
		case <-ctx.Done():
			ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureCancelled}
			return
		}

		for ctx.Err() == nil && c.nowFn().Before(deadline) {
			select {
			case <-ctx.Done():
			case <-time.After(c.pollInterval):
			}
			if ctx.Err() != nil {
				break
			}

			pollURL, _ := url.Parse(PollURL)
			pq := pollURL.Query()
			pq.Set("scode", scode)
			pollURL.RawQuery = pq.Encode()

			var pollResp struct {
				Data struct {
					Status  string `json:"status"`
					BotInfo *struct {
						BotID  string `json:"botid"`
						Secret string `json:"secret"`
					} `json:"bot_info"`
				} `json:"data"`
			}
			if err := c.getJSON(ctx, pollURL.String(), &pollResp); err != nil {
				continue
			}

			switch strings.ToLower(strings.TrimSpace(pollResp.Data.Status)) {
			case "success":
				info := pollResp.Data.BotInfo
				if info == nil || info.BotID == "" || info.Secret == "" {
					ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureUnknown}
					return
				}
				ch <- imsdk.OnboardingEvent{
					Kind: imsdk.EventSucceeded,
					Credentials: imsdk.Credentials{
						"botId":  info.BotID,
						"secret": info.Secret,
						"name":   botName,
					},
					Bot: imsdk.BotIdentity{ID: info.BotID, Name: botName},
				}
				return
			case "expired", "timeout":
				ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureExpired}
				return
			case "fail", "failed", "error":
				ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureDenied}
				return
			}
		}

		code := imsdk.FailureExpired
		if ctx.Err() != nil {
			code = imsdk.FailureCancelled
		}
		ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: code}
	}()
	return ch
}

type cachedFrame struct {
	frame Frame
	at    time.Time
}

type fileSource struct {
	url    string
	aesKey string
}

type connection struct {
	imsdk.BaseConnection
	client Client
	events imsdk.ChannelEvents
	nowFn  func() time.Time

	mu        sync.Mutex
	frames    map[string]cachedFrame
	files     map[string]fileSource
	fileOrder []string
}

func (c *channel) Connect(ctx context.Context, creds imsdk.Credentials, events imsdk.ChannelEvents) (imsdk.ChannelConnection, error) {
	botID := creds["botId"]
	secret := creds["secret"]
	botName := creds["name"]
	if botName == "" {
		botName = "WeCom bot"
	}

	client := c.newClient(botID, secret)
	conn := &connection{
		BaseConnection: imsdk.BaseConnection{
			BotValue:          imsdk.BotIdentity{ID: botID, Name: botName},
			CapabilitiesValue: defaultCapabilities,
		},
		client: client,
		events: events,
		nowFn:  c.nowFn,
		frames: make(map[string]cachedFrame),
		files:  make(map[string]fileSource),
	}

	client.SetCallbacks(ClientCallbacks{
		OnMessage: func(frame Frame) {
			inbound, ok := toInboundAt(frame, c.nowFn)
			if !ok || frame.Body == nil {
				return
			}
			conn.rememberInbound(inbound.PlatformID, frame, inbound.Attachments)
			if events.OnMessage != nil {
				events.OnMessage(inbound)
			}
		},
		OnReconnecting: func(attempt int) {
			if events.OnState != nil {
				events.OnState(imsdk.ConnectionState{
					Kind:    imsdk.StateReconnecting,
					Attempt: attempt,
				})
			}
		},
		OnAuthenticated: func() {
			if events.OnState != nil {
				events.OnState(imsdk.ConnectionState{Kind: imsdk.StateConnected})
			}
		},
		OnError: func(err error) {
			if isAuthError(err) && events.OnState != nil {
				events.OnState(imsdk.ConnectionState{
					Kind: imsdk.StateError,
					Code: imsdk.ConnErrCredentialsRevoked,
				})
			}
		},
	})

	connCtx, cancel := context.WithTimeout(ctx, c.connectTimeout)
	defer cancel()
	if err := client.Connect(connCtx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return conn, nil
}

func (c *connection) rememberInbound(id string, frame Frame, attachments []imsdk.InboundAttachment) {
	now := c.nowFn()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.frames[id] = cachedFrame{frame: frame, at: now}
	for k, v := range c.frames {
		if now.Sub(v.at) > FrameTTL {
			delete(c.frames, k)
		}
	}
	if frame.Body != nil {
		for _, a := range attachments {
			if src, ok := attachmentSource(*frame.Body, a.ID); ok {
				if _, exists := c.files[a.ID]; !exists {
					c.fileOrder = append(c.fileOrder, a.ID)
				}
				c.files[a.ID] = src
			}
		}
		for len(c.fileOrder) > maxCachedFiles {
			oldest := c.fileOrder[0]
			c.fileOrder = c.fileOrder[1:]
			delete(c.files, oldest)
		}
	}
}

func (c *connection) freshFrame(id string) (Frame, bool) {
	if id == "" {
		return Frame{}, false
	}
	now := c.nowFn()
	c.mu.Lock()
	defer c.mu.Unlock()
	known, ok := c.frames[id]
	if !ok {
		return Frame{}, false
	}
	if now.Sub(known.at) > FrameTTL {
		delete(c.frames, id)
		return Frame{}, false
	}
	return known.frame, true
}

func (c *connection) evictFrame(id string, reqID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if known, ok := c.frames[id]; ok && known.frame.Headers.ReqID == reqID {
		delete(c.frames, id)
	}
}

func (c *connection) getFileSource(id string) (fileSource, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	src, ok := c.files[id]
	return src, ok
}

func randomUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func isTimeoutFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "timeout")
}

func (c *connection) Send(ctx context.Context, msg imsdk.OutboundMessage) (imsdk.SendResult, error) {
	if err := ctx.Err(); err != nil {
		return imsdk.SendResult{}, err
	}
	textID := ""
	fail := func(err error) (imsdk.SendResult, error) {
		code := imsdk.SendErrUnknown
		if isTimeoutFailure(err) {
			code = imsdk.SendErrNetwork
		}
		res := imsdk.SendResult{
			Outcome:   imsdk.OutcomeFailed,
			Code:      code,
			Retryable: true,
			Detail:    err.Error(),
		}
		return imsdk.WithTextSent(res, textID), nil
	}

	var usedFrame *Frame
	if msg.ReplyTo != "" {
		if f, ok := c.freshFrame(msg.ReplyTo); ok {
			usedFrame = &f
		}
	}

	if msg.Text != "" {
		if usedFrame != nil {
			err := c.client.ReplyStream(ctx, *usedFrame, randomUUID(), msg.Text, true)
			if err != nil {
				code, ok := ErrCodeOf(err)
				if !ok || (code != ErrCodeInvalidReqID && code != ErrCodeStreamExpired) {
					return fail(err)
				}
				c.evictFrame(msg.ReplyTo, usedFrame.Headers.ReqID)
				usedFrame = nil
			}
		}
		if usedFrame == nil {
			err := c.client.SendMessage(ctx, msg.Chat.PlatformID, map[string]any{
				"msgtype":  "markdown",
				"markdown": map[string]string{"content": msg.Text},
			})
			if err != nil {
				return fail(err)
			}
		}
		textID = "out-" + randomUUID()
	}

	for _, f := range msg.Files {
		raw, err := os.ReadFile(f.Path)
		if err != nil {
			return fail(err)
		}
		name := f.Name
		if name == "" {
			name = filepath.Base(f.Path)
		}
		mediaType := "file"
		if _, isImg := imageExts[strings.ToLower(filepath.Ext(name))]; isImg {
			mediaType = "image"
		}
		mediaID, err := c.client.UploadMedia(ctx, raw, mediaType, name)
		if err != nil {
			return fail(err)
		}
		if err := c.client.SendMediaMessage(ctx, msg.Chat.PlatformID, mediaType, mediaID); err != nil {
			return fail(err)
		}
	}

	var degraded []imsdk.Degradation
	if msg.ReplyTo != "" && usedFrame == nil {
		degraded = []imsdk.Degradation{imsdk.DegradedQuoteDropped}
	}
	return imsdk.SendResult{
		Outcome:    imsdk.OutcomeSent,
		PlatformID: "out-" + randomUUID(),
		Degraded:   degraded,
	}, nil
}

type liveStream struct {
	client   Client
	frame    Frame
	streamID string
	until    time.Time
}

func (l *liveStream) Until() time.Time { return l.until }

func (l *liveStream) Update(ctx context.Context, markdown string) error {
	_ = l.client.ReplyStream(ctx, l.frame, l.streamID, markdown, false)
	return nil
}

func (l *liveStream) Finish(ctx context.Context, markdown string, _ []imsdk.Mention) (imsdk.SendResult, error) {
	if err := l.client.ReplyStream(ctx, l.frame, l.streamID, markdown, true); err != nil {
		return imsdk.SendResult{
			Outcome:   imsdk.OutcomeFailed,
			Code:      imsdk.SendErrUnknown,
			Retryable: false,
		}, nil
	}
	return imsdk.SendResult{
		Outcome:    imsdk.OutcomeSent,
		PlatformID: "stream-" + l.streamID,
	}, nil
}

func (c *connection) StartLive(ctx context.Context, _ imsdk.ChatRef, opts imsdk.LiveOptions) (imsdk.LiveReply, error) {
	frame, ok := c.freshFrame(opts.ReplyTo)
	if !ok {
		return nil, errors.New("no inbound frame to stream a reply to")
	}
	streamID := randomUUID()
	until := c.nowFn().Add(StreamWindow)
	if err := c.client.ReplyStream(ctx, frame, streamID, opts.Initial, false); err != nil {
		return nil, err
	}
	return &liveStream{
		client:   c.client,
		frame:    frame,
		streamID: streamID,
		until:    until,
	}, nil
}

func (c *connection) Download(ctx context.Context, att imsdk.InboundAttachment, toPath string) error {
	src, ok := c.getFileSource(att.ID)
	if !ok {
		return errors.New("this file is no longer available; ask for it to be sent again")
	}
	buf, _, err := c.client.DownloadFile(ctx, src.url, src.aesKey)
	if err != nil {
		return err
	}
	return os.WriteFile(toPath, buf, 0o644)
}

func (c *connection) Close() error {
	return c.client.Close()
}

func attachmentSource(body Message, id string) (fileSource, bool) {
	parts := strings.Split(id, ":")
	idx, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil || idx < 0 {
		return fileSource{}, false
	}
	var list []MediaRef
	if body.Image != nil && body.Image.URL != "" {
		list = append(list, *body.Image)
	}
	if body.File != nil && body.File.URL != "" {
		list = append(list, *body.File)
	}
	if body.Mixed != nil {
		for _, item := range body.Mixed.MsgItem {
			if item.MsgType == "image" && item.Image != nil && item.Image.URL != "" {
				list = append(list, *item.Image)
			}
		}
	}
	if idx >= len(list) {
		return fileSource{}, false
	}
	return fileSource{url: list[idx].URL, aesKey: list[idx].AESKey}, true
}

// ToInbound normalizes a WeCom callback Frame into an imsdk.InboundMessage.
func ToInbound(frame Frame) (imsdk.InboundMessage, bool) {
	return toInboundAt(frame, time.Now)
}

func toInboundAt(frame Frame, nowFn func() time.Time) (imsdk.InboundMessage, bool) {
	body := frame.Body
	if body == nil || body.MsgID == "" || body.From == nil || body.From.UserID == "" {
		return imsdk.InboundMessage{}, false
	}
	sender := body.From.UserID
	isGroup := body.ChatType == "group" && body.ChatID != ""

	var parts []string
	var attachments []imsdk.InboundAttachment
	addFile := func(name, mimeType string) {
		attachments = append(attachments, imsdk.InboundAttachment{
			ID:   fmt.Sprintf("%s:%d", body.MsgID, len(attachments)),
			Name: name,
			Mime: mimeType,
		})
	}

	if body.Text != nil && body.Text.Content != "" {
		parts = append(parts, body.Text.Content)
	}
	if body.Voice != nil && body.Voice.Content != "" {
		parts = append(parts, body.Voice.Content)
	}
	if body.Image != nil && body.Image.URL != "" {
		addFile("image-"+body.MsgID+".jpg", "image/jpeg")
	}
	if body.File != nil && body.File.URL != "" {
		fname := body.File.Filename
		if fname == "" {
			fname = "file-" + body.MsgID
		}
		addFile(fname, "")
	}
	if body.Mixed != nil {
		for _, item := range body.Mixed.MsgItem {
			if item.MsgType == "text" && item.Text != nil && item.Text.Content != "" {
				parts = append(parts, item.Text.Content)
			}
			if item.MsgType == "image" && item.Image != nil && item.Image.URL != "" {
				addFile(fmt.Sprintf("image-%s-%d.jpg", body.MsgID, len(attachments)), "image/jpeg")
			}
		}
	}

	joined := strings.Join(parts, "\n")
	text := strings.TrimSpace(leadingMentionRe.ReplaceAllString(joined, ""))

	chat := imsdk.ChatRef{PlatformID: sender, Kind: imsdk.ChatDM}
	if isGroup {
		chat = imsdk.ChatRef{PlatformID: body.ChatID, Kind: imsdk.ChatGroup}
	}

	var sentAt time.Time
	if body.CreateTime > 0 {
		sentAt = time.Unix(body.CreateTime, 0)
	} else {
		sentAt = nowFn()
	}

	return imsdk.InboundMessage{
		PlatformID:     body.MsgID,
		Chat:           chat,
		Sender:         imsdk.Sender{ID: sender, Name: "", IsBot: false},
		Text:           text,
		Attachments:    attachments,
		AddressedToBot: true,
		SentAt:         sentAt,
	}, true
}
