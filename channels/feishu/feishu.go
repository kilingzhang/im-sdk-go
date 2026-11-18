// Package feishu implements the Feishu and Lark channel adapter using pure Go
// HTTP, WebSocket, and pbbp2 protobuf wire framing.
package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"

	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/mention"
)

// Scope constants requested during Feishu app creation and re-authorization.
const (
	GroupMessagesScope = "im:message.group_msg"
	cardElementID      = "stream_md"
	historyLimit       = 50
	outboundChunkLimit = 3500
)

var (
	// BotMessagesScopes grants visibility into other bots' group messages.
	BotMessagesScopes = []string{
		"im:message.group_at_msg.include_bot:readonly",
		"im:message.group_msg.include_bot:read",
	}

	// FeishuScopes is the default set of tenant scopes requested for a new bot.
	FeishuScopes = []string{
		"im:message.p2p_msg:readonly",
		"im:message.group_at_msg:readonly",
		GroupMessagesScope,
		"im:message.group_at_msg.include_bot:readonly",
		"im:message.group_msg.include_bot:read",
		"im:message:readonly",
		"im:message:send_as_bot",
		"im:message.reactions:write_only",
		"im:resource",
		"im:chat:readonly",
		"contact:user.base:readonly",
	}

	// ProfileScopes is requested only when member_profiles is enabled.
	ProfileScopes = []string{
		"contact:contact.base:readonly",
		"contact:user.employee:readonly",
		"contact:user.department:readonly",
		"contact:department.base:readonly",
	}

	imageExtensions = map[string]struct{}{
		".png": {}, ".jpg": {}, ".jpeg": {}, ".gif": {}, ".webp": {}, ".bmp": {},
	}
)

var capabilities = imsdk.Capabilities{
	QuoteReply:      true,
	Reactions:       true,
	TypingIndicator: false,
	FileUpload:      true,
	FileDownload:    true,
	ListMembers:     true,
	Mentions:        true,
	MaxTextLength:   8000,
}

// Options configures a Feishu/Lark Channel adapter.
type Options struct {
	HTTPClient           *http.Client
	DialWS               func(ctx context.Context, url string, opts *websocket.DialOptions) (*websocket.Conn, *http.Response, error)
	FeishuBaseURL        string
	LarkBaseURL          string
	FeishuAccountsURL    string
	LarkAccountsURL      string
	ApprovalPollInterval time.Duration
	ScopesTTL            time.Duration
	StreamRenewAfter     time.Duration
	RequestTimeout       time.Duration
	Now                  func() time.Time
}

type channel struct {
	imsdk.BaseChannel
	rest             *restClient
	dialWS           func(ctx context.Context, url string, opts *websocket.DialOptions) (*websocket.Conn, *http.Response, error)
	approvalPoll     time.Duration
	scopesTTL        time.Duration
	streamRenewAfter time.Duration
	now              func() time.Time
}

// New constructs a Feishu/Lark Channel adapter.
func New(opts Options) imsdk.Channel {
	dialWS := opts.DialWS
	if dialWS == nil {
		dialWS = websocket.Dial
	}
	approvalPoll := opts.ApprovalPollInterval
	if approvalPoll <= 0 {
		approvalPoll = 30 * time.Second
	}
	scopesTTL := opts.ScopesTTL
	if scopesTTL <= 0 {
		scopesTTL = 15 * time.Second
	}
	streamRenew := opts.StreamRenewAfter
	if streamRenew <= 0 {
		streamRenew = 8 * time.Minute
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return &channel{
		BaseChannel: imsdk.BaseChannel{
			PlatformValue:      imsdk.PlatformFeishu,
			TrustValue:         imsdk.TrustOrg,
			ReauthorizeValue:   true,
			GroupMessagesValue: true,
			OnboardingValue: imsdk.OnboardingMethod{
				Kind:        imsdk.OnboardingQR,
				DesktopLink: true,
			},
			ManualSetupURLValue: "https://open.feishu.cn/app",
			ManualFieldsValue: []imsdk.CredentialField{
				{Key: "appId", LabelKey: "feishu.appId", Secret: false},
				{Key: "appSecret", LabelKey: "feishu.appSecret", Secret: true},
			},
		},
		rest:             newRESTClient(opts),
		dialWS:           dialWS,
		approvalPoll:     approvalPoll,
		scopesTTL:        scopesTTL,
		streamRenewAfter: streamRenew,
		now:              nowFn,
	}
}

type appCreds struct {
	appID     string
	appSecret string
	domain    string
}

func domainOf(creds imsdk.Credentials) string {
	if creds["domain"] == "lark" {
		return "lark"
	}
	return "feishu"
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
	if input["appId"] != "" && input["appSecret"] != "" {
		domain := domainOf(input)
		creds := appCreds{appID: input["appId"], appSecret: input["appSecret"], domain: domain}
		bot, err := c.rest.botInfo(ctx, creds)
		if err != nil {
			out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureInvalidCredentials}
			return
		}
		out <- imsdk.OnboardingEvent{
			Kind: imsdk.EventSucceeded,
			Credentials: imsdk.Credentials{
				"appId":     creds.appID,
				"appSecret": creds.appSecret,
				"domain":    domain,
			},
			Bot:             imsdk.BotIdentity{ID: bot.OpenID, Name: bot.Name},
			PendingApproval: !bot.Activated,
		}
		return
	}

	scopes := slices.Clone(FeishuScopes)
	updating := opts.Reauthorize
	profiles := slices.Contains(opts.Adding, imsdk.PermMemberProfiles)
	if !profiles && updating["appId"] != "" && updating["appSecret"] != "" {
		granted, err := c.rest.appScopes(ctx, appCreds{
			appID:     updating["appId"],
			appSecret: updating["appSecret"],
			domain:    domainOf(updating),
		})
		if err == nil && allScopesGranted(granted, ProfileScopes) {
			profiles = true
		}
	}
	if profiles {
		scopes = append(scopes, ProfileScopes...)
	}

	baseURL := c.rest.feishuAccountsURL
	if updating["domain"] == "lark" {
		baseURL = c.rest.larkAccountsURL
	}
	beginForm := url.Values{
		"action":            {"begin"},
		"archetype":         {"PersonalAgent"},
		"auth_method":       {"client_secret"},
		"request_user_info": {"open_id"},
	}
	beginRes, err := c.rest.postRegistration(ctx, baseURL, beginForm)
	if err != nil || beginRes.VerificationURIComplete == "" || beginRes.DeviceCode == "" {
		code := imsdk.FailureNetwork
		if ctx.Err() != nil {
			code = imsdk.FailureCancelled
		}
		out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: code}
		return
	}

	qrURL, err := url.Parse(beginRes.VerificationURIComplete)
	if err != nil {
		out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureNetwork}
		return
	}
	q := qrURL.Query()
	q.Set("from", "sdk")
	q.Set("source", "node-sdk/mingle")
	q.Set("tp", "sdk")
	if updating["appId"] != "" {
		q.Set("clientID", updating["appId"])
	} else {
		agentName := opts.AgentName
		if agentName == "" {
			agentName = "Mingle Agent"
		}
		q.Set("name", agentName)
		q.Set("desc", "An agent that lives in your chats, run by Mingle on its owner's computer.")
		q.Set("createOnly", "true")
	}
	encodedAddons, err := encodeAddonsParam(scopes)
	if err != nil {
		out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureUnknown}
		return
	}
	q.Set("addons", encodedAddons)
	qrURL.RawQuery = q.Encode()

	expireSec := beginRes.ExpiresIn
	if expireSec <= 0 {
		expireSec = 600
	}
	intervalSec := beginRes.Interval
	if intervalSec <= 0 {
		intervalSec = 5
	}
	expireDur := time.Duration(expireSec * float64(time.Second))
	interval := time.Duration(intervalSec * float64(time.Second))
	deadline := c.now().Add(expireDur)

	out <- imsdk.OnboardingEvent{
		Kind:      imsdk.EventQRReady,
		QRContent: qrURL.String(),
		ExpiresAt: deadline,
	}

	domainSwitched := updating["domain"] == "lark"
	firstPoll := true
	for {
		if ctx.Err() != nil {
			out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureCancelled}
			return
		}
		if !c.now().Before(deadline) {
			out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureExpired}
			return
		}
		if !firstPoll {
			timer := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureCancelled}
				return
			case <-timer.C:
			}
		}
		firstPoll = false

		pollRes, err := c.rest.postRegistration(ctx, baseURL, url.Values{
			"action":      {"poll"},
			"device_code": {beginRes.DeviceCode},
		})
		if err != nil {
			code := imsdk.FailureNetwork
			if ctx.Err() != nil {
				code = imsdk.FailureCancelled
			}
			out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: code}
			return
		}

		if pollRes.UserInfo.TenantBrand == "lark" && !domainSwitched {
			baseURL = c.rest.larkAccountsURL
			domainSwitched = true
			firstPoll = true
			continue
		}

		if pollRes.ClientID != "" && pollRes.ClientSecret != "" {
			domain := "feishu"
			if pollRes.UserInfo.TenantBrand == "lark" || domainSwitched {
				domain = "lark"
			}
			creds := appCreds{appID: pollRes.ClientID, appSecret: pollRes.ClientSecret, domain: domain}
			bot, err := c.rest.botInfo(ctx, creds)
			if err != nil {
				out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureNetwork}
				return
			}
			out <- imsdk.OnboardingEvent{
				Kind: imsdk.EventSucceeded,
				Credentials: imsdk.Credentials{
					"appId":     creds.appID,
					"appSecret": creds.appSecret,
					"domain":    domain,
				},
				Bot:             imsdk.BotIdentity{ID: bot.OpenID, Name: bot.Name},
				OwnerID:         pollRes.UserInfo.OpenID,
				PendingApproval: !bot.Activated,
			}
			return
		}

		switch pollRes.Error {
		case "", "authorization_pending":
		case "slow_down":
			interval += 5 * time.Second
		case "access_denied":
			out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureDenied}
			return
		case "expired_token":
			out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureExpired}
			return
		default:
			out <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureNetwork}
			return
		}
	}
}

func allScopesGranted(granted map[string]bool, required []string) bool {
	for _, s := range required {
		if !granted[s] {
			return false
		}
	}
	return true
}

// dedupCache is an isolated per-connection bounded LRU/TTL message ID set.
type dedupCache struct {
	mu      sync.Mutex
	entries map[string]time.Time
	order   []string
	maxSize int
	ttl     time.Duration
}

func newDedupCache(maxSize int, ttl time.Duration) *dedupCache {
	return &dedupCache{
		entries: make(map[string]time.Time),
		maxSize: maxSize,
		ttl:     ttl,
	}
}

func (d *dedupCache) markSeen(id string, now time.Time) bool {
	if id == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if exp, ok := d.entries[id]; ok && now.Before(exp) {
		return true
	}
	d.entries[id] = now.Add(d.ttl)
	d.order = append(d.order, id)
	for len(d.entries) > d.maxSize && len(d.order) > 0 {
		oldest := d.order[0]
		d.order = d.order[1:]
		delete(d.entries, oldest)
	}
	return false
}

type connection struct {
	imsdk.BaseConnection
	ch     *channel
	creds  appCreds
	events imsdk.ChannelEvents

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	ready atomic.Bool

	seen      *dedupCache
	fragments *fragmentAssembler
	eventCh   chan []byte

	cacheMu     sync.Mutex
	chatNames   map[string]string
	userNames   map[string]string
	departments map[string]string
	reactions   map[string]string

	scopesMu  sync.Mutex
	scopesAt  time.Time
	scopesVal []imsdk.BotPermission
	scopesSet bool

	wsMu   sync.Mutex
	wsConn *websocket.Conn
	wsCfg  wsClientConfig
}

func (c *channel) Connect(ctx context.Context, raw imsdk.Credentials, events imsdk.ChannelEvents) (imsdk.ChannelConnection, error) {
	creds := appCreds{
		appID:     raw["appId"],
		appSecret: raw["appSecret"],
		domain:    domainOf(raw),
	}
	if creds.appID == "" || creds.appSecret == "" {
		return nil, errors.New("feishu: appId and appSecret are required")
	}

	info, err := c.rest.botInfo(ctx, creds)
	if err != nil {
		return nil, err
	}

	connCtx, cancel := context.WithCancel(context.Background())
	conn := &connection{
		BaseConnection: imsdk.BaseConnection{
			BotValue:          imsdk.BotIdentity{ID: info.OpenID, Name: info.Name},
			CapabilitiesValue: capabilities,
		},
		ch:          c,
		creds:       creds,
		events:      events,
		ctx:         connCtx,
		cancel:      cancel,
		seen:        newDedupCache(5000, 12*time.Hour),
		fragments:   newFragmentAssembler(),
		eventCh:     make(chan []byte, 256),
		chatNames:   make(map[string]string),
		userNames:   make(map[string]string),
		departments: make(map[string]string),
		reactions:   make(map[string]string),
	}

	conn.wg.Add(1)
	go conn.eventWorker()

	if info.Activated {
		if err := conn.startWS(ctx); err != nil {
			cancel()
			conn.wg.Wait()
			return nil, err
		}
		conn.emitState(imsdk.ConnectionState{Kind: imsdk.StateConnected})
	} else {
		conn.emitState(imsdk.ConnectionState{Kind: imsdk.StateAwaitingApproval})
		conn.wg.Add(1)
		go conn.pollApprovalLoop()
	}

	return conn, nil
}

func (c *connection) emitState(st imsdk.ConnectionState) {
	if c.events.OnState != nil {
		c.events.OnState(st)
	}
}

func (c *connection) pollApprovalLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.ch.approvalPoll)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			info, err := c.ch.rest.botInfo(c.ctx, c.creds)
			if err != nil || !info.Activated {
				continue
			}
			if err := c.startWS(c.ctx); err != nil {
				continue
			}
			c.emitState(imsdk.ConnectionState{Kind: imsdk.StateConnected})
			return
		}
	}
}

func (c *connection) startWS(ctx context.Context) error {
	ep, err := c.ch.rest.pullWSEndpoint(ctx, c.creds)
	if err != nil {
		return err
	}
	wsConn, _, err := c.ch.dialWS(ctx, ep.URL, nil)
	if err != nil {
		return err
	}
	wsConn.SetReadLimit(16 << 20)

	c.wsMu.Lock()
	c.wsConn = wsConn
	c.wsCfg = ep.Config
	c.wsMu.Unlock()
	c.ready.Store(true)

	c.wg.Add(1)
	go c.wsSupervisor(wsConn, ep.ServiceID)
	return nil
}

func (c *connection) wsSupervisor(initialConn *websocket.Conn, initialServiceID int32) {
	defer c.wg.Done()
	wsConn := initialConn
	serviceID := initialServiceID

	for {
		c.serveWSConn(wsConn, serviceID)
		_ = wsConn.CloseNow()
		if c.ctx.Err() != nil {
			return
		}

		c.emitState(imsdk.ConnectionState{Kind: imsdk.StateReconnecting, Attempt: 1})
		attempt := 0
		for {
			if c.ctx.Err() != nil {
				return
			}
			attempt++
			c.wsMu.Lock()
			waitDur := c.wsCfg.ReconnectInterval
			maxCount := c.wsCfg.ReconnectCount
			c.wsMu.Unlock()
			if waitDur <= 0 {
				waitDur = 500 * time.Millisecond
			}
			if attempt > 1 {
				timer := time.NewTimer(waitDur)
				select {
				case <-c.ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}

			ep, err := c.ch.rest.pullWSEndpoint(c.ctx, c.creds)
			if err != nil {
				var ae *apiError
				if errors.As(err, &ae) && (ae.FeishuCode == 403 || ae.FeishuCode == 514 || ae.StatusCode == http.StatusUnauthorized || ae.StatusCode == http.StatusForbidden) {
					c.emitState(imsdk.ConnectionState{Kind: imsdk.StateError, Code: imsdk.ConnErrCredentialsRevoked})
					return
				}
				if maxCount >= 0 && attempt >= maxCount {
					c.emitState(imsdk.ConnectionState{Kind: imsdk.StateError, Code: imsdk.ConnErrNetwork})
					return
				}
				continue
			}

			nextConn, _, err := c.ch.dialWS(c.ctx, ep.URL, nil)
			if err != nil {
				if maxCount >= 0 && attempt >= maxCount {
					c.emitState(imsdk.ConnectionState{Kind: imsdk.StateError, Code: imsdk.ConnErrNetwork})
					return
				}
				continue
			}
			nextConn.SetReadLimit(16 << 20)
			c.wsMu.Lock()
			c.wsConn = nextConn
			c.wsCfg = ep.Config
			c.wsMu.Unlock()
			wsConn = nextConn
			serviceID = ep.ServiceID
			c.emitState(imsdk.ConnectionState{Kind: imsdk.StateConnected})
			break
		}
	}
}

func (c *connection) serveWSConn(wsConn *websocket.Conn, serviceID int32) {
	connCtx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	var writeMu sync.Mutex
	writeFrame := func(f Frame) {
		raw := EncodeFrame(f)
		writeMu.Lock()
		defer writeMu.Unlock()
		wctx, wcancel := context.WithTimeout(connCtx, 10*time.Second)
		defer wcancel()
		_ = wsConn.Write(wctx, websocket.MessageBinary, raw)
	}

	go func() {
		for {
			c.wsMu.Lock()
			interval := c.wsCfg.PingInterval
			c.wsMu.Unlock()
			if interval <= 0 {
				interval = 120 * time.Second
			}
			timer := time.NewTimer(interval)
			select {
			case <-connCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
				writeFrame(Frame{
					Service: serviceID,
					Method:  FrameMethodControl,
					Headers: []FrameHeader{{Key: headerType, Value: msgTypePing}},
				})
			}
		}
	}()

	for {
		_, raw, err := wsConn.Read(connCtx)
		if err != nil {
			return
		}
		frame, err := DecodeFrame(raw)
		if err != nil {
			continue
		}
		switch frame.Method {
		case FrameMethodControl:
			if frame.HeaderValue(headerType) == msgTypePong && len(frame.Payload) > 0 {
				var pong struct {
					PingInterval      int `json:"PingInterval"`
					ReconnectCount    int `json:"ReconnectCount"`
					ReconnectInterval int `json:"ReconnectInterval"`
					ReconnectNonce    int `json:"ReconnectNonce"`
				}
				if json.Unmarshal(frame.Payload, &pong) == nil {
					c.wsMu.Lock()
					if pong.PingInterval > 0 {
						c.wsCfg.PingInterval = time.Duration(pong.PingInterval) * time.Second
					}
					c.wsCfg.ReconnectCount = pong.ReconnectCount
					if pong.ReconnectInterval > 0 {
						c.wsCfg.ReconnectInterval = time.Duration(pong.ReconnectInterval) * time.Second
					}
					if pong.ReconnectNonce >= 0 {
						c.wsCfg.ReconnectNonce = time.Duration(pong.ReconnectNonce) * time.Second
					}
					c.wsMu.Unlock()
				}
			}

		case FrameMethodData:
			if frame.HeaderValue(headerType) != msgTypeEvent {
				continue
			}
			sum := 1
			if s := frame.HeaderValue(headerSum); s != "" {
				sum, _ = strconv.Atoi(s)
			}
			seq := 0
			if s := frame.HeaderValue(headerSeq); s != "" {
				seq, _ = strconv.Atoi(s)
			}
			merged, err := c.fragments.add(frame.HeaderValue(headerMessageID), sum, seq, frame.Payload, c.ch.now())
			if err != nil || merged == nil {
				continue
			}

			ackHeaders := make([]FrameHeader, len(frame.Headers)+1)
			copy(ackHeaders, frame.Headers)
			ackHeaders[len(frame.Headers)] = FrameHeader{Key: headerBizRT, Value: "0"}
			ack := frame
			ack.Headers = ackHeaders
			ack.Payload = []byte(`{"code":200}`)
			writeFrame(ack)

			select {
			case c.eventCh <- merged:
			case <-connCtx.Done():
				return
			}
		}
	}
}

func (c *connection) eventWorker() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case raw := <-c.eventCh:
			c.dispatchMergedEvent(raw)
		}
	}
}

func (c *connection) dispatchMergedEvent(raw []byte) {
	var env struct {
		Schema string `json:"schema"`
		Header struct {
			EventID   string `json:"event_id"`
			EventType string `json:"event_type"`
		} `json:"header"`
		Event json.RawMessage `json:"event"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return
	}

	switch env.Header.EventType {
	case "im.message.receive_v1":
		var ev rawMessageEvent
		if err := json.Unmarshal(env.Event, &ev); err != nil {
			return
		}
		if c.seen.markSeen(ev.Message.MessageID, c.ch.now()) {
			return
		}
		norm := normalizeRawEvent(ev, c.BotValue.ID)
		isBot := ev.Sender.SenderType == "app"
		if !isBot && norm.SenderName == "" && norm.SenderID != "" {
			norm.SenderName = c.lookupUserName(c.ctx, norm.SenderID)
		}
		var chatName string
		if norm.ChatType == "group" {
			chatName = c.lookupChatName(c.ctx, norm.ChatID)
		}
		if inbound, ok := toInbound(norm, c.BotValue, chatName, isBot, c.ch.now()); ok && c.events.OnMessage != nil {
			c.events.OnMessage(*inbound)
		}

	case "im.chat.member.bot.added_v1":
		var ev struct {
			ChatID string `json:"chat_id"`
		}
		if err := json.Unmarshal(env.Event, &ev); err != nil || ev.ChatID == "" {
			return
		}
		name := c.lookupChatName(c.ctx, ev.ChatID)
		if c.events.OnMembership != nil {
			c.events.OnMembership(imsdk.MembershipChange{
				Chat: imsdk.ChatRef{
					PlatformID: ev.ChatID,
					Kind:       imsdk.ChatGroup,
					Name:       name,
				},
				Joined: true,
			})
		}
		c.readEarlier(c.ctx, ev.ChatID, name)
	}
}

func (c *connection) readEarlier(ctx context.Context, chatID, chatName string) {
	q := url.Values{
		"container_id_type": {"chat"},
		"container_id":      {chatID},
		"sort_type":         {"ByCreateTimeDesc"},
		"page_size":         {strconv.Itoa(historyLimit)},
	}
	var resp struct {
		Data struct {
			Items []listedMessage `json:"items"`
		} `json:"data"`
	}
	if err := c.ch.rest.doJSON(ctx, c.creds, http.MethodGet, "/open-apis/im/v1/messages", q, nil, false, &resp); err != nil {
		return
	}
	for i := len(resp.Data.Items) - 1; i >= 0; i-- {
		item := resp.Data.Items[i]
		if item.Sender.SenderType == "app" && item.Sender.ID == c.creds.appID {
			continue
		}
		rawEv, ok := rawEventFromListed(item, chatID)
		if !ok {
			continue
		}
		norm := normalizeRawEvent(rawEv, c.BotValue.ID)
		isBot := item.Sender.SenderType == "app"
		if !isBot && norm.SenderID != "" {
			norm.SenderName = c.lookupUserName(ctx, norm.SenderID)
		}
		inbound, ok := toInbound(norm, c.BotValue, chatName, isBot, c.ch.now())
		if !ok {
			continue
		}
		inbound.Earlier = true
		if c.events.OnMessage != nil {
			c.events.OnMessage(*inbound)
		}
	}
}

func (c *connection) lookupUserName(ctx context.Context, openID string) string {
	c.cacheMu.Lock()
	if name, ok := c.userNames[openID]; ok {
		c.cacheMu.Unlock()
		return name
	}
	c.cacheMu.Unlock()

	var resp struct {
		Data struct {
			User struct {
				Name string `json:"name"`
			} `json:"user"`
		} `json:"data"`
	}
	q := url.Values{"user_id_type": {"open_id"}}
	_ = c.ch.rest.doJSON(ctx, c.creds, http.MethodGet, "/open-apis/contact/v3/users/"+url.PathEscape(openID), q, nil, false, &resp)
	name := resp.Data.User.Name

	c.cacheMu.Lock()
	c.userNames[openID] = name
	c.cacheMu.Unlock()
	return name
}

func (c *connection) lookupChatName(ctx context.Context, chatID string) string {
	c.cacheMu.Lock()
	if name, ok := c.chatNames[chatID]; ok {
		c.cacheMu.Unlock()
		return name
	}
	c.cacheMu.Unlock()

	var resp struct {
		Data struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	_ = c.ch.rest.doJSON(ctx, c.creds, http.MethodGet, "/open-apis/im/v1/chats/"+url.PathEscape(chatID), nil, nil, false, &resp)
	name := resp.Data.Name

	c.cacheMu.Lock()
	c.chatNames[chatID] = name
	c.cacheMu.Unlock()
	return name
}

func (c *connection) lookupDepartmentName(ctx context.Context, deptID string) string {
	c.cacheMu.Lock()
	if name, ok := c.departments[deptID]; ok {
		c.cacheMu.Unlock()
		return name
	}
	c.cacheMu.Unlock()

	var resp struct {
		Data struct {
			Department struct {
				Name string `json:"name"`
			} `json:"department"`
		} `json:"data"`
	}
	q := url.Values{"department_id_type": {"open_department_id"}}
	_ = c.ch.rest.doJSON(ctx, c.creds, http.MethodGet, "/open-apis/contact/v3/departments/"+url.PathEscape(deptID), q, nil, false, &resp)
	name := resp.Data.Department.Name

	c.cacheMu.Lock()
	c.departments[deptID] = name
	c.cacheMu.Unlock()
	return name
}

func (c *connection) Permissions(ctx context.Context) ([]imsdk.BotPermission, error) {
	now := c.ch.now()
	c.scopesMu.Lock()
	if c.scopesSet && now.Sub(c.scopesAt) <= c.ch.scopesTTL {
		cached := slices.Clone(c.scopesVal)
		c.scopesMu.Unlock()
		return cached, nil
	}
	c.scopesMu.Unlock()

	granted, err := c.ch.rest.appScopes(ctx, c.creds)
	var perms []imsdk.BotPermission
	if err == nil {
		perms = []imsdk.BotPermission{
			{ID: imsdk.PermGroupMessages, Granted: granted[GroupMessagesScope]},
			{ID: imsdk.PermBotMessages, Granted: allScopesGranted(granted, BotMessagesScopes)},
			{ID: imsdk.PermMemberProfiles, Granted: allScopesGranted(granted, ProfileScopes), Optional: true},
		}
	} else {
		perms = []imsdk.BotPermission{}
	}

	c.scopesMu.Lock()
	c.scopesAt = now
	c.scopesVal = perms
	c.scopesSet = true
	c.scopesMu.Unlock()
	return slices.Clone(perms), nil
}

func (c *connection) Profile(ctx context.Context, userID string) (string, error) {
	if !c.ready.Load() {
		return "", nil
	}
	perms, _ := c.Permissions(ctx)
	enabled := slices.ContainsFunc(perms, func(p imsdk.BotPermission) bool {
		return p.ID == imsdk.PermMemberProfiles && p.Granted
	})
	if !enabled {
		return "", nil
	}

	var resp struct {
		Data struct {
			User struct {
				JobTitle      string   `json:"job_title"`
				DepartmentIDs []string `json:"department_ids"`
			} `json:"user"`
		} `json:"data"`
	}
	q := url.Values{
		"user_id_type":       {"open_id"},
		"department_id_type": {"open_department_id"},
	}
	if err := c.ch.rest.doJSON(ctx, c.creds, http.MethodGet, "/open-apis/contact/v3/users/"+url.PathEscape(userID), q, nil, false, &resp); err != nil {
		return "", nil
	}
	deptIDs := resp.Data.User.DepartmentIDs
	if len(deptIDs) > 3 {
		deptIDs = deptIDs[:3]
	}
	var deptNames []string
	for _, id := range deptIDs {
		if name := c.lookupDepartmentName(ctx, id); name != "" {
			deptNames = append(deptNames, name)
		}
	}
	var parts []string
	if where := strings.Join(deptNames, ", "); where != "" {
		parts = append(parts, where)
	}
	if title := strings.TrimSpace(resp.Data.User.JobTitle); title != "" {
		parts = append(parts, title)
	}
	return strings.Join(parts, " · "), nil
}

func (c *connection) Send(ctx context.Context, msg imsdk.OutboundMessage) (imsdk.SendResult, error) {
	if !c.ready.Load() {
		return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrForbidden, Retryable: false}, nil
	}
	to := msg.Chat.PlatformID
	idType := detectReceiveIDType(to)
	textID := ""
	lastID := ""

	if msg.Text != "" {
		rendered := mention.Render(msg.Text, msg.Mentions, func(m imsdk.Mention) string {
			return fmt.Sprintf(`<at user_id="%s">%s</at>`, m.ID, m.Name)
		})
		chunks := splitWithCodeFences(rendered, outboundChunkLimit)
		for i, chunk := range chunks {
			replyTo := ""
			if i == 0 {
				replyTo = msg.ReplyTo
			}
			postObj := markdownToPostContent(chunk)
			id, err := c.sendOneWithFallback(ctx, to, idType, "post", postObj, chunk, replyTo)
			if err != nil {
				return imsdk.WithTextSent(sendResultFromError(err), textID), nil
			}
			if i == 0 {
				textID = id
				lastID = id
			}
		}
	}

	for _, file := range msg.Files {
		data, err := os.ReadFile(file.Path)
		if err != nil {
			return imsdk.WithTextSent(imsdk.SendResult{
				Outcome:   imsdk.OutcomeFailed,
				Code:      imsdk.SendErrTooLarge,
				Retryable: false,
				Detail:    err.Error(),
			}, textID), nil
		}
		name := file.Name
		if name == "" {
			name = filepath.Base(file.Path)
		}
		ext := strings.ToLower(filepath.Ext(name))
		var id string
		if _, isImg := imageExtensions[ext]; isImg {
			key, err := c.ch.rest.uploadImage(ctx, c.creds, data, name)
			if err != nil {
				return imsdk.WithTextSent(sendResultFromError(err), textID), nil
			}
			id, err = c.sendOneWithFallback(ctx, to, idType, "image", map[string]string{"image_key": key}, "", msg.ReplyTo)
			if err != nil {
				return imsdk.WithTextSent(sendResultFromError(err), textID), nil
			}
		} else {
			key, err := c.ch.rest.uploadFile(ctx, c.creds, data, name)
			if err != nil {
				return imsdk.WithTextSent(sendResultFromError(err), textID), nil
			}
			id, err = c.sendOneWithFallback(ctx, to, idType, "file", map[string]string{"file_key": key}, "", msg.ReplyTo)
			if err != nil {
				return imsdk.WithTextSent(sendResultFromError(err), textID), nil
			}
		}
		lastID = id
	}

	if strings.HasPrefix(to, "ou_") && lastID != "" {
		var got struct {
			Data struct {
				Items []struct {
					ChatID string `json:"chat_id"`
				} `json:"items"`
			} `json:"data"`
		}
		if err := c.ch.rest.doJSON(ctx, c.creds, http.MethodGet, "/open-apis/im/v1/messages/"+url.PathEscape(lastID), nil, nil, false, &got); err == nil {
			if len(got.Data.Items) > 0 && got.Data.Items[0].ChatID != "" {
				return imsdk.SendResult{
					Outcome:    imsdk.OutcomeSent,
					PlatformID: lastID,
					Chat:       got.Data.Items[0].ChatID,
				}, nil
			}
		}
	}

	return imsdk.SendResult{Outcome: imsdk.OutcomeSent, PlatformID: lastID}, nil
}

func (c *connection) sendOneWithFallback(ctx context.Context, to, idType, msgType string, content any, plainFallback, replyTo string) (string, error) {
	id, err := c.rawSend(ctx, to, idType, msgType, content, replyTo, msgType == "interactive")
	if err == nil {
		return id, nil
	}
	ae := classifyAPIError(err)
	if ae.Kind == "target_revoked" && replyTo != "" {
		return c.rawSend(ctx, to, idType, msgType, content, "", false)
	}
	if ae.Kind == "format_error" && msgType == "post" {
		plain := strings.TrimSpace(plainFallback)
		if plain == "" {
			plain = "[message]"
		}
		return c.rawSend(ctx, to, idType, "text", map[string]string{"text": plain}, replyTo, false)
	}
	return "", ae
}

func (c *connection) rawSend(ctx context.Context, to, idType, msgType string, content any, replyTo string, bounded bool) (string, error) {
	contentJSON, err := json.Marshal(content)
	if err != nil {
		return "", err
	}
	var resp struct {
		Data struct {
			MessageID string `json:"message_id"`
		} `json:"data"`
	}
	if replyTo != "" {
		body := map[string]any{
			"msg_type": msgType,
			"content":  string(contentJSON),
		}
		if err := c.ch.rest.doJSON(ctx, c.creds, http.MethodPost, "/open-apis/im/v1/messages/"+url.PathEscape(replyTo)+"/reply", nil, body, bounded, &resp); err != nil {
			return "", err
		}
	} else {
		q := url.Values{"receive_id_type": {idType}}
		body := map[string]any{
			"receive_id": to,
			"msg_type":   msgType,
			"content":    string(contentJSON),
		}
		if err := c.ch.rest.doJSON(ctx, c.creds, http.MethodPost, "/open-apis/im/v1/messages", q, body, bounded, &resp); err != nil {
			return "", err
		}
	}
	if resp.Data.MessageID == "" {
		return "", &apiError{Kind: "unknown", Msg: "feishu: message_id missing in response"}
	}
	return resp.Data.MessageID, nil
}

func (c *connection) React(ctx context.Context, _ imsdk.ChatRef, platformMsgID string, state imsdk.ReactionState) error {
	if !c.ready.Load() {
		return nil
	}
	c.cacheMu.Lock()
	prev, hasPrev := c.reactions[platformMsgID]
	if hasPrev {
		delete(c.reactions, platformMsgID)
	}
	c.cacheMu.Unlock()

	if hasPrev && prev != "" {
		_ = c.ch.rest.doJSON(ctx, c.creds, http.MethodDelete,
			fmt.Sprintf("/open-apis/im/v1/messages/%s/reactions/%s", url.PathEscape(platformMsgID), url.PathEscape(prev)),
			nil, nil, false, nil)
	}

	var emoji string
	switch state {
	case imsdk.ReactionProcessing:
		emoji = "OnIt"
	case imsdk.ReactionDone:
		emoji = "DONE"
	case imsdk.ReactionError:
		emoji = "ERROR"
	default:
		return nil
	}

	var resp struct {
		Data struct {
			ReactionID string `json:"reaction_id"`
		} `json:"data"`
	}
	err := c.ch.rest.doJSON(ctx, c.creds, http.MethodPost,
		fmt.Sprintf("/open-apis/im/v1/messages/%s/reactions", url.PathEscape(platformMsgID)),
		nil, map[string]any{"reaction_type": map[string]string{"emoji_type": emoji}}, false, &resp)
	if err == nil && state == imsdk.ReactionProcessing && resp.Data.ReactionID != "" {
		c.cacheMu.Lock()
		c.reactions[platformMsgID] = resp.Data.ReactionID
		c.cacheMu.Unlock()
	}
	return nil
}

func (c *connection) StartLive(ctx context.Context, chat imsdk.ChatRef, opts imsdk.LiveOptions) (imsdk.LiveReply, error) {
	if !c.ready.Load() {
		return nil, errors.New("feishu: connection is not ready")
	}

	var created struct {
		Data struct {
			CardID string `json:"card_id"`
		} `json:"data"`
	}
	createBody := map[string]any{
		"type": "card_json",
		"data": buildCardJSON(opts.Initial, nil),
	}
	if err := c.ch.rest.doJSON(ctx, c.creds, http.MethodPost, "/open-apis/cardkit/v1/cards", nil, createBody, true, &created); err != nil {
		return nil, err
	}
	cardID := created.Data.CardID
	if cardID == "" {
		return nil, errors.New("feishu: cardkit created no card_id")
	}

	cardRef := map[string]any{
		"type": "card",
		"data": map[string]string{"card_id": cardID},
	}
	messageID, err := c.rawSend(ctx, chat.PlatformID, detectReceiveIDType(chat.PlatformID), "interactive", cardRef, opts.ReplyTo, true)
	if err != nil {
		return nil, err
	}

	return &liveReply{
		conn:           c,
		cardID:         cardID,
		messageID:      messageID,
		mentionOpenID:  opts.Mention,
		streamingSince: c.ch.now(),
	}, nil
}

type liveReply struct {
	conn          *connection
	cardID        string
	messageID     string
	mentionOpenID string

	mu             sync.Mutex
	sequence       int
	streamingSince time.Time
}

func (l *liveReply) Until() time.Time { return time.Time{} }

func (l *liveReply) Update(ctx context.Context, markdown string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.conn.ch.now().Sub(l.streamingSince) > l.conn.ch.streamRenewAfter {
		_ = l.reopenLocked(ctx)
	}
	if err := l.writeContentLocked(ctx, markdown); err != nil {
		var ae *apiError
		if errors.As(err, &ae) && (ae.FeishuCode == 300309 || ae.FeishuCode == 200850) {
			if err := l.reopenLocked(ctx); err == nil {
				_ = l.writeContentLocked(ctx, markdown)
			}
		}
	}
	return nil
}

func (l *liveReply) Finish(ctx context.Context, markdown string, mentions []imsdk.Mention) (imsdk.SendResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lead := ""
	if l.mentionOpenID != "" {
		alreadyNamed := slices.ContainsFunc(mentions, func(m imsdk.Mention) bool {
			return m.ID == l.mentionOpenID
		})
		if !alreadyNamed {
			lead = fmt.Sprintf("<at id=%s></at> ", l.mentionOpenID)
		}
	}
	text := lead + mention.Render(markdown, mentions, func(m imsdk.Mention) string {
		return fmt.Sprintf("<at id=%s></at>", m.ID)
	})

	flat := strings.TrimSpace(anySpaceRe.ReplaceAllString(cardAtTagRe.ReplaceAllString(text, ""), " "))
	summary := flat
	if utf8.RuneCountInString(flat) > 50 {
		runes := []rune(flat)
		summary = string(runes[:49]) + "…"
	}

	l.sequence++
	seq := l.sequence
	updateBody := map[string]any{
		"card": map[string]any{
			"type": "card_json",
			"data": buildCardJSON(text, &summary),
		},
		"sequence": seq,
		"uuid":     fmt.Sprintf("u_%s_%d", l.cardID, seq),
	}
	if err := l.conn.ch.rest.doJSON(ctx, l.conn.creds, http.MethodPut, "/open-apis/cardkit/v1/cards/"+url.PathEscape(l.cardID), nil, updateBody, true, nil); err != nil {
		_ = l.settingsLocked(ctx, map[string]any{"streaming_mode": false})
		return imsdk.SendResult{
			Outcome:   imsdk.OutcomeFailed,
			Code:      imsdk.SendErrUnknown,
			Retryable: false,
			Detail:    err.Error(),
		}, nil
	}
	return imsdk.SendResult{
		Outcome:    imsdk.OutcomeSent,
		PlatformID: l.messageID,
	}, nil
}

func (l *liveReply) settingsLocked(ctx context.Context, cfg map[string]any) error {
	l.sequence++
	seq := l.sequence
	settingsJSON, _ := json.Marshal(map[string]any{"config": cfg})
	body := map[string]any{
		"settings": string(settingsJSON),
		"sequence": seq,
		"uuid":     fmt.Sprintf("s_%s_%d", l.cardID, seq),
	}
	return l.conn.ch.rest.doJSON(ctx, l.conn.creds, http.MethodPatch, "/open-apis/cardkit/v1/cards/"+url.PathEscape(l.cardID)+"/settings", nil, body, true, nil)
}

func (l *liveReply) reopenLocked(ctx context.Context) error {
	if err := l.settingsLocked(ctx, map[string]any{"streaming_mode": true}); err != nil {
		return err
	}
	l.streamingSince = l.conn.ch.now()
	return nil
}

func (l *liveReply) writeContentLocked(ctx context.Context, content string) error {
	l.sequence++
	seq := l.sequence
	body := map[string]any{
		"content":  content,
		"sequence": seq,
		"uuid":     fmt.Sprintf("c_%s_%d", l.cardID, seq),
	}
	path := fmt.Sprintf("/open-apis/cardkit/v1/cards/%s/elements/%s/content", url.PathEscape(l.cardID), cardElementID)
	return l.conn.ch.rest.doJSON(ctx, l.conn.creds, http.MethodPut, path, nil, body, true, nil)
}

func buildCardJSON(content string, summary *string) string {
	var cfg map[string]any
	if summary != nil {
		sumText := *summary
		if sumText == "" {
			sumText = "…"
		}
		cfg = map[string]any{
			"streaming_mode": false,
			"summary":        map[string]string{"content": sumText},
		}
	} else {
		cfg = map[string]any{
			"streaming_mode": true,
			"summary":        map[string]string{"content": "..."},
			"streaming_config": map[string]any{
				"print_frequency_ms": map[string]int{"default": 70},
				"print_step":         map[string]int{"default": 1},
				"print_strategy":     "fast",
			},
		}
	}
	raw, _ := json.Marshal(map[string]any{
		"schema": "2.0",
		"config": cfg,
		"body": map[string]any{
			"elements": []map[string]string{
				{
					"tag":        "markdown",
					"element_id": cardElementID,
					"content":    content,
				},
			},
		},
	})
	return string(raw)
}

func (c *connection) Download(ctx context.Context, att imsdk.InboundAttachment, toPath string) error {
	if !c.ready.Load() {
		return errors.New("feishu: connection is not ready")
	}
	parts := strings.SplitN(att.ID, "|", 3)
	if len(parts) < 3 {
		return fmt.Errorf("feishu: malformed attachment id %q", att.ID)
	}
	data, err := c.ch.rest.downloadMessageResource(ctx, c.creds, parts[0], parts[2], parts[1])
	if err != nil {
		return err
	}
	return os.WriteFile(toPath, data, 0o600)
}

func (c *connection) ListMembers(ctx context.Context, chat imsdk.ChatRef) ([]imsdk.Sender, error) {
	if !c.ready.Load() {
		return nil, nil
	}
	var members []imsdk.Sender
	pageToken := ""
	for page := 0; page < 10; page++ {
		q := url.Values{
			"member_id_type": {"open_id"},
			"page_size":      {"100"},
		}
		if pageToken != "" {
			q.Set("page_token", pageToken)
		}
		var resp struct {
			Data struct {
				Items []struct {
					MemberID string `json:"member_id"`
					Name     string `json:"name"`
				} `json:"items"`
				HasMore   bool   `json:"has_more"`
				PageToken string `json:"page_token"`
			} `json:"data"`
		}
		if err := c.ch.rest.doJSON(ctx, c.creds, http.MethodGet, "/open-apis/im/v1/chats/"+url.PathEscape(chat.PlatformID)+"/members", q, nil, false, &resp); err != nil {
			return nil, nil
		}
		for _, item := range resp.Data.Items {
			members = append(members, imsdk.Sender{
				ID:    item.MemberID,
				Name:  item.Name,
				IsBot: false,
			})
		}
		if !resp.Data.HasMore || resp.Data.PageToken == "" {
			break
		}
		pageToken = resp.Data.PageToken
	}

	var botsResp struct {
		Data struct {
			Items []struct {
				BotID   string `json:"bot_id"`
				BotName string `json:"bot_name"`
			} `json:"items"`
		} `json:"data"`
	}
	if err := c.ch.rest.doJSON(ctx, c.creds, http.MethodGet, "/open-apis/im/v1/chats/"+url.PathEscape(chat.PlatformID)+"/members/bots", nil, nil, false, &botsResp); err == nil {
		for _, b := range botsResp.Data.Items {
			if b.BotID != "" {
				members = append(members, imsdk.Sender{
					ID:    b.BotID,
					Name:  b.BotName,
					IsBot: true,
				})
			}
		}
	}
	return members, nil
}

func (c *connection) OpenDirect(_ context.Context, userID string) (string, error) {
	return userID, nil
}

func (c *connection) MessageUser(ctx context.Context, userID string, text string) (*imsdk.DirectMessageReceipt, error) {
	if !c.ready.Load() {
		return nil, errors.New("feishu: connection is not ready")
	}
	contentJSON, _ := json.Marshal(map[string]string{"text": text})
	var resp struct {
		Data struct {
			ChatID    string `json:"chat_id"`
			MessageID string `json:"message_id"`
		} `json:"data"`
	}
	err := c.ch.rest.doJSON(ctx, c.creds, http.MethodPost, "/open-apis/im/v1/messages",
		url.Values{"receive_id_type": {"open_id"}},
		map[string]any{
			"receive_id": userID,
			"msg_type":   "text",
			"content":    string(contentJSON),
		}, false, &resp)
	if err != nil {
		return nil, err
	}
	if resp.Data.ChatID == "" {
		return nil, nil
	}
	return &imsdk.DirectMessageReceipt{
		Chat:       resp.Data.ChatID,
		PlatformID: resp.Data.MessageID,
	}, nil
}

func (c *connection) Close() error {
	c.cancel()
	c.wsMu.Lock()
	ws := c.wsConn
	c.wsConn = nil
	c.wsMu.Unlock()
	if ws != nil {
		_ = ws.CloseNow()
	}
	c.wg.Wait()
	return nil
}
