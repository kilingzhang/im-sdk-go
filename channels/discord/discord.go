// Package discord implements the Discord channel adapter over the Discord v10
// Gateway WebSocket and REST API without requiring a public webhook endpoint.
package discord

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/mention"
)

const (
	defaultAPIBase = "https://discord.com/api/v10"
	setupURL       = "https://discord.com/developers/applications"

	apiTimeout      = 20 * time.Second
	transferTimeout = 120 * time.Second
	PermissionsTTL  = 15 * time.Second

	MaxUploadBytes = 10 * 1024 * 1024
	MaxUploadFiles = 10
	maxFileEntries = 5000
	wsReadLimit    = 16 << 20

	intentGuilds         = 1 << 0
	intentGuildMessages  = 1 << 9
	intentDirectMessages = 1 << 12
	intentMessageContent = 1 << 15

	// FlagMessageContent covers GATEWAY_MESSAGE_CONTENT and GATEWAY_MESSAGE_CONTENT_LIMITED on application flags.
	FlagMessageContent = (1 << 18) | (1 << 19)

	botPermissionsMask uint64 = (1 << 10) |
		(1 << 11) |
		(1 << 38) |
		(1 << 14) |
		(1 << 15) |
		(1 << 16) |
		(1 << 6)
)

// BotPermissions encodes View Channels, Send Messages, Send Messages in Threads,
// Embed Links, Attach Files, Read Message History, and Add Reactions.
var BotPermissions = strconv.FormatUint(botPermissionsMask, 10)

var defaultCapabilities = imsdk.Capabilities{
	QuoteReply:      true,
	Reactions:       true,
	TypingIndicator: true,
	FileUpload:      true,
	FileDownload:    true,
	ListMembers:     false,
	Mentions:        true,
	MaxTextLength:   1900,
}

var reactionEmoji = map[imsdk.ReactionState]string{
	imsdk.ReactionProcessing: "👀",
	imsdk.ReactionDone:       "✅",
	imsdk.ReactionError:      "❌",
}

var (
	reBotPrefix   = regexp.MustCompile(`(?i)^Bot\s+`)
	reTokenShape  = regexp.MustCompile(`^[\w-]+\.[\w-]+\.[\w-]+$`)
	reMultiSpaces = regexp.MustCompile(`[ \t]{2,}`)
)

// DialFunc dials a Discord Gateway WebSocket URL.
type DialFunc func(ctx context.Context, wsURL string) (*websocket.Conn, error)

// Options configures a Discord channel adapter.
type Options struct {
	HTTPClient      *http.Client
	Dial            DialFunc
	APIBase         string
	Backoff         func(attempt int) time.Duration
	Now             func() time.Time
	FirstBeatJitter func(interval time.Duration) time.Duration
}

// APIError represents an HTTP error returned by the Discord REST API.
type APIError struct {
	Status  int
	Code    int
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("discord: %d %s", e.Status, e.Message)
}

// User represents a Discord user object.
type User struct {
	ID         string `json:"id"`
	Username   string `json:"username"`
	GlobalName string `json:"global_name,omitempty"`
	Bot        bool   `json:"bot,omitempty"`
}

// Member represents guild member display metadata on a message or mention.
type Member struct {
	Nick string `json:"nick,omitempty"`
}

// MentionUser represents a user entry inside DiscordMessage.Mentions.
type MentionUser struct {
	User
	Member *Member `json:"member,omitempty"`
}

// Attachment represents a file attached to a Discord message.
type Attachment struct {
	ID          string `json:"id"`
	Filename    string `json:"filename"`
	Size        int64  `json:"size,omitempty"`
	URL         string `json:"url"`
	ContentType string `json:"content_type,omitempty"`
}

// MessageReference identifies a quoted parent message.
type MessageReference struct {
	MessageID string `json:"message_id,omitempty"`
}

// ReferencedMessage holds author metadata on a replied-to Discord message.
type ReferencedMessage struct {
	Author *User `json:"author,omitempty"`
}

// Message represents a Discord Gateway MESSAGE_CREATE payload.
type Message struct {
	ID                string             `json:"id"`
	ChannelID         string             `json:"channel_id"`
	GuildID           string             `json:"guild_id,omitempty"`
	Type              int                `json:"type"`
	Author            User               `json:"author"`
	Member            *Member            `json:"member,omitempty"`
	Content           string             `json:"content"`
	Timestamp         string             `json:"timestamp"`
	Mentions          []MentionUser      `json:"mentions,omitempty"`
	Attachments       []Attachment       `json:"attachments,omitempty"`
	MessageReference  *MessageReference  `json:"message_reference,omitempty"`
	ReferencedMessage *ReferencedMessage `json:"referenced_message,omitempty"`
}

type discordChannelObj struct {
	ID      string `json:"id"`
	Type    int    `json:"type"`
	Name    string `json:"name,omitempty"`
	GuildID string `json:"guild_id,omitempty"`
}

type gatewayPayload struct {
	Op int             `json:"op"`
	D  json.RawMessage `json:"d"`
	S  *int64          `json:"s,omitempty"`
	T  string          `json:"t,omitempty"`
}

func displayName(u User, m *Member) string {
	if m != nil && m.Nick != "" {
		return m.Nick
	}
	if u.GlobalName != "" {
		return u.GlobalName
	}
	return u.Username
}

type channelAdapter struct {
	imsdk.BaseChannel
	httpClient      *http.Client
	dial            DialFunc
	apiBase         string
	backoff         func(attempt int) time.Duration
	now             func() time.Time
	firstBeatJitter func(interval time.Duration) time.Duration
}

// New constructs a Discord Channel adapter.
func New(opts Options) imsdk.Channel {
	hc := opts.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}
	dial := opts.Dial
	if dial == nil {
		dial = func(ctx context.Context, wsURL string) (*websocket.Conn, error) {
			conn, _, err := websocket.Dial(ctx, wsURL, nil)
			if err != nil {
				return nil, err
			}
			conn.SetReadLimit(wsReadLimit)
			return conn, nil
		}
	}
	apiBase := strings.TrimRight(opts.APIBase, "/")
	if apiBase == "" {
		apiBase = defaultAPIBase
	}
	backoff := opts.Backoff
	if backoff == nil {
		backoff = defaultBackoff
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	jitterFn := opts.FirstBeatJitter
	if jitterFn == nil {
		jitterFn = func(interval time.Duration) time.Duration {
			return time.Duration(rand.Float64() * float64(interval))
		}
	}
	return &channelAdapter{
		BaseChannel: imsdk.BaseChannel{
			PlatformValue:      imsdk.PlatformDiscord,
			TrustValue:         imsdk.TrustPublic,
			GroupMessagesValue: true,
			OnboardingValue: imsdk.OnboardingMethod{
				Kind:     imsdk.OnboardingCredentials,
				SetupURL: setupURL,
				Fields: []imsdk.CredentialField{
					{Key: "token", LabelKey: "discord.token", Secret: true},
				},
			},
		},
		httpClient:      hc,
		dial:            dial,
		apiBase:         apiBase,
		backoff:         backoff,
		now:             nowFn,
		firstBeatJitter: jitterFn,
	}
}

func defaultBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 5 {
		return 30 * time.Second
	}
	d := time.Second * time.Duration(1<<attempt)
	if d > 30*time.Second {
		return 30 * time.Second
	}
	return d
}

func (c *channelAdapter) InviteURL(bot imsdk.BotIdentity) string {
	return "https://discord.com/oauth2/authorize?client_id=" + url.QueryEscape(bot.ID) + "&scope=bot&permissions=" + BotPermissions
}

type rawPayload struct {
	contentType string
	data        []byte
	timeout     time.Duration
}

func (c *channelAdapter) request(ctx context.Context, token, method, path string, body any, out any) error {
	var prepared *rawPayload
	switch v := body.(type) {
	case nil:
		prepared = &rawPayload{timeout: apiTimeout}
	case *rawPayload:
		prepared = v
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		prepared = &rawPayload{
			contentType: "application/json",
			data:        b,
			timeout:     apiTimeout,
		}
	}
	return c.doRequest(ctx, token, method, path, prepared, out, false)
}

func (c *channelAdapter) doRequest(ctx context.Context, token, method, path string, payload *rawPayload, out any, retried bool) error {
	reqCtx, cancel := context.WithTimeout(ctx, payload.timeout)
	defer cancel()

	var bodyReader io.Reader
	if payload.data != nil {
		bodyReader = bytes.NewReader(payload.data)
	}
	req, err := http.NewRequestWithContext(reqCtx, method, c.apiBase+path, bodyReader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bot "+token)
	if payload.contentType != "" {
		req.Header.Set("Content-Type", payload.contentType)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusTooManyRequests && !retried {
		var rl struct {
			RetryAfter *float64 `json:"retry_after"`
		}
		_ = json.Unmarshal(respBytes, &rl)
		retrySec := 99.0
		if rl.RetryAfter != nil {
			retrySec = *rl.RetryAfter
		}
		if retrySec <= 5 {
			wait := time.Duration(math.Ceil(retrySec*1000)) * time.Millisecond
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			return c.doRequest(ctx, token, method, path, payload, out, true)
		}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errBody struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(respBytes, &errBody)
		msg := errBody.Message
		if msg == "" {
			msg = "request failed"
		}
		return &APIError{
			Status:  resp.StatusCode,
			Code:    errBody.Code,
			Message: msg,
		}
	}

	if out != nil && len(respBytes) > 0 {
		if err := json.Unmarshal(respBytes, out); err != nil {
			return err
		}
	}
	return nil
}

type appInfo struct {
	ID    string `json:"id"`
	Flags int    `json:"flags"`
	Owner *struct {
		ID string `json:"id"`
	} `json:"owner,omitempty"`
	Team *struct {
		OwnerUserID string `json:"owner_user_id"`
	} `json:"team,omitempty"`
}

func (c *channelAdapter) identify(ctx context.Context, token string) (User, appInfo, error) {
	var u User
	if err := c.request(ctx, token, http.MethodGet, "/users/@me", nil, &u); err != nil {
		return User{}, appInfo{}, err
	}
	var app appInfo
	if err := c.request(ctx, token, http.MethodGet, "/oauth2/applications/@me", nil, &app); err != nil {
		return User{}, appInfo{}, err
	}
	return u, app, nil
}

func (c *channelAdapter) Onboard(ctx context.Context, input imsdk.Credentials, _ imsdk.OnboardingOptions) <-chan imsdk.OnboardingEvent {
	ch := make(chan imsdk.OnboardingEvent, 1)
	go func() {
		defer close(ch)
		token := reBotPrefix.ReplaceAllString(strings.TrimSpace(input["token"]), "")
		if !reTokenShape.MatchString(token) {
			ch <- imsdk.OnboardingEvent{
				Kind: imsdk.EventFailed,
				Code: imsdk.FailureInvalidCredentials,
			}
			return
		}
		user, app, err := c.identify(ctx, token)
		if err != nil {
			code := imsdk.FailureNetwork
			var apiErr *APIError
			if errors.As(err, &apiErr) && apiErr.Status == http.StatusUnauthorized {
				code = imsdk.FailureInvalidCredentials
			}
			ch <- imsdk.OnboardingEvent{
				Kind: imsdk.EventFailed,
				Code: code,
			}
			return
		}
		name := displayName(user, nil)
		var ownerID string
		if app.Team != nil {
			ownerID = app.Team.OwnerUserID
		} else if app.Owner != nil {
			ownerID = app.Owner.ID
		}
		ch <- imsdk.OnboardingEvent{
			Kind: imsdk.EventSucceeded,
			Credentials: imsdk.Credentials{
				"token": token,
				"name":  name,
			},
			Bot: imsdk.BotIdentity{
				ID:   user.ID,
				Name: name,
			},
			OwnerID: ownerID,
		}
	}()
	return ch
}

func (c *channelAdapter) Connect(ctx context.Context, creds imsdk.Credentials, events imsdk.ChannelEvents) (imsdk.ChannelConnection, error) {
	token := creds["token"]
	user, app, err := c.identify(ctx, token)
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.Status == http.StatusUnauthorized && events.OnState != nil {
			events.OnState(imsdk.ConnectionState{
				Kind: imsdk.StateError,
				Code: imsdk.ConnErrCredentialsRevoked,
			})
		}
		return nil, err
	}

	botName := creds["name"]
	if botName == "" {
		botName = displayName(user, nil)
	}
	bot := imsdk.BotIdentity{ID: user.ID, Name: botName}

	connCtx, connCancel := context.WithCancel(context.Background())
	dc := &discordConnection{
		BaseConnection: imsdk.BaseConnection{
			BotValue:          bot,
			CapabilitiesValue: defaultCapabilities,
		},
		adapter:        c,
		token:          token,
		botID:          user.ID,
		events:         events,
		connCtx:        connCtx,
		connCancel:     connCancel,
		messageContent: (app.Flags & FlagMessageContent) != 0,
		channels:       make(map[string]channelMeta),
		guilds:         make(map[string]string),
		files:          newFIFOMap[Attachment](maxFileEntries),
	}

	dc.openSocket(ctx)
	return dc, nil
}

type channelMeta struct {
	kind imsdk.ChatKind
	name string
}

type discordConnection struct {
	imsdk.BaseConnection
	adapter *channelAdapter
	token   string
	botID   string
	events  imsdk.ChannelEvents

	connCtx    context.Context
	connCancel context.CancelFunc

	gwMu           sync.Mutex
	ws             *websocket.Conn
	sockCancel     context.CancelFunc
	closed         bool
	attempt        int
	seq            *int64
	sessionID      string
	resumeURL      string
	gatewayURL     string
	messageContent bool
	timer          *time.Timer

	cacheMu  sync.Mutex
	channels map[string]channelMeta
	guilds   map[string]string
	files    *fifoMap[Attachment]

	permMu     sync.Mutex
	permAt     time.Time
	permCached []imsdk.BotPermission
	permValid  bool
}

func (c *discordConnection) emitState(st imsdk.ConnectionState) {
	if c.events.OnState != nil {
		c.events.OnState(st)
	}
}

func (c *discordConnection) emitMessage(msg imsdk.InboundMessage) {
	if c.events.OnMessage != nil {
		c.events.OnMessage(msg)
	}
}

func (c *discordConnection) learnChannel(ch discordChannelObj) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	c.learnChannelLocked(ch)
}

func (c *discordConnection) learnChannelLocked(ch discordChannelObj) {
	var guild string
	if ch.GuildID != "" {
		guild = c.guilds[ch.GuildID]
	}
	kind := imsdk.ChatGroup
	switch ch.Type {
	case 1, 3:
		kind = imsdk.ChatDM
	case 10, 11, 12:
		kind = imsdk.ChatThread
	}
	var name string
	if ch.Name != "" {
		if guild != "" {
			name = guild + " #" + ch.Name
		} else {
			name = "#" + ch.Name
		}
	}
	c.channels[ch.ID] = channelMeta{kind: kind, name: name}
}

func (c *discordConnection) chatOf(ctx context.Context, msg Message) imsdk.ChatRef {
	if msg.GuildID == "" {
		return imsdk.ChatRef{PlatformID: msg.ChannelID, Kind: imsdk.ChatDM}
	}
	c.cacheMu.Lock()
	known, ok := c.channels[msg.ChannelID]
	c.cacheMu.Unlock()
	if !ok {
		var fetched discordChannelObj
		if err := c.adapter.request(ctx, c.token, http.MethodGet, "/channels/"+msg.ChannelID, nil, &fetched); err == nil && fetched.ID != "" {
			if fetched.GuildID == "" {
				fetched.GuildID = msg.GuildID
			}
			c.learnChannel(fetched)
		}
		c.cacheMu.Lock()
		known, ok = c.channels[msg.ChannelID]
		c.cacheMu.Unlock()
		if !ok {
			known = channelMeta{kind: imsdk.ChatGroup}
		}
	}
	return imsdk.ChatRef{
		PlatformID: msg.ChannelID,
		Kind:       known.kind,
		Name:       known.name,
		FollowUp:   known.kind == imsdk.ChatThread,
	}
}

func (c *discordConnection) scheduleReconnect() {
	c.gwMu.Lock()
	if c.closed {
		c.gwMu.Unlock()
		return
	}
	c.attempt++
	attempt := c.attempt
	delay := c.adapter.backoff(attempt)
	if c.timer != nil {
		c.timer.Stop()
	}
	c.timer = time.AfterFunc(delay, func() {
		c.openSocket(c.connCtx)
	})
	c.gwMu.Unlock()

	c.emitState(imsdk.ConnectionState{
		Kind:    imsdk.StateReconnecting,
		Attempt: attempt,
	})
}

func (c *discordConnection) openSocket(dialParentCtx context.Context) {
	c.gwMu.Lock()
	if c.closed {
		c.gwMu.Unlock()
		return
	}
	resuming := c.sessionID != "" && c.resumeURL != ""
	gwURL := c.gatewayURL
	resURL := c.resumeURL
	c.gwMu.Unlock()

	if !resuming && gwURL == "" {
		var gw struct {
			URL string `json:"url"`
		}
		if err := c.adapter.request(dialParentCtx, c.token, http.MethodGet, "/gateway/bot", nil, &gw); err != nil {
			var apiErr *APIError
			if errors.As(err, &apiErr) && apiErr.Status == http.StatusUnauthorized {
				c.emitState(imsdk.ConnectionState{
					Kind: imsdk.StateError,
					Code: imsdk.ConnErrCredentialsRevoked,
				})
				return
			}
			c.scheduleReconnect()
			return
		}
		gwURL = gw.URL
		c.gwMu.Lock()
		c.gatewayURL = gwURL
		c.gwMu.Unlock()
	}

	targetBase := gwURL
	if resuming {
		targetBase = resURL
	}
	dialURL := strings.TrimRight(targetBase, "/") + "/?v=10&encoding=json"

	dialCtx, cancel := context.WithTimeout(dialParentCtx, apiTimeout)
	ws, err := c.adapter.dial(dialCtx, dialURL)
	cancel()
	if err != nil {
		c.scheduleReconnect()
		return
	}

	sockCtx, sockCancel := context.WithCancel(c.connCtx)
	c.gwMu.Lock()
	if c.closed {
		c.gwMu.Unlock()
		sockCancel()
		_ = ws.CloseNow()
		return
	}
	oldWS := c.ws
	oldCancel := c.sockCancel
	c.ws = ws
	c.sockCancel = sockCancel
	c.gwMu.Unlock()

	if oldCancel != nil {
		oldCancel()
	}
	if oldWS != nil && oldWS != ws {
		_ = oldWS.CloseNow()
	}

	go c.readLoop(ws, sockCtx, sockCancel)
}

func (c *discordConnection) closeSocketForReconnect(ws *websocket.Conn, sockCancel context.CancelFunc) {
	go func() {
		_ = ws.Close(websocket.StatusCode(4000), "")
	}()
	sockCancel()
}

func (c *discordConnection) sendOp(ctx context.Context, ws *websocket.Conn, op int, d any) {
	raw, err := json.Marshal(map[string]any{"op": op, "d": d})
	if err != nil {
		return
	}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	_ = ws.Write(writeCtx, websocket.MessageText, raw)
	cancel()
}

func (c *discordConnection) currentSeq() any {
	c.gwMu.Lock()
	defer c.gwMu.Unlock()
	if c.seq == nil {
		return nil
	}
	return *c.seq
}

func (c *discordConnection) readLoop(ws *websocket.Conn, sockCtx context.Context, sockCancel context.CancelFunc) {
	var acked atomic.Bool
	acked.Store(true)

	var hbOnce sync.Once
	startHeartbeat := func(interval time.Duration) {
		hbOnce.Do(func() {
			go func() {
				beat := func() bool {
					if !acked.Swap(false) {
						c.closeSocketForReconnect(ws, sockCancel)
						return false
					}
					c.sendOp(sockCtx, ws, 1, c.currentSeq())
					return true
				}
				jitter := c.adapter.firstBeatJitter(interval)
				timer := time.NewTimer(jitter)
				select {
				case <-sockCtx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				if !beat() {
					return
				}
				ticker := time.NewTicker(interval)
				defer ticker.Stop()
				for {
					select {
					case <-sockCtx.Done():
						return
					case <-ticker.C:
						if !beat() {
							return
						}
					}
				}
			}()
		})
	}

	var readErr error
	for {
		var raw []byte
		_, raw, readErr = ws.Read(sockCtx)
		if readErr != nil {
			break
		}
		var payload gatewayPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			continue
		}
		if payload.S != nil {
			s := *payload.S
			c.gwMu.Lock()
			c.seq = &s
			c.gwMu.Unlock()
		}

		switch payload.Op {
		case 10:
			var hello struct {
				HeartbeatInterval float64 `json:"heartbeat_interval"`
			}
			if err := json.Unmarshal(payload.D, &hello); err == nil && hello.HeartbeatInterval > 0 {
				acked.Store(true)
				startHeartbeat(time.Duration(hello.HeartbeatInterval) * time.Millisecond)
			}
			c.gwMu.Lock()
			sessID := c.sessionID
			resURL := c.resumeURL
			var seqVal any
			if c.seq != nil {
				seqVal = *c.seq
			}
			msgContent := c.messageContent
			c.gwMu.Unlock()

			if sessID != "" && resURL != "" {
				c.sendOp(sockCtx, ws, 6, map[string]any{
					"token":      c.token,
					"session_id": sessID,
					"seq":        seqVal,
				})
			} else {
				intents := intentGuilds | intentGuildMessages | intentDirectMessages
				if msgContent {
					intents |= intentMessageContent
				}
				c.sendOp(sockCtx, ws, 2, map[string]any{
					"token":   c.token,
					"intents": intents,
					"properties": map[string]string{
						"os":      runtime.GOOS,
						"browser": "mingle",
						"device":  "mingle",
					},
				})
			}

		case 11:
			acked.Store(true)

		case 1:
			c.sendOp(sockCtx, ws, 1, c.currentSeq())

		case 7:
			c.closeSocketForReconnect(ws, sockCancel)
			readErr = nil
			goto closed

		case 9:
			var resumable bool
			_ = json.Unmarshal(payload.D, &resumable)
			if !resumable {
				c.gwMu.Lock()
				c.sessionID = ""
				c.resumeURL = ""
				c.seq = nil
				c.gwMu.Unlock()
			}
			c.closeSocketForReconnect(ws, sockCancel)
			readErr = nil
			goto closed

		case 0:
			if payload.T == "READY" || payload.T == "RESUMED" {
				c.gwMu.Lock()
				wasReconnect := c.attempt > 0
				c.attempt = 0
				c.gwMu.Unlock()
				if wasReconnect {
					c.emitState(imsdk.ConnectionState{Kind: imsdk.StateConnected})
				}
			}
			c.handleDispatch(payload.T, payload.D)
		}
	}

closed:
	sockCancel()
	_ = ws.CloseNow()

	c.gwMu.Lock()
	if c.closed || c.ws != ws {
		c.gwMu.Unlock()
		return
	}
	code := 0
	if readErr != nil {
		if st := websocket.CloseStatus(readErr); st != -1 {
			code = int(st)
		}
	}
	switch code {
	case 4004:
		c.gwMu.Unlock()
		c.emitState(imsdk.ConnectionState{
			Kind: imsdk.StateError,
			Code: imsdk.ConnErrCredentialsRevoked,
		})
		return
	case 4014:
		if c.messageContent {
			c.messageContent = false
			c.sessionID = ""
			c.resumeURL = ""
		} else {
			c.gwMu.Unlock()
			c.emitState(imsdk.ConnectionState{
				Kind: imsdk.StateError,
				Code: imsdk.ConnErrPermissionMissing,
			})
			return
		}
	case 4010, 4011, 4012, 4013:
		c.gwMu.Unlock()
		c.emitState(imsdk.ConnectionState{
			Kind: imsdk.StateError,
			Code: imsdk.ConnErrUnknown,
		})
		return
	case 4007, 4009:
		c.sessionID = ""
		c.resumeURL = ""
		c.seq = nil
	}
	c.gwMu.Unlock()

	c.scheduleReconnect()
}

func (c *discordConnection) handleDispatch(eventType string, raw json.RawMessage) {
	switch eventType {
	case "READY":
		var ready struct {
			SessionID        string `json:"session_id"`
			ResumeGatewayURL string `json:"resume_gateway_url"`
		}
		if err := json.Unmarshal(raw, &ready); err == nil {
			c.gwMu.Lock()
			c.sessionID = ready.SessionID
			c.resumeURL = ready.ResumeGatewayURL
			c.gwMu.Unlock()
		}

	case "GUILD_CREATE":
		var guild struct {
			ID       string              `json:"id"`
			Name     string              `json:"name"`
			Channels []discordChannelObj `json:"channels"`
			Threads  []discordChannelObj `json:"threads"`
		}
		if err := json.Unmarshal(raw, &guild); err == nil {
			c.cacheMu.Lock()
			c.guilds[guild.ID] = guild.Name
			for _, ch := range guild.Channels {
				ch.GuildID = guild.ID
				c.learnChannelLocked(ch)
			}
			for _, th := range guild.Threads {
				th.GuildID = guild.ID
				c.learnChannelLocked(th)
			}
			c.cacheMu.Unlock()
		}

	case "CHANNEL_CREATE", "CHANNEL_UPDATE", "THREAD_CREATE", "THREAD_UPDATE":
		var ch discordChannelObj
		if err := json.Unmarshal(raw, &ch); err == nil && ch.ID != "" {
			c.learnChannel(ch)
		}

	case "MESSAGE_CREATE":
		var msg Message
		if err := json.Unmarshal(raw, &msg); err != nil || msg.Author.ID == c.botID {
			return
		}
		chat := c.chatOf(c.connCtx, msg)
		inbound := ToInbound(msg, c.botID, chat)
		if inbound == nil {
			return
		}
		for _, a := range msg.Attachments {
			if a.ID != "" {
				c.files.Set(a.ID, a)
			}
		}
		c.emitMessage(*inbound)
	}
}

func (c *discordConnection) Permissions(ctx context.Context) ([]imsdk.BotPermission, error) {
	c.permMu.Lock()
	defer c.permMu.Unlock()

	now := c.adapter.now()
	if c.permValid && now.Sub(c.permAt) <= PermissionsTTL {
		return slices.Clone(c.permCached), nil
	}

	_, app, err := c.adapter.identify(ctx, c.token)
	if err != nil {
		c.permAt = now
		c.permCached = []imsdk.BotPermission{}
		c.permValid = true
		return []imsdk.BotPermission{}, nil
	}

	granted := (app.Flags & FlagMessageContent) != 0
	if granted {
		c.gwMu.Lock()
		needUpgrade := !c.messageContent && !c.closed
		if needUpgrade {
			c.messageContent = true
			c.sessionID = ""
			c.resumeURL = ""
		}
		ws := c.ws
		sockCancel := c.sockCancel
		c.gwMu.Unlock()
		if needUpgrade && ws != nil && sockCancel != nil {
			c.closeSocketForReconnect(ws, sockCancel)
		}
	}

	var perms []imsdk.BotPermission
	if granted {
		perms = []imsdk.BotPermission{{
			ID:      imsdk.PermGroupMessages,
			Granted: true,
		}}
	} else {
		perms = []imsdk.BotPermission{{
			ID:      imsdk.PermGroupMessages,
			Granted: false,
			Grant: &imsdk.PermissionGrant{
				URL:   "https://discord.com/developers/applications/" + app.ID + "/bot",
				Steps: "discord",
			},
		}}
	}
	c.permAt = now
	c.permCached = perms
	c.permValid = true
	return slices.Clone(perms), nil
}

type postResult struct {
	id       string
	degraded []imsdk.Degradation
}

func (c *discordConnection) post(ctx context.Context, chat imsdk.ChatRef, text, replyTo string, files []imsdk.OutboundFile) (postResult, error) {
	var degraded []imsdk.Degradation
	addDegraded := func(d imsdk.Degradation) {
		if !slices.Contains(degraded, d) {
			degraded = append(degraded, d)
		}
	}

	body := map[string]any{
		"allowed_mentions": map[string]any{
			"parse":        []string{"users"},
			"replied_user": chat.Kind != imsdk.ChatDM,
		},
	}
	if text != "" {
		body["content"] = text
	}
	if replyTo != "" {
		body["message_reference"] = map[string]any{
			"message_id":         replyTo,
			"fail_if_not_exists": false,
		}
	}

	var reqBody any = body
	if len(files) > 0 {
		type attMeta struct {
			ID       int    `json:"id"`
			Filename string `json:"filename"`
		}
		var meta []attMeta
		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		var total int64

		for _, f := range files {
			info, err := os.Stat(f.Path)
			if err != nil {
				return postResult{}, err
			}
			if len(meta) >= MaxUploadFiles || total+info.Size() > MaxUploadBytes {
				addDegraded(imsdk.DegradedFilesDropped)
				continue
			}
			data, err := os.ReadFile(f.Path)
			if err != nil {
				return postResult{}, err
			}
			total += info.Size()
			name := f.Name
			if name == "" {
				name = filepath.Base(f.Path)
			}
			idx := len(meta)
			part, err := mw.CreateFormFile(fmt.Sprintf("files[%d]", idx), name)
			if err != nil {
				return postResult{}, err
			}
			if _, err := part.Write(data); err != nil {
				return postResult{}, err
			}
			meta = append(meta, attMeta{ID: idx, Filename: name})
		}

		if len(meta) == 0 && text == "" {
			return postResult{id: "", degraded: []imsdk.Degradation{imsdk.DegradedFilesDropped}}, nil
		}
		if len(meta) > 0 {
			body["attachments"] = meta
			payloadJSON, err := json.Marshal(body)
			if err != nil {
				return postResult{}, err
			}
			if err := mw.WriteField("payload_json", string(payloadJSON)); err != nil {
				return postResult{}, err
			}
			if err := mw.Close(); err != nil {
				return postResult{}, err
			}
			reqBody = &rawPayload{
				contentType: mw.FormDataContentType(),
				data:        buf.Bytes(),
				timeout:     transferTimeout,
			}
		}
	}

	var sent struct {
		ID string `json:"id"`
	}
	if err := c.adapter.request(ctx, c.token, http.MethodPost, "/channels/"+chat.PlatformID+"/messages", reqBody, &sent); err != nil {
		return postResult{}, err
	}
	return postResult{id: sent.ID, degraded: degraded}, nil
}

func (c *discordConnection) edit(ctx context.Context, chat imsdk.ChatRef, msgID, content string) error {
	return c.adapter.request(ctx, c.token, http.MethodPatch, "/channels/"+chat.PlatformID+"/messages/"+msgID, map[string]any{
		"content": content,
		"allowed_mentions": map[string]any{
			"parse": []string{"users"},
		},
	}, nil)
}

func (c *discordConnection) Send(ctx context.Context, msg imsdk.OutboundMessage) (imsdk.SendResult, error) {
	text := discordMentions(strings.TrimSpace(msg.Text), msg.Mentions)
	sent, err := c.post(ctx, msg.Chat, text, msg.ReplyTo, msg.Files)
	if err != nil {
		return sendFailure(ctx, err), nil
	}
	if sent.id == "" {
		return imsdk.SendResult{
			Outcome:   imsdk.OutcomeFailed,
			Code:      imsdk.SendErrTooLarge,
			Retryable: false,
		}, nil
	}
	return imsdk.SendResult{
		Outcome:    imsdk.OutcomeSent,
		PlatformID: sent.id,
		Degraded:   sent.degraded,
	}, nil
}

func (c *discordConnection) React(ctx context.Context, chat imsdk.ChatRef, platformMsgID string, state imsdk.ReactionState) error {
	set := func(emoji, method string) {
		path := "/channels/" + chat.PlatformID + "/messages/" + platformMsgID + "/reactions/" + url.PathEscape(emoji) + "/@me"
		_ = c.adapter.request(ctx, c.token, method, path, nil, nil)
	}
	if state == imsdk.ReactionProcessing {
		set(reactionEmoji[imsdk.ReactionProcessing], http.MethodPut)
		return nil
	}
	set(reactionEmoji[imsdk.ReactionProcessing], http.MethodDelete)
	if emoji, ok := reactionEmoji[state]; ok {
		set(emoji, http.MethodPut)
	}
	return nil
}

type discordLiveReply struct {
	conn *discordConnection
	chat imsdk.ChatRef
	id   string

	mu   sync.Mutex
	last string
}

func (l *discordLiveReply) Until() time.Time { return time.Time{} }

func (l *discordLiveReply) Update(ctx context.Context, markdown string) error {
	l.mu.Lock()
	if markdown == l.last {
		l.mu.Unlock()
		return nil
	}
	l.last = markdown
	l.mu.Unlock()

	_ = l.conn.edit(ctx, l.chat, l.id, markdown)
	return nil
}

func (l *discordLiveReply) Finish(ctx context.Context, markdown string, mentions []imsdk.Mention) (imsdk.SendResult, error) {
	if err := l.conn.edit(ctx, l.chat, l.id, discordMentions(markdown, mentions)); err != nil {
		return sendFailure(ctx, err), nil
	}
	return imsdk.SendResult{
		Outcome:    imsdk.OutcomeSent,
		PlatformID: l.id,
	}, nil
}

func (c *discordConnection) StartLive(ctx context.Context, chat imsdk.ChatRef, opts imsdk.LiveOptions) (imsdk.LiveReply, error) {
	sent, err := c.post(ctx, chat, opts.Initial, opts.ReplyTo, nil)
	if err != nil {
		return nil, err
	}
	return &discordLiveReply{
		conn: c,
		chat: chat,
		id:   sent.id,
		last: opts.Initial,
	}, nil
}

func (c *discordConnection) SetTyping(ctx context.Context, chat imsdk.ChatRef, typing bool) error {
	if typing {
		_ = c.adapter.request(ctx, c.token, http.MethodPost, "/channels/"+chat.PlatformID+"/typing", nil, nil)
	}
	return nil
}

func (c *discordConnection) Download(ctx context.Context, att imsdk.InboundAttachment, toPath string) error {
	file, ok := c.files.Get(att.ID)
	if !ok || file.URL == "" {
		return errors.New("discord: this file is no longer available; ask for it to be sent again")
	}

	dlCtx, cancel := context.WithTimeout(ctx, transferTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(dlCtx, http.MethodGet, file.URL, nil)
	if err != nil {
		return err
	}
	resp, err := c.adapter.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("discord: download failed: HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return os.WriteFile(toPath, data, 0o644)
}

func (c *discordConnection) OpenDirect(ctx context.Context, userID string) (string, error) {
	var dm struct {
		ID string `json:"id"`
	}
	if err := c.adapter.request(ctx, c.token, http.MethodPost, "/users/@me/channels", map[string]string{
		"recipient_id": userID,
	}, &dm); err != nil {
		return "", err
	}
	return dm.ID, nil
}

func (c *discordConnection) MessageUser(ctx context.Context, userID, text string) (*imsdk.DirectMessageReceipt, error) {
	dmID, err := c.OpenDirect(ctx, userID)
	if err != nil {
		return nil, err
	}
	var sent struct {
		ID string `json:"id"`
	}
	if err := c.adapter.request(ctx, c.token, http.MethodPost, "/channels/"+dmID+"/messages", map[string]any{
		"content": text,
		"allowed_mentions": map[string]any{
			"parse": []string{},
		},
	}, &sent); err != nil {
		return nil, err
	}
	return &imsdk.DirectMessageReceipt{
		Chat:       dmID,
		PlatformID: sent.ID,
	}, nil
}

func (c *discordConnection) Close() error {
	c.gwMu.Lock()
	c.closed = true
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	ws := c.ws
	sockCancel := c.sockCancel
	c.ws = nil
	c.sockCancel = nil
	c.gwMu.Unlock()

	c.connCancel()
	if sockCancel != nil {
		sockCancel()
	}
	if ws != nil {
		_ = ws.CloseNow()
	}
	return nil
}

// ToInbound maps a Discord Message to an InboundMessage, returning nil for non-conversation or self messages.
func ToInbound(msg Message, botID string, chat imsdk.ChatRef) *imsdk.InboundMessage {
	if msg.Type != 0 && msg.Type != 19 {
		return nil
	}
	if msg.Author.ID == botID {
		return nil
	}

	mentioned := slices.ContainsFunc(msg.Mentions, func(u MentionUser) bool {
		return u.ID == botID
	})
	reBotTag := regexp.MustCompile(`<@!?` + regexp.QuoteMeta(botID) + `>`)
	typed := reBotTag.MatchString(msg.Content)
	repliedToMe := msg.ReferencedMessage != nil && msg.ReferencedMessage.Author != nil && msg.ReferencedMessage.Author.ID == botID

	text := reBotTag.ReplaceAllLiteralString(msg.Content, "")
	var others []imsdk.Sender
	for _, u := range msg.Mentions {
		if u.ID == botID {
			continue
		}
		reUserTag := regexp.MustCompile(`<@!?` + regexp.QuoteMeta(u.ID) + `>`)
		if reUserTag.MatchString(text) {
			name := displayName(u.User, u.Member)
			if !slices.ContainsFunc(others, func(s imsdk.Sender) bool { return s.ID == u.ID }) {
				others = append(others, imsdk.Sender{
					ID:    u.ID,
					Name:  name,
					IsBot: u.Bot,
				})
			}
			text = reUserTag.ReplaceAllLiteralString(text, "@"+name)
		}
	}
	text = reMultiSpaces.ReplaceAllString(text, " ")
	text = strings.TrimSpace(text)

	attachments := make([]imsdk.InboundAttachment, 0, len(msg.Attachments))
	for _, a := range msg.Attachments {
		attachments = append(attachments, imsdk.InboundAttachment{
			ID:   a.ID,
			Name: a.Filename,
			Size: a.Size,
			Mime: a.ContentType,
		})
	}

	var replyTo string
	if msg.MessageReference != nil {
		replyTo = msg.MessageReference.MessageID
	}

	sentAt, err := time.Parse(time.RFC3339Nano, msg.Timestamp)
	if err != nil {
		sentAt = time.Now()
	}

	return &imsdk.InboundMessage{
		PlatformID: msg.ID,
		Chat:       chat,
		Sender: imsdk.Sender{
			ID:    msg.Author.ID,
			Name:  displayName(msg.Author, msg.Member),
			IsBot: msg.Author.Bot,
		},
		Text:           text,
		Attachments:    attachments,
		ReplyTo:        replyTo,
		AddressedToBot: chat.Kind == imsdk.ChatDM || mentioned || repliedToMe,
		Mentioned:      imsdk.BoolPtr(typed),
		Mentions:       others,
		SentAt:         sentAt,
	}
}

func discordMentions(markdown string, mentions []imsdk.Mention) string {
	return mention.Render(markdown, mentions, func(m imsdk.Mention) string {
		return "<@" + m.ID + ">"
	})
}

func sendFailure(callerCtx context.Context, err error) imsdk.SendResult {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		if apiErr.Status == http.StatusTooManyRequests {
			return imsdk.SendResult{
				Outcome:   imsdk.OutcomeFailed,
				Code:      imsdk.SendErrRateLimited,
				Retryable: true,
			}
		}
		if apiErr.Status == http.StatusRequestEntityTooLarge || apiErr.Code == 40005 || apiErr.Code == 50035 {
			return imsdk.SendResult{
				Outcome:   imsdk.OutcomeFailed,
				Code:      imsdk.SendErrTooLarge,
				Retryable: false,
			}
		}
		if apiErr.Status == http.StatusNotFound || apiErr.Code == 10003 {
			return imsdk.SendResult{
				Outcome:   imsdk.OutcomeFailed,
				Code:      imsdk.SendErrChatGone,
				Retryable: false,
			}
		}
		if apiErr.Status == http.StatusForbidden {
			return imsdk.SendResult{
				Outcome:   imsdk.OutcomeFailed,
				Code:      imsdk.SendErrForbidden,
				Retryable: false,
			}
		}
		return imsdk.SendResult{
			Outcome:   imsdk.OutcomeFailed,
			Code:      imsdk.SendErrUnknown,
			Retryable: apiErr.Status >= 500,
		}
	}
	if isTimeout(callerCtx, err) {
		return imsdk.SendResult{Outcome: imsdk.OutcomeUncertain}
	}
	return imsdk.SendResult{
		Outcome:   imsdk.OutcomeFailed,
		Code:      imsdk.SendErrNetwork,
		Retryable: true,
	}
}

func isTimeout(callerCtx context.Context, err error) bool {
	if callerCtx != nil && errors.Is(callerCtx.Err(), context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

type fifoMap[V any] struct {
	mu    sync.Mutex
	max   int
	items map[string]V
	order []string
}

func newFIFOMap[V any](max int) *fifoMap[V] {
	return &fifoMap[V]{
		max:   max,
		items: make(map[string]V),
	}
}

func (m *fifoMap[V]) Set(key string, val V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.items[key]; !exists {
		m.order = append(m.order, key)
	}
	m.items[key] = val
	if len(m.items) > m.max {
		oldest := m.order[0]
		delete(m.items, oldest)
		m.order[0] = ""
		m.order = m.order[1:]
		if cap(m.order) > 2*len(m.order) && cap(m.order) > 1024 {
			m.order = slices.Clone(m.order)
		}
	}
}

func (m *fifoMap[V]) Get(key string) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.items[key]
	return v, ok
}
